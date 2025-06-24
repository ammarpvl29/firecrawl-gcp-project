import os
import re
import json
import time
from datetime import datetime
from typing import Dict, List

import requests

from flask import Flask, jsonify
from google.cloud import storage, bigquery
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import html2text

from google.cloud import aiplatform
import vertexai
from vertexai.language_models import TextEmbeddingModel

# --- Initialize Flask App and GCP Clients ---
app = Flask(__name__)
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# --- Load Configuration from Environment Variables ---
BUCKET_NAME = os.environ.get("BUCKET_NAME")
BIGQUERY_DATASET = "telkom_university"
STRUCTURED_TABLE_ID = "structured_content"
CHUNKS_TABLE_ID = "text_chunks"
# NEW - For Vertex AI
VERTEX_INDEX_ID = os.environ.get("VERTEX_INDEX_ID")
VERTEX_ENDPOINT_ID = os.environ.get("VERTEX_ENDPOINT_ID")
VERTEX_REGION = "europe-west1"


# ==============================================================================
# SECTION 1: CRAWLER ENDPOINT
# This is your existing code. It works perfectly.
# ==============================================================================
@app.route('/start-crawl', methods=['POST'])
def start_telkom_crawl():
    """
    This endpoint initiates a custom web crawl of a target website.
    """
    print("Custom crawl initiation request received...")

    if not BUCKET_NAME:
        print("FATAL ERROR: BUCKET_NAME environment variable is not set.")
        return "Internal server configuration error", 500

    start_url = "https://telkomuniversity.ac.id/"
    allowed_domain = "telkomuniversity.ac.id"
    max_pages = 200
    
    urls_to_visit = [start_url]
    visited_urls = set()
    pages_crawled = 0

    h = html2text.HTML2Text()
    h.ignore_links = True

    while urls_to_visit and pages_crawled < max_pages:
        current_url = urls_to_visit.pop(0)
        if current_url in visited_urls:
            continue
        try:
            print(f"[{pages_crawled + 1}/{max_pages}] Crawling: {current_url}")
            headers = {'User-Agent': 'Google-Cloud-Scheduler-Bot/1.0'}
            response = requests.get(current_url, headers=headers, timeout=10)
            response.raise_for_status()
            visited_urls.add(current_url)
            pages_crawled += 1
            soup = BeautifulSoup(response.content, 'html.parser')
            for link in soup.find_all('a', href=True):
                absolute_link = urljoin(current_url, link['href'])
                if urlparse(absolute_link).netloc.endswith(allowed_domain) and absolute_link not in visited_urls:
                    urls_to_visit.append(absolute_link)
            main_content = soup.find('main') or soup.find('article') or soup.find('div', class_='content')
            html_content = str(main_content) if main_content else str(soup.body)
            markdown_content = h.handle(html_content)
            filename = "".join(c for c in current_url if c.isalnum() or c in ('-', '_')).rstrip() + ".md"
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"custom-crawl/{filename}")
            blob.upload_from_string(markdown_content, content_type='text/markdown')
            time.sleep(0.5)
        except requests.exceptions.RequestException as e:
            print(f"Could not fetch {current_url}. Reason: {e}")
            visited_urls.add(current_url)

    print(f"Crawl finished. Visited {pages_crawled} pages.")
    return jsonify(success=True, pages_crawled=pages_crawled, total_urls_found=len(visited_urls)), 200


# ==============================================================================
# SECTION 2: NEW DATA CLEANING & PROCESSING ENDPOINT
# ==============================================================================
@app.route('/process-data', methods=['POST'])
def process_crawled_data():
    """
    Reads all raw markdown from GCS, cleans it using your logic, 
    and loads the results into two BigQuery tables.
    """
    print("Data processing request received...")
    if not BUCKET_NAME:
        print("FATAL ERROR: BUCKET_NAME environment variable is not set.")
        return "Internal server configuration error", 500
        
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix="custom-crawl/")
    
    all_structured_rows = []
    all_chunk_rows = []

    for blob in blobs:
        if not blob.name.endswith('.md'):
            continue
        
        print(f"Processing file: {blob.name}")
        content = blob.download_as_text()
        
        # --- Use your excellent cleaning logic directly ---
        cleaned_data = extract_telkom_data(content, blob.name)

        if not cleaned_data:
            continue

        # Prepare structured data row
        structured_row = {
            'source_file': cleaned_data['source_file'],
            'content_type': cleaned_data['content_type'],
            'full_structured_data': json.dumps(cleaned_data['structured_data']),
            'processed_at': cleaned_data['processed_at']
        }
        all_structured_rows.append(structured_row)

        # Prepare chunk rows
        for chunk in cleaned_data['text_chunks']:
            chunk_row = {
                'source_file': cleaned_data['source_file'],
                'content_type': cleaned_data['content_type'],
                'chunk_id': chunk['chunk_id'],
                'text': chunk['text'],
                'word_count': chunk['word_count'],
                'processed_at': cleaned_data['processed_at']
            }
            all_chunk_rows.append(chunk_row)

    # --- Load all collected data into BigQuery in batches ---
    project_id = os.environ.get("GCP_PROJECT")
    
    # Load structured data
    if all_structured_rows:
        print(f"Loading {len(all_structured_rows)} rows into structured_content table...")
        table_ref = f"{project_id}.{BIGQUERY_DATASET}.{STRUCTURED_TABLE_ID}"
        errors = bigquery_client.insert_rows_json(table_ref, all_structured_rows)
        if not errors:
            print("Successfully loaded rows into structured_content.")
        else:
            print(f"Errors encountered while loading to structured_content: {errors}")

    # Load chunk data
    if all_chunk_rows:
        print(f"Loading {len(all_chunk_rows)} rows into text_chunks table...")
        table_ref = f"{project_id}.{BIGQUERY_DATASET}.{CHUNKS_TABLE_ID}"
        errors = bigquery_client.insert_rows_json(table_ref, all_chunk_rows)
        if not errors:
            print("Successfully loaded rows into text_chunks.")
        else:
            print(f"Errors encountered while loading to text_chunks: {errors}")
            
    return jsonify(success=True, structured_rows_processed=len(all_structured_rows), chunk_rows_processed=len(all_chunk_rows)), 200

# ==============================================================================
# SECTION 3: EMBEDDING GENERATION ENDPOINT (Final Region Fix)
# ==============================================================================
@app.route('/generate-embeddings', methods=['POST'])
def generate_embeddings():
    """
    Reads text chunks from BigQuery, generates embeddings using Vertex AI,
    and upserts them into Vertex AI Vector Search with robust error handling.
    """
    print("Embedding generation request received...")

    # --- THE FIX IS HERE: We will call the AI model from a region where it is available ---
    VERTEX_REGION = "us-central1"

    required_vars = {
        'VERTEX_INDEX_ID': VERTEX_INDEX_ID,
        'VERTEX_ENDPOINT_ID': VERTEX_ENDPOINT_ID,
        'GCP_PROJECT': os.environ.get("GCP_PROJECT"),
        'BUCKET_NAME': BUCKET_NAME
    }
    
    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        print(f"FATAL ERROR: {error_msg}")
        return jsonify({"error": error_msg, "success": False}), 500

    try:
        print(f"Initializing Vertex AI clients in region: {VERTEX_REGION}...")
        vertexai.init(project=required_vars['GCP_PROJECT'], location=VERTEX_REGION)
        
        # We go back to using the newest model, as it exists in us-central1
        embedding_model = TextEmbeddingModel.from_pretrained("gemini-embedding-001")
        
        # The Index Endpoint client needs to be initialized in its own region
        aiplatform.init(project=required_vars['GCP_PROJECT'], location="europe-west1")
        my_index_endpoint = aiplatform.MatchingEngineIndexEndpoint(index_endpoint_name=VERTEX_ENDPOINT_ID)
        
        project_id = required_vars['GCP_PROJECT']
        query = f"""
        SELECT 
            source_file, content_type, chunk_id, text, word_count, processed_at
        FROM `{project_id}.{BIGQUERY_DATASET}.{CHUNKS_TABLE_ID}`
        WHERE LENGTH(TRIM(text)) > 0
        ORDER BY source_file, chunk_id
        """
        
        print(f"Running query: {query}")
        query_job = bigquery_client.query(query)
        rows = list(query_job.result())
            
        print(f"Found {len(rows)} text chunks to process.")
        
        if not rows:
            return jsonify({"message": "No text chunks found to process.", "success": True}), 200

        batch_size = 5
        max_retries = 3
        retry_delay = 2
        all_embeddings = []
        failed_chunks = []
        total_batches = (len(rows) + batch_size - 1) // batch_size
        for batch_idx in range(0, len(rows), batch_size):
            batch_rows = rows[batch_idx:batch_idx + batch_size]
            batch_num = batch_idx // batch_size + 1
            print(f"Processing batch {batch_num}/{total_batches} ({len(batch_rows)} chunks)...")
            texts_to_embed, valid_rows = [], []
            for row in batch_rows:
                text = row['text'].strip()
                if len(text) < 10: continue
                if len(text) > 8000: text = text[:8000] + "..."
                texts_to_embed.append(text)
                valid_rows.append(row)
            if not texts_to_embed: continue
            for attempt in range(max_retries):
                try:
                    embeddings = embedding_model.get_embeddings(texts_to_embed)
                    for row, embedding in zip(valid_rows, embeddings):
                        source_clean = re.sub(r'[^a-zA-Z0-9_-]', '_', row['source_file'])
                        unique_id = f"{source_clean}_{row['chunk_id']}"
                        if not embedding.values: continue
                        all_embeddings.append({"id": unique_id,"embedding": embedding.values,"restricts": [{"namespace": "content_type", "allow": [row['content_type']]},{"namespace": "source_file", "allow": [row['source_file']]},{"namespace": "word_count", "allow": [str(row['word_count'])]}]})
                    print(f"Successfully generated {len(embeddings)} embeddings for batch {batch_num}")
                    break
                except Exception as e:
                    print(f"Attempt {attempt + 1} failed for batch {batch_num}: {e}")
                    if attempt == max_retries - 1:
                        failed_chunks.extend([f"{row['source_file']}_{row['chunk_id']}" for row in valid_rows])
                        print(f"Failed to generate embeddings for batch {batch_num} after {max_retries} attempts")
                    else:
                        time.sleep(retry_delay * (attempt + 1))
            time.sleep(1)
        if not all_embeddings:
            return jsonify({"error": "No embeddings were successfully generated", "failed_chunks": failed_chunks,"success": False}), 500
        print(f"Saving {len(all_embeddings)} embeddings to GCS...")
        jsonl_lines = [json.dumps(e, ensure_ascii=False) for e in all_embeddings]
        jsonl_string = "\n".join(jsonl_lines)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        gcs_path = f"embeddings/telkom_embeddings_{timestamp}.json"
        gcs_bucket = storage_client.bucket(BUCKET_NAME)
        gcs_blob = gcs_bucket.blob(gcs_path)
        gcs_blob.upload_from_string(jsonl_string, content_type='application/json')
        print(f"Embeddings saved to gs://{BUCKET_NAME}/{gcs_path}")
        print("Upserting embeddings to Vertex AI Vector Search...")
        gcs_uri = f"gs://{BUCKET_NAME}/{gcs_path}"
        my_index_endpoint.upsert_from_gcs(gcs_uri=gcs_uri, deployed_index_id="telkom_kb_v1")
        print("Successfully upserted embeddings to Vector Search")
        print("Embedding generation and upsert process completed successfully.")
        return jsonify({"success": True, "embeddings_generated": len(all_embeddings), "failed_chunks": failed_chunks, "gcs_path": gcs_path}), 200
        
    except Exception as e:
        print(f"Unexpected error in embedding generation: {e}")
        return jsonify({"error": "Unexpected error during embedding generation", "details": str(e), "success": False}), 500


# ==============================================================================
# SECTION 4: YOUR DATA CLEANING LOGIC (as standalone functions)
# ==============================================================================
def extract_telkom_data(content: str, file_path: str) -> Dict:
    result = {'source_file': file_path,'processed_at': datetime.utcnow().isoformat(),'content_type': identify_content_type(content),'structured_data': {},'text_chunks': []}
    cleaned_content = clean_markdown_content(content)
    if 'jalur seleksi' in cleaned_content.lower(): result['structured_data'] = extract_admission_info(cleaned_content)
    elif 'program studi' in cleaned_content.lower() or 'fakultas' in cleaned_content.lower(): result['structured_data'] = extract_program_info(cleaned_content)
    elif 'biaya' in cleaned_content.lower() or 'ukt' in cleaned_content.lower(): result['structured_data'] = extract_fee_info(cleaned_content)
    elif 'beasiswa' in cleaned_content.lower(): result['structured_data'] = extract_scholarship_info(cleaned_content)
    else: result['structured_data'] = extract_general_info(cleaned_content)
    result['text_chunks'] = create_text_chunks(cleaned_content)
    return result

def clean_markdown_content(content: str) -> str:
    content = re.sub(r'data:image/[^;]+;base64,[A-Za-z0-9+/=]+', '', content)
    content = re.sub(r'!\[([^\]]*)\]\([^)]+\)', r'\1', content)
    content = re.sub(r'\n\s*\n\s*\n+', '\n\n', content)
    content = re.sub(r'\s+', ' ', content)
    content = re.sub(r'^#{1,6}\s*', '', content, flags=re.MULTILINE)
    content = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', content)
    content = re.sub(r'\*+([^*]+)\*+', r'\1', content)
    content = re.sub(r'^\s*[\*\-\+]\s*', '• ', content, flags=re.MULTILINE)
    return content.strip()

def identify_content_type(content: str) -> str:
    content_lower = content.lower()
    if 'jalur seleksi' in content_lower: return 'admission_pathway'
    elif 'program studi' in content_lower or 'jurusan' in content_lower: return 'academic_program'
    elif 'fakultas' in content_lower: return 'faculty_info'
    elif 'biaya' in content_lower or 'ukt' in content_lower: return 'fee_information'
    elif 'beasiswa' in content_lower or 'kip' in content_lower: return 'scholarship'
    elif 'pendaftaran' in content_lower or 'daftar' in content_lower: return 'registration'
    elif 'fasilitas' in content_lower or 'kampus' in content_lower: return 'facilities'
    elif 'alumni' in content_lower or 'lulusan' in content_lower: return 'alumni_info'
    else: return 'general_info'

def extract_admission_info(content: str) -> Dict:
    info = {'pathways': [],'requirements': [],'deadlines': [],'contact_info': []}
    pathway_patterns = [r'jalur\s+([^.\n]+)',r'seleksi\s+([^.\n]+)',r'pendaftaran\s+([^.\n]+)']
    for pattern in pathway_patterns: info['pathways'].extend([match.strip() for match in re.findall(pattern, content, re.IGNORECASE)])
    date_patterns = [r'(\d{1,2}\s+\w+\s+\d{4})',r'(\d{1,2}/\d{1,2}/\d{4})',r'(\d{4}-\d{2}-\d{2})']
    for pattern in date_patterns: info['deadlines'].extend(re.findall(pattern, content))
    return info

def extract_program_info(content: str) -> Dict:
    info = {'faculties': [],'programs': [],'degrees': [],'specializations': []}
    faculties = re.findall(r'fakultas\s+([^.\n]+)', content, re.IGNORECASE)
    info['faculties'] = [f.strip() for f in faculties]
    program_patterns = [r'program\s+studi\s+([^.\n]+)',r'jurusan\s+([^.\n]+)',r'prodi\s+([^.\n]+)']
    for pattern in program_patterns: info['programs'].extend([match.strip() for match in re.findall(pattern, content, re.IGNORECASE)])
    degree_patterns = [r'(S1|S2|S3|D3|D4)', r'(sarjana|magister|doktor|diploma)']
    for pattern in degree_patterns: info['degrees'].extend(re.findall(pattern, content, re.IGNORECASE))
    return info

def extract_fee_info(content: str) -> Dict:
    info = {'tuition_fees': [],'other_costs': [],'payment_methods': []}
    money_patterns = [r'Rp\.?\s?(\d{1,3}(?:\.\d{3})*(?:,\d{2})?)',r'(\d+)\s*juta',r'(\d+)\s*ribu']
    for pattern in money_patterns: info['tuition_fees'].extend(re.findall(pattern, content, re.IGNORECASE))
    return info

def extract_scholarship_info(content: str) -> Dict:
    info = {'scholarship_types': [],'eligibility': [],'benefits': [],'application_process': []}
    scholarship_patterns = [r'beasiswa\s+([^.\n]+)',r'kip[^.\n]*',r'bantuan\s+([^.\n]+)']
    for pattern in scholarship_patterns: info['scholarship_types'].extend([match.strip() for match in re.findall(pattern, content, re.IGNORECASE)])
    return info

def extract_general_info(content: str) -> Dict:
    info = {'key_points': [],'contacts': [],'links': [],'statistics': []}
    contact_patterns = [r'(\+?\d{2,4}[-.\s]?\d{3,4}[-.\s]?\d{3,4}[-.\s]?\d{0,4})', r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', r'(https?://[^\s]+)']
    for pattern in contact_patterns:
        matches = re.findall(pattern, content)
        if 'http' in pattern: info['links'].extend(matches)
        elif '@' in str(matches): info['contacts'].extend(matches)
        else: info['contacts'].extend(matches)
    stats_patterns = [r'(\d+)\s*(?:mahasiswa|alumni|dosen|program)',r'(\d+)\s*(?:tahun|semester|bulan)',r'#(\d+)\s*(?:ranking|peringkat|terbaik)']
    for pattern in stats_patterns: info['statistics'].extend(re.findall(pattern, content, re.IGNORECASE))
    return info

def create_text_chunks(content: str, chunk_size: int = 400, overlap: int = 50) -> List[Dict]:
    chunks = []
    words = content.split()
    if not words: return []
    if len(words) <= chunk_size: return [{'chunk_id': 0,'text': content,'word_count': len(words)}]
    start = 0
    chunk_id = 0
    while start < len(words):
        end = min(start + chunk_size, len(words))
        chunk_text = ' '.join(words[start:end])
        chunks.append({'chunk_id': chunk_id,'text': chunk_text,'word_count': end - start,'start_position': start,'end_position': end})
        chunk_id += 1
        if end == len(words): break
        start = end - overlap
    return chunks

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))