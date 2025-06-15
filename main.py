# main.py (Final Unified Version: Crawler + Cleaner)
import os
import re
import json
import time
from datetime import datetime
from typing import Dict, List

from flask import Flask, request, jsonify
from google.cloud import storage, bigquery
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import html2text

# --- Initialize Flask App and GCP Clients ---
app = Flask(__name__)
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# --- Load Configuration from Environment Variables ---
BUCKET_NAME = os.environ.get("BUCKET_NAME")
BIGQUERY_DATASET = "telkom_university"
STRUCTURED_TABLE_ID = "structured_content"
CHUNKS_TABLE_ID = "text_chunks"


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

    start_url = "https://smb.telkomuniversity.ac.id/"
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
# SECTION 3: YOUR DATA CLEANING LOGIC (as standalone functions)
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