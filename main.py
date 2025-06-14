# main.py (Final Corrected Version)
import os
import json
import requests
from flask import Flask, request, jsonify
from google.cloud import storage

# --- Initialize Flask App and GCP Clients ---
app = Flask(__name__)
storage_client = storage.Client()

# --- Load Configuration from Environment Variables ---
BUCKET_NAME = os.environ.get("BUCKET_NAME", "your-telkom-chatbot-data")
FIRECRAWL_API_KEY = os.environ.get("FIRECRAWL_API_KEY", "fc-your-api-key")
SERVICE_URL = os.environ.get("SERVICE_URL", "")

# --- Firecrawl API Endpoint ---
CRAWL_API_URL = "https://api.firecrawl.dev/v1/crawl"

@app.route('/webhook', methods=['POST'])
def firecrawl_webhook():
    """
    Public endpoint to receive webhook events from Firecrawl.
    It processes crawl data and saves it to Google Cloud Storage.
    """
    if request.method != 'POST':
        return 'Only POST requests are accepted', 405

    print("Webhook received...")
    try:
        payload = request.get_json()
    except Exception as e:
        print(f"Error: Could not parse incoming JSON. {e}")
        return 'Invalid JSON payload', 400

    event_type = payload.get('type')
    crawl_id = payload.get('id')
    data = payload.get('data')

    print(f"Processing event. Type: {event_type}, Crawl ID: {crawl_id}")

    if event_type == 'crawl.page' and data:
        page_data = data[0]
        if page_data.get('success') and 'markdown' in page_data:
            source_url = page_data.get('metadata', {}).get('sourceURL', 'unknown_url')
            filename = "".join(c for c in source_url if c.isalnum() or c in ('-', '_')).rstrip() + ".md"
            try:
                blob_path = f"{crawl_id}/{filename}"
                bucket = storage_client.bucket(BUCKET_NAME)
                blob = bucket.blob(blob_path)
                blob.upload_from_string(page_data['markdown'], content_type='text/markdown')
                print(f"SUCCESS: Saved {source_url} to gs://{BUCKET_NAME}/{blob_path}")
            except Exception as e:
                print(f"ERROR: Failed to upload to GCS. {e}")
                return 'Error processing data internally', 500

    elif event_type == 'crawl.completed':
        print(f"INFO: Crawl job {crawl_id} completed.")
    elif event_type == 'crawl.failed':
        print(f"ERROR: Crawl job {crawl_id} failed. Details: {payload.get('error')}")

    return jsonify(success=True), 200

@app.route('/start-crawl', methods=['POST'])
def start_telkom_crawl():
    """
    Internal endpoint to be triggered by Cloud Scheduler.
    This initiates the Firecrawl crawl job.
    """
    print("Crawl initiation request received...")

    if not FIRECRAWL_API_KEY or not SERVICE_URL:
        print("FATAL ERROR: Environment variables FIRECRAWL_API_KEY or SERVICE_URL are not set.")
        return "Internal server configuration error", 500

    headers = {
        "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
        "Content-Type": "application/json"
    }

    # --- FINAL CORRECTED PAYLOAD STRUCTURE ---
    # The structure now includes the required 'formats' key within scrapeOptions.
    crawl_payload = {
        "url": "https://smb.telkomuniversity.ac.id/",
        "webhook": {
            "url": f"{SERVICE_URL}/webhook",
            "events": ["crawl.page", "crawl.completed", "crawl.failed"]
        },
        "maxDepth": 10,
        "limit": 2000,
        "excludePaths": ["**/login/**", "**/register/**"],
        "scrapeOptions": {
            # THE FIX IS HERE: Explicitly telling the crawler to get markdown
            "formats": ["markdown"], 
            "onlyMainContent": True
        }
    }
    
    print(f"DEBUG: Sending the following payload to Firecrawl: {json.dumps(crawl_payload, indent=2)}")

    print(f"Initiating crawl job. Sending results to {SERVICE_URL}/webhook.")
    try:
        response = requests.post(CRAWL_API_URL, headers=headers, json=crawl_payload, timeout=30)
        response.raise_for_status() 
        
        job_id = response.json().get('jobId')
        print(f"Crawl job submitted successfully. Job ID: {job_id}")
        return jsonify(success=True, jobId=job_id), 200

    except requests.exceptions.RequestException as e:
        print(f"CRITICAL ERROR: Failed to start crawl job. Exception: {e}")
        if e.response:
            print(f"CRITICAL ERROR DETAILS: Status Code = {e.response.status_code}")
            print(f"CRITICAL ERROR BODY: {e.response.text}")
        
        error_details = e.response.text if e.response else "No response from server"
        return jsonify(success=False, error=str(e), details=error_details), 500

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))