# main.py
import os
import json
import requests
from flask import Flask, request, jsonify
from google.cloud import storage

# --- Initialize Flask App and GCP Clients ---
app = Flask(__name__)
storage_client = storage.Client()

# --- Load Configuration from Environment Variables ---
# These will be set in the Cloud Run service configuration
BUCKET_NAME = os.environ.get("BUCKET_NAME", "your-telkom-chatbot-data")
FIRECRAWL_API_KEY = os.environ.get("FIRECRAWL_API_KEY", "fc-your-api-key")
# We will get the service URL after deployment and add it as an env var
SERVICE_URL = os.environ.get("SERVICE_URL", "")

# --- Firecrawl API Endpoint ---
CRAWL_API_URL = "https://api.firecrawl.dev/v1/crawl"

@app.route('/webhook', methods=['POST'])
def firecrawl_webhook():
    """
    Public endpoint to receive webhook events from Firecrawl.
    It processes crawl data and saves it to Google Cloud Storage.
    """
    # Verify the request is a POST request
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

    # Process 'crawl.page' events which contain the scraped data [21]
    if event_type == 'crawl.page' and data:
        page_data = data[0]
        if page_data.get('success') and 'markdown' in page_data:
            source_url = page_data.get('metadata', {}).get('sourceURL', 'unknown_url')
            
            # Sanitize the URL to create a valid filename
            filename = "".join(c for c in source_url if c.isalnum() or c in ('-', '_')).rstrip() + ".md"
            
            try:
                # Organize files in the bucket by their crawl ID for better management
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

    # Acknowledge receipt to Firecrawl so it doesn't retry
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

    # The payload for the Firecrawl API, directing results to our own webhook
    crawl_payload = {
        "url": "https://smb.telkomuniversity.ac.id/",
        "crawlerOptions": {
            "includes": ["**/*"], # Using includes with glob pattern
            "excludes": ["**/login/**", "**/register/**"],
            "maxDepth": 10,
            "limit": 2000
        },
        "pageOptions": { # Use pageOptions for single page scrape settings within a crawl
            "onlyMainContent": True
        },
        "webhook": {
            "url": f"{SERVICE_URL}/webhook", # The full URL to our webhook endpoint
            "events": ["crawl.page", "crawl.completed", "crawl.failed"]
        }
    }

    print(f"Initiating crawl job. Sending results to {SERVICE_URL}/webhook.")
    try:
        response = requests.post(CRAWL_API_URL, headers=headers, json=crawl_payload, timeout=30)
        response.raise_for_status() # This will raise an HTTPError for bad responses (4xx or 5xx)
        
        job_id = response.json().get('jobId')
        print(f"Crawl job submitted successfully. Job ID: {job_id}")
        return jsonify(success=True, jobId=job_id), 200

    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to start crawl job. {e}")
        error_details = e.response.text if e.response else "No response from server"
        return jsonify(success=False, error=str(e), details=error_details), 500

if __name__ == "__main__":
    # This part is for local testing. Gunicorn will run the app in Cloud Run.
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))