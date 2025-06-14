# main.py (Custom Crawler Version)
import os
import time
import requests
from flask import Flask, request, jsonify
from google.cloud import storage
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import html2text

# --- Initialize Flask App and GCP Clients ---
app = Flask(__name__)
storage_client = storage.Client()

# --- Load Configuration from Environment Variables ---
BUCKET_NAME = os.environ.get("BUCKET_NAME")

# --- Webhook endpoint (can be left empty for now, not used by this crawler) ---
@app.route('/webhook', methods=['POST'])
def webhook_placeholder():
    return "Webhook endpoint is not used in this version.", 200

# --- The Main Crawler Logic ---
@app.route('/start-crawl', methods=['POST'])
def start_telkom_crawl():
    """
    This endpoint initiates a custom web crawl of a target website.
    """
    print("Custom crawl initiation request received...")

    if not BUCKET_NAME:
        print("FATAL ERROR: BUCKET_NAME environment variable is not set.")
        return "Internal server configuration error", 500

    # --- Crawler Configuration ---
    start_url = "https://smb.telkomuniversity.ac.id/"
    allowed_domain = "telkomuniversity.ac.id"
    max_pages = 200  # Safety limit to prevent running forever
    
    urls_to_visit = [start_url]
    visited_urls = set()
    pages_crawled = 0

    h = html2text.HTML2Text()
    h.ignore_links = True # We only want the text content as markdown

    while urls_to_visit and pages_crawled < max_pages:
        current_url = urls_to_visit.pop(0)

        if current_url in visited_urls:
            continue

        try:
            print(f"[{pages_crawled + 1}/{max_pages}] Crawling: {current_url}")
            
            # 1. Fetch the page content
            headers = {'User-Agent': 'Google-Cloud-Scheduler-Bot/1.0'}
            response = requests.get(current_url, headers=headers, timeout=10)
            response.raise_for_status() # Raise an exception for bad status codes

            visited_urls.add(current_url)
            pages_crawled += 1

            # 2. Parse with BeautifulSoup to find new links
            soup = BeautifulSoup(response.content, 'html.parser')

            # 3. Find and process all links on the page
            for link in soup.find_all('a', href=True):
                absolute_link = urljoin(current_url, link['href'])
                
                # Check if the link is within the allowed domain and not already visited
                if urlparse(absolute_link).netloc.endswith(allowed_domain) and absolute_link not in visited_urls:
                    urls_to_visit.append(absolute_link)

            # 4. Convert the main content to Markdown and save it
            # Try to find a 'main' tag or a common content div
            main_content = soup.find('main') or soup.find('article') or soup.find('div', class_='content')
            if main_content:
                html_content = str(main_content)
            else:
                html_content = str(soup.body) # Fallback to the whole body
            
            markdown_content = h.handle(html_content)

            # 5. Save the markdown to Cloud Storage
            # Sanitize the URL to create a valid filename
            filename = "".join(c for c in current_url if c.isalnum() or c in ('-', '_')).rstrip() + ".md"
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"custom-crawl/{filename}") # Store in a new folder
            blob.upload_from_string(markdown_content, content_type='text/markdown')

            # Be a good web citizen: wait a moment before the next request
            time.sleep(0.5) 

        except requests.exceptions.RequestException as e:
            print(f"Could not fetch {current_url}. Reason: {e}")
            visited_urls.add(current_url) # Mark as visited so we don't retry

    print(f"Crawl finished. Visited {pages_crawled} pages.")
    return jsonify(success=True, pages_crawled=pages_crawled, total_urls_found=len(visited_urls)), 200

if __name__ == "__main__":
    # This part is for local testing. Gunicorn will run the app in Cloud Run.
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))