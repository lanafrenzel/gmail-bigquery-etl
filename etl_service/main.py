"""
Gmail to BigQuery ETL Service

This Flask-based service fetches Gmail metadata using OAuth tokens stored in Google Drive,
then inserts unique email metadata into BigQuery in batches.
Uses Google Cloud Storage to securely download BigQuery service account credentials at runtime.

Before running:
- Set environment variables for all configuration values (see below).
- Store OAuth tokens (not service account JSON keys) in the configured Google Drive folder.
- Ensure the service account has appropriate IAM permissions for BigQuery and Storage.

Environment Variables:
- PROJECT_ID: Google Cloud project ID
- DATASET_ID: BigQuery dataset name
- TABLE_ID: BigQuery table name
- BUCKET_NAME: GCS bucket name holding BigQuery service account key
- DRIVE_FOLDER_ID: Google Drive folder ID containing OAuth tokens
- MAX_WORKERS: Number of parallel workers for token processing (default: 3)
- BATCH_SIZE: Batch size for BigQuery inserts (default: 1000)
- PORT: Port to run Flask app on (default: 8080)

Sensitive data such as keys and tokens must NOT be hardcoded.
"""

from google.cloud import storage, bigquery
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google.auth import default
from flask import Flask, jsonify
from concurrent.futures import ThreadPoolExecutor
import functools
import logging
import os
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger("gmail-bigquery")

# Configuration from environment variables (set externally, never hardcoded)
PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID")
TABLE_ID = os.environ.get("TABLE_ID")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID")
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", 3))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 1000))

app = Flask(__name__)

def cache_with_timeout(timeout=3600):
    def decorator(func):
        cache = {}
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = str(args) + str(kwargs)
            current_time = time.time()
            if key in cache and current_time - cache[key]["timestamp"] < timeout:
                return cache[key]["result"]
            result = func(*args, **kwargs)
            cache[key] = {"result": result, "timestamp": current_time}
            return result
        return wrapper
    return decorator

class GmailBigQueryService:
    def __init__(self):
        self.bigquery_client = None
        self.storage_client = None
        self.drive_service = None
        self._initialized = False
        
    def initialize_clients(self):
        if self._initialized:
            return
        
        self._download_bigquery_key()
        
        self.bigquery_client = bigquery.Client()
        self.storage_client = storage.Client()
        
        creds, _ = default()
        self.drive_service = build("drive", "v3", credentials=creds)
        
        self._initialized = True
        logger.info("✅ Clients initialized successfully")
        
    def _ensure_initialized(self):
        if not self._initialized:
            self.initialize_clients()
            
    @cache_with_timeout(timeout=3600)
    def fetch_existing_email_ids(self):
        self._ensure_initialized()
        query = f"SELECT id FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
        logger.info(f"Running BigQuery query: {query}")
        try:
            query_job = self.bigquery_client.query(query)
            return {row.id for row in query_job}
        except Exception as e:
            logger.error(f"❌ Error fetching existing email IDs: {e}")
            return set()

    def list_drive_tokens(self):
        self._ensure_initialized()
        try:
            results = self.drive_service.files().list(
                q=f"'{DRIVE_FOLDER_ID}' in parents",
                fields="files(id, name, mimeType)"
            ).execute()
            return results.get("files", [])
        except Exception as e:
            logger.error(f"❌ Error listing Drive tokens: {e}")
            return []

    def download_token(self, file_id, file_name, mime_type):
        self._ensure_initialized()
        token_path = f"/tmp/{file_name}"
        
        # Skip JSON keys to avoid using service account keys as OAuth tokens
        if file_name.endswith(".json"):
            logger.warning(f"⚠️ Skipping JSON key file: {file_name} (not an OAuth token)")
            return None
        
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            with open(token_path, "wb") as file:
                file.write(request.execute())
            
            # Validate and refresh token if needed
            creds = Credentials.from_authorized_user_file(token_path)
            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
                with open(token_path, 'w') as token_file:
                    token_file.write(creds.to_json())
            elif creds.expired:
                logger.error(f"❌ Token {file_name} is expired and has no refresh token")
                return None
            
            return token_path
        except Exception as e:
            logger.error(f"❌ Failed to download token {file_name}: {e}")
            return None

    def fetch_emails(self, user_token_path, existing_email_ids):
        try:
            creds = Credentials.from_authorized_user_file(user_token_path)
            if creds.expired:
                if creds.refresh_token:
                    creds.refresh(Request())
                else:
                    logger.error("❌ Token expired and no refresh token available.")
                    return []
            
            service = build("gmail", "v1", credentials=creds)
            email_data = {}
            next_page_token = None
            query = "in:inbox OR in:sent OR in:trash -in:spam -in:allmail"
            count = 0
            
            while True:
                response = service.users().messages().list(
                    userId="me",
                    pageToken=next_page_token,
                    q=query,
                    maxResults=500
                ).execute()
                
                messages = response.get("messages", [])
                if not messages:
                    break
                
                new_msg_ids = [msg["id"] for msg in messages if msg["id"] not in existing_email_ids]
                
                # Batch fetch details
                batch_size = 50
                for i in range(0, len(new_msg_ids), batch_size):
                    batch_ids = new_msg_ids[i:i+batch_size]
                    batch_requests = [
                        service.users().messages().get(
                            userId="me", id=msg_id, format="metadata",
                            metadataHeaders=["Subject", "From", "To", "Date"]
                        ) for msg_id in batch_ids
                    ]
                    
                    for idx, resp in enumerate(self._batch_execute(service, batch_requests)):
                        if isinstance(resp, Exception):
                            logger.error(f"❌ Error fetching message details: {resp}")
                            continue
                        
                        msg_id = batch_ids[idx]
                        headers = resp["payload"]["headers"]
                        
                        email_entry = {
                            "id": msg_id,
                            "threadId": resp.get("threadId", ""),
                            "subject": next((h["value"] for h in headers if h["name"].lower() == "subject"), None),
                            "sender": next((h["value"] for h in headers if h["name"].lower() == "from"), None),
                            "recipient": next((h["value"] for h in headers if h["name"].lower() == "to"), None),
                            "timestamp": next((h["value"] for h in headers if h["name"].lower() == "date"), None),
                            "combined_labels": ",".join(resp.get("labelIds", []))
                        }
                        email_data[msg_id] = email_entry
                        count += 1
                
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break
                time.sleep(0.5)  # Rate limiting
            
            logger.info(f"✅ Fetched {count} new emails for token {os.path.basename(user_token_path)}")
            return list(email_data.values())
        except Exception as e:
            logger.error(f"❌ Error fetching emails for token {user_token_path}: {e}")
            return []

    def _batch_execute(self, service, requests):
        from googleapiclient.errors import HttpError
        
        def execute_request(req):
            try:
                return req.execute()
            except HttpError as err:
                return err
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            return list(executor.map(execute_request, requests))

    def insert_into_bigquery(self, email_data):
        self._ensure_initialized()
        if not email_data:
            logger.info("⚠️ No new emails to insert.")
            return 0
        
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        inserted = 0
        
        for i in range(0, len(email_data), BATCH_SIZE):
            batch = email_data[i:i+BATCH_SIZE]
            try:
                errors = self.bigquery_client.insert_rows_json(table_ref, batch)
                if errors:
                    logger.error(f"❌ BigQuery Insert Errors: {errors}")
                else:
                    inserted += len(batch)
                    logger.info(f"✅ Inserted batch of {len(batch)} emails")
            except Exception as e:
                logger.error(f"❌ Error inserting data: {e}")
            
            if i + BATCH_SIZE < len(email_data):
                time.sleep(1)
        return inserted

    def _download_bigquery_key(self):
        try:
            if not self.storage_client:
                self.storage_client = storage.Client()
            bucket = self.storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob("bigquery-key.json")
            temp_key_path = "/tmp/bigquery-key.json"
            blob.download_to_filename(temp_key_path)
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_key_path
            logger.info("✅ BigQuery key downloaded and environment variable set")
        except Exception as e:
            logger.error(f"❌ Failed to download BigQuery key: {e}")
            raise

    def process_user_token(self, token_path, existing_email_ids):
        if not token_path:
            return 0
        emails = self.fetch_emails(token_path, existing_email_ids)
        if emails:
            return self.insert_into_bigquery(emails)
        return 0

gmail_bq_service = GmailBigQueryService()

@app.route("/fetch", methods=["GET"])
def run_fetch():
    try:
        gmail_bq_service.initialize_clients()
        existing_email_ids = gmail_bq_service.fetch_existing_email_ids()
        logger.info(f"Existing emails in BigQuery: {len(existing_email_ids)}")
        
        token_files = gmail_bq_service.list_drive_tokens()
        if not token_files:
            return jsonify({"status": "warning", "message": "No OAuth tokens found in Drive."})
        
        logger.info(f"Found {len(token_files)} OAuth token files in Drive")
        
        token_paths = []
        for file in token_files:
            token_path = gmail_bq_service.download_token(file["id"], file["name"], file["mimeType"])
            if token_path:
                token_paths.append(token_path)
        
        total_inserted = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(gmail_bq_service.process_user_token, path, existing_email_ids) for path in token_paths]
            for future in futures:
                total_inserted += future.result()
        
        # Cleanup temp files
        for path in token_paths:
            try:
                os.remove(path)
            except Exception:
                pass
        try:
            os.remove("/tmp/bigquery-key.json")
        except Exception:
            pass
        
        return jsonify({
            "status": "success",
            "message": f"Email fetch completed. Total new emails inserted: {total_inserted}"
        })
    except Exception as e:
        logger.error(f"❌ /fetch error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
