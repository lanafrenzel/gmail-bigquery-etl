from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
import os

# === CONFIGURATION ===
# Replace with your Google Drive folder ID where tokens will be uploaded
DRIVE_FOLDER_ID = "YOUR_GOOGLE_DRIVE_FOLDER_ID_HERE"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CLIENT_SECRET_FILE = os.path.join(SCRIPT_DIR, "client_secret.json")
SERVICE_ACCOUNT_FILE = os.path.join(SCRIPT_DIR, "drive-key.json")
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

def check_required_files():
    """Check if required credential files exist before proceeding"""
    missing_files = []
    
    if not os.path.exists(CLIENT_SECRET_FILE):
        missing_files.append("client_secret.json")
    
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        missing_files.append("drive-key.json")
    
    if missing_files:
        print("❌ Error: The following required files are missing:")
        for file in missing_files:
            print(f"  - {file}")
        print("\nPlease add these files with your credentials before running the script.")
        return False
    
    return True

def authorize_user():
    """Authorize user, retrieve their email, and save their token"""
    try:
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
        creds = flow.run_local_server(port=0)

        # Get user email
        service = build("gmail", "v1", credentials=creds)
        user_info = service.users().getProfile(userId="me").execute()
        user_email = user_info.get("emailAddress", "unknown_user").replace("@", "_").replace(".", "_")
        
        # Save credentials with a unique filename
        token_filename = f"user_token_{user_email}.json"
        token_path = os.path.join(SCRIPT_DIR, token_filename)
        with open(token_path, "w") as token:
            token.write(creds.to_json())

        print(f"✅ Authorization complete! Token saved as {token_filename}")
        return token_filename
    except Exception as e:
        print(f"❌ Authorization failed: {str(e)}")
        return None

def upload_to_drive(token_filename):
    """Upload the user token file to Google Drive"""
    token_path = os.path.join(SCRIPT_DIR, token_filename)
    if not os.path.exists(token_path):
        print("❌ Error: Token file not found. Authorization may have failed.")
        return False
    
    try:
        creds = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, 
            scopes=["https://www.googleapis.com/auth/drive.file"]
        )
        drive_service = build("drive", "v3", credentials=creds)

        file_metadata = {
            "name": token_filename,
            "parents": [DRIVE_FOLDER_ID]
        }
        
        media = MediaFileUpload(token_path, mimetype="application/json")
        file = drive_service.files().create(
            body=file_metadata, 
            media_body=media, 
            fields="id"
        ).execute()

        print(f"✅ Token uploaded successfully to Google Drive! File ID: {file.get('id')}")
        return True
    except HttpError as e:
        print(f"❌ Google Drive API error: {str(e)}")
        return False
    except Exception as e:
        print(f"❌ Error uploading to Google Drive: {str(e)}")
        return False

def main():
    """Main function to run the authorization and upload process"""
    print("📧 Gmail Authorization Tool 📧")
    print("This tool will authorize Gmail access and upload the token to Google Drive.\n")
    
    if not check_required_files():
        input("Press Enter to exit...")
        return
    
    print("Starting authorization process...")
    token_filename = authorize_user()
    
    if token_filename:
        print("Uploading token to Google Drive...")
        if upload_to_drive(token_filename):
            print("\n✅ Process completed successfully!")
        else:
            print("\n⚠️ Authorization succeeded but upload failed.")
    else:
        print("\n❌ Process failed at authorization stage.")
    
    input("\nPress Enter to exit...")

if __name__ == "__main__":
    main()
