# Token Uploader

This small app is responsible for uploading Gmail OAuth token JSON files to Google Drive.

## Usage

- **Important:** You must place your `client_secret.json` and `drive-key.json` files **locally** in this folder before running the app.
- These JSON files contain sensitive credentials and **should NOT be committed to the repository**.
- The app uses these credentials to authenticate and upload token files securely.

## Security

Make sure your `.gitignore` file excludes all JSON credential files to keep them private.

---

This app is a helper tool to manage OAuth tokens and is used alongside the main ETL service.
