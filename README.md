# Gmail to BigQuery ETL

This project enables you to extract Gmail data using user OAuth tokens and load it into BigQuery. It consists of two parts:

* **Token Uploader** – CLI tool to authorize Gmail access and securely upload user tokens to Google Drive.
* **ETL Service** – A multithreaded Python service that retrieves emails using saved tokens and loads the data into BigQuery.

---

## 🔧 Project Structure

```
gmail-to-bigquery-etl/
├── token_uploader/         # CLI app to authorize users & upload tokens to Drive
│   ├── app.py
│   ├── client_secret.json  # (ignored from version control)
│   ├── drive-key.json      # (ignored from version control)
│
├── etl_service/            # Gmail to BigQuery ETL pipeline
│   ├── main.py
│   ├── requirements.txt
│   ├── Dockerfile
│
├── .gitignore
├── LICENSE
└── README.md               # You're here
```

---

## ⚙️ 1. Token Uploader

### What it does

* Guides the user through Gmail OAuth via browser
* Saves the generated token locally as `user_token_*.json`
* Uploads the token to a secure Google Drive folder using a service account

### How to use

1. Place `client_secret.json` and `drive-key.json` in `token_uploader/`
2. Run:

```bash
cd token_uploader
python app.py
```

3. Follow the on-screen instructions

---

## 🚀 2. ETL Service

### What it does

* Pulls saved Gmail tokens from Google Drive
* Uses each token to connect to the Gmail API
* Retrieves emails using multithreading
* Uploads structured data to BigQuery

### Requirements

* BigQuery dataset and table must already exist
* Google service account with BigQuery write access
* Tokens uploaded via the `token_uploader` tool

### How to run

**Locally:**

```bash
cd etl_service
pip install -r requirements.txt
python main.py
```

**Docker:**

```bash
docker build -t gmail-etl .
docker run --rm gmail-etl
```

---

## 📁 Environment Files

You’ll need:

* `client_secret.json` – for user authentication
* `drive-key.json` – for service account access to Google Drive (and optionally BigQuery)
* `token_uploader/` must have access to Drive upload folder
* `etl_service/` must have access to:

  * Google Drive (to download tokens)
  * Gmail API
  * BigQuery (for data loading)

---

## 🛡 License

[MIT License](LICENSE)

---

## ✨ Credits

Built with 💻 by me, using Google APIs, BigQuery, and Python multithreading.
