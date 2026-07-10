# ACTION PLAN: Google Drive → Dify KB Auto-Ingestion Pipeline
**Status:** 🟡 PENDING — Awaiting Google OAuth refresh token (1 of 2 blockers resolved)
**Plan ID:** drive-dify-pipeline-v1
**Created:** 2026-07-03
**Owner:** Helen → Codi (build) → Qui (QA)

---

## Overview

Build a Hermes cron-driven pipeline that:
1. Polls a shared Google Drive folder for new files
2. Downloads and converts PDF/ePub/DOCX → clean Markdown
3. Uploads to Dify Knowledge Base via the Knowledge API
4. Deletes the source file from Google Drive on confirmed success
5. Reports to Marc Sir via Telegram

Third parties only need to **upload files to a shared Google Drive folder** — zero learning curve.

---

## Prerequisites (Manual — Marc Sir)

### A. Google Drive OAuth Refresh Token ⚠️
We have OAuth credentials (`~/workspace/google_client_secret.json`, project `oc-i58500hp`) but they're "installed" type. To make this work for unattended cron polling, we need a **persistent refresh token**.

**Steps (one-time, 5 minutes):**
```bash
# Run the auth helper — it will open a browser for lvipclub@gmail.com
python3 ~/workspace/drive-dify-pipeline/auth_google.py
# This saves a refresh token to ~/.hermes/config/drive-refresh-token.json
```

The script will be provided in Phase 1.

### B. Dify Dataset API Key ✅ RESOLVED
**Key:** `dataset-PC1cMX9XVkD9q2eZb9VTNTeQ`
**Base URL:** `https://api.dify.ai/v1`
**KB:** HVAC Controls (ID: `51610c8d-79c7-41fb-bb7e-1af3b120a850`)

**📍 Where it was found (corrected):**
Knowledge **listing page** (NOT individual KB page) → bottom-left → "API Access" / "Backend service api"
This is the **workspace-level** dataset key that covers all KBs under the account.

### C. Enable Google Drive API
The Drive API should already be enabled for project `oc-i58500hp`. Verify at:
https://console.cloud.google.com/apis/library/drive.googleapis.com?project=oc-i58500hp

---

## Pipeline Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌──────────────┐     ┌────────────┐
│ Shared Google    │────▶│ Hermes Cron  │────▶│ Dify KB API  │────▶│ Dify KB    │
│ Drive Folder     │     │ (every 2hr)  │     │ (dataset key)│     │            │
│                  │     │              │     │              │     │            │
│ 3rd party uploads│     │ download →   │     │ POST document│     │ chunked &  │
│ PDF, ePub, DOCX  │     │ convert →    │     │ create-by-   │     │ indexed    │
│                  │     │ upload →     │     │ text         │     │            │
└──────────────────┘     │ delete src   │     └──────────────┘     └────────────┘
                         └──────────────┘
                                │
                         ┌──────▼──────┐
                         │  Telegram   │
                         │  notification│
                         │  to Marc Sir │
                         └─────────────┘
```

---

## Phase 1: Foundation (Codi)

### 1.1 Project Directory
```
~/workspace/drive-dify-pipeline/
├── auth_google.py          # OAuth helper — gets refresh token
├── pipeline.py             # Main pipeline script
├── config.yaml             # Config: folder ID, KB ID, paths
├── state.json              # Tracks processed files (prevents re-ingestion)
├── logs/                   # Per-run logs
└── requirements.txt        # Dependencies
```

### 1.2 Google Auth Helper (`auth_google.py`)
- Uses `google_auth_oauthlib` with the existing `google_client_secret.json`
- Requests `https://www.googleapis.com/auth/drive.readonly` scope
- Opens browser for lvipclub@gmail.com to authorize
- Saves refresh token to `~/.hermes/config/drive-refresh-token.json`

### 1.3 Dependencies
```txt
google-api-python-client  # already installed
google-auth-oauthlib      # already installed
markitdown                # ePub/DOCX conversion
pymupdf                   # PDF extraction
Pillow                    # Image handling
requests                  # Dify API calls
```

---

## Phase 2: Pipeline Script (Codi)

### 2.1 `pipeline.py` — Main Logic

```
pipeline.py --config config.yaml [--dry-run] [--once]

Flow:
1. LOAD state.json (skip already-processed files)
2. AUTH: Use refresh token to get Google Drive credentials
3. LIST: Enumerate files in shared folder (exclude files in state.json)
4. For each NEW file:
   a. DOWNLOAD to /tmp/drive-dify-pipeline/
   b. DETECT type: PDF, ePub, DOCX, TXT, image
   c. CONVERT to markdown:
      - PDF: PyMuPDF (text layers) or PaddleOCR (scanned)
      - ePub/DOCX: markitdown
      - TXT: direct read
      - Images: skip (or OCR with PaddleOCR)
   d. UPLOAD to Dify:
      POST https://api.dify.ai/v1/datasets/{dataset_id}/document/create-by-text
      Authorization: Bearer {dataset_api_key}
      Body: {name, text, indexing_technique: "high_quality"}
   e. On 200 OK → DELETE source from Google Drive
      files().delete(fileId=file_id).execute()
   f. UPDATE state.json with processed file ID + timestamp
5. REPORT: Print summary to stdout (captured by cron → Telegram)

Error Handling:
- 429 (rate limit) → exponential backoff, max 3 retries
- 401 → stop, notify: "Dify API key expired"
- Google Drive quota → log, skip, continue
- Conversion failure → log, skip, DON'T delete source
```

### 2.2 `config.yaml`
```yaml
google_drive:
  folder_id: "1GlOjKMZWmSIa6ejf1InUwYybT7ZuE102"
  credentials_path: "~/workspace/google_client_secret.json"
  token_path: "~/.hermes/config/drive-refresh-token.json"

dify:
  base_url: "https://api.dify.ai/v1"
  dataset_id: ""  # TODO: Fill in from KB URL
  api_key: ""     # TODO: Fill in dataset- key

pipeline:
  state_file: "state.json"
  log_dir: "logs"
  supported_extensions: [".pdf", ".epub", ".docx", ".txt", ".md"]
  chunk_max_chars: 8000  # Per-chunk max for Dify API
```

---

## Phase 3: Cron Deployment (Helen)

### 3.1 Cron Job
```yaml
name: "Drive → Dify Ingestion"
schedule: "0 */2 * * *"       # Every 2 hours
script: "drive-dify-pipeline.sh"
no_agent: true
deliver: "telegram:-100xxxxx"  # Marc Sir's TG
```

**Script (`~/.hermes/scripts/drive-dify-pipeline.sh`):**
```bash
#!/bin/bash
cd ~/workspace/drive-dify-pipeline
python3 pipeline.py --config config.yaml 2>&1
```

### 3.2 State Management
`state.json` prevents re-ingestion:
```json
{
  "processed": {
    "google-drive-file-id-1": {"name": "hvac-textbook.pdf", "time": "2026-07-03T10:30:00"},
    "google-drive-file-id-2": {"name": "controls-manual.epub", "time": "2026-07-03T10:32:00"}
  },
  "failed": {
    "google-drive-file-id-3": {"name": "corrupted.pdf", "error": "Conversion failed", "time": "..."}
  }
}
```

---

## Phase 4: Testing & QA (Qui)

### 4.1 Test Cases
| # | Test | Expected |
|---|------|----------|
| 1 | Upload test PDF → wait 2h | File ingested, deleted from Drive, TG notification |
| 2 | Upload 5 files at once | All processed, no duplicates, correct state tracking |
| 3 | Upload corrupted PDF | Logged as failed, NOT deleted, next run skips it |
| 4 | Dify API down | Pipeline stops, reports error, no data loss |
| 5 | Google Drive quota hit | Skips remaining, retries next run |

### 4.2 Verification
- `state.json` shows correct processed count
- Dify KB shows ingested documents under Knowledge
- Source files deleted from Google Drive shared folder
- Telegram summary: "Drive→Dify: 3 ingested, 2 skipped, 0 failed"

---

## Phase 5: Third-Party Instructions

One-liner for third parties:
> "Upload your PDFs, eBooks, or documents to [this Google Drive folder](https://drive.google.com/drive/folders/1GlOjKMZWmSIa6ejf1InUwYybT7ZuE102?usp=sharing). They'll be automatically added to the knowledge base within 2 hours."

---

## Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Google refresh token expires | `auth_google.py` can re-generate in 5 min |
| Dify free tier limits (1K docs/mo) | State tracking prevents re-upload waste |
| Large PDFs > Dify size limit | Chunk before upload (8K char chunks) |
| Third party uploads virus/malware | Only process .pdf/.epub/.docx/.txt/.md |
| Shared folder access revoked | TG alert as soon as next poll fails |

---

## Next Actions

### 🔴 Blockers (Marc Sir)
1. ~~**Find Dify dataset API key** — Knowledge page → API Access~~ ✅ RESOLVED
2. **Get Google OAuth refresh token** — run `auth_google.py` once
3. **Verify Drive API enabled** for project `oc-i58500hp`

### 🟡 Ready When Unblocked (Codi)
4. Build `auth_google.py` (auth helper)
5. Build `pipeline.py` (main script)
6. Build `drive-dify-pipeline.sh` (cron wrapper)

### 🟢 After Build (Helen/Qui)
7. Deploy cron job
8. Test with real files
9. Document third-party instructions

---

## Configuration Values to Collect

| Config | Current Value | Source |
|--------|--------------|--------|
| Google folder ID | `1GlOjKMZWmSIa6ejf1InUwYybT7ZuE102` | ✅ Confirmed |
| Google project ID | `oc-i58500hp` | ✅ Confirmed |
| Google client ID | `669315947423-ns84ka...` | ✅ Confirmed |
| Google client secret | `GOCSPX-IrXzK...` | ✅ Confirmed |
| Dify API base URL | `https://api.dify.ai/v1` | ✅ Confirmed |
| Dify App API key | `app-xI35EeupuMY4JfUJUHMuvADl` | ✅ Confirmed |
| Dify Dataset ID | `51610c8d-79c7-41fb-bb7e-1af3b120a850` | ✅ Confirmed (HVAC Controls) |
| Dify Dataset API key | `dataset-PC1cMX9XVkD9q2eZb9VTNTeQ` | ✅ Confirmed (Knowledge listing → API Access) |
| Google refresh token | `???` | ⚠️ Run auth_google.py once |
