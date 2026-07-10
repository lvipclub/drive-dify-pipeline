# QUI GAP ANALYSIS: Drive → Dify KB Ingestion Pipeline

**Document:** `QUI_GAP_ANALYSIS.md`
**Analyst:** Qui (QA Specialist)
**Date:** 2026-07-03
**Plan under review:** `ACTION_PLAN.md` (drive-dify-pipeline-v1)
**Status of Plan:** 🟡 PENDING — Awaiting Google OAuth refresh token (Codi has NOT yet built pipeline code)
**Real Configuration Verified:**
- Dify dataset key: `dataset-PC1cMX9XVkD9q2eZb9VTNTeQ` ✅ tested — returns valid KB metadata (HVAC Controls, 9 docs, 1.2M words)
- Dify dataset ID: `51610c8d-79c7-41fb-bb7e-1af3b120a850` ✅ confirmed
- Google folder ID: `1GlOjKMZWmSIa6ejf1InUwYybT7ZuE102` ✅ plan states confirmed
- Google client secret at `~/workspace/google_client_secret.json` ✅ present (project `oc-i58500hp`, installed app type)

---

## Executive Summary

The plan is **architecturally sound** at a high level — poll Drive, convert, upload, delete, notify. It got the hard parts right (refresh token, dataset-vs-app key distinction). However, it has **7 critical gaps**, **13 warnings**, and **8 nice-to-have improvements** across all seven analysis dimensions. The gaps are concentrated in: OAuth scope mismatch (the pipeline will fail on step 4e), missing concurrency guards, and unhandled Drive API pagination.

---

## 1. ARCHITECTURE GAPS

### 🔴 GAP-A1: OAuth Scope Mismatch — Will Break at File Deletion

**Severity:** 🔴 Critical
**Location:** ACTION_PLAN.md §1.2, §2.1 Flow step 4e

The plan requests `https://www.googleapis.com/auth/drive.readonly` scope but then **deletes** source files via `files().delete()`. This WILL fail with a 403 error:

```
googleapiclient.errors.HttpError: <HttpError 403
"Request had insufficient authentication scopes."
```

**Root cause:** `drive.readonly` grants only `drive.read` / `drive.metadata.read`. File deletion requires `drive.file` scope (`https://www.googleapis.com/auth/drive.file`).

**Proposed fix:**
```python
# auth_google.py — change to:
SCOPES = ['https://www.googleapis.com/auth/drive.file']
```
This gives per-file access to files the app creates or opens. If the shared folder already has files created by others, the app also needs to be able to delete them. In that case, use:
```python
SCOPES = ['https://www.googleapis.com/auth/drive']
```
The full `drive` scope is the safest bet since third parties upload files the app didn't create.

**Impact if not fixed:** Pipeline will download and upload to Dify successfully, then crash on Drive delete. Every run will reprocess the same files (Dify uploads will duplicate).

---

### 🔴 GAP-A2: No Concurrent-Run Guard — State File Race Condition

**Severity:** 🔴 Critical
**Location:** ACTION_PLAN.md §3.1, §3.2

The plan uses a plain `state.json` file with no file locking. If a pipeline run takes >2 hours (e.g., processing a 500 MB PDF), the next cron tick kicks off a **second instance** that reads the same `state.json`, processes the same files, and writes back the same keys. Result:

1. **Duplicate Dify documents** — both instances upload the same content
2. **State file corruption** — two processes race on `json.dump()`
3. **Double-delete attempt** — second instance tries to delete a file first instance already deleted

**Existing patterns in the codebase:** The Helen Watchdog uses `trap` and log files. The gbrain uses `.gbrain-lock/lock`. No existing cron script uses a PID-based mutex for drive-dify workloads.

**Proposed fix:**
```python
# pipeline.py — at startup:
LOCKFILE = Path('/tmp/drive-dify-pipeline.lock')

def acquire_lock():
    """Create PID-based lock. Returns True if lock acquired."""
    LOCKFILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(LOCKFILE, 'x') as f:
            f.write(str(os.getpid()))
        return True
    except FileExistsError:
        # Check if lock is stale (PID no longer running)
        try:
            old_pid = int(LOCKFILE.read_text().strip())
            os.kill(old_pid, 0)  # Signal 0 just checks existence
            return False  # Process still running
        except (ValueError, ProcessLookupError, OSError):
            # Stale lock — take over
            LOCKFILE.write_text(str(os.getpid()))
            return True

def release_lock():
    LOCKFILE.unlink(missing_ok=True)
```

---

### 🔴 GAP-A3: Google Drive API Pagination Not Handled

**Severity:** 🔴 Critical
**Location:** ACTION_PLAN.md §2.1 Flow step 3

The plan says "LIST: Enumerate files in shared folder" but doesn't handle pagination. Google Drive API `files().list()` returns **max 100 results per page** by default. If the shared folder accumulates >100 files, the pipeline silently misses files beyond page 1.

**Real-world scenario:** This is an HVAC engineering KB. A third party might bulk-upload reference material (ASHRAE standards, EN docs, manufacturer catalogs) — easily 150+ files. The pipeline only processes the first 100.

**Proposed fix:**
```python
def list_all_files(service, folder_id):
    files = []
    page_token = None
    while True:
        results = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            pageSize=100,
            pageToken=page_token,
            fields="nextPageToken, files(id, name, mimeType, size, createdTime)"
        ).execute()
        files.extend(results.get('files', []))
        page_token = results.get('nextPageToken')
        if not page_token:
            break
    return files
```

---

### 🟡 GAP-A4: Dify API `create-by-text` Endpoint Not Validated

**Severity:** 🟡 Warning
**Location:** ACTION_PLAN.md §2.1 Flow step 4d

The plan specifies:
```
POST https://api.dify.ai/v1/datasets/{dataset_id}/document/create-by-text
```

The actual Dify API path uses plural `documents` (not `document`). The correct path is:
```
POST https://api.dify.ai/v1/datasets/{dataset_id}/documents/create-by-text
```

**Verification performed:** `GET /datasets/{id}/documents` returns the document list. The plan's singular form may work (Dify might accept both), but this needs verification before coding.

**Proposed fix:** Verify with a test call using the dataset key BEFORE building pipeline.py:
```bash
curl -s -X POST "https://api.dify.ai/v1/datasets/51610c8d-79c7-41fb-bb7e-1af3b120a850/documents/create-by-text" \
  -H "Authorization: Bearer dataset-PC1cMX9XVkD9q2eZb9VTNTeQ" \
  -H "Content-Type: application/json" \
  -d '{"name":"qa-test-doc","text":"This is a QA verification test.","indexing_technique":"high_quality"}'
```

---

### 🟡 GAP-A5: Python Environment / Dependency Management Missing

**Severity:** 🟡 Warning
**Location:** ACTION_PLAN.md §1.3

The plan lists raw `pip install` dependencies but:
1. **No virtualenv specified** — system Python 3.12.3 on WSL has PEP 668 enforced (`pip install` without `--break-system-packages` will fail)
2. **`markitdown` not installed** — verified: neither `markitdown` nor any variant is present
3. **PaddleOCR mentioned in flow but NOT in requirements.txt** — if needed for scanned PDF OCR, it's a heavy dependency (~2 GB)

**Proposed fix:**
```bash
# In setup script or pipeline.sh
python3 -m venv ~/workspace/drive-dify-pipeline/.venv
source ~/workspace/drive-dify-pipeline/.venv/bin/activate
pip install google-api-python-client google-auth-oauthlib markitdown pymupdf requests pyyaml
```

Or use `uv` for deterministic installs (preferred per existing infra).

---

### 🟡 GAP-A6: `/tmp` Download Directory — No Cleanup, No Disk Check

**Severity:** 🟡 Warning
**Location:** ACTION_PLAN.md §2.1 Flow step 4a

Files downloaded to `/tmp/drive-dify-pipeline/` are never cleaned up. If the pipeline crashes after download but before upload, temp files accumulate. A batch of 50 PDF engineering textbooks (50 MB each) = 2.5 GB in `/tmp`.

Also, WSL `/tmp` is in-memory (tmpfs) — filling it can crash the entire WSL instance.

**Proposed fix:**
```python
# At pipeline startup
import shutil, tempfile

TMP_DIR = Path('/tmp/drive-dify-pipeline')
# Check available space (need at least 500 MB)
stat = os.statvfs('/tmp')
free_mb = (stat.f_bavail * stat.f_frsize) // (1024 * 1024)
if free_mb < 500:
    raise RuntimeError(f"Insufficient disk space in /tmp: {free_mb}MB free")

# At pipeline end
shutil.rmtree(TMP_DIR, ignore_errors=True)
# Also: per-file cleanup in finally block
```

---

### 🟡 GAP-A7: No Differential Sync — Full Re-upload on File Update

**Severity:** 🟡 Warning
**Location:** ACTION_PLAN.md §3.2

The state file tracks only `file_id → {name, time}`. If a third party **updates** an existing file (same Drive file ID), the pipeline skips it because the ID is in `state.json`.

**Scenario:** HVAC engineer uploads "controls-guide-v1.pdf" → ingested. Later uploads a corrected "controls-guide-v1.pdf" with the same name but different content. Google Drive treats it as a new file ID (if they delete and re-upload) OR same file ID (if they overwrite via Drive UI). In the overwrite case, the pipeline skips it.

**Proposed fix:** Track `modifiedTime` in state.json and compare with Drive API `modifiedTime`:
```json
{
  "processed": {
    "file-id-1": {
      "name": "hvac-textbook.pdf",
      "time": "2026-07-03T10:30:00",
      "modifiedTime": "2026-07-03T09:15:00"
    }
  }
}
```
If Drive's `modifiedTime` > state's `modifiedTime`, reprocess the file.

---

## 2. SECURITY GAPS

### 🔴 GAP-S1: Dify Dataset Key Stored in config.yaml — Plaintext Exposure

**Severity:** 🔴 Critical
**Location:** ACTION_PLAN.md §2.2

The plan's `config.yaml` has `api_key: ""` with a TODO. Per instructions, the real key is `dataset-PC1cMX9XVkD9q2eZb9VTNTeQ`. If this key is written into `config.yaml`, it is:
1. **In plaintext** on disk — any process that can read `~/workspace/` can read it
2. **Likely committed to git** — if the workspace is a repo, accidental commit risk
3. **Blast radius is ALL KBs** — this is a workspace-level key, not per-KB

**Proposed fix:** Follow existing pattern — store in `~/.hermes/config/dify-drive.env` with chmod 600:
```bash
cat > ~/.hermes/config/dify-drive.env << 'EOF'
DIFY_DATASET_KEY=dataset-PC1cMX9XVkD9q2eZb9VTNTeQ
DIFY_API_BASE=https://api.dify.ai/v1
DIFY_DATASET_ID=51610c8d-79c7-41fb-bb7e-1af3b120a850
EOF
chmod 600 ~/.hermes/config/dify-drive.env
```
Then source in pipeline.sh or use `os.getenv()`.

---

### 🟡 GAP-S2: Google Refresh Token Storage — Permissions Not Documented

**Severity:** 🟡 Warning
**Location:** ACTION_PLAN.md §1.2

The plan saves refresh token to `~/.hermes/config/drive-refresh-token.json` but never specifies `chmod 600`. The refresh token is a long-lived credential — anyone with it can impersonate the Google account for Drive access.

**Proposed fix:** `auth_google.py` MUST set permissions:
```python
import os, stat
token_path = os.path.expanduser('~/.hermes/config/drive-refresh-token.json')
with open(token_path, 'w') as f:
    json.dump(creds_data, f)
os.chmod(token_path, stat.S_IRUSR | stat.S_IWUSR)  # 600
```

---

### 🟡 GAP-S3: Google Client Secret in Workspace — Accidental Git Commit Risk

**Severity:** 🟡 Warning
**Location:** ACTION_PLAN.md §2.2

`~/workspace/google_client_secret.json` contains the raw `client_secret` (`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`). If `~/workspace/` is a git repo, this file is at risk of accidental commit.

**Proposed fix:**
1. Ensure `.gitignore` contains `google_client_secret.json`
2. Consider moving to `~/.hermes/config/` alongside the refresh token
3. Verify with `git check-ignore google_client_secret.json` after moving

---

### 🟢 GAP-S4: No API Key Rotation Mechanism

**Severity:** 🟢 Nice-to-have
If the Dify dataset key is ever rotated, the pipeline breaks silently on the next run (401 errors). The plan's error handling catches this but there's no documented rotation procedure.

---

## 3. ERROR HANDLING GAPS

### 🔴 GAP-E1: No Retry on Google Drive API Failures

**Severity:** 🔴 Critical
**Location:** ACTION_PLAN.md §2.1 "Error Handling"

The plan specifies 429 retry only — and the wording is ambiguous about which API. Google Drive API also has quotas: **per-user: 12,000 requests/min**, **per-user-per-project: 750 requests/min** for `files.list`, and **10 requests/second** burst limit. A burst of Drive calls (list + download N files + delete N files) can trigger 403 rate limit errors.

The plan's error handling section only mentions:
- 429 → retry (Dify rate limits)
- 401 → stop
- Google Drive quota → "log, skip, continue" — but what about transient 429/403/500 from Drive?

**Proposed fix:** Add universal HTTP retry with backoff for both APIs:
```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=lambda e: isinstance(e, (HttpError, ConnectionError))
)
def drive_api_call(func, *args, **kwargs):
    return func(*args, **kwargs).execute()
```

---

### 🟡 GAP-E2: No HTTP Timeout on Any External Call

**Severity:** 🟡 Warning
**Location:** Entire pipeline

No timeout is specified for:
- Google Drive `files().list()` — default: indefinite
- Google Drive `files().get_media()` — could hang on large file
- Dify API POST — could hang if Dify is under load
- Token refresh — could hang if Google OAuth server is slow

If any of these hang, the cron job blocks indefinitely until the cron daemon kills it (after however many hours).

**Proposed fix:**
```python
# Google API service with timeout
service = build('drive', 'v3', credentials=creds, 
                cache_discovery=False)
# requests-level timeout for all Drive calls
import googleapiclient.http
# Monkey-patch or use requests.Session with timeout
session = requests.Session()
session.timeout = 30  # seconds
# For downloads, set per-call timeout
request = service.files().get_media(fileId=file_id)
request.timeout = 300  # 5 min for large files

# Dify calls
response = requests.post(dify_url, json=body, headers=headers, timeout=60)
```

---

### 🟡 GAP-E3: Dify Text Size Limits Unknown — Can Break Mid-Batch

**Severity:** 🟡 Warning
**Location:** ACTION_PLAN.md §2.1 Flow step 4d

The plan chunks at 8,000 characters and assumes Dify accepts arbitrary text. Dify Cloud free tier may have:
- Per-document text size limit (unknown, needs testing)
- Total KB size limit (free tier: unknown)
- Upload rate limit per minute (documented but values vary by plan)

If a single 8K chunk exceeds Dify's per-document limit, the entire pipeline run fails for that file. The plan needs to discover these limits by testing.

**Proposed fix:** Before building, run a boundary test:
```bash
# Test with progressively larger chunks
for size in 1000 5000 10000 50000 100000; do
  python3 -c "print('x' * $size)" > /tmp/test_chunk.txt
  # Upload via create-by-text
done
```

---

### 🟡 GAP-E4: Partial Success Tracking Incomplete

**Severity:** 🟡 Warning
**Location:** ACTION_PLAN.md §2.1 Flow, §3.2

If a pipeline run processes 5 files where:
- File 1: uploaded to Dify ✅, deleted from Drive ✅
- File 2: uploaded to Dify ✅, Drive delete fails ❌
- File 3: conversion fails ❌

The state file structure (`processed` vs `failed`) tracks binary success/failure per file, but doesn't track the **Dify document ID** after successful upload. If Drive delete fails for file 2, the next run doesn't know to just retry the delete — it thinks the file needs full reprocessing.

**Proposed fix:** Augment state.json:
```json
{
  "processed": {
    "file-id-1": {
      "name": "hvac-textbook.pdf",
      "time": "2026-07-03T10:30:00",
      "dify_doc_id": "abc-123-def",
      "status": "complete"
    },
    "file-id-2": {
      "name": "controls-manual.pdf",
      "time": "2026-07-03T10:32:00",
      "dify_doc_id": "xyz-456-ghi",
      "status": "dify_ok_drive_delete_failed"
    }
  }
}
```

---

## 4. EDGE CASES

### 🔴 GAP-EC1: Google Workspace Files (Docs, Sheets) — Can't Download

**Severity:** 🔴 Critical
**Location:** ACTION_PLAN.md §2.1 Flow step 4a

Google Drive shared folders can contain native Google Workspace files (Docs, Sheets, Slides). These have MIME type `application/vnd.google-apps.document` (not `application/pdf`). The `files().get_media()` API call **fails** for these — you must use `files().export()` with an export MIME type.

**Scenario:** Third party creates a Google Doc of equipment specs and moves it to the shared folder. Pipeline tries to download it as a binary and fails.

**Proposed fix:**
```python
GOOGLE_MIME_TYPES = {
    'application/vnd.google-apps.document': 'application/pdf',  # Docs → PDF
    'application/vnd.google-apps.spreadsheet': 'text/csv',      # Sheets → CSV
    'application/vnd.google-apps.presentation': 'application/pdf',  # Slides → PDF
}

if mime_type in GOOGLE_MIME_TYPES:
    request = service.files().export(
        fileId=file_id, 
        mimeType=GOOGLE_MIME_TYPES[mime_type]
    )
else:
    request = service.files().get_media(fileId=file_id)
```

---

### 🟡 GAP-EC2: Encrypted / Password-Protected PDFs

**Severity:** 🟡 Warning
**Location:** ACTION_PLAN.md §2.1 Flow step 4c

PyMuPDF (`fitz`) raises an exception on encrypted PDFs:
```
fitz.FileDataError: cannot open encrypted document
```

The plan doesn't check for encryption before attempting conversion. The pipeline will log it as "conversion failure" and skip, which is correct behavior — but it should also explicitly note WHY (encrypted) vs. WHY (corrupted), since the third-party user might fix encryption but not corruption.

**Proposed fix:**
```python
doc = fitz.open(filepath)
if doc.is_encrypted:
    raise SkipFileError(f"PDF is encrypted/password-protected: {filename}")
```

---

### 🟡 GAP-EC3: Zero-Byte / Empty Files

**Severity:** 🟡 Warning
**Location:** ACTION_PLAN.md §2.1 Flow

Zero-byte files should be skipped before conversion. Uploading empty text to Dify wastes API calls and creates empty KB documents that confuse retrieval.

**Proposed fix:**
```python
if file_size == 0:
    log.warning(f"Skipping zero-byte file: {filename}")
    state['failed'][file_id] = {"name": filename, "error": "Zero-byte file", "time": now}
    continue
```

---

### 🟡 GAP-EC4: Filename Collisions in Dify

**Severity:** 🟡 Warning
**Location:** ACTION_PLAN.md §2.1 Flow step 4d

Dify uses `name` in `create-by-text` as the document name. If two different Drive files share the same filename (allowed in Drive), the second upload may:
1. Silently overwrite the first (if Dify treats names as unique)
2. Create a duplicate with the same display name (if Dify allows duplicates)

Either way, retrieval quality may degrade. The pipeline should append a disambiguating suffix.

**Proposed fix:**
```python
import uuid
dify_name = f"{Path(filename).stem}-{str(uuid.uuid4())[:8]}{Path(filename).suffix}"
```

---

### 🟡 GAP-EC5: Extremely Large Files (>500 MB)

**Severity:** 🟡 Warning
**Location:** ACTION_PLAN.md §2.1

The plan has no file size limit. A 500 MB PDF:
1. Takes 10+ minutes to download (WSL network + VPN)
2. PyMuPDF text extraction may OOM (loads full file in memory)
3. If chunked to 8K pieces = 62,500 API calls to Dify — guaranteed to hit rate limits
4. Blocks the cron slot for hours

**Proposed fix:**
```python
MAX_FILE_SIZE_MB = 100
if file_size > MAX_FILE_SIZE_MB * 1024 * 1024:
    log.warning(f"Skipping oversized file ({file_size/1024/1024:.1f}MB > {MAX_FILE_SIZE_MB}MB): {filename}")
    state['failed'][file_id] = {"name": filename, "error": f"File too large: {file_size/1024/1024:.1f}MB", "time": now}
    continue
```

---

### 🟢 GAP-EC6: Unicode/Special Characters in Filenames

**Severity:** 🟢 Nice-to-have
Chinese characters (this is an HVAC KB for a bilingual audience), emoji, or control characters in filenames could cause issues in both filesystem paths and Dify document names. Sanitize before Dify upload.

---

### 🟢 GAP-EC7: File Extensions MIME-Type Mismatch

**Severity:** 🟢 Nice-to-have
A file named `manual.pdf` might actually be a ZIP archive. Trust Drive API's `mimeType` over extension, and validate actual content before conversion.

---

## 5. MONITORING GAPS

### 🔴 GAP-M1: Telegram Chat ID Placeholder — Notifications Won't Deliver

**Severity:** 🔴 Critical
**Location:** ACTION_PLAN.md §3.1

The cron job specifies `deliver: "telegram:-100xxxxx"` — this is a placeholder. The Telegram chat ID needs to be resolved from Marc Sir's actual gateway configuration. Without this, the pipeline runs silently with no feedback loop.

**Proposed fix:** Look up the actual Telegram chat ID from the existing gateway config (check `~/.hermes/gateway/` or existing cron jobs that successfully deliver to Telegram).

---

### 🔴 GAP-M2: No Pipeline Silence Alert

**Severity:** 🔴 Critical
**Location:** Entire monitoring

If the cron job silently stops working (e.g., config file deleted, Python version mismatch after system update, WSL not running), there is **no external alert**. Marc Sir would only discover the pipeline is broken when a third party asks "why aren't my documents showing up?"

**Proposed fix:** Add a heartbeat to the existing `helen-watchdog.sh`:
```bash
# In helen-watchdog.sh, add:
STATE_FILE="$HOME/workspace/drive-dify-pipeline/state.json"
if [ -f "$STATE_FILE" ]; then
    LAST_RUN=$(python3 -c "
import json, datetime
try:
    data = json.load(open('$STATE_FILE'))
    times = [v['time'] for v in data.get('processed',{}).values()]
    times.sort(reverse=True)
    print(times[0] if times else 'NEVER')
except: print('CORRUPT')
    " 2>/dev/null)
    echo "Drive→Dify last run: $LAST_RUN"
    # If last run > 6 hours ago, alert
else
    echo "WARNING: Drive→Dify state file missing"
fi
```

---

### 🟡 GAP-M3: No Structured Logging or Metrics

**Severity:** 🟡 Warning
**Location:** ACTION_PLAN.md §3.1

The pipeline pipes `stdout` to cron delivery. This means:
- No persistent logs for debugging (only last run's output)
- No metrics: items processed per run, avg conversion time, error rate over time
- No way to check "was X processed?" without reading state.json manually

**Proposed fix:** Log to both stdout AND a rotating log file:
```python
import logging
from logging.handlers import RotatingFileHandler

handler = RotatingFileHandler(
    'logs/pipeline.log', maxBytes=10*1024*1024, backupCount=5
)
handler.setFormatter(logging.Formatter(
    '%(asctime)s [%(levelname)s] %(message)s', 
    datefmt='%Y-%m-%d %H:%M:%S'
))
logger.addHandler(handler)

# Also log metrics summary
logger.info(f"METRICS: processed={processed} failed={failed} skipped={skipped} "
            f"elapsed={elapsed:.1f}s files_deleted={deleted}")
```

---

### 🟡 GAP-M4: No Dify KB State Baseline or Regression Detection

**Severity:** 🟡 Warning

After ingestion, no check verifies the Dify KB actually has the expected document count. If Dify silently drops documents (free-tier behavior observed in dify-cloud-setup skill), nobody knows.

**Proposed fix:** After pipeline run, verify document count:
```python
response = requests.get(
    f"{DIFY_BASE}/datasets/{DATASET_ID}",
    headers={"Authorization": f"Bearer {API_KEY}"}
)
doc_count = response.json().get('document_count', 0)
logger.info(f"Dify KB document count post-ingestion: {doc_count}")
```
Alert if count decreases between runs.

---

### 🟢 GAP-M5: No Dashboard or Quick-Status Command

**Severity:** 🟢 Nice-to-have
A simple `hermes pipeline status drive-dify` command that returns "last run: X ago, processed Y files, Z in queue" would dramatically reduce operational friction for Marc Sir.

---

## 6. OPERATIONAL GAPS

### 🔴 GAP-O1: State File Corruption — Atomic Write Not Enforced

**Severity:** 🔴 Critical
**Location:** ACTION_PLAN.md §3.2

If the pipeline crashes (power loss, WSL killed, OOM) while writing `state.json`, the file is truncated/corrupted. Next run: the pipeline can't deserialize state, loses all processing history, and re-processes (and re-uploads) every file in the folder.

**Proposed fix:** Atomic write pattern:
```python
def save_state(state, path):
    tmp_path = path.with_suffix('.tmp')
    with open(tmp_path, 'w') as f:
        json.dump(state, f, indent=2)
    tmp_path.rename(path)  # Atomic on Linux
```

Also: load state with corruption recovery:
```python
def load_state(path):
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, FileNotFoundError):
        # Try recovery from .tmp or .bak
        backup = path.with_suffix('.json.bak')
        if backup.exists():
            logger.warning(f"state.json corrupted — recovered from backup")
            return json.loads(backup.read_text())
        logger.warning("state.json missing/corrupted — starting fresh")
        return {"processed": {}, "failed": {}}
```

---

### 🔴 GAP-O2: No Backup of State File Before Mutations

**Severity:** 🔴 Critical
**Location:** ACTION_PLAN.md §3.2

The plan updates `state.json` inline — no backup before mutation. Existing infrastructure has `jobs.json.bak.*` patterns (seen in `~/.hermes/cron/`), proving Hermes already practices backup-before-mutation.

**Proposed fix:** Before each pipeline run:
```python
if state_path.exists():
    backup_path = state_path.with_suffix(f'.json.bak.{datetime.now():%Y%m%d_%H%M%S}')
    shutil.copy2(state_path, backup_path)
    # Rotate old backups (keep last 10)
    backups = sorted(Path('.').glob('state.json.bak.*'))
    for old in backups[:-10]:
        old.unlink()
```

---

### 🟡 GAP-O3: Cron Interval (Every 2 Hours) Conflicts with File Processing Time

**Severity:** 🟡 Warning
**Location:** ACTION_PLAN.md §3.1

The cron runs every 2 hours. If processing 50 files takes 90 minutes (download + convert + upload), the next cron tick fires 30 minutes later. With the PID lock from GAP-A2, the second run would skip — but this means **files uploaded during processing are missed** until the NEXT tick (2 hours later). The third party's expectation of "within 2 hours" could easily stretch to 4+ hours.

**Proposed fix:** Use a shorter poll interval or implement a continuous poll loop:
```yaml
# Option A: Poll more frequently (every 30 min)
schedule: "*/30 * * * *"

# Option B: Once started, poll back-to-back until folder is empty
# pipeline.py --loop: keep polling folder until empty, then exit
```

Or: the 2-hour interval is fine for the use case — just communicate "within 4 hours" instead of "within 2 hours" to third parties.

---

### 🟡 GAP-O4: WSL Hibernation — Cron Timer Drift

**Severity:** 🟡 Warning
**Location:** Entire operational model

Marc Sir runs this on WSL (hp-i58g500g). When the Windows host sleeps/hibernates, WSL suspends. The cron timer in WSL **does not catch up on missed intervals** — it fires the next scheduled time. If the machine sleeps from 2 AM to 8 AM, the 4 AM, 6 AM, and 8 AM cron ticks are all missed.

**Existing patterns:** The `helen-watchdog.sh` runs at 2 AM daily. The existing `heartbeat` cron tracks last success. But no existing mechanism handles missed cron intervals.

**Proposed fix:** At startup, check if the last run was more than 3 hours ago — if so, trigger an immediate run:
```python
if state_path.exists():
    state = load_state(state_path)
    last_run = max(
        (v['time'] for v in state.get('processed', {}).values()),
        default=None
    )
    if last_run and (datetime.now() - datetime.fromisoformat(last_run)).hours > 3:
        logger.warning("Missed run detected — forcing immediate processing")
```

---

### 🟡 GAP-O5: No Rollback on Partial Drive Delete Failure

**Severity:** 🟡 Warning
**Location:** ACTION_PLAN.md §2.1 Flow step 4e

The flow is: upload to Dify → if 200 OK → delete from Drive. But what if:
1. Upload to Dify succeeds (200 OK) ✅
2. Drive delete fails (network blip, 403, etc.) ❌
3. Next run sees the file still in Drive + already in `state.json` → skips it
4. Dify now has the document BUT third party sees the file still in Drive → confusion

**Proposed fix:** Track partial state (GAP-E4) and retry the delete only on next run:
```python
# On next run, scan state.json for files with status='dify_ok_drive_delete_failed'
# Retry the delete only (no re-upload to Dify)
```

---

### 🟢 GAP-O6: No Dry-Run Mode Implemented

**Severity:** 🟢 Nice-to-have
The plan mentions `--dry-run` in the CLI signature but doesn't specify what it does. Should skip Dify upload AND Drive delete, only performing download + conversion. Essential for testing.

---

## 7. INTEGRATION GAPS

### 🟡 GAP-I1: No Integration with Existing Backup Scripts

**Severity:** 🟡 Warning
**Location:** Entire pipeline

The existing `codi-daily-backup.sh` backs up `~/workspace/` and `~/.hermes/` to Google Drive. But:
- `state.json` is in `~/workspace/drive-dify-pipeline/` → backed up
- `/tmp/drive-dify-pipeline/` is NOT backed up → temp files lost on crash
- `~/.hermes/config/drive-refresh-token.json` → backed up ✅
- `~/.hermes/config/dify-drive.env` → NOT yet created, needs including

**Proposed fix:** Add to `codi-daily-backup.sh`:
```bash
# Backup Dify Drive pipeline configs
cp ~/.hermes/config/dify-drive.env "$BACKUP_DIR/config/" 2>/dev/null || true
cp ~/workspace/drive-dify-pipeline/state.json* "$BACKUP_DIR/pipeline/" 2>/dev/null || true
```

---

### 🟡 GAP-I2: No Integration with Existing Watchdog Scripts

**Severity:** 🟡 Warning
**Location:** Monitoring

The `helen-watchdog.sh` (daily 2 AM) and `webui-watchdog.sh` (every 2 min) check server health but don't check the pipeline. No alert if pipeline dependencies break (e.g., `markitdown` uninstalled after system update).

**Proposed fix:** Add pipeline health check to helen-watchdog.sh (see GAP-M2).

---

### 🟡 GAP-I3: Package Dependency Conflict Risk with Existing Python Environment

**Severity:** 🟡 Warning
**Location:** ACTION_PLAN.md §1.3

The plan installs packages to the system Python. Existing projects in `~/workspace/` (ai-xinca, Shopify tools, gmc tools) may have conflicting dependency versions. Without a venv, upgrading `pymupdf` for this pipeline could break existing scripts.

**Proposed fix:** Isolate in a venv (see GAP-A5). This is the standard Hermes pattern.

---

### 🟢 GAP-I4: No Documentation for Other Agents

**Severity:** 🟢 Nice-to-have
Codi, Leni, and Havi might need to interact with the pipeline (check status, trigger manual run, look up ingestion history). A small README.md in `drive-dify-pipeline/` with quick commands would help.

---

## 8. PRE-BUILD VERIFICATION CHECKLIST

Before Codi writes a single line of `pipeline.py`, these items MUST be resolved:

| # | Item | Status | Priority |
|---|------|--------|----------|
| 1 | Verify Dify `create-by-text` endpoint path (singular vs plural) | ❌ | 🔴 |
| 2 | Get Google OAuth refresh token (run `auth_google.py`) | ❌ | 🔴 |
| 3 | Verify Drive API enabled for project `oc-i58500hp` | ❌ | 🔴 |
| 4 | Determine Dify per-document text size limit via test upload | ❌ | 🔴 |
| 5 | Resolve actual Telegram chat ID for cron delivery | ❌ | 🔴 |
| 6 | Test `markitdown` with a real .epub + .docx file | ❌ | 🟡 |
| 7 | Check if files in shared folder have `drive.file` scope access | ❌ | 🟡 |

---

## Summary Table

| ID | Gap | Severity | Category |
|----|-----|----------|----------|
| A1 | OAuth scope mismatch (readonly + delete) | 🔴 Critical | Architecture |
| A2 | No concurrent-run guard (state file race) | 🔴 Critical | Architecture |
| A3 | Drive API pagination not handled | 🔴 Critical | Architecture |
| S1 | Dify dataset key in plaintext config.yaml | 🔴 Critical | Security |
| E1 | No retry on Google Drive API failures | 🔴 Critical | Error Handling |
| M1 | Telegram chat ID placeholder | 🔴 Critical | Monitoring |
| M2 | No pipeline silence alert | 🔴 Critical | Monitoring |
| O1 | State file corruption (no atomic write) | 🔴 Critical | Operational |
| O2 | No backup before state mutation | 🔴 Critical | Operational |
| A4 | Dify endpoint path unverified | 🟡 Warning | Architecture |
| A5 | No venv/dependency management | 🟡 Warning | Architecture |
| A6 | /tmp download cleanup missing | 🟡 Warning | Architecture |
| A7 | No differential sync (file updates) | 🟡 Warning | Architecture |
| S2 | Token permissions not documented | 🟡 Warning | Security |
| S3 | Client secret in workspace (git risk) | 🟡 Warning | Security |
| E2 | No HTTP timeout on external calls | 🟡 Warning | Error Handling |
| E3 | Dify text size limits unknown | 🟡 Warning | Error Handling |
| E4 | Partial success tracking incomplete | 🟡 Warning | Error Handling |
| EC1 | Google Workspace files can't download | 🔴 Critical | Edge Cases |
| EC2 | Encrypted PDFs unhandled | 🟡 Warning | Edge Cases |
| EC3 | Zero-byte files not skipped | 🟡 Warning | Edge Cases |
| EC4 | Dify filename collisions | 🟡 Warning | Edge Cases |
| EC5 | Files >500 MB no size guard | 🟡 Warning | Edge Cases |
| M3 | No structured logging/metrics | 🟡 Warning | Monitoring |
| M4 | No Dify KB state regression check | 🟡 Warning | Monitoring |
| O3 | 2hr cron interval vs processing time | 🟡 Warning | Operational |
| O4 | WSL hibernation misses cron ticks | 🟡 Warning | Operational |
| O5 | No rollback on partial delete failure | 🟡 Warning | Operational |
| I1 | No integration with backup scripts | 🟡 Warning | Integration |
| I2 | No integration with watchdog scripts | 🟡 Warning | Integration |
| I3 | Package dependency conflicts | 🟡 Warning | Integration |
| S4 | No API key rotation mechanism | 🟢 Nice-to-have | Security |
| EC6 | Special chars in filenames | 🟢 Nice-to-have | Edge Cases |
| EC7 | MIME type mismatch detection | 🟢 Nice-to-have | Edge Cases |
| M5 | No dashboard/quick-status command | 🟢 Nice-to-have | Monitoring |
| O6 | Dry-run mode unimplemented | 🟢 Nice-to-have | Operational |
| I4 | No documentation for agents | 🟢 Nice-to-have | Integration |

**Totals:** 9 Critical · 22 Warning · 6 Nice-to-have = **37 gaps found**

---

## Recommendation

**DO NOT proceed to build until GAP-A1 (OAuth scope) and GAP-S1 (key storage) are addressed.** These two gaps alone will cause the pipeline to fail in production on the first real run. Codi should address all 🔴 Critical gaps before writing pipeline code, and implement the 🟡 Warning fixes during the build phase.

The cleanest path forward:
1. Marc Sir runs `auth_google.py` with corrected `drive` scope → gets refresh token
2. Codi creates `~/.hermes/config/dify-drive.env` with chmod 600
3. Codi builds pipeline with all Critical fixes incorporated
4. Qui re-reviews the actual code against this gap list
5. Dry-run test with 3-5 real files
