# CODI REVIEW: Drive-Dify Pipeline ACTION_PLAN
**Reviewer:** Codi (coding specialist)
**Date:** 2026-07-03
**Plan ID:** drive-dify-pipeline-v1
**Overall Grade:** B+ — Solid foundation, needs hardening for production

---

## Executive Summary

The ACTION_PLAN covers the happy path well but has gaps in error recovery, race-condition safety, and conversion robustness. The architecture is sound — poll → download → convert → upload → delete → notify. Below are specific issues and concrete improvements, ordered by severity.

---

## 1. STATE FILE RACE CONDITIONS (CRITICAL)

### 1.1 Read-Modify-Write Is Not Atomic

```python
# Vulnerable pattern (what the plan implies):
state = json.load(open("state.json"))    # READ
state["processed"][file_id] = {...}       # MODIFY
json.dump(state, open("state.json", "w")) # WRITE — NOT ATOMIC
```

**Risk:** Two overlapping cron runs (e.g., previous run runs long past its 2-hour window) could:
- Read the same state, both process the same file, both delete it (second delete is a 404 — benign). More importantly, they could clobber each other's writes, losing processed-file records.

**Fix:** Three layers of protection:

```python
# Layer 1: PID file prevents concurrent runs
PID_FILE = Path("/tmp/drive-dify-pipeline.pid")

def acquire_lock() -> bool:
    try:
        fd = os.open(PID_FILE, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
        os.write(fd, str(os.getpid()).encode())
        return True
    except FileExistsError:
        # Check if the old PID is still alive
        old_pid = int(open(PID_FILE).read().strip())
        try:
            os.kill(old_pid, 0)  # Signal 0 = probe only
            return False          # Still running
        except OSError:
            # Stale lock — clean up and retry
            os.remove(PID_FILE)
            return acquire_lock()

# Layer 2: Atomic state writes (write to .tmp, then rename)
def save_state(state, path):
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(state, f)
    tmp.replace(path)  # atomic on same filesystem

# Layer 3: Use fcntl.flock for additional safety
import fcntl
with open("state.json", "r+") as f:
    fcntl.flock(f, fcntl.LOCK_EX)
    state = json.load(f)
    # ... modify ...
    f.seek(0)
    f.truncate()
    json.dump(state, f)
    fcntl.flock(f, fcntl.LOCK_UN)
```

### 1.2 Failed Files Are Permanently Stuck

The plan logs failed files in `state.json["failed"]` but never retries them. A transient error (network blip) becomes permanent.

**Fix:** Add a retry policy:

```json
{
  "failed": {
    "file-id-3": {
      "name": "corrupted.pdf",
      "error": "Conversion failed",
      "time": "2026-07-03T10:30:00",
      "retries": 1,
      "max_retries": 3
    }
  }
}
```

On each run, re-attempt files where `retries < max_retries`. After max_retries, move to a `permanently_failed` bucket and notify Marc Sir.

---

## 2. DEPENDENCY & EDGE CASE GAPS

### 2.1 Missing Dependencies

| What's Missing | Why It Matters |
|---|---|
| `fcntl` / file locking | Race condition prevention (see §1) |
| `tenacity` or custom retry decorator | Production-grade retry with jitter |
| `python-magic` / `mimetypes` | Reliable MIME type detection (file extensions are unreliable) |
| `pathvalidate` | Sanitize filenames (3rd parties may upload files with `../`, null bytes, etc.) |

### 2.2 Google-Native Format Handling

The plan only handles `.pdf`, `.epub`, `.docx`, `.txt`, `.md`. But Google Drive shared folders often contain Google Docs, Sheets, and Slides — these have no file extension and appear with `mimeType: application/vnd.google-apps.*`.

**Fix:** Add export support for Google formats:

```python
GOOGLE_MIME_EXPORT_MAP = {
    "application/vnd.google-apps.document": ("application/pdf", ".pdf"),
    "application/vnd.google-apps.spreadsheet": (
        "text/csv", ".csv"
    ),
    "application/vnd.google-apps.presentation": (
        "application/pdf", ".pdf"
    ),
}

# Use files().export_media() instead of files().get_media()
if mime_type in GOOGLE_MIME_EXPORT_MAP:
    export_mime, ext = GOOGLE_MIME_EXPORT_MAP[mime_type]
    request = service.files().export_media(
        fileId=file_id, mimeType=export_mime
    )
else:
    request = service.files().get_media(fileId=file_id)
```

### 2.3 Legacy .doc Format

`markitdown` supports `.docx` but **not** `.doc` (binary format). Many third parties may still use `.doc`.

**Fix:** Add `antiword` or `catdoc` as a fallback for `.doc`:

```bash
# Install
sudo apt-get install antiword catdoc

# In pipeline:
if ext == ".doc":
    text = subprocess.check_output(["antiword", str(filepath)]).decode()
```

### 2.4 ePub DRM

If a third party uploads a DRM-protected ePub, `markitdown` (which uses `ebooklib` under the hood) will fail silently or return garbled text.

**Fix:** Catch the specific exception and log it with a clear message. Consider detecting DRM with a pre-check:

```python
import zipfile
def is_drm_epub(path):
    try:
        with zipfile.ZipFile(path) as zf:
            return "META-INF/encryption.xml" in zf.namelist()
    except zipfile.BadZipFile:
        return False
```

### 2.5 Encrypted PDFs

PyMuPDF will fail on password-protected PDFs.

**Fix:** Check `doc.needs_pass` and skip with a clear error message.

---

## 3. PDF/EPUB CONVERSION — BETTER APPROACHES

### 3.1 Current Plan Uses PyMuPDF + markitdown

PyMuPDF (`fitz`) is fast and handles text-layer PDFs well. `markitdown` is okay for DOCX/ePub but its conversion quality is "good enough" not great.

### 3.2 Recommended Two-Stage Approach

```
Stage 1: Extract raw text (fast, deterministic)
  - PDF: PyMuPDF / pdfplumber / marker-pdf
  - ePub: ebooklib → HTML → html2text
  - DOCX: python-docx → markdownify

Stage 2: LLM cleanup (optional, quality gate)
  - Send raw text to a cheap LLM (GPT-4o-mini, Claude Haiku)
  - Prompt: "Clean this extracted text: fix OCR artifacts, merge broken
    paragraphs, preserve tables as markdown, remove headers/footers"
  - Cost: ~$0.01 per 10 pages
```

### 3.3 Specific Recommendations

| Format | Current | Better Alternative | Why |
|---|---|---|---|
| Text-layer PDF | PyMuPDF | **PyMuPDF** (keep) | Fast, good enough |
| Scanned PDF | PaddleOCR | **Tesseract + pdf2image** OR **marker-pdf** | PaddleOCR is 1GB+; Tesseract is 50MB and adequate for clean scans |
| ePub | markitdown | **ebooklib + html2text** | More control over heading levels, image alt text |
| DOCX | markitdown | **python-docx + markdownify** | Better table handling, style mapping |
| Complex PDFs | — | **marker-pdf** (open source) | Preserves layout, tables, equations |

### 3.4 Note on PaddleOCR

The plan mentions PaddleOCR as a fallback for scanned PDFs. PaddleOCR requires:
- `paddlepaddle` (~400MB)
- `paddleocr` (~200MB)
- Model downloads on first use (~200MB)

This is **too heavy for a cron job on a small VPS**. Use `tesseract-ocr` instead:

```bash
sudo apt-get install tesseract-ocr tesseract-ocr-eng
pip install pytesseract pdf2image
```

---

## 4. RATE LIMITING & QUOTA IMPROVEMENTS

### 4.1 Google Drive API Quotas

| Limit | Value | Impact |
|---|---|---|
| Queries/day/user | 10,000,000 | Not an issue for 2hr polling |
| Queries/100sec/user | 10,000 | Not an issue |
| **Queries/100sec/project** | **10,000** | Shared across all project users |
| File downloads | Unlimited (within reason) | Not documented, but ~10TB/day practical |

The real bottleneck is the **per-user rate limit**: 1,000 queries per 100 seconds per user. While unlikely to hit this, it's good practice to:

```python
import time

class RateLimiter:
    def __init__(self, max_calls=900, period=100):
        self.max_calls = max_calls
        self.period = period
        self.calls = []

    def wait_if_needed(self):
        now = time.time()
        self.calls = [t for t in self.calls if now - t < self.period]
        if len(self.calls) >= self.max_calls:
            sleep_time = self.calls[0] + self.period - now + 1
            time.sleep(max(0, sleep_time))
        self.calls.append(now)
```

### 4.2 Dify API Rate Limits

The plan mentions `exponential backoff, max 3 retries` for 429s but doesn't specify **which** Dify API endpoints have rate limits. The free tier likely has:
- 1,000 documents per knowledge base
- Upload throughput limits (undocumented)

**Improvement:** Add a pre-flight check at startup:

```python
# Check remaining capacity before processing
resp = requests.get(
    f"{DIFY_BASE}/datasets/{dataset_id}",
    headers={"Authorization": f"Bearer {API_KEY}"}
)
doc_count = resp.json().get("document_count", 0)
if doc_count > 950:  # 95% full
    notify("⚠️ Dify KB approaching document limit ({doc_count}/1000)")
```

### 4.3 Exponential Backoff Needs Jitter

Without jitter, multiple retries can synchronize and cause thundering herd:

```python
# BAD (from plan):
time.sleep(2 ** attempt)  # 2s, 4s, 8s — deterministic

# GOOD:
import random
time.sleep((2 ** attempt) + random.uniform(0, 1))  # 2-3s, 4-5s, 8-9s
```

Use `tenacity` library for production retry:

```python
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=60, jitter=2),
    retry=lambda e: isinstance(e, (RateLimitError, ConnectionError)),
)
def upload_to_dify(text, name):
    ...
```

---

## 5. ERROR RECOVERY SCENARIOS

### 5.1 Crash Mid-Processing: The "Gotcha" State

The plan flow is:
```
download → convert → upload → delete → update state
```

**Scenario A:** Crash after upload, before delete
- Source file still in Drive → re-processed next run → **duplicate in Dify KB**

**Scenario B:** Crash after delete, before state update
- File gone from Drive, not in state → next run tries to list it → 404 → harmless, but state is inconsistent

**Scenario C:** Crash mid-upload (partial chunks)
- Dify may have ingested partial content
- File not deleted (good) → will re-process → **duplicate partial content**

### 5.2 Recommended Fix: Transaction Journal

```python
JOURNAL_PATH = Path("journal.jsonl")

def journal(phase, file_id, status, detail=""):
    """Append-only transaction log."""
    entry = {
        "ts": datetime.utcnow().isoformat(),
        "phase": phase,   # "downloaded", "converted", "uploading", "uploaded",
                          # "deleting", "deleted", "state_updated"
        "file_id": file_id,
        "status": status, # "ok", "error"
        "detail": detail,
    }
    with open(JOURNAL_PATH, "a") as f:
        f.write(json.dumps(entry) + "\n")
```

On startup, replay the journal to determine each file's actual state — don't trust `state.json` alone.

### 5.3 Idempotency for Dify Uploads

Dify's `create-by-text` creates a **new document every time** — it's not idempotent. If a file is re-processed, you get a duplicate.

**Fix:** Use the source file's MD5 hash or Drive file ID as a unique name prefix:

```python
doc_name = f"[{file_id[:8]}] {original_filename}"
```

Then check before uploading:

```python
existing = dify_list_documents(dataset_id)
if any(doc_name in d["name"] for d in existing):
    skip("Already in Dify KB")
```

### 5.4 Token Refresh Mid-Run

If a pipeline run takes long (processing 20+ files), the access token (valid for 1 hour) may expire mid-run. The Google client library handles refresh automatically **if** the credentials object contains a refresh token.

**The plan handles this correctly** — `auth_google.py` saves a refresh token, and `google-auth` auto-refreshes. No action needed, just noting it's covered.

### 5.5 Disk Full on /tmp

The plan downloads to `/tmp/drive-dify-pipeline/`. If `/tmp` fills up:

**Fix:** Add a disk space check:

```python
import shutil

def check_disk_space(path, required_mb=500):
    stat = shutil.disk_usage(path)
    free_mb = stat.free / (1024 * 1024)
    if free_mb < required_mb:
        raise RuntimeError(
            f"Insufficient disk space: {free_mb:.0f}MB free, "
            f"{required_mb}MB required"
        )
```

Also: clean up `/tmp/drive-dify-pipeline/` on every run (start + exit with `atexit`).

### 5.6 File Deleted Between List and Download

Third party could delete a file after the listing but before the download:

```python
try:
    request = service.files().get_media(fileId=file_id)
    ...
except HttpError as e:
    if e.resp.status == 404:
        log(f"File {file_id} vanished — skipping")
        continue
    raise
```

---

## 6. CONFIG.YAML INCONSISTENCY

The `config.yaml` template in the plan has empty `dataset_id` and `api_key`:

```yaml
dify:
  dataset_id: ""  # TODO: Fill in from KB URL
  api_key: ""     # TODO: Fill in dataset- key
```

But the "Configuration Values to Collect" table at the bottom shows these values are already **confirmed**:
- `dataset_id`: `51610c8d-79c7-41fb-bb7e-1af3b120a850`
- `api_key`: `dataset-PC1cMX9XVkD9q2eZb9VTNTeQ`

**Fix:** When building `config.yaml`, pre-fill these with the confirmed values.

---

## 7. LOGGING IMPROVEMENTS

### 7.1 Structured Logging

Plain `print()` output is consumed by cron → Telegram. But for debugging, structured logs are better:

```python
import logging
import logging.handlers

logger = logging.getLogger("drive-dify")
logger.setLevel(logging.INFO)

# Rotation: keep last 30 days
handler = logging.handlers.TimedRotatingFileHandler(
    "logs/pipeline.log", when="midnight", backupCount=30
)
handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s"
))
logger.addHandler(handler)
```

### 7.2 Telegram Summary Format

The plan says "Print summary to stdout (captured by cron → Telegram)" but doesn't specify the format. Suggest:

```
📥 Drive→Dify Pipeline — 2026-07-03 14:00 UTC
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ Ingested: 3
⏭️  Skipped:  2 (already processed)
❌ Failed:   1 (conversion error)
📊 KB total: 47 documents
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
❌ hvac-old.doc — legacy .doc format (use antiword)
```

### 7.3 Silent Failures

The plan doesn't specify what happens if the cron job fails to start (Python not found, virtualenv missing, etc.). The cron wrapper should capture stderr too:

```bash
#!/bin/bash
set -euo pipefail
exec 2>&1  # Merge stderr into stdout
cd ~/workspace/drive-dify-pipeline
python3 pipeline.py --config config.yaml
```

---

## 8. ADDITIONAL EDGE CASES

### 8.1 Very Large Files

Dify has an undocumented upload size limit (likely 15MB per document). For large PDFs (>100 pages), chunking is needed:

```python
MAX_CHUNK_CHARS = 8000

def chunk_text(text, max_chars=MAX_CHUNK_CHARS):
    """Split on paragraph boundaries, respecting max chars."""
    paragraphs = text.split("\n\n")
    chunks = []
    current = ""
    for p in paragraphs:
        if len(current) + len(p) > max_chars and current:
            chunks.append(current.strip())
            current = p
        else:
            current += "\n\n" + p if current else p
    if current:
        chunks.append(current.strip())
    return chunks

# Upload as multiple documents with suffix
for i, chunk in enumerate(chunk_text(full_text)):
    dify_upload(name=f"{filename} (part {i+1})", text=chunk)
```

### 8.2 Filename Sanitization

Third parties may upload files with special characters, null bytes, or path traversal:

```python
from pathvalidate import sanitize_filename

safe_name = sanitize_filename(original_name, replacement_text="_")
# "hvac/../../etc/passwd.pdf" → "hvac....etc_passwd.pdf"
```

### 8.3 Duplicate Filenames (Different Files)

Two different files with the same name are different Drive objects (different IDs). The state file correctly tracks by ID, not name. But the Dify upload name should disambiguate:

```python
dify_name = f"{safe_name} [{file_id[:8]}]"
```

### 8.4 Cron Overlap with DST

At daylight saving time transitions, cron may skip or double-run. The PID file lock (§1.1) handles the double-run case.

---

## 9. PRIORITY ACTION ITEMS

| Priority | Item | Effort |
|---|---|---|
| 🔴 P0 | Add PID file lock to prevent concurrent runs | 30 min |
| 🔴 P0 | Atomic state file writes (tmp+rename) | 15 min |
| 🟠 P1 | Add retry policy for failed files (not permanent) | 1 hr |
| 🟠 P1 | Handle Google Docs/Sheets/Slides export | 1 hr |
| 🟠 P1 | Add transaction journal for crash recovery | 2 hr |
| 🟡 P2 | Replace PaddleOCR with Tesseract | 1 hr |
| 🟡 P2 | Chunk large files before Dify upload | 2 hr |
| 🟡 P2 | Add disk space check + tmp cleanup | 30 min |
| 🟢 P3 | Structured logging with rotation | 30 min |
| 🟢 P3 | Add Dify pre-flight capacity check | 30 min |
| 🟢 P3 | Filename sanitization | 15 min |

---

## 10. VERDICT

The plan is **shippable as v0.1** after addressing the P0 items (concurrency safety + atomic writes). The rest can be layered on in subsequent iterations. The architecture is sound — the issues are all at the implementation-detail level, which is exactly where a review like this should focus.

**Recommendation:** Proceed with Phase 1 (auth_google.py ✅ complete). For Phase 2 (pipeline.py), implement P0 + P1 items before cron deployment. P2/P3 can follow in v0.2.
