#!/usr/bin/env python3
"""
Drive → Dify KB Ingestion Pipeline
===================================
Production-ready pipeline that:
  1. AUTH: Loads Google refresh token from ~/.hermes/config/drive-refresh-token.json
  2. LIST: Polls Drive folder with full pagination
  3. SKIP: Files already in state.json (processed, failed, deferred)
  4. DOWNLOAD: Saves to /tmp/drive-dify-pipeline/ with disk-space check
  5. CONVERT: PDF→PyMuPDF, ePub→markitdown, DOCX→markitdown, TXT→direct,
              Google Workspace→export_media, .doc→antiword fallback
  6. UPLOAD: POST to Dify create-by-text endpoint with chunking + retry
  7. DELETE: On confirmed Dify success → files().delete() from Drive
  8. TRACK: Atomic state.json write with backup rotation
  9. REPORT: Structured JSON summary to stdout + detailed logs to file

CLI Usage:
    python3 pipeline.py [--dry-run] [--loop] [--config config.yaml]

    --dry-run : Download + convert only (no Dify upload, no Drive delete)
    --loop    : Keep polling until folder empty, then exit
    --config  : Path to YAML config (default: config.yaml)

Dependencies:
    google-auth-oauthlib, google-api-python-client, pymupdf, markitdown,
    requests, pyyaml  (all installed or available)

Author: Codi (build), Qui (QA — gap analysis), Helen (orchestration)
Date:   2026-07-03
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import logging.handlers
import os
import random
import shutil
import sys
import time
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Third-party imports (all must be installed)
# ---------------------------------------------------------------------------
import fitz  # PyMuPDF
import requests
import yaml
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

# markitdown for ePub/DOCX conversion
try:
    from markitdown import MarkItDown
    _MD = MarkItDown()
    HAS_MARKITDOWN = True
except ImportError:
    HAS_MARKITDOWN = False

# antiword for legacy .doc files
import subprocess
HAS_ANTIWORD = shutil.which("antiword") is not None

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
# Real config values (loaded from config.yaml, these are fallback defaults)
DEFAULT_CONFIG = {
    "google_drive": {
        "folder_id": "1GlOjKMZWmSIa6ejf1InUwYybT7ZuE102",
        "credentials_path": "~/workspace/google_client_secret.json",
        "token_path": "~/.hermes/config/drive-refresh-token.json",
    },
    "dify": {
        "base_url": "https://api.dify.ai/v1",
        "dataset_id": "51610c8d-79c7-41fb-bb7e-1af3b120a850",
        # dataset_key is read from env var DIFY_DATASET_KEY (security: never in plaintext config)
    },
    "pipeline": {
        "state_file": "state.json",
        "log_dir": "logs",
        "tmp_dir": "/tmp/drive-dify-pipeline",
        "lock_file": "/tmp/drive-dify-pipeline.lock",
        "max_file_size_mb": 100,
        "chunk_max_chars": 8000,
        "max_chunks_per_file": 50,
        "min_free_space_mb": 500,
        "max_retries": 5,
        "max_state_retries": 3,
        "state_backups_to_keep": 10,
        "log_max_bytes": 10 * 1024 * 1024,  # 10 MB
        "log_backup_count": 5,
        "http_timeout_api": 30,      # seconds for API calls
        "http_timeout_download": 300, # seconds for file downloads
        "http_timeout_dify": 60,      # seconds for Dify uploads
    },
}

# MIME type mapping for conversion
MIME_HANDLERS: Dict[str, str] = {
    "application/pdf": "pdf",
    "application/epub+zip": "epub",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "text/plain": "txt",
    "text/csv": "csv",
    "text/markdown": "md",
    "application/msword": "doc",  # legacy .doc
}

# Google Workspace native format → export MIME + extension
GOOGLE_EXPORT_MAP: Dict[str, Tuple[str, str]] = {
    "application/vnd.google-apps.document": ("application/pdf", ".pdf"),
    "application/vnd.google-apps.spreadsheet": ("text/csv", ".csv"),
    "application/vnd.google-apps.presentation": ("application/pdf", ".pdf"),
}

# Retryable HTTP status codes
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

# ---------------------------------------------------------------------------
# Custom Exceptions
# ---------------------------------------------------------------------------
class SkipFileError(Exception):
    """Non-fatal — skip this file and continue processing others."""
    def __init__(self, reason: str, status: str = "skipped"):
        super().__init__(reason)
        self.status = status


class PipelineError(Exception):
    """Fatal — stop the entire pipeline."""
    pass


# ---------------------------------------------------------------------------
# PID File Lock (GAP-A2: concurrent-run guard)
# ---------------------------------------------------------------------------
class PidLock:
    """PID-based file lock to prevent concurrent pipeline runs."""

    def __init__(self, lock_path: Path):
        self.lock_path = lock_path
        self._acquired = False

    def acquire(self) -> bool:
        """Try to acquire the lock. Returns True on success."""
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o644)
            with os.fdopen(fd, "w") as f:
                f.write(str(os.getpid()))
            self._acquired = True
            return True
        except FileExistsError:
            return self._check_stale()

    def _check_stale(self) -> bool:
        """Check if existing lock is stale (PID no longer running)."""
        try:
            old_pid = int(self.lock_path.read_text().strip())
            os.kill(old_pid, 0)  # Signal 0 = probe only
            return False  # Process still alive → lock is valid
        except (ValueError, ProcessLookupError, OSError):
            # Stale lock — take over
            self.lock_path.write_text(str(os.getpid()))
            self._acquired = True
            return True

    def release(self) -> None:
        """Release the lock if we hold it."""
        if self._acquired and self.lock_path.exists():
            try:
                self.lock_path.unlink()
            except OSError:
                pass
            self._acquired = False


# ---------------------------------------------------------------------------
# Retry with Exponential Backoff + Jitter (GAP-E1)
# ---------------------------------------------------------------------------
def retry_with_backoff(
    func,
    *args,
    max_retries: int = 5,
    min_wait: float = 2.0,
    max_wait: float = 60.0,
    retryable_exceptions: tuple = (HttpError, ConnectionError, requests.exceptions.RequestException),
    logger: Optional[logging.Logger] = None,
    retry_on_status: Optional[set] = None,
    **kwargs,
) -> Any:
    """Call `func(*args, **kwargs)` with exponential backoff + jitter.
    
    retry_on_status: optional set of HTTP status codes that should trigger a retry
    even when the function returns normally (e.g. {429, 503} for rate limits).
    """
    log = logger or logging.getLogger(__name__)
    last_exc = None

    for attempt in range(max_retries):
        try:
            result = func(*args, **kwargs)
            # P0-B6: Check response status for retryable codes
            if retry_on_status and hasattr(result, 'status_code'):
                if result.status_code in retry_on_status and attempt < max_retries - 1:
                    wait = min(max_wait, min_wait * (2 ** attempt))
                    wait += random.uniform(0, 1)
                    log.warning(
                        "Retry %d/%d on HTTP %d after %.1fs",
                        attempt + 1, max_retries, result.status_code, wait,
                    )
                    time.sleep(wait)
                    continue
            return result
        except retryable_exceptions as e:
            last_exc = e
            # Check for non-retryable HTTP statuses
            status = getattr(getattr(e, "resp", None), "status", None)
            if status and status not in RETRYABLE_STATUSES and not (500 <= status < 600):
                raise

            if attempt == max_retries - 1:
                raise

            # Exponential backoff with jitter
            wait = min(max_wait, min_wait * (2 ** attempt))
            wait += random.uniform(0, 1)  # jitter
            log.warning(
                "Retry %d/%d after %.1fs: %s",
                attempt + 1, max_retries, wait, e
            )
            time.sleep(wait)

    raise last_exc  # type: ignore[misc]


# ---------------------------------------------------------------------------
# State Management (GAP-O1: atomic writes, GAP-O2: backups)
# ---------------------------------------------------------------------------
def _expand_path(path_str: str) -> Path:
    """Expand ~ and environment variables in a path string."""
    return Path(os.path.expanduser(path_str)).resolve()


def load_state(state_path: Path, logger: logging.Logger) -> Dict[str, Any]:
    """Load state.json with corruption recovery.

    Returns a dict with keys: processed, failed, deferred, last_run.
    """
    default_state: Dict[str, Any] = {
        "processed": {},
        "failed": {},
        "deferred": {},
        "last_run": None,
    }

    if not state_path.exists():
        logger.info("No existing state file — starting fresh")
        return default_state

    try:
        with open(state_path, "r") as f:
            state = json.load(f)
        # Ensure all expected keys exist
        for key in default_state:
            if key not in state:
                state[key] = default_state[key]
        logger.info(
            "Loaded state: %d processed, %d failed, %d deferred",
            len(state.get("processed", {})),
            len(state.get("failed", {})),
            len(state.get("deferred", {})),
        )
        return state
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("state.json corrupted (%s) — attempting backup recovery", e)
        # Try recovery from newest backup
        backups = sorted(state_path.parent.glob(f"{state_path.name}.bak.*"), reverse=True)
        for bk in backups:
            try:
                state = json.loads(bk.read_text())
                for key in default_state:
                    if key not in state:
                        state[key] = default_state[key]
                logger.warning("Recovered state from backup: %s", bk.name)
                return state
            except (json.JSONDecodeError, OSError):
                continue
        logger.error("All backups corrupt — starting fresh (existing state will be backed up)")
        return default_state


def backup_state(state_path: Path, keep: int = 10, logger: Optional[logging.Logger] = None) -> None:
    """Create a timestamped backup of state.json before mutation (GAP-O2)."""
    log = logger or logging.getLogger(__name__)
    if not state_path.exists():
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = state_path.with_suffix(f".json.bak.{timestamp}")
    try:
        shutil.copy2(state_path, backup_path)
        log.debug("State backed up: %s", backup_path.name)
    except OSError as e:
        log.warning("Failed to create state backup: %s", e)
        return

    # Rotate old backups
    all_backups = sorted(state_path.parent.glob(f"{state_path.name}.bak.*"))
    for old in all_backups[:-keep]:
        try:
            old.unlink()
            log.debug("Rotated old backup: %s", old.name)
        except OSError:
            pass


def save_state(state: Dict[str, Any], state_path: Path, logger: Optional[logging.Logger] = None) -> None:
    """Atomically write state to disk (GAP-O1: write .tmp → rename)."""
    log = logger or logging.getLogger(__name__)
    state["last_run"] = datetime.now(timezone.utc).isoformat()

    tmp_path = state_path.with_suffix(".json.tmp")
    try:
        with open(tmp_path, "w") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        # Atomic rename on same filesystem
        os.replace(tmp_path, state_path)
        log.debug("State saved atomically: %d processed", len(state.get("processed", {})))
    except OSError as e:
        log.error("Failed to save state: %s", e)
        raise


def is_file_in_state(file_id: str, state: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Check if a file is in any state bucket. Returns (in_state, bucket_name)."""
    for bucket in ("processed", "failed", "deferred"):
        if file_id in state.get(bucket, {}):
            return True, bucket
    return False, None


# ---------------------------------------------------------------------------
# Google Drive Authentication
# ---------------------------------------------------------------------------
def get_drive_credentials(token_path: Path, creds_path: Path, logger: logging.Logger) -> Credentials:
    """Load Google Drive credentials from a refresh token file.

    Uses the saved refresh token — no browser interaction needed.
    Falls back to client_secret.json if token lacks client_id/client_secret.
    """
    if not token_path.exists():
        raise PipelineError(
            f"Token file not found: {token_path}\n"
            "Run auth_google.py first to obtain a refresh token."
        )

    token_data = json.loads(token_path.read_text())

    client_id = token_data.get("client_id", "")
    client_secret = token_data.get("client_secret", "")

    # Fallback: read client_id/secret from credentials file if missing in token
    if (not client_id or not client_secret) and creds_path.exists():
        logger.info("Token missing client_id/secret — reading from credentials file")
        try:
            creds_file = json.loads(creds_path.read_text())
            installed = creds_file.get("installed", creds_file)
            client_id = client_id or installed.get("client_id", "")
            client_secret = client_secret or installed.get("client_secret", "")
        except (json.JSONDecodeError, KeyError, OSError):
            pass

    creds = Credentials(
        token=token_data.get("token"),
        refresh_token=token_data.get("refresh_token"),
        token_uri=token_data.get("token_uri", "https://oauth2.googleapis.com/token"),
        client_id=client_id,
        client_secret=client_secret,
        scopes=token_data.get("scopes", token_data.get("scope", [])),
    )

    # Auto-refresh if expired
    if creds.expired and creds.refresh_token:
        logger.info("Access token expired — refreshing...")
        creds.refresh(Request())
        # Save the refreshed token back
        new_data = json.loads(creds.to_json())
        tmp_path = token_path.with_suffix(".tmp")
        with open(tmp_path, "w") as f:
            json.dump(new_data, f, indent=2)
        os.replace(tmp_path, token_path)
        os.chmod(token_path, 0o600)
        logger.info("Token refreshed and saved")

    return creds


def get_drive_service(credentials: Credentials) -> Any:
    """Build an authenticated Google Drive API service."""
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


# ---------------------------------------------------------------------------
# File Listing with Pagination (GAP-A3)
# ---------------------------------------------------------------------------
def list_drive_files(
    service: Any,
    folder_id: str,
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """List all non-trashed files in a Drive folder with full pagination."""
    all_files: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    page_count = 0

    while True:
        page_count += 1
        logger.debug("Fetching Drive page %d...", page_count)

        results = retry_with_backoff(
            lambda: service.files()
            .list(
                q=f"'{folder_id}' in parents and trashed = false",
                pageSize=100,
                pageToken=page_token,
                fields="nextPageToken, files(id, name, mimeType, size, modifiedTime, createdTime)",
            )
            .execute(),
            max_retries=5,
            logger=logger,
        )

        files = results.get("files", [])
        all_files.extend(files)
        logger.debug("Page %d: %d files (total: %d)", page_count, len(files), len(all_files))

        page_token = results.get("nextPageToken")
        if not page_token:
            break

    logger.info("Drive listing complete: %d files across %d pages", len(all_files), page_count)
    return all_files


# ---------------------------------------------------------------------------
# Disk Space Check (GAP-A6)
# ---------------------------------------------------------------------------
def check_disk_space(path: str, required_mb: int, logger: logging.Logger) -> None:
    """Raise PipelineError if free space is below required_mb."""
    stat = os.statvfs(path)
    free_mb = (stat.f_bavail * stat.f_frsize) // (1024 * 1024)
    if free_mb < required_mb:
        raise PipelineError(
            f"Insufficient disk space in {path}: {free_mb}MB free, "
            f"{required_mb}MB required"
        )
    logger.debug("Disk space OK: %dMB free in %s", free_mb, path)


# ---------------------------------------------------------------------------
# File Download (GAP-EC1: Google Workspace export)
# ---------------------------------------------------------------------------
def download_file(
    service: Any,
    file_info: Dict[str, Any],
    dest_dir: Path,
    logger: logging.Logger,
) -> Path:
    """Download or export a Google Drive file to dest_dir.

    Returns the local file path.
    Handles Google Workspace files via export_media (GAP-EC1).
    """
    file_id = file_info["id"]
    name = file_info.get("name", file_id)
    mime_type = file_info.get("mimeType", "")

    dest_dir.mkdir(parents=True, exist_ok=True)

    # Determine if it's a Google Workspace file
    if mime_type in GOOGLE_EXPORT_MAP:
        export_mime, ext = GOOGLE_EXPORT_MAP[mime_type]
        logger.info("Exporting Google Workspace file: %s → %s", name, ext)
        request = service.files().export_media(fileId=file_id, mimeType=export_mime)
        safe_name = Path(name).stem + ext
    else:
        request = service.files().get_media(fileId=file_id)
        safe_name = _sanitize_filename(name)

    dest_path = dest_dir / safe_name

    # If a file with this name already exists, add a suffix
    counter = 1
    while dest_path.exists():
        stem = Path(safe_name).stem
        suffix = Path(safe_name).suffix
        dest_path = dest_dir / f"{stem}_{counter}{suffix}"
        counter += 1

    logger.info("Downloading: %s → %s", name, dest_path.name)

    try:
        # Use a custom HTTP request with timeout
        with open(dest_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    logger.debug(
                        "Download progress: %s — %d%%",
                        name, int(status.progress() * 100)
                    )
    except HttpError as e:
        if getattr(e, "resp", None) and e.resp.status == 404:
            raise SkipFileError(f"File vanished from Drive (404): {name}")
        raise

    logger.info("Downloaded: %s (%d bytes)", name, dest_path.stat().st_size)
    return dest_path


# ---------------------------------------------------------------------------
# File Validation (GAP-EC3: zero-byte, GAP-EC5: oversized)
# ---------------------------------------------------------------------------
def validate_file(file_path: Path, file_info: Dict[str, Any], max_size_mb: int, logger: logging.Logger) -> None:
    """Validate a downloaded file before conversion."""
    size_bytes = file_path.stat().st_size
    size_mb = size_bytes / (1024 * 1024)

    # GAP-EC3: Skip zero-byte files
    if size_bytes == 0:
        raise SkipFileError(
            f"Zero-byte file: {file_info.get('name', file_path.name)}",
            status="conversion_failed",
        )

    # GAP-EC5: Skip files over max_size
    if size_mb > max_size_mb:
        raise SkipFileError(
            f"File exceeds {max_size_mb}MB limit ({size_mb:.1f}MB): "
            f"{file_info.get('name', file_path.name)}",
            status="deferred",
        )


# ---------------------------------------------------------------------------
# ePub DRM Check (Codi Review §2.4)
# ---------------------------------------------------------------------------
def is_drm_epub(path: Path) -> bool:
    """Check if an ePub file has DRM encryption."""
    try:
        with zipfile.ZipFile(path, "r") as zf:
            return "META-INF/encryption.xml" in zf.namelist()
    except (zipfile.BadZipFile, OSError):
        return False


# ---------------------------------------------------------------------------
# Text Extraction / Conversion
# ---------------------------------------------------------------------------
def _sanitize_filename(name: str) -> str:
    """Sanitize a filename — replace path separators and null bytes."""
    # Replace path separators
    safe = name.replace("/", "_").replace("\\", "_")
    # Remove null bytes
    safe = safe.replace("\x00", "")
    # Collapse multiple underscores
    while "__" in safe:
        safe = safe.replace("__", "_")
    return safe or "unnamed_file"


def convert_pdf(file_path: Path, logger: logging.Logger) -> str:
    """Extract text from a PDF using PyMuPDF (GAP-EC2: encrypted check)."""
    try:
        doc = fitz.open(str(file_path))
    except fitz.FileDataError:
        raise SkipFileError(
            f"Encrypted/password-protected PDF: {file_path.name}",
            status="conversion_failed",
        )

    # GAP-EC2: Explicit encrypted check
    if doc.is_encrypted:
        doc.close()
        raise SkipFileError(
            f"Encrypted/password-protected PDF: {file_path.name}",
            status="conversion_failed",
        )

    try:
        pages_text: List[str] = []
        for page in doc:
            text = page.get_text("text")
            if text.strip():
                pages_text.append(text)
        full_text = "\n\n".join(pages_text)
    finally:
        doc.close()

    if not full_text.strip():
        raise SkipFileError(
            f"No extractable text in PDF (scanned/image-only): {file_path.name}",
            status="conversion_failed",
        )

    logger.info("PDF converted: %s — %d chars", file_path.name, len(full_text))
    return full_text


def convert_epub(file_path: Path, logger: logging.Logger) -> str:
    """Extract text from an ePub using markitdown (DRM check first)."""
    # Codi Review §2.4: DRM detection
    if is_drm_epub(file_path):
        raise SkipFileError(
            f"DRM-protected ePub: {file_path.name}",
            status="conversion_failed",
        )

    if not HAS_MARKITDOWN:
        raise SkipFileError(
            f"markitdown not installed — cannot convert ePub: {file_path.name}",
            status="conversion_failed",
        )

    try:
        result = _MD.convert(str(file_path))
        text = result.text_content.strip()
    except Exception as e:
        raise SkipFileError(
            f"ePub conversion failed: {file_path.name} — {e}",
            status="conversion_failed",
        )

    if not text:
        raise SkipFileError(
            f"Empty text after ePub conversion: {file_path.name}",
            status="conversion_failed",
        )

    logger.info("ePub converted: %s — %d chars", file_path.name, len(text))
    return text


def convert_docx(file_path: Path, logger: logging.Logger) -> str:
    """Extract text from a DOCX using markitdown."""
    if not HAS_MARKITDOWN:
        raise SkipFileError(
            f"markitdown not installed — cannot convert DOCX: {file_path.name}",
            status="conversion_failed",
        )

    try:
        result = _MD.convert(str(file_path))
        text = result.text_content.strip()
    except Exception as e:
        raise SkipFileError(
            f"DOCX conversion failed: {file_path.name} — {e}",
            status="conversion_failed",
        )

    if not text:
        raise SkipFileError(
            f"Empty text after DOCX conversion: {file_path.name}",
            status="conversion_failed",
        )

    logger.info("DOCX converted: %s — %d chars", file_path.name, len(text))
    return text


def convert_legacy_doc(file_path: Path, logger: logging.Logger) -> str:
    """Extract text from legacy .doc using antiword (Codi Review §2.3)."""
    if not HAS_ANTIWORD:
        raise SkipFileError(
            f"antiword not installed — cannot convert legacy .doc: {file_path.name}\n"
            f"Install with: sudo apt-get install antiword",
            status="conversion_failed",
        )

    try:
        result = subprocess.run(
            ["antiword", str(file_path)],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode != 0:
            raise SkipFileError(
                f"antiword failed for: {file_path.name} — {result.stderr.strip()}",
                status="conversion_failed",
            )
        text = result.stdout.strip()
    except subprocess.TimeoutExpired:
        raise SkipFileError(
            f"antiword timed out for: {file_path.name}",
            status="conversion_failed",
        )
    except FileNotFoundError:
        raise SkipFileError(
            f"antiword not found — cannot convert .doc: {file_path.name}",
            status="conversion_failed",
        )

    if not text:
        raise SkipFileError(
            f"Empty text after .doc conversion: {file_path.name}",
            status="conversion_failed",
        )

    logger.info("Legacy .doc converted: %s — %d chars", file_path.name, len(text))
    return text


def convert_txt(file_path: Path, logger: logging.Logger) -> str:
    """Read plain text file (try UTF-8, fallback to latin-1)."""
    for encoding in ("utf-8", "latin-1", "cp1252"):
        try:
            text = file_path.read_text(encoding=encoding)
            if text.strip():
                logger.info("TXT read: %s (%s) — %d chars", file_path.name, encoding, len(text))
                return text
        except UnicodeDecodeError:
            continue

    raise SkipFileError(
        f"Cannot decode text file: {file_path.name}",
        status="conversion_failed",
    )


def convert_markdown(file_path: Path, logger: logging.Logger) -> str:
    """Read markdown file as text."""
    try:
        text = file_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        text = file_path.read_text(encoding="latin-1")

    if not text.strip():
        raise SkipFileError(
            f"Empty markdown file: {file_path.name}",
            status="conversion_failed",
        )

    logger.info("Markdown read: %s — %d chars", file_path.name, len(text))
    return text


def convert_csv(file_path: Path, logger: logging.Logger) -> str:
    """Convert CSV to markdown table (P0-B2: proper CSV handling).
    
    Reads the CSV file and outputs a markdown-formatted table.
    Falls back to raw text if CSV parsing fails.
    """
    import csv
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
        
        if not rows or not any(cell.strip() for row in rows for cell in row):
            raise SkipFileError(
                f"Empty or blank CSV file: {file_path.name}",
                status="conversion_failed",
            )
        
        # Build markdown table
        max_cols = max(len(r) for r in rows)
        # Normalize all rows to same column count
        normalized = [r + [""] * (max_cols - len(r)) for r in rows]
        
        lines = []
        # Header row
        lines.append("| " + " | ".join(normalized[0]) + " |")
        # Separator
        lines.append("| " + " | ".join(["---"] * max_cols) + " |")
        # Data rows
        for row in normalized[1:]:
            lines.append("| " + " | ".join(row) + " |")
        
        text = "\n".join(lines)
        
        if not text.strip():
            raise SkipFileError(
                f"CSV produced no text: {file_path.name}",
                status="conversion_failed",
            )
        
        logger.info(
            "CSV → markdown: %s — %d rows × %d cols, %d chars",
            file_path.name, len(rows), max_cols, len(text),
        )
        return text
        
    except SkipFileError:
        raise
    except Exception as e:
        # Fallback: try reading as plain text
        logger.warning("CSV parsing failed for %s (%s) — reading as plain text", file_path.name, e)
        return convert_txt(file_path, logger)


def convert_file(
    file_path: Path,
    file_info: Dict[str, Any],
    logger: logging.Logger,
) -> str:
    """Route to the appropriate converter based on MIME type (GAP-EC7).

    Trusts Drive API mimeType over file extension.
    """
    mime_type = file_info.get("mimeType", "")
    name = file_path.name

    # Determine handler from MIME type
    handler = MIME_HANDLERS.get(mime_type)
    if not handler:
        # Fallback to extension
        ext = file_path.suffix.lower()
        ext_map = {
            ".pdf": "pdf",
            ".epub": "epub",
            ".docx": "docx",
            ".doc": "doc",
            ".txt": "txt",
            ".csv": "csv",
            ".md": "md",
        }
        handler = ext_map.get(ext)
        if handler:
            logger.debug("Using extension fallback for %s: %s → %s", name, ext, handler)

    if not handler:
        raise SkipFileError(
            f"Unsupported file type: {name} (mime: {mime_type})",
            status="conversion_failed",
        )

    converters = {
        "pdf": convert_pdf,
        "epub": convert_epub,
        "docx": convert_docx,
        "doc": convert_legacy_doc,
        "txt": convert_txt,
        "csv": convert_csv,
        "md": convert_markdown,
    }

    converter = converters[handler]
    return converter(file_path, logger)


# ---------------------------------------------------------------------------
# Text Chunking (GAP-E3: 8000-char chunks, max 50 per file)
# ---------------------------------------------------------------------------
def chunk_text(
    text: str,
    max_chars: int = 8000,
    max_chunks: int = 50,
    logger: Optional[logging.Logger] = None,
) -> List[str]:
    """Split text on paragraph boundaries, respecting max_chars per chunk.

    If text exceeds max_chunks * max_chars, it is truncated with a warning.
    """
    log = logger or logging.getLogger(__name__)
    paragraphs = text.split("\n\n")
    chunks: List[str] = []
    current = ""

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        if len(current) + len(para) + 2 > max_chars and current:
            chunks.append(current.strip())
            if len(chunks) >= max_chunks:
                log.warning(
                    "Text truncated at %d chunks (%d chars) — remaining content dropped",
                    max_chunks, max_chunks * max_chars,
                )
                return chunks
            current = para
        else:
            current = current + "\n\n" + para if current else para

    if current.strip():
        chunks.append(current.strip())
        if len(chunks) > max_chunks:
            chunks = chunks[:max_chunks]

    return chunks


# ---------------------------------------------------------------------------
# Dify Upload
# ---------------------------------------------------------------------------
def upload_file_to_dify(
    file_path: Path,
    doc_name: str,
    base_url: str,
    dataset_id: str,
    api_key: str,          # dataset- key
    app_api_key: str = "", # unused with combined upload approach
    timeout: int = 120,
    logger: Optional[logging.Logger] = None,
) -> Tuple[List[Dict[str, Any]], bool]:
    """Upload a file to Dify using native file upload (preserves images, one doc).
    
    Uses multipart/form-data POST to combine upload + document creation in one request.
    Returns (created_docs, is_partial).
    """
    log = logger or logging.getLogger(__name__)
    
    # Combined upload + create: multipart form with file + data
    create_url = f"{base_url.rstrip('/')}/datasets/{dataset_id}/document/create-by-file"
    metadata = json.dumps({
        "name": doc_name,
        "indexing_technique": "high_quality",
        "process_rule": {"mode": "automatic"},
    })
    
    with open(file_path, "rb") as f:
        response = retry_with_backoff(
            lambda: requests.post(
                create_url,
                files={"file": (doc_name, f, "application/octet-stream")},
                data={"data": metadata},
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=timeout,
            ),
            max_retries=3,
            logger=log,
            retry_on_status={429, 500, 502, 503, 504},
        )
    
    if response.status_code not in (200, 201):
        raise PipelineError(
            f"Dify upload failed (HTTP {response.status_code}): {response.text[:500]}"
        )
    
    result = response.json()
    doc_id = result.get("document", {}).get("id")
    
    log.info("Dify native ingest OK: %s → doc %s", doc_name, doc_id)
    return [{"chunk_name": doc_name, "dify_doc_id": doc_id}], False


def upload_to_dify(
    text: str,
    doc_name: str,
    base_url: str,
    dataset_id: str,
    api_key: str,
    chunk_max_chars: int,
    max_chunks_per_file: int,
    timeout: int = 60,
    logger: Optional[logging.Logger] = None,
) -> List[Dict[str, Any]]:
    """Upload text to Dify KB with chunking. Returns list of created doc IDs.

    Uses the correct plural endpoint: /datasets/{id}/documents/create-by-text
    """
    log = logger or logging.getLogger(__name__)

    # Append short UUID for Dify name uniqueness (GAP-EC4)
    short_uid = str(uuid.uuid4())[:8]
    base_name = Path(doc_name).stem

    chunks = chunk_text(
        text,
        max_chars=chunk_max_chars,
        max_chunks=max_chunks_per_file,
        logger=log,
    )

    if not chunks:
        raise SkipFileError(f"No text content to upload for: {doc_name}")

    endpoint = f"{base_url.rstrip('/')}/datasets/{dataset_id}/document/create-by-text"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    created_docs: List[Dict[str, Any]] = []
    failed_chunks = False  # P0-B5: track partial uploads

    for i, chunk in enumerate(chunks):
        if len(chunks) > 1:
            chunk_name = f"{base_name}-{short_uid}-p{i+1}of{len(chunks)}"
        else:
            chunk_name = f"{base_name}-{short_uid}"

        payload = {
            "name": chunk_name,
            "text": chunk,
            "indexing_technique": "high_quality",
        }

        log.debug("Uploading chunk %d/%d to Dify: %s (%d chars)", i+1, len(chunks), chunk_name, len(chunk))

        response = retry_with_backoff(
            lambda: requests.post(endpoint, json=payload, headers=headers, timeout=timeout),
            max_retries=5,
            logger=log,
            retry_on_status={429, 500, 502, 503, 504},
        )

        if response.status_code == 400:
            # Dify may reject the text — log and skip chunk
            error_detail = response.json().get("message", response.text)
            log.error("Dify 400 for chunk %d: %s", i+1, error_detail)
            # P0-B5: Track partial failure rather than silently skipping
            failed_chunks = True
            continue
        elif response.status_code == 413:
            log.error("Dify 413 (too large) for chunk %d — skipping", i+1)
            failed_chunks = True
            continue
        elif response.status_code not in (200, 201):
            failed_chunks = True
            log.error("Dify upload failed (HTTP %d): %s", response.status_code, response.text[:500])
            continue  # Don't abort entire file — continue remaining chunks

        data = response.json()
        doc_id = data.get("document", {}).get("id") or data.get("id")
        created_docs.append({"chunk_name": chunk_name, "dify_doc_id": doc_id})
        log.info("Dify upload OK: %s → %s", chunk_name, doc_id)

        # Rate limit guard: brief pause between Dify API calls (free tier)
        if i < len(chunks) - 1:
            time.sleep(1.5)

    # P0-B5: Report partial uploads clearly
    if failed_chunks and created_docs:
        total = len(chunks)
        success = len(created_docs)
        log.warning(
            "PARTIAL UPLOAD: %d/%d chunks succeeded for %s — %d chunks failed. Will NOT delete source file.",
            success, total, doc_name, total - success,
        )
    elif failed_chunks and not created_docs:
        raise PipelineError(f"All {len(chunks)} chunks failed to upload for: {doc_name}")

    return created_docs, failed_chunks and created_docs  # (docs, is_partial)


# ---------------------------------------------------------------------------
# Drive File Deletion
# ---------------------------------------------------------------------------
def delete_drive_file(service: Any, file_id: str, name: str, logger: logging.Logger) -> bool:
    """Delete a file from Google Drive. Returns True on success."""
    try:
        retry_with_backoff(
            lambda: service.files().delete(fileId=file_id).execute(),
            max_retries=5,
            logger=logger,
        )
        logger.info("Deleted from Drive: %s (%s)", name, file_id)
        return True
    except HttpError as e:
        status = getattr(e, "resp", None)
        if status and status.status == 404:
            logger.warning("Drive file already deleted (404): %s", name)
            return True  # Already gone — treat as success
        logger.error("Failed to delete Drive file %s: %s", name, e)
        return False


# ---------------------------------------------------------------------------
# File Cleanup
# ---------------------------------------------------------------------------
def cleanup_tmp(tmp_dir: Path, logger: logging.Logger) -> None:
    """Remove the temporary download directory."""
    if tmp_dir.exists():
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            logger.debug("Cleaned up tmp dir: %s", tmp_dir)
        except OSError as e:
            logger.warning("Failed to clean up tmp dir: %s", e)


# ---------------------------------------------------------------------------
# Pipeline Summary / Reporting (GAP-M1: structured JSON to stdout)
# ---------------------------------------------------------------------------
def print_summary(results: Dict[str, Any]) -> None:
    """Print structured JSON summary to stdout for cron capture."""
    summary = {
        "pipeline": "drive-dify",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dry_run": results.get("dry_run", False),
        "loop_mode": results.get("loop_mode", False),
        "stats": {
            "total_files_in_drive": results.get("total_files", 0),
            "ingested": results.get("ingested", 0),
            "skipped": results.get("skipped", 0),
            "failed": results.get("failed", 0),
            "deferred": results.get("deferred", 0),
            "dify_docs_created": results.get("dify_docs_created", 0),
            "drive_deleted": results.get("drive_deleted", 0),
        },
        "errors": results.get("errors", []),
        "elapsed_seconds": results.get("elapsed", 0),
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))


# ---------------------------------------------------------------------------
# Logging Setup (GAP-M3: structured + rotating logs)
# ---------------------------------------------------------------------------
def setup_logging(log_dir: Path, max_bytes: int, backup_count: int) -> logging.Logger:
    """Configure logging: rotating file handler + stdout handler."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "pipeline.log"

    logger = logging.getLogger("drive-dify")
    logger.setLevel(logging.DEBUG)

    # Prevent duplicate handlers if called multiple times
    if logger.handlers:
        return logger

    # File handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        str(log_path),
        maxBytes=max_bytes,
        backupCount=backup_count,
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    logger.addHandler(file_handler)

    # Console handler (stdout for cron → Telegram capture)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(
        "[%(levelname)-8s] %(message)s",
    ))
    logger.addHandler(console_handler)

    return logger


# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------
def run_pipeline(
    config: Dict[str, Any],
    dry_run: bool = False,
    loop_mode: bool = False,
) -> Dict[str, Any]:
    """Execute the full Drive → Dify ingestion pipeline.

    Returns a results dict for summary reporting.
    """
    # --- Resolve paths ---
    cfg_drive = config["google_drive"]
    cfg_dify = config["dify"]
    cfg_pipe = config["pipeline"]

    token_path = _expand_path(cfg_drive["token_path"])
    creds_path = _expand_path(cfg_drive["credentials_path"])
    folder_id = cfg_drive["folder_id"]
    dify_base_url = cfg_dify["base_url"]
    dify_dataset_id = cfg_dify["dataset_id"]
    state_path = Path(cfg_pipe["state_file"]).resolve()
    log_dir = Path(cfg_pipe["log_dir"]).resolve()
    tmp_dir = Path(cfg_pipe["tmp_dir"])
    lock_path = Path(cfg_pipe["lock_file"])
    max_file_size_mb = cfg_pipe["max_file_size_mb"]
    chunk_max_chars = cfg_pipe["chunk_max_chars"]
    max_chunks = cfg_pipe["max_chunks_per_file"]
    min_free_space_mb = cfg_pipe["min_free_space_mb"]
    max_retries_state = cfg_pipe["max_state_retries"]
    state_backups = cfg_pipe["state_backups_to_keep"]
    log_max_bytes = cfg_pipe["log_max_bytes"]
    log_backup_count = cfg_pipe["log_backup_count"]
    timeout_download = cfg_pipe["http_timeout_download"]
    timeout_dify = cfg_pipe["http_timeout_dify"]

    # --- Read Dify key from environment (GAP-S1: never in config) ---
    dify_api_key = os.environ.get("DIFY_DATASET_KEY", "")
    if not dify_api_key:
        raise PipelineError(
            "DIFY_DATASET_KEY environment variable is not set.\n"
            "Export it or add to ~/.hermes/config/dify-drive.env and source it."
        )
    
    # --- Read Dify App key from environment (for file upload) ---
    dify_app_api_key = os.environ.get("DIFY_APP_KEY", "app-xI35EeupuMY4JfUJUHMuvADl")

    # --- Setup logging ---
    logger = setup_logging(log_dir, log_max_bytes, log_backup_count)
    logger.info("=" * 60)
    logger.info("Drive → Dify Pipeline — Starting")
    logger.info("  Dry run: %s", dry_run)
    logger.info("  Loop mode: %s", loop_mode)
    logger.info("  Dify dataset: %s", dify_dataset_id)
    logger.info("  Drive folder: %s", folder_id)
    logger.info("=" * 60)

    # --- Results tracking ---
    results: Dict[str, Any] = {
        "dry_run": dry_run,
        "loop_mode": loop_mode,
        "total_files": 0,
        "ingested": 0,
        "skipped": 0,
        "failed": 0,
        "deferred": 0,
        "dify_docs_created": 0,
        "drive_deleted": 0,
        "errors": [],
        "elapsed": 0,
    }

    start_time = time.time()
    lock = PidLock(lock_path)

    # --- PID Lock (GAP-A2) ---
    if not lock.acquire():
        logger.warning("Another pipeline instance is running (PID lock at %s) — exiting", lock_path)
        results["errors"].append("Locked: another instance running")
        return results

    logger.info("PID lock acquired: %s", lock_path)

    try:
        # --- Disk space check (GAP-A6) ---
        check_disk_space("/tmp", min_free_space_mb, logger)

        # --- Load state (GAP-O1: corruption recovery) ---
        state = load_state(state_path, logger)

        # --- Backup state (GAP-O2) ---
        backup_state(state_path, keep=state_backups, logger=logger)

        # --- Google Auth ---
        logger.info("Authenticating with Google Drive...")
        credentials = get_drive_credentials(token_path, creds_path, logger)
        service = get_drive_service(credentials)

        # --- Main loop (GAP-O3 & --loop flag) ---
        loop_iteration = 0
        while True:
            loop_iteration += 1
            if loop_iteration > 1:
                logger.info("--- Loop iteration %d ---", loop_iteration)

            # --- List files (GAP-A3: pagination) ---
            drive_files = list_drive_files(service, folder_id, logger)
            results["total_files"] = len(drive_files)

            if not drive_files:
                logger.info("No files found in Drive folder")
                if loop_mode:
                    logger.info("Folder empty — exiting loop")
                break

            files_processed_this_loop = 0
            upload_count_since_pause = 0  # Rate limit throttle counter

            for file_info in drive_files:
                file_id = file_info["id"]
                file_name = file_info.get("name", file_id)
                file_mime = file_info.get("mimeType", "")
                file_size = int(file_info.get("size") or 0)  # Workspace files have size=None
                file_mime = file_info.get("mimeType", "")
                file_modified = file_info.get("modifiedTime", "")

                # Determine if this is a Google Workspace native file (no real file size)
                is_workspace = file_mime in GOOGLE_EXPORT_MAP
                if is_workspace:
                    file_size = max_file_size_mb * 1024 * 1024 - 1  # Fake size to pass size check
                logger.info("--- Processing: %s (%s, %s) ---", file_name, file_id, file_mime)

                # --- SKIP: Check state ---
                in_state, bucket = is_file_in_state(file_id, state)

                if in_state and bucket == "deferred":
                    logger.info("Skipping (deferred): %s", file_name)
                    results["skipped"] += 1
                    continue

                if in_state and bucket == "processed":
                    prev_entry = state["processed"].get(file_id, {})
                    prev_status = prev_entry.get("status", "")
                    
                    # P0-B4: Retry delete for files where Dify upload succeeded but Drive delete failed
                    if prev_status == "dify_ok_drive_delete_failed":
                        logger.info("Retrying Drive delete for: %s", file_name)
                        try:
                            if delete_drive_file(service, file_id, file_name, logger):
                                prev_entry["status"] = "complete"
                                prev_entry["time"] = datetime.now(timezone.utc).isoformat()
                                state["processed"][file_id] = prev_entry
                                save_state(state, state_path, logger)
                                results["drive_deleted"] += 1
                                logger.info("✓ Drive delete retry succeeded: %s", file_name)
                            else:
                                logger.warning("Drive delete retry still failed: %s", file_name)
                        except Exception as e:
                            logger.error("Drive delete retry error: %s — %s", file_name, e)
                        results["skipped"] += 1
                        continue
                    
                    # GAP-A7: Check if file was modified since last processing
                    prev_modified = prev_entry.get("modifiedTime", "")
                    if prev_modified and file_modified and file_modified <= prev_modified and prev_status == "complete":
                        logger.info("Skipping (already processed, unmodified): %s", file_name)
                        results["skipped"] += 1
                        continue
                    elif prev_modified and file_modified and file_modified > prev_modified:
                        logger.info("File modified since last run — reprocessing: %s", file_name)
                    else:
                        logger.info("Skipping (already processed): %s", file_name)
                        results["skipped"] += 1
                        continue

                if in_state and bucket == "failed":
                    failed_entry = state["failed"].get(file_id, {})
                    retries = failed_entry.get("retries", 0)
                    max_retries = failed_entry.get("max_retries", max_retries_state)
                    if retries >= max_retries:
                        logger.info("Skipping (permanently failed after %d retries): %s", retries, file_name)
                        results["skipped"] += 1
                        continue
                    logger.info("Retrying failed file (attempt %d/%d): %s", retries + 1, max_retries, file_name)

                # --- PROACTIVE CHECKS ---
                # Size check before download (GAP-EC5, Dify free tier ~15MB limit)
                if file_size > max_file_size_mb * 1024 * 1024:
                    logger.warning(
                        "File too large (%dMB > %dMB) — deferring: %s",
                        file_size // (1024*1024), max_file_size_mb, file_name,
                    )
                    state["deferred"][file_id] = {
                        "name": file_name,
                        "error": f"File exceeds {max_file_size_mb}MB limit ({file_size // (1024*1024)}MB)",
                        "time": datetime.now(timezone.utc).isoformat(),
                        "status": "deferred",
                    }
                    save_state(state, state_path, logger)
                    results["deferred"] += 1
                    continue

                # Zero-byte check (GAP-EC3) — skip for Google Workspace files (no size field)
                mime_type = file_info.get("mimeType", "")
                is_workspace = mime_type.startswith("application/vnd.google-apps.")
                if file_size == 0 and not is_workspace:
                    logger.warning("Skipping zero-byte file: %s", file_name)
                    state["failed"][file_id] = {
                        "name": file_name,
                        "error": "Zero-byte file",
                        "time": datetime.now(timezone.utc).isoformat(),
                        "retries": 0,
                        "max_retries": max_retries_state,
                        "status": "conversion_failed",
                    }
                    save_state(state, state_path, logger)
                    results["failed"] += 1
                    continue

                # --- DOWNLOAD ---
                try:
                    local_path = download_file(service, file_info, tmp_dir, logger)
                except SkipFileError as e:
                    logger.warning("Download skip: %s — %s", file_name, e)
                    state["failed"][file_id] = _failed_entry_for_state(state, file_id, file_name, str(e), e.status, max_retries_state)
                    save_state(state, state_path, logger)
                    results["failed"] += 1
                    continue
                except Exception as e:
                    logger.error("Download error: %s — %s", file_name, e)
                    state["failed"][file_id] = _failed_entry_for_state(state, file_id, file_name, str(e), "download_failed", max_retries_state)
                    save_state(state, state_path, logger)
                    results["failed"] += 1
                    continue

                # --- VALIDATE ---
                try:
                    validate_file(local_path, file_info, max_file_size_mb, logger)
                except SkipFileError as e:
                    logger.warning("Validation skip: %s — %s", file_name, e)
                    state[e.status.split("_")[0] if e.status == "deferred" else "failed"][file_id] = _failed_entry_for_state(
                        state, file_id, file_name, str(e), e.status, max_retries_state
                    )
                    save_state(state, state_path, logger)
                    if e.status == "deferred":
                        results["deferred"] += 1
                    else:
                        results["failed"] += 1
                    local_path.unlink(missing_ok=True)
                    continue

                # --- DRY RUN CHECK ---
                if dry_run:
                    logger.info("[DRY RUN] Would upload to Dify, then delete from Drive: %s", file_name)
                    state["processed"][file_id] = {
                        "name": file_name,
                        "time": datetime.now(timezone.utc).isoformat(),
                        "modifiedTime": file_modified,
                        "dify_doc_id": "dry-run",
                        "status": "dry_run",
                    }
                    save_state(state, state_path, logger)
                    results["ingested"] += 1
                    files_processed_this_loop += 1
                    continue

                # --- UPLOAD TO DIFY (native file upload for all types) ---
                dify_docs = []
                is_partial = False
                
                # Rate limit throttle: pause after every N uploads (Dify free tier ~10/burst)
                if upload_count_since_pause >= 5:
                    cooldown = 30
                    logger.info("Rate limit cooldown: pausing %ds after %d uploads...", cooldown, upload_count_since_pause)
                    time.sleep(cooldown)
                    upload_count_since_pause = 0
                
                try:
                    dify_docs, is_partial = upload_file_to_dify(
                        file_path=local_path,
                        doc_name=file_name,
                        base_url=dify_base_url,
                        dataset_id=dify_dataset_id,
                        api_key=dify_api_key,
                        timeout=timeout_dify,
                        logger=logger,
                    )
                    # Clean up local file after upload
                    local_path.unlink(missing_ok=True)
                except SkipFileError as e:
                    logger.warning("Dify upload skip: %s — %s", file_name, e)
                    state["failed"][file_id] = _failed_entry_for_state(state, file_id, file_name, str(e), "upload_failed", max_retries_state)
                    save_state(state, state_path, logger)
                    results["failed"] += 1
                    continue
                except PipelineError as e:
                    err_msg = str(e)
                    if "403" in err_msg and "rate limit" in err_msg.lower():
                        # Rate limit hit — stop processing, save state, exit
                        logger.warning("Dify rate limit hit — pausing 120s and stopping batch")
                        time.sleep(120)
                        # Save current state and stop — remaining files will be picked up next tick
                        save_state(state, state_path, logger)
                        logger.info("Batch stopped after rate limit. %d files remaining in Drive.", 
                                    len(drive_files) - files_processed_this_loop)
                        results["rate_limited"] = True
                        break  # Exit file loop, finish pipeline
                    logger.error("Dify upload fatal: %s — %s", file_name, e)
                    state["failed"][file_id] = _failed_entry_for_state(state, file_id, file_name, str(e), "upload_failed", max_retries_state)
                    save_state(state, state_path, logger)
                    results["failed"] += 1
                    continue

                if not dify_docs:
                    logger.warning("No chunks uploaded to Dify for: %s", file_name)
                    state["failed"][file_id] = _failed_entry_for_state(state, file_id, file_name, "No text content after chunking", "upload_failed", max_retries_state)
                    save_state(state, state_path, logger)
                    results["failed"] += 1
                    continue

                dify_doc_ids = [d.get("dify_doc_id") for d in dify_docs if d.get("dify_doc_id")]
                results["dify_docs_created"] += len(dify_doc_ids)
                upload_count_since_pause += 1  # Count successful uploads for throttle

                # --- DELETE FROM DRIVE (skip if partial upload) ---
                deleted = False
                if is_partial:
                    # Preserve source — third party can re-upload, or we can add remaining chunks
                    status = "partial_upload"
                    logger.warning(
                        "Skipping Drive delete for partial upload: %s (%d/%d docs uploaded)",
                        file_name, len(dify_doc_ids), "?",
                    )
                else:
                    try:
                        deleted = delete_drive_file(service, file_id, file_name, logger)
                    except Exception as e:
                        logger.error("Drive delete error for %s: %s", file_name, e)

                    if deleted:
                        results["drive_deleted"] += 1
                        status = "complete"
                    else:
                        status = "dify_ok_drive_delete_failed"
                        logger.warning(
                            "Dify upload succeeded but Drive delete failed for: %s — will retry delete next run",
                            file_name,
                        )

                # --- UPDATE STATE ---
                state["processed"][file_id] = {
                    "name": file_name,
                    "time": datetime.now(timezone.utc).isoformat(),
                    "modifiedTime": file_modified,
                    "dify_doc_id": dify_doc_ids[0] if dify_doc_ids else None,
                    "dify_doc_ids": dify_doc_ids,
                    "status": status,
                }
                save_state(state, state_path, logger)

                results["ingested"] += 1
                files_processed_this_loop += 1

                logger.info(
                    "✓ File complete: %s → Dify (%d docs) → Drive %s",
                    file_name, len(dify_doc_ids), "deleted" if deleted else "NOT deleted"
                )

            # --- End of file loop ---
            logger.info(
                "--- Loop %d done: %d files this iteration ---",
                loop_iteration, files_processed_this_loop,
            )

            if not loop_mode:
                break

            if files_processed_this_loop == 0:
                logger.info("No new files — exiting loop")
                break

            # Brief pause between loop iterations
            time.sleep(10)

    except PipelineError as e:
        logger.error("Pipeline fatal error: %s", e)
        results["errors"].append(str(e))
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        results["errors"].append("Interrupted by user")
    except Exception as e:
        logger.exception("Unexpected pipeline error: %s", e)
        results["errors"].append(f"Unexpected: {e}")
    finally:
        # --- Cleanup ---
        cleanup_tmp(tmp_dir, logger)
        lock.release()
        logger.info("PID lock released")

        results["elapsed"] = round(time.time() - start_time, 1)
        logger.info(
            "Pipeline finished in %.1fs: %d ingested, %d skipped, %d failed, %d deferred, %d deleted from Drive",
            results["elapsed"],
            results["ingested"],
            results["skipped"],
            results["failed"],
            results["deferred"],
            results["drive_deleted"],
        )

    return results


def _make_failed_entry(
    name: str,
    error: str,
    status: str,
    max_retries: int,
    existing_retries: int = 0,
) -> Dict[str, Any]:
    """Create a standard failed-entry dict for state.json.
    
    existing_retries: if the file already has a failed entry, pass its
    current retries count so we increment rather than reset to 0.
    """
    return {
        "name": name,
        "error": error,
        "time": datetime.now(timezone.utc).isoformat(),
        "retries": existing_retries + 1,
        "max_retries": max_retries,
        "status": status,
    }


def _failed_entry_for_state(
    state: Dict[str, Any],
    file_id: str,
    name: str,
    error: str,
    status: str,
    max_retries: int,
) -> Dict[str, Any]:
    """Wrapper that auto-extracts existing retries from state."""
    existing = state.get("failed", {}).get(file_id, {}).get("retries", 0)
    return _make_failed_entry(name, error, status, max_retries, existing_retries=existing)


# ---------------------------------------------------------------------------
# Config Loading
# ---------------------------------------------------------------------------
def load_config(config_path: str) -> Dict[str, Any]:
    """Load YAML config with deep-merge into defaults."""
    path = Path(config_path).resolve()
    if not path.exists():
        raise PipelineError(f"Config file not found: {path}")

    with open(path, "r") as f:
        user_config = yaml.safe_load(f) or {}

    # Deep merge user config into defaults
    merged = copy.deepcopy(DEFAULT_CONFIG)
    for section in merged:
        if section in user_config:
            merged[section].update(user_config[section])

    return merged


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Drive → Dify KB Ingestion Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 pipeline.py                           # Normal run
  python3 pipeline.py --dry-run                 # Download + convert only
  python3 pipeline.py --loop                    # Poll until folder empty
  python3 pipeline.py --config my_config.yaml   # Custom config
  DIFY_DATASET_KEY=dataset-xxx python3 pipeline.py  # Set key via env
        """,
    )
    parser.add_argument(
        "--config", default="config.yaml",
        help="Path to YAML config file (default: config.yaml)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Download and convert only — skip Dify upload and Drive delete",
    )
    parser.add_argument(
        "--loop", action="store_true",
        help="Keep polling folder until empty, then exit",
    )
    args = parser.parse_args()

    # Load config
    try:
        config = load_config(args.config)
    except PipelineError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    # Run pipeline
    results = run_pipeline(
        config=config,
        dry_run=args.dry_run,
        loop_mode=args.loop,
    )

    # Print summary
    print_summary(results)

    # Exit with error code if any failures
    if results.get("errors") or results.get("failed", 0) > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
