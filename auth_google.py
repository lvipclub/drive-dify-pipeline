#!/usr/bin/env python3
"""
Google Drive OAuth Authentication Helper
=========================================
Interactive script that:
  1. Reads OAuth credentials from ~/workspace/google_client_secret.json
  2. Opens a browser for lvipclub@gmail.com to authorize
  3. Saves the refresh token + credentials to ~/.hermes/config/drive-refresh-token.json
  4. Validates the token by listing files from the shared Drive folder

Usage:
    python3 auth_google.py

After running, the refresh token is saved and ready for pipeline.py.
"""

import json
import os
import sys
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCOPES = ["https://www.googleapis.com/auth/drive"]

# Paths (absolute — resolve ~ to $HOME)
HOME = Path(os.environ.get("HOME", os.path.expanduser("~")))
CLIENT_SECRET_PATH = HOME / "workspace" / "google_client_secret.json"
TOKEN_PATH = HOME / ".hermes" / "config" / "drive-refresh-token.json"

# The folder we want to validate access to
FOLDER_TO_CHECK = "1GlOjKMZWmSIa6ejf1InUwYybT7ZuE102"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def die(msg: str, code: int = 1) -> None:
    """Print error and exit."""
    print(f"\n❌ ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def check_prerequisites() -> None:
    """Verify the client-secret file exists before starting the flow."""
    if not CLIENT_SECRET_PATH.exists():
        die(
            f"Client secret file not found at {CLIENT_SECRET_PATH}\n"
            "Expected: ~/workspace/google_client_secret.json (project oc-i58500hp)"
        )


def run_oauth_flow() -> dict:
    """
    Run the interactive OAuth 2.0 flow.

    Opens the system browser so the user (lvipclub@gmail.com) can sign in and
    grant full drive access (needed for file deletion after ingestion).
    Uses `run_local_server()` which listens on
    http://localhost for the redirect — no web-server setup required.
    """
    flow = InstalledAppFlow.from_client_secrets_file(
        str(CLIENT_SECRET_PATH),
        scopes=SCOPES,
    )

    print("🌐 Opening browser for Google OAuth…")
    print("   Please sign in as lvipclub@gmail.com and approve the requested permissions.\n")

    # run_local_server will:
    #   1. Open the authorization URL in the default browser
    #   2. Start a temporary HTTP server on localhost to catch the redirect
    #   3. Exchange the auth code for tokens (access + refresh)
    credentials = flow.run_local_server(
        host="localhost",
        port=0,  # OS-assigned free port
        open_browser=True,
    )

    print("\n✅ Authentication successful!\n")
    return credentials


def save_token(credentials) -> None:
    """Persist the credentials (including refresh token) to disk."""
    # Ensure the parent directory exists
    TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Serialize as JSON — this includes access_token, refresh_token, expiry, etc.
    token_data = json.loads(credentials.to_json())

    # Write atomically: write to temp file then rename
    tmp_path = TOKEN_PATH.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(token_data, fh, indent=2)
    tmp_path.replace(TOKEN_PATH)  # atomic on same filesystem

    # Restrict permissions (owner read/write only — contains secrets)
    os.chmod(TOKEN_PATH, 0o600)

    print(f"💾 Refresh token saved to {TOKEN_PATH}")
    print(f"   Permissions: 600 (owner read/write only)")


def validate_token(credentials) -> None:
    """Verify the token works by calling the Drive API."""
    print(f"\n🔍 Validating token — listing files in folder {FOLDER_TO_CHECK}…")

    try:
        service = build("drive", "v3", credentials=credentials)

        results = (
            service.files()
            .list(
                q=f"'{FOLDER_TO_CHECK}' in parents and trashed = false",
                pageSize=1,
                fields="files(id, name, mimeType, size)",
            )
            .execute()
        )

        files = results.get("files", [])
        print(f"\n✅ Token is valid! Found {len(files)} file(s) in the shared folder:\n")
        if files:
            for f in files:
                size = f.get("size", "?")
                print(f"   📄 {f['name']} ({f['mimeType']}) — {size} bytes")
        else:
            print("   (folder is empty — this is fine, token still works)")

    except HttpError as exc:
        # Decode Google API JSON error body
        reason = str(exc)
        try:
            detail = json.loads(exc.content.decode())
            reason = detail.get("error", {}).get("message", reason)
        except (json.JSONDecodeError, AttributeError):
            pass
        die(f"Drive API call failed: {reason}")


def print_refresh_token_info(token_data: dict) -> None:
    """Print a summary of what was saved."""
    print("\n" + "=" * 60)
    print("📋 Token Summary")
    print("=" * 60)
    print(f"  Refresh token : {'✅ present' if token_data.get('refresh_token') else '⚠️  MISSING'}")
    print(f"  Client ID     : {token_data.get('client_id', '?')}")
    print(f"  Scopes        : {token_data.get('scopes', token_data.get('scope', '?'))}")
    print(f"  Expiry        : {token_data.get('expiry', '?')}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    print("=" * 60)
    print("🔐 Google Drive OAuth — Refresh Token Setup")
    print("=" * 60)
    print(f"   Client secret : {CLIENT_SECRET_PATH}")
    print(f"   Token output  : {TOKEN_PATH}")
    print(f"   Scope         : {SCOPES[0]}")
    print(f"   Folder        : {FOLDER_TO_CHECK}")
    print("=" * 60 + "\n")

    # 1. Pre-flight checks
    check_prerequisites()

    # 2. If a valid token already exists, offer to skip
    if TOKEN_PATH.exists():
        print("⚠️  A token file already exists at:")
        print(f"   {TOKEN_PATH}\n")
        answer = input("   Overwrite? [y/N]: ").strip().lower()
        if answer not in ("y", "yes"):
            print("   👋 Exiting without changes.")
            sys.exit(0)

    # 3. Run interactive OAuth
    credentials = run_oauth_flow()

    # 4. Persist
    save_token(credentials)

    # 5. Print summary
    token_data = json.loads(credentials.to_json())
    print_refresh_token_info(token_data)

    # 6. Validate
    validate_token(credentials)

    print("\n🎉 Done! The pipeline (pipeline.py) can now use this token for unattended Drive access.\n")


if __name__ == "__main__":
    main()
