"""
lib/gdrive.py
-------------
Google Drive read-only access for the avis ETL pipelines. Auth, file
listing, and now (Step 2) in-memory content download. Not yet wired into
ds.py/cp.py/parc.py/bc.py/run.py — that's Step 3.
"""

import base64
import binascii
import io
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# The four pipeline input filenames avis expects to find in the Drive
# folder. YBONTEC.xlsx is optional — bc.py already tolerates its absence.
EXPECTED_FILENAMES = {
    "YFACSCALDS.xlsx",
    "ConditionParticulieres.xls",
    "Fullparcs.xls",
    "YBONTEC.xlsx",
}
REQUIRED_FILENAMES = EXPECTED_FILENAMES - {"YBONTEC.xlsx"}


def _load_service_account_credentials(env_path: Path | None = None) -> Credentials:
    """Load GOOGLE_SERVICE_ACCOUNT_KEY_B64 from .env, decode + parse it, and
    build in-memory Credentials. The decoded JSON is never written to disk."""
    if env_path is None:
        env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)

    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY_B64")
    if not raw:
        raise EnvironmentError(
            "❌ GOOGLE_SERVICE_ACCOUNT_KEY_B64 not set in .env — see .env.example "
            "for how to create the service account key and base64-encode it."
        )

    try:
        decoded = base64.b64decode(raw, validate=True)
    except binascii.Error as e:
        raise ValueError(
            f"❌ GOOGLE_SERVICE_ACCOUNT_KEY_B64 is not valid base64: {e}. "
            "Re-encode the key file with `base64 -w 0 keyfile.json` and make sure "
            "the whole output was pasted onto a single .env line."
        ) from e

    try:
        info = json.loads(decoded)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"❌ GOOGLE_SERVICE_ACCOUNT_KEY_B64 decoded to invalid JSON: {e}. "
            "The base64 string must decode to the full service account JSON key file, unmodified."
        ) from e

    try:
        return Credentials.from_service_account_info(info, scopes=SCOPES)
    except ValueError as e:
        raise ValueError(
            f"❌ Decoded JSON is not a valid service account key: {e}. "
            "Make sure GOOGLE_SERVICE_ACCOUNT_KEY_B64 was built from the JSON key file "
            "Google Cloud Console generated, not some other file."
        ) from e


def list_drive_files(folder_id: str) -> list[dict]:
    """Return [{id, name, mimeType, modifiedTime}, ...] for every non-trashed
    file directly inside folder_id, using Drive API v3, read-only scope."""
    if not folder_id:
        raise ValueError("❌ folder_id is required (pass GOOGLE_DRIVE_FOLDER_ID).")

    creds = _load_service_account_credentials()
    service = build("drive", "v3", credentials=creds)

    files = []
    page_token = None
    while True:
        response = (
            service.files()
            .list(
                q=f"'{folder_id}' in parents and trashed = false",
                fields="nextPageToken, files(id, name, mimeType, modifiedTime)",
                pageToken=page_token,
            )
            .execute()
        )
        files.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return files


def download_file_bytes(file_id: str) -> bytes:
    """Download a Drive file's raw content via files().get_media(), fully
    in memory — no temp files, no disk writes."""
    creds = _load_service_account_credentials()
    service = build("drive", "v3", credentials=creds)

    request = service.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    return buffer.getvalue()


def get_latest_expected_files(folder_id: str) -> dict[str, bytes]:
    """List folder_id, keep only the four known pipeline input filenames,
    dedupe by filename (keeping the one with the latest modifiedTime when a
    name appears more than once), download each survivor's bytes, and
    return {filename: bytes}. YBONTEC.xlsx is optional and silently omitted
    if absent; the other three are required and raise if any are missing."""
    files = list_drive_files(folder_id)
    candidates = [f for f in files if f["name"] in EXPECTED_FILENAMES]

    latest_by_name: dict[str, dict] = {}
    for f in candidates:
        name = f["name"]
        current = latest_by_name.get(name)
        if current is None or f["modifiedTime"] > current["modifiedTime"]:
            latest_by_name[name] = f

    missing = REQUIRED_FILENAMES - latest_by_name.keys()
    if missing:
        raise FileNotFoundError(
            f"❌ Missing required file(s) in Drive folder {folder_id}: "
            f"{', '.join(sorted(missing))}"
        )

    return {
        name: download_file_bytes(f["id"])
        for name, f in latest_by_name.items()
    }
