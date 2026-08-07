"""
lib/gdrive.py
-------------
Google Drive read-only access for the avis ETL pipelines: auth, file
listing, and in-memory content download. GOOGLE_DRIVE_FOLDER_ID is the
single source of truth for pipeline input — ds.py/cp.py/parc.py/bc.py
read file bytes fetched from here instead of a local input/ path.
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
    """Return [{id, name, mimeType, modifiedTime, size}, ...] for every
    non-trashed file directly inside folder_id, using Drive API v3,
    read-only scope. `size` (bytes, as a string) is populated for binary
    files like the .xlsx/.xls inputs here; Drive omits it for native
    Google Docs types, which this folder never contains."""
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
                fields="nextPageToken, files(id, name, mimeType, modifiedTime, size)",
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


def verify_download_size(file_bytes: bytes, file_meta: dict) -> None:
    """Raise ValueError if file_bytes' length doesn't match Drive's own
    reported `size` for this file (already fetched in file_meta, otherwise
    unused). A short read that Drive terminates as a clean 200 could
    otherwise yield a truncated file that calamine/openpyxl silently parses
    as fewer rows rather than erroring -- shipping partial data. A no-op if
    Drive didn't report a size at all (native Google Docs types omit it;
    never true for the .xlsx/.xls files this project downloads, but
    tolerated rather than treated as a mismatch)."""
    expected = file_meta.get("size")
    if not expected:
        return
    actual = len(file_bytes)
    expected = int(expected)
    if actual != expected:
        raise ValueError(
            f"❌ downloaded {actual:,} bytes for '{file_meta.get('name', file_meta.get('id'))}' "
            f"but Drive reported {expected:,} bytes -- likely a truncated/short download, not a "
            f"safe file to process"
        )


def _latest_by_name(files: list[dict], names: set[str]) -> dict[str, dict]:
    """Filter files to those whose name is in names, then dedupe by name —
    keeping the entry with the latest modifiedTime when a name repeats."""
    latest: dict[str, dict] = {}
    for f in files:
        name = f["name"]
        if name not in names:
            continue
        current = latest.get(name)
        if current is None or f["modifiedTime"] > current["modifiedTime"]:
            latest[name] = f
    return latest


def list_expected_files(folder_id: str) -> dict[str, dict]:
    """List folder_id and return {filename: {id, name, mimeType,
    modifiedTime, size}} for the four known pipeline input filenames,
    deduped by latest modifiedTime when a name repeats. Does NOT download
    any bytes — this is the cheap metadata-only pass used to decide (via
    lib.pipeline_state) whether a file has actually changed before paying
    for a download. YBONTEC.xlsx is optional and silently omitted if
    absent; the other three are required and raise if any are missing.

    Used by run.py to fetch every pipeline's metadata with a single Drive
    listing call."""
    files = list_drive_files(folder_id)
    latest = _latest_by_name(files, EXPECTED_FILENAMES)

    missing = REQUIRED_FILENAMES - latest.keys()
    if missing:
        raise FileNotFoundError(
            f"❌ Missing required file(s) in Drive folder {folder_id}: "
            f"{', '.join(sorted(missing))}"
        )

    return latest


def get_latest_expected_files(folder_id: str) -> dict[str, bytes]:
    """List folder_id via list_expected_files(), then unconditionally
    download every survivor's bytes and return {filename: bytes}. Used by
    the manual test_gdrive_download.py debugging tool; run.py itself
    downloads selectively (see lib.pipeline_state.resolve_pipeline_run)
    rather than calling this."""
    latest = list_expected_files(folder_id)
    return {name: download_file_bytes(f["id"]) for name, f in latest.items()}


def find_file_metadata(folder_id: str, filename: str) -> dict | None:
    """List folder_id and return the {id, name, mimeType, modifiedTime,
    size} metadata for filename (deduping by latest modifiedTime if the
    name repeats), or None if absent — no bytes downloaded. Used by each
    pipeline script's standalone `__main__` block (`python ds.py`) so it
    can run the same skip-if-unchanged check run.py does before deciding
    whether to call download_file_bytes()."""
    if not folder_id:
        raise ValueError("❌ folder_id is required (pass GOOGLE_DRIVE_FOLDER_ID).")

    files = list_drive_files(folder_id)
    latest = _latest_by_name(files, {filename})
    return latest.get(filename)
