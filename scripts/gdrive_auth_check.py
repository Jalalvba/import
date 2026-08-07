#!/usr/bin/env python3
"""
scripts/gdrive_auth_check.py
-----------------------------
Optional, standalone manual debugging tool — not part of the automated
run.py flow, not called by anything else in the repo, and NOT a pytest
test despite its old test_*.py name/location (moved into scripts/ so
pytest doesn't try to collect and execute it, which would trigger a live
Drive API call). Confirms GOOGLE_SERVICE_ACCOUNT_KEY_B64 /
GOOGLE_DRIVE_FOLDER_ID are set up correctly by listing the target Drive
folder's contents. Useful when diagnosing a pipeline failure: run this
first to isolate "Drive access is broken" from "pipeline transform logic
is broken."

Usage:
    python3 scripts/gdrive_auth_check.py
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from lib.gdrive import list_drive_files

load_dotenv(dotenv_path=ROOT / ".env")

folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
if not folder_id:
    raise EnvironmentError("❌ GOOGLE_DRIVE_FOLDER_ID not set in .env")

files = list_drive_files(folder_id)

if not files:
    print("⚠️  Auth succeeded but the folder listing is empty — check the "
          "folder ID and that it's shared with the service account's email.")
else:
    print(f"✅ Found {len(files)} file(s) in folder {folder_id}:\n")
    for f in files:
        print(f"  {f['name']:40s}  modified {f['modifiedTime']}  ({f['mimeType']})")
