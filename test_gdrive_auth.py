#!/usr/bin/env python3
"""
test_gdrive_auth.py
--------------------
Temporary, standalone check: not part of run.py yet. Confirms
GOOGLE_SERVICE_ACCOUNT_KEY_B64 / GOOGLE_DRIVE_FOLDER_ID are set up
correctly by listing the target Drive folder's contents.

Usage:
    python3 test_gdrive_auth.py
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.gdrive import list_drive_files

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

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
