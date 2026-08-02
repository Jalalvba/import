#!/usr/bin/env python3
"""
test_gdrive_download.py
------------------------
Temporary, standalone check: not part of run.py yet. Confirms
get_latest_expected_files() downloads correct, valid-looking byte content
for the expected pipeline input files — dedupe-by-latest-modifiedTime
included — before anything is wired into the real pipeline.

Usage:
    python3 test_gdrive_download.py
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.gdrive import get_latest_expected_files

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
if not folder_id:
    raise EnvironmentError("❌ GOOGLE_DRIVE_FOLDER_ID not set in .env")

XLSX_MAGIC = b"PK\x03\x04"       # zip archive (xlsx)
XLS_MAGIC = b"\xd0\xcf\x11\xe0"  # OLE2 compound file (legacy xls)

files = get_latest_expected_files(folder_id)

print(f"✅ Downloaded {len(files)} file(s):\n")
for name, content in files.items():
    size = len(content)
    if name.endswith(".xlsx"):
        expected_magic = XLSX_MAGIC
    else:
        expected_magic = XLS_MAGIC
    ok = content.startswith(expected_magic)
    status = "✅ valid signature" if ok else "❌ UNEXPECTED signature"
    actual_prefix = content[:4].hex(" ")
    print(f"  {name:28s}  {size:>10,} bytes  {status}  (first bytes: {actual_prefix})")
