#!/usr/bin/env python3
"""
run.py
------
Fetches the latest pipeline input files from the Google Drive folder
(GOOGLE_DRIVE_FOLDER_ID in .env) ONCE, in memory, then runs each
matching single-file pipeline (bytes -> CSV -> Mongo) for each file that
was found, skipping pipelines whose input file is absent from Drive.

Pipelines:
    YFACSCALDS.xlsx            → ds.py
    ConditionParticulieres.xls → cp.py
    Fullparcs.xls               → parc.py
    YBONTEC.xlsx                → bc.py

Usage:
    python run.py
"""

import os
import traceback
from pathlib import Path

from dotenv import load_dotenv

import bc
import cp
import ds
import parc
from lib.gdrive import get_latest_expected_files

# ── Pipeline map: Drive filename → single-file pipeline module ───────────────
PIPELINES = [
    {
        "label":    "DS (Consumption sheets)",
        "filename": "YFACSCALDS.xlsx",
        "module":   ds,
    },
    {
        "label":    "CP (Contract particulars)",
        "filename": "ConditionParticulieres.xls",
        "module":   cp,
    },
    {
        "label":    "PARC (Fleet parks)",
        "filename": "Fullparcs.xls",
        "module":   parc,
    },
    {
        "label":    "BC (Purchase orders)",
        "filename": "YBONTEC.xlsx",
        "module":   bc,
    },
]

ROOT = Path(__file__).parent


def run_pipeline(pipeline: dict, file_bytes: bytes) -> bool:
    """Call a pipeline module's main() in-process. Returns True if successful.
    A pipeline's own sys.exit(1) on a failed Mongo push (cp.py/parc.py) is
    caught here too, so one pipeline failing doesn't kill run.py itself —
    same behavior as the old subprocess-per-script return-code check."""
    try:
        pipeline["module"].main(file_bytes)
        return True
    except SystemExit as e:
        return e.code in (None, 0)
    except Exception:
        traceback.print_exc()
        return False


def main():
    load_dotenv(dotenv_path=ROOT / ".env")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not folder_id:
        raise EnvironmentError("❌ GOOGLE_DRIVE_FOLDER_ID not set in .env")

    print(f"🔍 Fetching input files from Drive folder {folder_id}...\n")
    files = get_latest_expected_files(folder_id)

    found   = [p for p in PIPELINES if p["filename"] in files]
    skipped = [p for p in PIPELINES if p["filename"] not in files]

    print(f"✅ Found {len(found)} file(s) to process:\n")
    for p in found:
        print(f"   • {p['filename']}  →  {p['label']}")
    for p in skipped:
        print(f"   ⚠️  {p['filename']}  →  {p['label']}  (not found in Drive — skipping)")
    print()

    errors = []

    for pipeline in found:
        label      = pipeline["label"]
        module     = pipeline["module"]
        file_bytes = files[pipeline["filename"]]

        print(f"{'─' * 60}")
        print(f"▶  {label}")
        print(f"{'─' * 60}")

        print(f"\n  ⚙️  Running {module.__name__}.py ...")
        ok = run_pipeline(pipeline, file_bytes)
        if not ok:
            print(f"  ❌ {module.__name__}.py failed")
            errors.append(f"{label} / {module.__name__}.py")
        print()

    print(f"{'═' * 60}")
    if errors:
        print(f"⚠️  Completed with errors:")
        for e in errors:
            print(f"   - {e}")
    else:
        print(f"✅ All pipelines completed successfully.")
    print(f"{'═' * 60}")


if __name__ == "__main__":
    main()
