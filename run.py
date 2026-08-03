#!/usr/bin/env python3
"""
run.py
------
Fetches the latest pipeline input files from the Google Drive folder
(GOOGLE_DRIVE_FOLDER_ID in .env) ONCE, in memory, then runs each
matching single-file pipeline (bytes -> Mongo, no CSV intermediate) for
each file that was found, skipping pipelines whose input file is absent
from Drive.

Every pipeline run -- found, skipped, succeeded, or failed -- gets a
durable step-by-step log persisted as one document in the
`pipeline_runs` Mongo collection (see lib/pipeline_log.py). run_all()
returns each pipeline's run_id and step summary alongside its status, so
a run can be looked up later; it's shared by main() below and by
api/run-pipeline.py so the two don't duplicate this loop.

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
from lib.pipeline_log import PipelineLogger

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


def run_pipeline(pipeline: dict, file_bytes: bytes, logger: PipelineLogger) -> bool:
    """Call a pipeline module's main() in-process. Returns True if successful.
    A pipeline's own sys.exit(1) on a failed Mongo push (cp.py/parc.py) is
    caught here too, so one pipeline failing doesn't kill run.py itself —
    same behavior as the old subprocess-per-script return-code check. The
    module's own main() is responsible for finishing + persisting the
    logger's run document regardless of outcome."""
    try:
        pipeline["module"].main(file_bytes, logger=logger)
        return True
    except SystemExit as e:
        return e.code in (None, 0)
    except Exception:
        traceback.print_exc()
        return False


def run_all(files: dict[str, bytes]) -> list[dict]:
    """Run every pipeline whose input file is present in `files`. Every
    pipeline -- found or skipped -- gets its own PipelineLogger and a
    persisted pipeline_runs document. Returns one result dict per pipeline
    with label, filename, status, run_id, and a compact step summary."""
    found   = [p for p in PIPELINES if p["filename"] in files]
    skipped = [p for p in PIPELINES if p["filename"] not in files]

    results = []

    for p in skipped:
        logger = PipelineLogger(p["module"].__name__)
        logger.log("file_download", "skipped", f"{p['filename']} not found in Drive folder")
        logger.finish("skipped")
        results.append({
            "label":    p["label"],
            "filename": p["filename"],
            "status":   "skipped",
            "run_id":   logger.run_id,
            "steps":    [f"{s['step']}:{s['status']}" for s in logger.steps],
        })

    for p in found:
        module     = p["module"]
        file_bytes = files[p["filename"]]

        logger = PipelineLogger(module.__name__)
        logger.log("drive_auth", "success", "authenticated with Drive service account")
        logger.log("drive_listing", "success", f"found {len(files)} of {len(PIPELINES)} expected file(s) in Drive folder")
        logger.log("file_download", "success", f"{p['filename']}: {len(file_bytes):,} bytes")

        ok = run_pipeline(p, file_bytes, logger)
        results.append({
            "label":    p["label"],
            "filename": p["filename"],
            "status":   "success" if ok else "failed",
            "run_id":   logger.run_id,
            "steps":    [f"{s['step']}:{s['status']}" for s in logger.steps],
        })

    return results


def main():
    load_dotenv(dotenv_path=ROOT / ".env")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not folder_id:
        raise EnvironmentError("❌ GOOGLE_DRIVE_FOLDER_ID not set in .env")

    print(f"🔍 Fetching input files from Drive folder {folder_id}...\n")
    files = get_latest_expected_files(folder_id)
    print(f"✅ Found {len(files)} file(s) to process\n")

    results = run_all(files)

    print(f"\n{'═' * 60}")
    errors = [r for r in results if r["status"] == "failed"]
    if errors:
        print("⚠️  Completed with errors:")
        for r in errors:
            print(f"   - {r['label']}")
    else:
        print("✅ All pipelines completed successfully.")
    print()
    print("Run IDs (see pipeline_runs collection for full step detail):")
    for r in results:
        print(f"   - {r['label']}: {r['run_id']} ({r['status']})")
    print(f"{'═' * 60}")


if __name__ == "__main__":
    main()
