#!/usr/bin/env python3
"""
run.py
------
Lists the latest pipeline input files' metadata from the Google Drive
folder (GOOGLE_DRIVE_FOLDER_ID in .env) ONCE -- no bytes downloaded yet
-- then for each matching pipeline whose file is present, checks
`pipeline_state` (see lib/pipeline_state.py) to decide whether the file
has actually changed since the last successful run. Only a changed (or
first-run, or force-requested) file gets downloaded and pushed to Mongo
(no CSV intermediate); an unchanged file is skipped entirely, with no
download and no write to its data collection. Set PIPELINE_FORCE_RUN=1
to bypass the unchanged-skip for a manual re-run.

Every pipeline run -- absent-from-Drive, skipped-unchanged, succeeded, or
failed -- gets a durable step-by-step log persisted as one document in
the `pipeline_runs` Mongo collection (see lib/pipeline_log.py). run_all()
returns each pipeline's run_id, status ("success" | "failed" |
"skipped_absent" | "skipped_unchanged"), and step summary, so a run can
be looked up later; it's shared by main() below and by api/index.py so
the two don't duplicate this loop.

Pipelines:
    YFACSCALDS.xlsx            → ds.py
    ConditionParticulieres.xls → cp.py
    Fullparcs.xls               → parc.py
    YBONTEC.xlsx                → bc.py

Usage:
    python run.py
    PIPELINE_FORCE_RUN=1 python run.py   # bypass skip-if-unchanged
"""

import os
import traceback
from pathlib import Path

from dotenv import load_dotenv

import bc
import cp
import ds
import parc
from lib.gdrive import download_file_bytes, list_expected_files
from lib.pipeline_log import PipelineLogger
from lib.pipeline_state import force_requested, resolve_pipeline_run, update_state

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


def run_all(file_metadata: dict[str, dict], force: bool = False) -> list[dict]:
    """Run every pipeline whose input file is present in `file_metadata`
    (filename -> Drive metadata dict, no bytes downloaded yet). Every
    pipeline -- absent, skipped-unchanged, succeeded, or failed -- gets its
    own PipelineLogger and a persisted pipeline_runs document. Returns one
    result dict per pipeline with label, filename, status, run_id, and a
    compact step summary. Status is one of: "success", "failed",
    "skipped_absent" (file not in Drive at all), "skipped_unchanged"
    (unchanged since the last successful run -- see lib.pipeline_state)."""
    found  = [p for p in PIPELINES if p["filename"] in file_metadata]
    absent = [p for p in PIPELINES if p["filename"] not in file_metadata]

    results = []

    for p in absent:
        logger = PipelineLogger(p["module"].__name__)
        logger.log("file_download", "skipped", f"{p['filename']} not found in Drive folder")
        logger.finish("skipped")
        results.append({
            "label":    p["label"],
            "filename": p["filename"],
            "status":   "skipped_absent",
            "run_id":   logger.run_id,
            "steps":    [f"{s['step']}:{s['status']}" for s in logger.steps],
        })

    for p in found:
        module = p["module"]
        pipeline_name = module.__name__
        meta = file_metadata[p["filename"]]

        logger = PipelineLogger(pipeline_name)
        logger.log("drive_auth", "success", "authenticated with Drive service account")
        logger.log("drive_listing", "success", f"found {len(file_metadata)} of {len(PIPELINES)} expected file(s) in Drive folder")

        decision = resolve_pipeline_run(pipeline_name, meta, logger, force=force)
        if decision in ("skip_unchanged", "hard_fail"):
            results.append({
                "label":    p["label"],
                "filename": p["filename"],
                "status":   "skipped_unchanged" if decision == "skip_unchanged" else "failed",
                "run_id":   logger.run_id,
                "steps":    [f"{s['step']}:{s['status']}" for s in logger.steps],
            })
            continue

        file_bytes = download_file_bytes(meta["id"])
        logger.log("file_download", "success", f"{p['filename']}: {len(file_bytes):,} bytes")

        ok = run_pipeline(p, file_bytes, logger)
        if ok:
            update_state(pipeline_name, p["filename"], meta["id"], meta["modifiedTime"], logger.run_id)
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

    print(f"🔍 Listing input files in Drive folder {folder_id}...\n")
    file_metadata = list_expected_files(folder_id)
    print(f"✅ Found {len(file_metadata)} file(s) in Drive\n")

    force = force_requested()
    if force:
        print("⚡ PIPELINE_FORCE_RUN is set -- skip-if-unchanged check will be bypassed\n")

    results = run_all(file_metadata, force=force)

    tally = {"success": 0, "skipped_unchanged": 0, "skipped_absent": 0, "failed": 0}
    for r in results:
        tally[r["status"]] += 1

    print(f"\n{'═' * 60}")
    print(
        f"✅ {tally['success']} succeeded, "
        f"⏭️  {tally['skipped_unchanged']} skipped (unchanged), "
        f"📭 {tally['skipped_absent']} skipped (absent), "
        f"❌ {tally['failed']} failed"
    )
    print()
    print("Run IDs (see pipeline_runs collection for full step detail):")
    for r in results:
        print(f"   - {r['label']}: {r['run_id']} ({r['status']})")
    print(f"{'═' * 60}")


if __name__ == "__main__":
    main()
