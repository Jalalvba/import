"""
lib/pipeline_state.py
----------------------
Tracks the last SUCCESSFULLY processed file per pipeline in the
`pipeline_state` collection (one document per pipeline: ds/cp/parc/bc),
so a run can skip entirely when Drive's file hasn't changed since --
no download, no touch of the pipeline's actual data collection, only a
lightweight "skipped" pipeline_runs document. A failed run never updates
this state, so the next trigger retries it properly.

resolve_pipeline_run() is the single entry point both run.py (batch) and
each pipeline script's standalone `__main__` block call: it composes the
unchanged-skip check with lib.size_check's size-tier check and logs both
decisions as steps on the caller's PipelineLogger, so the two entrypoints
can't drift into different behavior.
"""

import os
from datetime import datetime, timezone

from lib.mongo import get_mongo_db
from lib.size_check import HARD_FAIL_BYTES, WARN_BYTES, classify

_COLLECTION = "pipeline_state"


def get_state(pipeline: str) -> dict | None:
    """Read the pipeline_state document for `pipeline` (e.g. "ds"), or
    None if it has never completed a successful run."""
    db = get_mongo_db()
    try:
        return db[_COLLECTION].find_one({"pipeline": pipeline})
    finally:
        db.client.close()


def update_state(pipeline: str, filename: str, file_id: str, modified_time: str, run_id: str) -> None:
    """Upsert the last-successful-run record for `pipeline`. Callers must
    only call this after a pipeline run has actually succeeded -- never on
    a failed or skipped run."""
    db = get_mongo_db()
    try:
        db[_COLLECTION].update_one(
            {"pipeline": pipeline},
            {"$set": {
                "pipeline": pipeline,
                "filename": filename,
                "drive_file_id": file_id,
                "modified_time": modified_time,
                "processed_at": datetime.now(timezone.utc),
                "run_id": run_id,
            }},
            upsert=True,
        )
    finally:
        db.client.close()


def force_requested() -> bool:
    """PIPELINE_FORCE_RUN=1/true/yes forces a re-run even if the source
    file is unchanged since the last successful run -- for manual testing,
    or recovering from Mongo data that was corrupted independently of the
    source file. Read fresh on every call (not cached), and does NOT
    bypass the hard-fail size check -- force can't make an unsafe-size
    file safe to process on Vercel."""
    return os.getenv("PIPELINE_FORCE_RUN", "").strip().lower() in {"1", "true", "yes"}


def resolve_pipeline_run(pipeline: str, file_meta: dict, logger, force: bool = False) -> str:
    """Decide whether `pipeline` should actually process file_meta
    ({id, name, modifiedTime, size, ...}), logging a `skip_check` step and
    (if not skipped) a `size_check` step on `logger` either way. Returns:

      "proceed"         -> caller should download bytes and run the pipeline
      "skip_unchanged"   -> same drive_file_id + modified_time as the last
                             successful run; logger.finish("skipped") has
                             already been called -- caller must not touch
                             Mongo data collections at all
      "hard_fail"        -> file exceeds the safe-processing size ceiling
                             for a Vercel-triggered run; logger.finish("failed")
                             has already been called

    A local run (VERCEL env var unset) that exceeds the hard-fail
    threshold is NOT blocked -- it logs a "warning" size_check step and
    returns "proceed", since a laptop doesn't share Vercel's memory/
    duration ceiling and is the documented escape hatch for oversized
    files (see CLAUDE.md)."""
    state = get_state(pipeline)
    file_id = file_meta["id"]
    modified_time = file_meta["modifiedTime"]
    unchanged = bool(state) and state.get("drive_file_id") == file_id and state.get("modified_time") == modified_time

    if unchanged and not force:
        logger.log("skip_check", "skipped", f"unchanged since {modified_time}, skipping run")
        logger.finish("skipped")
        return "skip_unchanged"

    if unchanged and force:
        reason = f"unchanged since {modified_time}, but force requested -- proceeding anyway"
    elif state is None:
        reason = "no prior pipeline_state entry (first run)"
    else:
        reason = f"changed since last processed ({state.get('modified_time')} -> {modified_time})"
    logger.log("skip_check", "success", reason)

    size = int(file_meta["size"]) if file_meta.get("size") else None
    tier = classify(size)
    size_label = f"{size:,} bytes" if size is not None else "size unknown (Drive did not report one)"
    on_vercel = bool(os.getenv("VERCEL"))

    if tier == "hard_fail" and on_vercel:
        logger.log(
            "size_check", "failed",
            f"{size_label} exceeds the {HARD_FAIL_BYTES:,}-byte safe processing size for "
            f"Vercel -- run this pipeline locally instead: python3 {pipeline}.py",
        )
        logger.finish("failed")
        return "hard_fail"
    elif tier == "hard_fail":
        logger.log(
            "size_check", "warning",
            f"{size_label} exceeds the {HARD_FAIL_BYTES:,}-byte Vercel-safe ceiling, but this "
            f"is a local run -- proceeding anyway",
        )
    elif tier == "warn":
        logger.log(
            "size_check", "warning",
            f"{size_label} exceeds {WARN_BYTES:,} bytes -- unusually large, processing may "
            f"take longer / use more memory",
        )
    else:
        logger.log("size_check", "success", size_label)

    return "proceed"
