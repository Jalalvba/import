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
from datetime import datetime, timedelta, timezone

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from lib.mongo import get_mongo_db
from lib.size_check import HARD_FAIL_BYTES, WARN_BYTES, classify

_COLLECTION = "pipeline_state"

# Vercel's maxDuration (180s, see vercel.json) plus a 5-minute safety
# margin -- a lock older than this is assumed to belong to a crashed/killed
# run rather than one still legitimately in progress, so a new run is
# allowed to reclaim it instead of deadlocking forever.
LOCK_LEASE_SECONDS = 480


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


def acquire_lock(pipeline: str, run_id: str) -> bool:
    """Atomically claim the run lock for `pipeline`, or reclaim it if the
    existing lock is older than LOCK_LEASE_SECONDS (a crashed/killed run
    never released it). Returns True if `run_id` now holds the lock, False
    if another run currently holds it. The match+update happen as a single
    find_one_and_update -- atomic at the Mongo layer, so two overlapping
    callers can't both believe they acquired it."""
    db = get_mongo_db()
    try:
        # Enforced so upsert can never construct two documents for the
        # same pipeline under a genuine race (idempotent -- a no-op once
        # the index already exists).
        db[_COLLECTION].create_index("pipeline", unique=True)
        now = datetime.now(timezone.utc)
        stale_before = now - timedelta(seconds=LOCK_LEASE_SECONDS)
        try:
            doc = db[_COLLECTION].find_one_and_update(
                {
                    "pipeline": pipeline,
                    "$or": [
                        {"running": {"$ne": True}},
                        {"lock_acquired_at": {"$lt": stale_before}},
                    ],
                },
                {"$set": {
                    "pipeline": pipeline,
                    "running": True,
                    "lock_acquired_at": now,
                    "lock_run_id": run_id,
                }},
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
        except DuplicateKeyError:
            # Another concurrent caller's upsert won the race to create
            # this pipeline's document first -- we lost, so we don't hold
            # the lock.
            return False
        return doc.get("lock_run_id") == run_id
    finally:
        db.client.close()


def release_lock(pipeline: str, run_id: str) -> None:
    """Release the run lock, but only if `run_id` is still the one holding
    it -- if the lease already expired and a newer run reclaimed it, this
    must not clobber that newer run's lock."""
    db = get_mongo_db()
    try:
        db[_COLLECTION].update_one(
            {"pipeline": pipeline, "lock_run_id": run_id},
            {"$set": {"running": False}},
        )
    finally:
        db.client.close()


_RATE_LIMIT_COLLECTION = "api_rate_limit"


def check_trigger_rate_limit(max_requests: int = 3, window_seconds: int = 900) -> bool:
    """Basic in-Mongo rate limit for api/index.py's /api trigger route,
    mirroring the 3-per-15-minute limit already enforced by the front-end
    proxy -- but enforced server-side too, since a direct curl against the
    Vercel endpoint bypasses that proxy entirely. Records this request's
    timestamp and returns False (reject) if max_requests were already made
    in the last window_seconds, True (allow) otherwise. This is a
    low-traffic internal endpoint, so a simple count-and-prune query is
    sufficient -- no need for a dedicated rate-limiting service."""
    db = get_mongo_db()
    try:
        now = datetime.now(timezone.utc)
        window_start = now - timedelta(seconds=window_seconds)
        col = db[_RATE_LIMIT_COLLECTION]
        recent_count = col.count_documents({"ts": {"$gte": window_start}})
        if recent_count >= max_requests:
            return False
        col.insert_one({"ts": now})
        col.delete_many({"ts": {"$lt": window_start}})
        return True
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

      "proceed"          -> caller should download bytes and run the
                             pipeline; the run lock is now held under
                             logger.run_id and MUST be released via
                             release_lock(pipeline, logger.run_id) once the
                             run finishes, success or failure
      "skip_unchanged"   -> same drive_file_id + modified_time as the last
                             successful run; logger.finish("skipped") has
                             already been called -- caller must not touch
                             Mongo data collections at all
      "hard_fail"        -> file exceeds the safe-processing size ceiling
                             for a Vercel-triggered run; logger.finish("failed")
                             has already been called
      "already_running"  -> another run for this pipeline currently holds
                             the lock (not yet expired); logger.finish("skipped")
                             has already been called -- caller must not touch
                             Mongo data collections at all, same as
                             "skip_unchanged"

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

    if not acquire_lock(pipeline, logger.run_id):
        logger.log(
            "already_running", "skipped",
            f"another run for '{pipeline}' currently holds the lock (lease <= {LOCK_LEASE_SECONDS}s) -- skipping",
        )
        logger.finish("skipped")
        return "already_running"

    return "proceed"
