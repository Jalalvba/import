"""
lib/pipeline_log.py
--------------------
Structured, durable step-by-step logging for pipeline runs, written to
MongoDB incrementally as each step happens rather than batched into one
write at the end. Since the CSV intermediate has been removed entirely
from every pipeline, there is no more output/*.csv artifact to inspect
after the fact if something goes wrong -- so a run's `pipeline_runs`
document is the only record of what happened, and it needs to reflect
reality *during* the run (for a live-updating frontend, and so a hard
kill -- e.g. a Vercel maxDuration timeout -- leaves behind whatever
steps actually completed instead of losing them all).

PipelineLogger.log() is the single source of truth for a step: it
appends the step to steps in memory, prints it to the console
immediately, and pushes it to the persisted document in the same call
-- console output and the persisted record can never drift out of sync,
and Mongo is never more than one step behind the console.
"""

import os
import uuid
from datetime import datetime, timezone

from lib.mongo import get_mongo_db

_STATUS_MARKERS = {
    "started": "▶",
    "success": "✅",
    "failed": "❌",
    "skipped": "⚠️",
    "warning": "🟠",
}

_COLLECTION = "pipeline_runs"


def _triggered_from() -> str:
    return "vercel" if os.getenv("VERCEL") else "local"


class PipelineLogger:
    """One instance per pipeline run. steps is an ordered in-memory copy
    of every {step, timestamp, status, detail} dict recorded so far --
    including whichever step was left at "started" with no matching
    "success"/"failed" if the run crashed mid-step. The same data is
    mirrored into Mongo incrementally: an empty-steps document is
    inserted on the first log() call, then each step (including that
    first one) is appended via an atomic $push, and finish() flips
    status/finished_at in place. Opens one Mongo connection lazily on
    first use and reuses it for the rest of the run's writes."""

    def __init__(self, pipeline: str):
        self.run_id = str(uuid.uuid4())
        self.pipeline = pipeline
        self.triggered_from = _triggered_from()
        self.started_at = datetime.now(timezone.utc)
        self.finished_at = None
        self.status = "running"
        self.steps: list[dict] = []
        self._db = None
        self._inserted = False

    def _collection(self):
        if self._db is None:
            self._db = get_mongo_db()
        return self._db[_COLLECTION]

    def log(self, step: str, status: str, detail: str = "") -> None:
        entry = {
            "step": step,
            "timestamp": datetime.now(timezone.utc),
            "status": status,
            "detail": detail,
        }
        self.steps.append(entry)
        marker = _STATUS_MARKERS.get(status, "•")
        suffix = f": {detail}" if detail else ""
        print(f"  {marker} [{self.pipeline}] {step}{suffix}", flush=True)
        self._write_step(entry)

    def _write_step(self, entry: dict) -> None:
        now = datetime.now(timezone.utc)
        try:
            col = self._collection()
            if not self._inserted:
                col.insert_one({
                    "run_id": self.run_id,
                    "pipeline": self.pipeline,
                    "started_at": self.started_at,
                    "finished_at": None,
                    "status": self.status,
                    "triggered_from": self.triggered_from,
                    "steps": [],
                    "last_updated": now,
                })
                self._inserted = True
            col.update_one(
                {"run_id": self.run_id},
                {"$push": {"steps": entry}, "$set": {"last_updated": now}},
            )
        except Exception as e:
            print(f"  ⚠️  Could not write step to {_COLLECTION} for {self.run_id}: {e}", flush=True)

    def finish(self, status: str) -> None:
        self.status = status
        self.finished_at = datetime.now(timezone.utc)
        try:
            col = self._collection()
            if not self._inserted:
                # Defensive fallback: finish() called with zero prior log()
                # calls shouldn't normally happen (every code path logs at
                # least one step, even on an immediate failure), but insert
                # the full document rather than silently losing the run.
                col.insert_one(self.to_document())
                self._inserted = True
            else:
                col.update_one(
                    {"run_id": self.run_id},
                    {"$set": {
                        "status": self.status,
                        "finished_at": self.finished_at,
                        "last_updated": self.finished_at,
                    }},
                )
        except Exception as e:
            print(f"  ⚠️  Could not finalize {_COLLECTION} status for {self.run_id}: {e}", flush=True)
        finally:
            if self._db is not None:
                self._db.client.close()
                self._db = None

    def to_document(self) -> dict:
        return {
            "run_id": self.run_id,
            "pipeline": self.pipeline,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "status": self.status,
            "triggered_from": self.triggered_from,
            "steps": self.steps,
        }


def get_run_status(run_id: str) -> dict | None:
    """Read-only lookup of a run's current pipeline_runs document by
    run_id -- used by api/status.py to poll a run's live progress.
    Returns None if no such run_id exists (yet, or ever)."""
    db = get_mongo_db()
    try:
        return db[_COLLECTION].find_one({"run_id": run_id})
    finally:
        db.client.close()
