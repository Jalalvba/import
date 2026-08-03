"""
lib/pipeline_log.py
--------------------
Structured, durable step-by-step logging for pipeline runs. Since the
CSV intermediate has been removed entirely from every pipeline, there is
no more output/*.csv artifact to inspect after the fact if something
goes wrong -- so each run's full step breakdown is persisted as one
document in the `pipeline_runs` MongoDB collection instead, which
survives independently of the console output (Vercel's /tmp would lose
that anyway once the invocation ends).

PipelineLogger.log() is the single source of truth for a step: it both
appends the step to the run's document and prints it to the console
immediately, so console output and the persisted record can never drift
out of sync with each other.
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
}


def _triggered_from() -> str:
    return "vercel" if os.getenv("VERCEL") else "local"


class PipelineLogger:
    """One instance per pipeline run. steps is an ordered list of
    {step, timestamp, status, detail} dicts recording exactly what
    happened, in order -- including whichever step was left at
    "started" with no matching "success"/"failed" if the run crashed
    mid-step."""

    def __init__(self, pipeline: str):
        self.run_id = str(uuid.uuid4())
        self.pipeline = pipeline
        self.triggered_from = _triggered_from()
        self.started_at = datetime.now(timezone.utc)
        self.finished_at = None
        self.status = "running"
        self.steps: list[dict] = []

    def log(self, step: str, status: str, detail: str = "") -> None:
        self.steps.append({
            "step": step,
            "timestamp": datetime.now(timezone.utc),
            "status": status,
            "detail": detail,
        })
        marker = _STATUS_MARKERS.get(status, "•")
        suffix = f": {detail}" if detail else ""
        print(f"  {marker} [{self.pipeline}] {step}{suffix}", flush=True)

    def finish(self, status: str) -> None:
        self.status = status
        self.finished_at = datetime.now(timezone.utc)

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


def persist_run(logger: PipelineLogger) -> None:
    """Insert this run's full step log as one document into
    pipeline_runs, via its own short-lived Mongo connection -- so the log
    can still be written even when the failure happened before the
    pipeline's own connection was ever opened. A Mongo outage here is
    reported to the console but never raised, so it can't mask or
    replace the pipeline's real exception."""
    try:
        db = get_mongo_db()
        db["pipeline_runs"].insert_one(logger.to_document())
        db.client.close()
    except Exception as e:
        print(f"  ⚠️  Could not persist run log for {logger.run_id} to pipeline_runs: {e}", flush=True)
