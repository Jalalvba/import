"""
api/index.py
-------------
Vercel Python serverless function that triggers the avis ETL pipeline
(Drive -> Mongo, no CSV intermediate) over HTTP, so a run can be kicked
off from a phone browser or curl instead of only `python run.py` locally.
Named index.py (not run-pipeline.py) because Vercel's Python runtime only
recognizes a fixed set of entrypoint filenames under api/ — app.py,
index.py, server.py, main.py, wsgi.py, asgi.py — so the route this
function serves is /api, not /api/run-pipeline.

Reuses run.py's run_all() directly rather than duplicating any
fetch/transform/push/logging logic — this file is only an HTTP + auth
wrapper around the existing, unmodified pipeline code. The JSON response
includes each pipeline's run_id and step summary (from run_all()), so a
run triggered here can be looked up afterward in the `pipeline_runs`
Mongo collection the same way a local run.py run can.

Skip-if-unchanged and size-tier handling live entirely in run_all() (see
lib/pipeline_state.py, lib/size_check.py) -- this file just passes
?force=true/1/yes through as run_all()'s `force` kwarg, which bypasses
the unchanged-skip (never the hard-fail size check) for a manual re-run,
e.g. after suspecting Mongo data was corrupted independently of the
source file:

    https://<deployment-domain>/api?token=<secret>&force=true

The response's `results[].status` is one of "success" / "failed" /
"skipped_absent" (file not in the Drive folder at all) /
"skipped_unchanged" (unchanged since the last successful run) -- a
top-level `summary` dict tallies counts per status so a caller doesn't
have to scan `results` to tell a real run from a no-op one.

This is still a single, synchronous Vercel invocation — one request in,
one JSON response out only once every pipeline has finished (or the
whole thing times out). It does NOT stream run_ids to the caller as
each pipeline starts, so a frontend can't discover *this* request's
run_ids until this response arrives, by which point every pipeline in
it is already done (there's no "list currently-running pipelines"
endpoint yet — only api/status.py's exact-run_id lookup). What
incremental writes actually buy today: lib/pipeline_log.py now writes
each step to Mongo as it happens rather than batching a pipeline's
whole step log into one write at the end, so (a) a document survives
with a partial step history even if the invocation is killed mid-run
(e.g. a maxDuration timeout) instead of losing that pipeline's whole
log the way the old batched write did, and (b) once a run_id IS known
(from a completed response, or a future "list recent runs" endpoint),
api/status.py shows its real per-step timeline rather than a single
end-of-run snapshot.

NOTE ON DURATION: running all four pipelines from Vercel's iad1 region
took ~45s+ end-to-end against real Drive/Mongo Atlas traffic (slower
than a local run) and blew past an initial maxDuration of 60, killing
the invocation mid-run before the last pipeline started -- confirmed
safe (no partial Mongo writes) by checking pipeline_runs and the
un-touched collection afterward, but still killed the run. maxDuration
is now 180 in vercel.json (functions["api/index.py"]) for headroom.
"""

import contextlib
import hmac
import io
import json
import os
import sys
import traceback
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse

# run.py and lib/ live at the repo root, one level up from api/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

import run as pipeline_run
from lib.gdrive import list_expected_files

ROOT = Path(__file__).resolve().parent.parent


def _run_all_pipelines(force: bool = False) -> list[dict]:
    """Lists all expected files' metadata from Drive once (no bytes
    downloaded yet), then delegates the per-pipeline skip-check/size-check/
    run/log/persist loop entirely to run.run_all() -- so this file doesn't
    duplicate that logic, and the response gets the same run_id + step
    summary + status per pipeline that run.py's own console output shows."""
    load_dotenv(dotenv_path=ROOT / ".env")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not folder_id:
        raise EnvironmentError("GOOGLE_DRIVE_FOLDER_ID not set")

    print(f"Listing input files in Drive folder {folder_id}...\n")
    file_metadata = list_expected_files(folder_id)

    return pipeline_run.run_all(file_metadata, force=force)


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self._handle()

    def do_POST(self):
        self._handle()

    def _handle(self):
        expected_token = os.getenv("PIPELINE_TRIGGER_SECRET")
        query = parse_qs(urlparse(self.path).query)
        token = (query.get("token") or [None])[0]
        force = (query.get("force") or [""])[0].strip().lower() in ("1", "true", "yes")

        # Constant-time comparison; bail out before touching Drive/Mongo
        # at all if the token is missing or wrong.
        if not expected_token or not token or not hmac.compare_digest(token, expected_token):
            self._respond(401, {"success": False, "error": "unauthorized"})
            return

        log_buffer = io.StringIO()
        try:
            with contextlib.redirect_stdout(log_buffer), contextlib.redirect_stderr(log_buffer):
                results = _run_all_pipelines(force=force)
        except Exception:
            traceback.print_exc(file=log_buffer)
            self._respond(500, {
                "success": False,
                "results": [],
                "log": log_buffer.getvalue(),
            })
            return

        summary = {"success": 0, "skipped_unchanged": 0, "skipped_absent": 0, "failed": 0}
        for r in results:
            summary[r["status"]] += 1

        any_failed = summary["failed"] > 0
        self._respond(200 if not any_failed else 207, {
            "success": not any_failed,
            "summary": summary,
            "results": results,
            "log": log_buffer.getvalue(),
        })

    def _respond(self, status_code: int, body: dict):
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
