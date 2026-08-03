"""
api/index.py
-------------
Vercel Python serverless function serving TWO routes for the avis ETL
pipeline (Drive -> Mongo, no CSV intermediate):

    GET/POST /api?token=<secret>            -- triggers a full pipeline run
    GET/POST /api?token=<secret>&force=true  -- same, bypassing unchanged-skip
    GET      /api/status?run_id=<uuid>       -- read-only progress poll, NO token

Both routes are handled by this single file/class rather than living in
separate api/index.py + api/status.py files. This is NOT a stylistic
choice -- Vercel's Python runtime treats api/index.py as the sole
entrypoint for the whole /api/* path (index.py, app.py, server.py,
main.py, wsgi.py, asgi.py are the only filenames it recognizes as
entrypoints at all), so a second file like api/status.py is never
actually routed to; every request under /api/* -- including
/api/status and even nonexistent paths -- hits this handler. A prior
version of this project split status polling into its own api/status.py
and it silently never worked: curl against /api/status returned the
exact same 401 as the token-gated trigger route, because /api/status
was resolving to this file's handler with no token check at all. If a
future reader is tempted to "clean up" this file by splitting the
status branch back out into its own api/status.py, don't -- it will
reintroduce this exact bug. Route dispatch below is manual, based on
parsing self.path (Vercel's BaseHTTPRequestHandler-style functions
receive the full incoming request path, including query string, in
self.path -- the same attribute the trigger logic already parses for
its own query params).

Reuses run.py's run_all() directly rather than duplicating any
fetch/transform/push/logging logic -- the trigger branch is only an
HTTP + auth wrapper around the existing, unmodified pipeline code. The
JSON response includes each pipeline's run_id and step summary (from
run_all()), so a run triggered here can be looked up afterward via the
/api/status route, or directly in the `pipeline_runs` Mongo collection.

Skip-if-unchanged and size-tier handling live entirely in run_all() (see
lib/pipeline_state.py, lib/size_check.py) -- this file just passes
?force=true/1/yes through as run_all()'s `force` kwarg, which bypasses
the unchanged-skip (never the hard-fail size check) for a manual re-run,
e.g. after suspecting Mongo data was corrupted independently of the
source file:

    https://<deployment-domain>/api?token=<secret>&force=true

The trigger response's `results[].status` is one of "success" / "failed"
/ "skipped_absent" (file not in the Drive folder at all) /
"skipped_unchanged" (unchanged since the last successful run) -- a
top-level `summary` dict tallies counts per status so a caller doesn't
have to scan `results` to tell a real run from a no-op one.

The trigger route is still a single, synchronous Vercel invocation --
one request in, one JSON response out only once every pipeline has
finished (or the whole thing times out). It does NOT stream run_ids to
the caller as each pipeline starts, so a frontend can't discover *this*
request's run_ids until this response arrives, by which point every
pipeline in it is already done. What incremental writes actually buy
today: lib/pipeline_log.py writes each step to Mongo as it happens
rather than batching a pipeline's whole step log into one write at the
end, so (a) a document survives with a partial step history even if the
invocation is killed mid-run (e.g. a maxDuration timeout) instead of
losing that pipeline's whole log the way an old batched write would,
and (b) once a run_id IS known (from a completed trigger response, or a
future "list recent runs" endpoint), the /api/status route shows its
real per-step timeline rather than a single end-of-run snapshot.

The /api/status route requires no auth token, unlike the trigger route
-- this is deliberate, not an oversight. Read-only status lookups don't
trigger Drive fetches or Mongo writes to the actual fleet-data
collections, and run_id is a random UUID4 (122 bits of entropy)
generated server-side and never guessable, so it functions as its own
unguessable capability token: you can only look up a run you already
know the run_id of (e.g. from a prior trigger response). If that
tradeoff ever needs to change, add the same token check the trigger
route uses.

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
from lib.pipeline_log import get_run_status

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
        path = urlparse(self.path).path.rstrip("/") or "/"

        if path == "/api/status":
            self._handle_status()
        elif path in ("/api", "/api/index"):
            self._handle_trigger()
        else:
            self._respond(404, {"success": False, "error": "not found"})

    def _handle_status(self):
        load_dotenv(dotenv_path=ROOT / ".env")

        query = parse_qs(urlparse(self.path).query)
        run_id = (query.get("run_id") or [None])[0]

        if not run_id:
            self._respond(400, {"error": "run_id query param is required"})
            return

        try:
            doc = get_run_status(run_id)
        except Exception as e:
            self._respond(500, {"error": f"could not read run status: {e}"})
            return

        if doc is None:
            self._respond(404, {"error": f"no run found for run_id {run_id}"})
            return

        doc.pop("_id", None)
        self._respond(200, doc)

    def _handle_trigger(self):
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
        payload = json.dumps(body, default=str).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
