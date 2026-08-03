"""
api/status.py
--------------
Vercel Python serverless function for read-only polling of a pipeline
run's live progress: GET /api/status?run_id=<uuid> returns the current
pipeline_runs document for that run_id, whose `steps` array grows in
real time as lib/pipeline_log.py's PipelineLogger writes each step
incrementally (rather than only appearing once the run finishes).

No auth token required, unlike api/index.py -- this is deliberate, not
an oversight. Read-only status lookups don't trigger Drive fetches or
Mongo writes to the actual fleet-data collections, and run_id is a
random UUID4 (122 bits of entropy) generated server-side and never
guessable, so it functions as its own unguessable capability token: you
can only look up a run you already know the run_id of (e.g. from a
prior api/index.py response). If that tradeoff ever needs to change,
add the same token check api/index.py uses.
"""

import json
import os
import sys
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse

# lib/ lives at the repo root, one level up from api/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

from lib.pipeline_log import get_run_status

ROOT = Path(__file__).resolve().parent.parent


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
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

    def _respond(self, status_code: int, body: dict):
        payload = json.dumps(body, default=str).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
