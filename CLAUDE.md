# CLAUDE.md

@AGENTS.md

This file provides guidance to Claude Code (claude.ai/code) when working
with code in this repository — the rules imported above apply regardless
of which AI assistant is driving. Gemini CLI / Antigravity sessions use
[`GEMINI.md`](./GEMINI.md); if the two disagree, that's a doc bug, fix
both.

## Project Overview

**avis** is a Python ETL pipeline that converts raw fleet vehicle management data from Excel (XLSX/XLS) files into clean, standardized records and pushes them directly to MongoDB Atlas — there is no CSV intermediate anywhere in the pipeline. Every run's full step-by-step breakdown is persisted durably in MongoDB itself (the `pipeline_runs` collection), since there's no more local file artifact to inspect after the fact.

## Setup & Running

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Input files are fetched directly from a Google Drive folder via a service
account — `GOOGLE_DRIVE_FOLDER_ID` (`.env`) is the single source of truth
for pipeline input, there is no local `input/` folder to populate anymore.
See `.env.example` for how to create the service account, generate +
base64-encode its JSON key, and share the target folder with it.

### Pipelines: one file per source, Drive → Mongo in a single run, no CSV

```bash
python ds.py    # YFACSCALDS.xlsx (Drive)             → `ds` collection   (date-scoped partial refresh)
python cp.py    # ConditionParticulieres.xls (Drive)  → `cp` collection   (atomic full reload)
python bc.py    # YBONTEC.xlsx (Drive)                → `bc` collection   (date-scoped partial refresh)
python parc.py  # Fullparcs.xls (Drive)               → `parc` collection (atomic full reload)
```

Each script runs the complete pipeline for its source — running any of these four scripts fetches its file from Drive, transforms it in memory, and pushes to MongoDB in one invocation, with no file written to disk at any point. `ds.py`/`bc.py` accept an optional year argument (`python ds.py 2026`, defaults to the current year). Run standalone, each script fetches only its own file from Drive (one API call). Every run — regardless of outcome — persists a full step-by-step log as one document in the `pipeline_runs` collection; see [pipeline_runs collection](#pipeline_runs-collection-durable-run-log) below.

### Orchestrator

```bash
python run.py
```

Lists all four expected filenames' **metadata** from the Drive folder **once** (a single Drive listing pass, no bytes downloaded yet — so running all four pipelines together costs one Drive round trip, not four), skips pipelines whose input file is absent from Drive (`YBONTEC.xlsx` is optional; the other three are required and `run.py` raises if any of them is missing), then for each present file checks `pipeline_state` and downloads + calls the matching pipeline module's `main()` in-process only if the file has actually changed since the last successful run (or `PIPELINE_FORCE_RUN=1` is set — see [Skip-if-unchanged](#skip-if-unchanged-pipeline_state-collection)). Continues to the next pipeline if one fails — a pipeline's own `sys.exit(1)` on a failed Mongo push no longer terminates `run.py`, since pipelines are called as in-process functions rather than subprocesses. `run.py`'s summary output distinguishes five outcomes per pipeline: `success` / `failed` / `skipped_absent` (file not in Drive) / `skipped_unchanged` (file unchanged since last success, or another run for this pipeline currently holds the run lock — see [pipeline_state collection](#skip-if-unchanged-pipeline_state-collection)) / `skipped_empty` (the file parsed to zero usable records — `pipeline_state` is deliberately not updated for this, so the next trigger retries).

A `tests/` pytest suite exists for the highest-risk logic (Mongo write
safety, auth checks, skip-if-unchanged). Run it with:

```bash
pip install -r requirements.txt
pytest tests/
```

Every test mocks Drive/Mongo entirely (via `mongomock` and hand-rolled
fakes in `tests/conftest.py`) — no `.env`, no network, no real credentials
required. There are no linters or CI configured.

## Architecture

### `lib/` — shared logic, used by all four pipelines

- **`lib/transform.py`** (pure, no I/O) — `format_date(val, formats)` normalizes dates to ISO 8601 (`YYYY-MM-DDTHH:MM:SS.000Z`, returns `""` for invalid/null); `clean_val(val)` strips Excel encoding artifacts (`_x000a_`, `_x000d_`, `_x[hex]_`) and collapses whitespace. `format_date` takes an explicit `formats` tuple rather than a merged list — `BC_DS_FORMATS` and `CP_PARC_FORMATS` are separate named constants because the two source systems have historically needed different tried-format orders (`BC_DS_FORMATS` uniquely tries `%m/%d/%Y`).
- **`lib/validate.py`** (pure, no I/O) — `validate_columns(df, required_columns)` raises `ValueError` (never a silent return) if any required column is missing.
- **`lib/mongo.py`** (I/O) — `get_mongo_db()` (`.env` loading + DNS resolver workaround + connected db handle); `df_to_mongo_records(df, date_columns)` (converts a pipeline's transformed DataFrame directly into Mongo-ready dicts — date columns parsed to Python `datetime`, blank fields dropped — with no CSV round trip); `atomic_reload(db, collection_name, records, index_specs)` (staged insert into `<collection>_staging`, verified count, atomic `rename(dropTarget=True)` swap — never a bare drop-then-insert, so the live collection is never dropped before the replacement data is fully written and verified); `date_scoped_reload(db, collection_name, records, date_field, earliest_date, year)` (deletes only `[earliest_date, end-of-year)`, then inserts — records before `earliest_date` are untouched); `log_refresh_counts(before, after)` (before/after/diff print).
- **`lib/gdrive.py`** (I/O) — `list_drive_files(folder_id)` (Drive API v3, `drive.readonly` scope, returns `[{id, name, mimeType, modifiedTime, size}, ...]`); `download_file_bytes(file_id)` (in-memory `files().get_media()` download, no temp files); `list_expected_files(folder_id)` (lists all four known pipeline filenames' **metadata only** in one pass, deduping by latest `modifiedTime` when a name repeats — no bytes downloaded — used by `run.py`/`api/index.py` so the skip-if-unchanged check below can run before paying for a download); `get_latest_expected_files(folder_id)` (same listing, but downloads every survivor unconditionally — kept only for the manual `test_gdrive_download.py` debugging tool, no longer used by the pipelines themselves); `find_file_metadata(folder_id, filename)` (single-file metadata lookup, no download, `None` if absent — used by each pipeline script's standalone `__main__` block for the same skip-if-unchanged check). Auth loads `GOOGLE_SERVICE_ACCOUNT_KEY_B64` from `.env`, base64-decodes + JSON-parses it, and builds `Credentials` in memory — the decoded key is never written to disk.
- **`lib/pipeline_log.py`** (I/O) — `PipelineLogger` (one instance per pipeline run; `.log(step, status, detail)` records a step — `started`/`success`/`failed`/`skipped`/`warning` — appends it to an in-memory list, prints it immediately, and writes it to the run's `pipeline_runs` document incrementally (an atomic `$push` per step, not one batched write at the end), so console output and the persisted record can never drift apart and a hard kill mid-run (e.g. a Vercel `maxDuration` timeout) still leaves a partial step history instead of losing the whole log; `.finish(status)` flips `status`/`finished_at` in place; `.to_document()`); `get_run_status(run_id)` reads a run's current `pipeline_runs` document by `run_id` — used by `api/index.py`'s `/api/status` route to poll a run's live progress before it finishes.
- **`lib/pipeline_state.py`** (I/O) — `get_state(pipeline)`/`update_state(...)` read/upsert the `pipeline_state` collection (see below); `force_requested()` reads `PIPELINE_FORCE_RUN` from the environment; `resolve_pipeline_run(pipeline, file_meta, logger, force=False)` is the shared skip-if-unchanged + size-tier decision used by both `run.py` and each script's standalone `__main__` — see [Skip-if-unchanged](#skip-if-unchanged-pipeline_state-collection) and [File Size Handling](#file-size-handling--heavy-excel-exports) below.
- **`lib/size_check.py`** (pure, no I/O) — `WARN_BYTES`/`HARD_FAIL_BYTES` thresholds and `classify(size_bytes)` (`"unknown"|"normal"|"warn"|"hard_fail"`), checked against Drive's own reported file size before any download.

### Pipeline scripts (`ds.py`, `cp.py`, `bc.py`, `parc.py`)

Each is a standalone, independently-runnable module with the same shape. Every step below is recorded via a `PipelineLogger` (`lib/pipeline_log.py`) passed into `extract_transform()`/`push_to_mongo()` — the same `.log()` call both prints to the console and appends to the run's `pipeline_runs` document, so the two can never drift out of sync:

1. **Fetch** the source file's bytes from the Google Drive folder (`GOOGLE_DRIVE_FOLDER_ID` in `.env`) via `lib.gdrive` — but only after two gates, both logged as their own steps: a `skip_check` step compares the file's Drive `id` + `modifiedTime` against `pipeline_state` (see [Skip-if-unchanged](#skip-if-unchanged-pipeline_state-collection) below) and skips the entire run — no download, no write to the pipeline's data collection — if unchanged since the last successful run; then a `size_check` step classifies the file's Drive-reported byte size (see [File Size Handling](#file-size-handling--heavy-excel-exports) below) as normal / warn / hard-fail. Immediately after `download_file_bytes()` returns, `lib.gdrive.verify_download_size()` compares the actual byte count against Drive's own reported `size` (already fetched during listing) and raises if they don't match — catching a short read that Drive terminates as a clean 200, which would otherwise let calamine/openpyxl silently parse a truncated file as fewer rows instead of erroring. A mismatch is logged as a `file_download: failed` step and fails that pipeline's run (run via `run.py`, the other three pipelines still proceed). Run standalone, a script's own `if __name__ == "__main__"` block looks up its file's metadata only (`lib.gdrive.find_file_metadata`, no bytes yet), runs both gates, then downloads, verifies the size, and logs `drive_auth`/`drive_listing`/`skip_check`/`size_check`/`file_download` steps before invoking `main()`; run via `run.py`, all four files' **metadata** is listed once upfront (one shared Drive listing pass, no bytes yet) and the same gates + steps are logged into each pipeline's own logger, with bytes downloaded only for the pipelines that actually need to run
2. **Read** Excel from those in-memory bytes (`pd.read_excel(io.BytesIO(file_bytes), ...)`) — XLSX files use openpyxl engine, legacy XLS files use calamine engine (`excel_parse` step)
3. **Validate** required columns exist via `lib.validate.validate_columns` (fail loudly if missing; `column_validation` step)
4. **Transform** — apply `lib.transform.format_date()`/`clean_val()`
5. **Filter** rows based on business rules (drop records missing key identifiers), and for `cp.py`, deduplicate by WW identifier (preferring a valid `IMM` code, keeping the latest contract end date) — this dedup logic is CP-specific and stays inline rather than living in `lib/`. Steps 4–5 together are logged as one `transform_filter` step with rows-in/rows-out/dropped-and-why detail
6. **Push** directly to MongoDB via `lib.mongo` — the transformed DataFrame is converted straight to Mongo-ready records in memory via `df_to_mongo_records()`, with no CSV or other file written at any point (`mongo_connect` and `mongo_push` steps, the latter carrying the before/after/diff counts):
   - `ds.py`/`bc.py` — date-scoped partial refresh (`date_scoped_reload`), scoped on `Date DS`/`Date BC` respectively
   - `cp.py`/`parc.py` — atomic full reload (`atomic_reload`)
7. **Finish the run log** — `main()` calls `logger.finish("success"|"failed")` in a `try/except`, which flips the already-incrementally-written `pipeline_runs` document's `status`/`finished_at` in place (including a `pipeline_error` step with the exception detail on failure) — there's no separate persist step, since every prior step was already written to Mongo as it happened (see the `lib/pipeline_log.py` bullet above)

`parc.py` and `cp.py` read legacy XLS with `engine="calamine"`.

Each script's `extract_transform()`/`main()` take `file_bytes: bytes` as a parameter rather than reading a hardcoded local path — there is no `INPUT_FILE`/`INPUT_DIR` constant in any of the four scripts anymore, and the local `input/` folder has been deleted entirely (it's gone from disk, not just unreferenced). There is likewise no `output/` folder, `OUTPUT_CSV` constant, or `lib/paths.py` anymore — the CSV intermediate was removed entirely, not just made conditional on environment.

Each script's `main()` returns a status string: `"success"` (records were pushed), `"skipped"` (the source file parsed to zero usable records after filtering — nothing was pushed), or it raises on failure. This distinction matters because a **zero-record run must never be recorded as `"success"`** — `pipeline_state` (see below) is only updated when `main()` returns `"success"`, so a genuinely empty/broken export doesn't get remembered as "already processed" and silently blocks all future retries via skip-if-unchanged. `run.py`'s `run_pipeline()` maps this to a `"skipped_empty"` result status, distinct from `"skipped_unchanged"`/`"skipped_absent"`.

### `pipeline_runs` collection (durable run log)

Since there's no more CSV artifact to inspect after a run, every pipeline invocation — local, standalone, or triggered via `run.py`/`api/index.py` — writes exactly one document to the `pipeline_runs` collection, whether it succeeded, failed, or was skipped (e.g. `bc.py` when `YBONTEC.xlsx` is absent from Drive). Document shape:

```
{
  run_id: "<uuid4>",
  pipeline: "ds" | "cp" | "bc" | "parc",
  started_at: <datetime>,
  finished_at: <datetime | null>,
  status: "success" | "failed" | "skipped",
  triggered_from: "vercel" | "local",
  steps: [
    { step: "drive_auth", timestamp: <datetime>, status: "success", detail: "..." },
    { step: "excel_parse", timestamp: <datetime>, status: "success", detail: "read 11934 rows, 52 columns" },
    { step: "mongo_push", timestamp: <datetime>, status: "success", detail: "before=257433 after=257433 diff=0" },
    ...
  ],
}
```

To check what happened on a recent run, query it directly in `mongosh` (or via `pymongo`):

```js
db.pipeline_runs.find().sort({ started_at: -1 }).limit(5)          // most recent 5 runs, any pipeline
db.pipeline_runs.find({ pipeline: "cp" }).sort({ started_at: -1 }).limit(1)   // last cp.py run
db.pipeline_runs.find({ status: "failed" }).sort({ started_at: -1 })          // every failed run
```

A step left at `"started"` with no matching `"success"`/`"failed"` entry after it means the run crashed mid-step — that's the first thing to look at when debugging a failure.

**Indexes:** `lib/pipeline_log.py`'s `PipelineLogger` lazily creates two indexes on `pipeline_runs` on first write of a run (idempotent, no-op once they exist): a unique index on `run_id` (so `/api/status` lookups — high-traffic since it's unauthenticated — aren't a full collection scan), and a TTL index on `started_at` with a 90-day expiry (`expireAfterSeconds`), so the collection doesn't grow unbounded from an unauthenticated, free-to-hit endpoint. Old run documents past 90 days are pruned automatically by Mongo; there's no manual cleanup step.

## Skip-if-unchanged (`pipeline_state` collection)

The `pipeline_state` collection holds exactly one document per pipeline
(`ds`/`cp`/`parc`/`bc`), tracking the last **successfully** processed file:

```
{
  pipeline: "ds" | "cp" | "parc" | "bc",
  filename: "YFACSCALDS.xlsx",
  drive_file_id: "<drive file id>",
  modified_time: "<Drive's modifiedTime, RFC3339 string>",
  processed_at: <datetime>,
  run_id: "<uuid4, links back to the pipeline_runs document>",
}
```

Before downloading a file's bytes, `lib.pipeline_state.resolve_pipeline_run()`
compares the currently-listed Drive file's `id` + `modifiedTime` against this
document (via `lib.gdrive.list_expected_files`/`find_file_metadata`, which
list metadata only — `download_file_bytes()` is never called for a file
that turns out to be unchanged). If identical, the whole run is skipped: a
single `skip_check` step (status `"skipped"`, detail `"unchanged since
<timestamp>, skipping run"`) is logged, the run finishes immediately, and
**nothing else is touched** — no download, no write to `ds`/`cp`/`parc`/`bc`.
If different (new file, changed `modifiedTime`, or no `pipeline_state`
document yet — first run), the pipeline runs normally and `pipeline_state`
is updated **only on success** — a failed run is never remembered as
"processed," so the next trigger retries it properly.

**Forcing a re-run** even when unchanged (for manual testing, or recovering
from Mongo data that was corrupted independently of the source file) bypasses
the skip check but never the hard-fail size check (below):

```bash
PIPELINE_FORCE_RUN=1 python3 run.py       # or ds.py/cp.py/bc.py/parc.py directly
```

```
https://<deployment-domain>/api?token=<secret>&force=true
```

## File Size Handling — heavy Excel exports

Right after Drive's listing call (before any download), `lib.size_check`
classifies each file's Drive-reported `size` (bytes) into a tier, logged as
a `size_check` step:

- **normal** (≤ `WARN_BYTES` = 20MB) — no special handling. Chosen as
  roughly 2x the largest file seen in production to date (`cp.py`'s
  `ConditionParticulieres.xls`, ~11MB), so ordinary month-to-month growth
  doesn't trigger noise.
- **warn** (> 20MB, ≤ `HARD_FAIL_BYTES` = 100MB) — logged as a `"warning"`
  step ("unusually large, processing may take longer / use more memory");
  the pipeline still runs normally. A "full year of data" export landing in
  this range is expected and fine.
- **hard_fail** (> 100MB) — **on a Vercel-triggered run only**
  (`VERCEL` env var set), the run fails fast with a clear `size_check`
  step ("file exceeds safe processing size for Vercel — run this pipeline
  locally instead: `python3 <script>.py`") and never attempts a download,
  avoiding a silent OOM kill or `maxDuration` timeout. **A local run is not
  blocked** at this tier — it logs a `"warning"` step and proceeds, since a
  laptop doesn't share Vercel's memory/duration ceiling and running locally
  is the documented escape hatch for a genuinely huge (e.g. multi-year)
  export. `force=true`/`PIPELINE_FORCE_RUN` do **not** bypass this — force
  only bypasses the unchanged-skip, not the size safety check.

See [Vercel HTTP Trigger](#vercel-http-trigger) below for the current
`maxDuration`/`memory` ceiling these thresholds are sized against.

## Vercel HTTP Trigger

`api/index.py` is a **single** Vercel Python serverless function that serves two routes, dispatched by manually parsing `self.path` inside one `handler` class:

- `GET/POST /api?token=<PIPELINE_TRIGGER_SECRET>` — token-gated trigger. Exposes `run.py`'s `run_all()` over HTTP so the pipeline can be kicked off from a phone browser or `curl` instead of only `python run.py` locally. Reuses `run_all()` directly; does not duplicate any fetch/transform/push/logging logic. Skip-if-unchanged and size-tier handling apply here exactly as they do locally (see [Skip-if-unchanged](#skip-if-unchanged-pipeline_state-collection) and [File Size Handling](#file-size-handling--heavy-excel-exports) above) — add `&force=true` to bypass the unchanged-skip for a manual re-run.
- `GET /api/status?run_id=<uuid>` — read-only, **no token required** (see the docstring in `api/index.py` for why that's deliberate: `run_id` is an unguessable UUID4, and this route touches nothing but a `pipeline_runs` read). Returns the current `pipeline_runs` document for that `run_id` — since `PipelineLogger` writes steps incrementally, this shows a run's live progress before it finishes, not just an end-of-run snapshot.

Any other path under `/api/*` (including a typo'd or nonexistent one) returns a plain `404`.

**Both routes live in one file/class — this is not a stylistic choice.** Vercel's Python runtime only recognizes a fixed set of entrypoint filenames under `api/` (`index.py`, `app.py`, `server.py`, `main.py`, `wsgi.py`, `asgi.py`) and treats whichever one is present as the sole handler for the *entire* `/api/*` path — a second file like a former `api/status.py` is never actually routed to independently. This project shipped exactly that bug once: `api/status.py` existed as a separate file implementing the status-lookup route, but every request to `/api/status` was silently landing on `api/index.py`'s handler instead — which has no route for it and, worse, no token check for it either, so `/api/status` calls returned the trigger route's `401 unauthorized` no matter what. `curl` against `/api/status`, `/api`, and even a nonexistent path all returned the identical `401`, which is what surfaced the bug. The fix was merging `api/status.py`'s logic into `api/index.py` as a second branch, selected by checking `urlparse(self.path).path`, and deleting `api/status.py` entirely. **If a future session is tempted to split this back into two files for tidiness, don't — it silently reintroduces this exact bug**, since Vercel will accept the extra file without error and simply never route to it.

**Endpoint URL pattern:**

```
https://<deployment-domain>/api?token=<PIPELINE_TRIGGER_SECRET>
https://<deployment-domain>/api?token=<PIPELINE_TRIGGER_SECRET>&force=true
https://<deployment-domain>/api/status?run_id=<uuid-from-a-prior-response>
```

`PIPELINE_TRIGGER_SECRET` is required as a query param (`?token=...`) and is checked with a constant-time comparison (`hmac.compare_digest`) against the `PIPELINE_TRIGGER_SECRET` env var set in Vercel's dashboard (Production, Preview, Development). A missing or wrong token returns `401` immediately, before Drive or Mongo are touched at all. Both `GET` and `POST` are accepted identically.

> **⚠️ The URL+token combination is equivalent to a password for triggering real production Mongo writes.** Anyone with it can run all four pipelines against Atlas at will. Treat it with exactly the same care as `MONGODB_URI`:
> - Never commit it to git, in any file, including examples or docs (`.env.example` uses placeholders only — see [AGENTS.md rule 1](./AGENTS.md)).
> - Never paste the full URL+token into a shared doc, ticket, or chat that isn't already trusted with production Mongo access.
> - Never log it anywhere retrievable (shell history included — see the Termux alias pattern below, which keeps the token in an exported env var rather than typed/pasted inline).
> - If it leaks, rotate it immediately: generate a new value (`python3 -c "import secrets; print(secrets.token_urlsafe(32))"`), update `PIPELINE_TRIGGER_SECRET` in Vercel's dashboard for all three environments, and redeploy.

**Looking up a specific run afterward:** the endpoint's JSON response includes a `run_id` per pipeline (`results[].run_id`). Look it up directly in Mongo:

```js
db.pipeline_runs.find_one({ run_id: "<id>" })
```

**Duration:** `maxDuration` is `180` (in `vercel.json`, `functions["api/index.py"]`). It started at `60` but a live production trigger hit that cap and was killed mid-run — DS/CP/PARC had already completed successfully, but BC never started, confirmed safe only because `bc`'s collection count matched its last known-good state (no partial `date_scoped_reload` delete-without-reinsert occurred). Real Drive + Mongo Atlas round trips from Vercel's `iad1` region run slower than a local run (~66s end-to-end for all four pipelines observed in testing), so `180` gives real headroom rather than being flush against the observed time.

**Memory:** `vercel.json`'s `functions["api/index.py"]` requests `"memory": 2048` (MB), up from the platform default (1024 MB), for headroom against a larger-than-tested file (e.g. a full year of data). This was previously set to `3009` (the documented absolute maximum configurable value), but a deployment on this project's Hobby plan clamped/rejected it — `2048` is the actual usable ceiling on Hobby, confirmed by checking the effective value after deploying (Vercel dashboard → Project → Functions, or `vercel inspect`) rather than assuming the requested number took effect. **Do not bump this back toward 3009 without first confirming the plan has been upgraded past Hobby** — otherwise it's a config value Vercel will silently clamp again. Together, `maxDuration: 180` + `memory: 2048` are the practical ceiling for what "too big to run on Vercel" means in this project — a file that trips the `hard_fail` size tier (see [File Size Handling](#file-size-handling--heavy-excel-exports)) is explicitly meant to be run locally instead of pushed against these limits.

**Bundle size watch item:** the build log reports `Bundle size (228.12 MB) exceeds the standard size; optimizing dependencies` — Vercel's standard compressed-size limit is 250MB. It builds and deploys fine today, but there's limited headroom left; adding further heavy dependencies (another large Python package, a new SDK) could push this over the limit and break the build. Check the build log's bundle-size line after any `requirements.txt` change.

## Data & File Paths

All paths below are relative to the repo root (wherever this project is checked out) — nothing is hardcoded to a fixed home-directory location, though older docstrings in some scripts still reference a prior `~/avis/` path from before the project was renamed.

```
./
├── run.py                  # orchestrator: lists all input files' metadata from Drive once, skips unchanged files, runs the rest
├── ds.py                   # YFACSCALDS.xlsx (Drive) → `ds` (date-scoped), no CSV
├── cp.py                   # ConditionParticulieres.xls (Drive) → `cp` (atomic full reload), no CSV
├── bc.py                   # YBONTEC.xlsx (Drive) → `bc` (date-scoped), no CSV
├── parc.py                 # Fullparcs.xls (Drive) → `parc` (atomic full reload), no CSV
├── lib/
│   ├── transform.py        # pure: clean_val(), format_date()
│   ├── validate.py         # pure: validate_columns()
│   ├── mongo.py             # I/O: get_mongo_db(), df_to_mongo_records(), atomic_reload(), date_scoped_reload(), log_refresh_counts()
│   ├── gdrive.py            # I/O: list_drive_files(), download_file_bytes(), list_expected_files(), find_file_metadata(), get_latest_expected_files()
│   ├── pipeline_log.py      # I/O: PipelineLogger (incremental writes), get_run_status() — durable step log → `pipeline_runs` collection
│   ├── pipeline_state.py    # I/O: get_state(), update_state(), resolve_pipeline_run() — skip-if-unchanged + size gate → `pipeline_state` collection
│   └── size_check.py        # pure: WARN_BYTES, HARD_FAIL_BYTES, classify()
├── api/
│   └── index.py             # single Vercel serverless function, manually path-routed: /api (token-gated trigger, ?force=true) + /api/status (no token, live progress polling) — one file because Vercel's Python runtime won't route a second file under api/ independently
├── test_gdrive_auth.py      # optional manual debugging tool — Drive auth + listing only
├── test_gdrive_download.py  # optional manual debugging tool — Drive download + signature check
├── vercel.json              # maxDuration + memory override for api/index.py
├── .env                    # never committed
└── requirements.txt
```

## Environment Variables (.env)

```
MONGODB_URI=mongodb+srv://...
MONGODB_DB=avis
GOOGLE_SERVICE_ACCOUNT_KEY_B64=...   # base64 -w 0 of the full service account JSON key
GOOGLE_DRIVE_FOLDER_ID=...           # the Drive folder ds.py/cp.py/parc.py/bc.py fetch input from
```

`PIPELINE_TRIGGER_SECRET` is a separate, Vercel-only env var — it's not read from local `.env` (the four pipeline scripts and `run.py` never touch it), only from Vercel's dashboard by `api/index.py`. See [Vercel HTTP Trigger](#vercel-http-trigger) above.

`PIPELINE_FORCE_RUN` is an optional, ephemeral runtime override, not a
persistent `.env` entry — set it inline for a single invocation
(`PIPELINE_FORCE_RUN=1 python3 run.py`) to bypass the skip-if-unchanged
check. See [Skip-if-unchanged](#skip-if-unchanged-pipeline_state-collection)
above.

## Where to find more detail

- **[`AGENTS.md`](./AGENTS.md)** — mandatory cross-assistant rules, imported at the top of this file.
- **[`GEMINI.md`](./GEMINI.md)** — the equivalent entry point for Gemini CLI / Antigravity sessions.
- **[`CHANGELOG.md`](./CHANGELOG.md)** — narrative summary of how the system reached its current shape (Drive migration, CSV removal, Vercel HTTP trigger, skip/size safety, bugs found and fixed) — read this before `git log` if you need the "why," not just the "what."
