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

Fetches all four expected filenames from the Drive folder **once** (a single Drive listing + download pass, so running all four pipelines together costs one round trip, not four), then calls each matching pipeline module's `main()` in-process for every file that was found, skipping pipelines whose input file is absent from Drive (`YBONTEC.xlsx` is optional; the other three are required and `run.py` raises if any of them is missing). Continues to the next pipeline if one fails — a pipeline's own `sys.exit(1)` on a failed Mongo push no longer terminates `run.py`, since pipelines are called as in-process functions rather than subprocesses.

There are no tests, linters, or CI configured.

## Architecture

### `lib/` — shared logic, used by all four pipelines

- **`lib/transform.py`** (pure, no I/O) — `format_date(val, formats)` normalizes dates to ISO 8601 (`YYYY-MM-DDTHH:MM:SS.000Z`, returns `""` for invalid/null); `clean_val(val)` strips Excel encoding artifacts (`_x000a_`, `_x000d_`, `_x[hex]_`) and collapses whitespace. `format_date` takes an explicit `formats` tuple rather than a merged list — `BC_DS_FORMATS` and `CP_PARC_FORMATS` are separate named constants because the two source systems have historically needed different tried-format orders (`BC_DS_FORMATS` uniquely tries `%m/%d/%Y`).
- **`lib/validate.py`** (pure, no I/O) — `validate_columns(df, required_columns)` raises `ValueError` (never a silent return) if any required column is missing.
- **`lib/mongo.py`** (I/O) — `get_mongo_db()` (`.env` loading + DNS resolver workaround + connected db handle); `df_to_mongo_records(df, date_columns)` (converts a pipeline's transformed DataFrame directly into Mongo-ready dicts — date columns parsed to Python `datetime`, blank fields dropped — with no CSV round trip); `atomic_reload(db, collection_name, records, index_specs)` (staged insert into `<collection>_staging`, verified count, atomic `rename(dropTarget=True)` swap — never a bare drop-then-insert, so the live collection is never dropped before the replacement data is fully written and verified); `date_scoped_reload(db, collection_name, records, date_field, earliest_date, year)` (deletes only `[earliest_date, end-of-year)`, then inserts — records before `earliest_date` are untouched); `log_refresh_counts(before, after)` (before/after/diff print).
- **`lib/gdrive.py`** (I/O) — `list_drive_files(folder_id)` (Drive API v3, `drive.readonly` scope, returns `[{id, name, mimeType, modifiedTime}, ...]`); `download_file_bytes(file_id)` (in-memory `files().get_media()` download, no temp files); `get_latest_expected_files(folder_id)` (fetches all four known pipeline filenames in one listing pass, deduping by latest `modifiedTime` when a name repeats — used by `run.py`); `fetch_file(folder_id, filename)` (single-file fetch — used when a pipeline script is run standalone). Auth loads `GOOGLE_SERVICE_ACCOUNT_KEY_B64` from `.env`, base64-decodes + JSON-parses it, and builds `Credentials` in memory — the decoded key is never written to disk.
- **`lib/pipeline_log.py`** (I/O) — `PipelineLogger` (one instance per pipeline run; `.log(step, status, detail)` records a step — `started`/`success`/`failed`/`skipped` — and prints it immediately, so console output and the persisted document can never drift apart; `.finish(status)`; `.to_document()`); `persist_run(logger)` inserts the run's full step log as one document into the `pipeline_runs` collection via its own short-lived connection, so a run's log can still be written even if the failure happened before the pipeline's own Mongo connection was ever opened.

### Pipeline scripts (`ds.py`, `cp.py`, `bc.py`, `parc.py`)

Each is a standalone, independently-runnable module with the same shape. Every step below is recorded via a `PipelineLogger` (`lib/pipeline_log.py`) passed into `extract_transform()`/`push_to_mongo()` — the same `.log()` call both prints to the console and appends to the run's `pipeline_runs` document, so the two can never drift out of sync:

1. **Fetch** the source file's bytes from the Google Drive folder (`GOOGLE_DRIVE_FOLDER_ID` in `.env`) via `lib.gdrive` — run standalone, a script's own `if __name__ == "__main__"` block fetches its file and logs `drive_auth`/`drive_listing`/`file_download` steps around that call before invoking `main()`; run via `run.py`, all four files are fetched once upfront (one shared Drive listing pass) and those same three step entries are logged into each pipeline's own logger to reflect that the fetch already succeeded
2. **Read** Excel from those in-memory bytes (`pd.read_excel(io.BytesIO(file_bytes), ...)`) — XLSX files use openpyxl engine, legacy XLS files use calamine engine (`excel_parse` step)
3. **Validate** required columns exist via `lib.validate.validate_columns` (fail loudly if missing; `column_validation` step)
4. **Transform** — apply `lib.transform.format_date()`/`clean_val()`
5. **Filter** rows based on business rules (drop records missing key identifiers), and for `cp.py`, deduplicate by WW identifier (preferring a valid `IMM` code, keeping the latest contract end date) — this dedup logic is CP-specific and stays inline rather than living in `lib/`. Steps 4–5 together are logged as one `transform_filter` step with rows-in/rows-out/dropped-and-why detail
6. **Push** directly to MongoDB via `lib.mongo` — the transformed DataFrame is converted straight to Mongo-ready records in memory via `df_to_mongo_records()`, with no CSV or other file written at any point (`mongo_connect` and `mongo_push` steps, the latter carrying the before/after/diff counts):
   - `ds.py`/`bc.py` — date-scoped partial refresh (`date_scoped_reload`), scoped on `Date DS`/`Date BC` respectively
   - `cp.py`/`parc.py` — atomic full reload (`atomic_reload`)
7. **Persist the run log** — `main()` calls `logger.finish("success"|"failed")` and `lib.pipeline_log.persist_run(logger)` in a `try/except/else`, so a `pipeline_runs` document is written whether the run succeeded or failed (including a `pipeline_error` step with the exception detail on failure)

`parc.py` and `cp.py` read legacy XLS with `engine="calamine"`.

Each script's `extract_transform()`/`main()` take `file_bytes: bytes` as a parameter rather than reading a hardcoded local path — there is no `INPUT_FILE`/`INPUT_DIR` constant in any of the four scripts anymore, and the local `input/` folder has been deleted entirely (it's gone from disk, not just unreferenced). There is likewise no `output/` folder, `OUTPUT_CSV` constant, or `lib/paths.py` anymore — the CSV intermediate was removed entirely, not just made conditional on environment.

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

## Data & File Paths

All paths below are relative to the repo root (wherever this project is checked out) — nothing is hardcoded to a fixed home-directory location, though older docstrings in some scripts still reference a prior `~/avis/` path from before the project was renamed.

```
./
├── run.py                  # orchestrator: fetches all input files from Drive once, runs the matching pipeline per file
├── ds.py                   # YFACSCALDS.xlsx (Drive) → `ds` (date-scoped), no CSV
├── cp.py                   # ConditionParticulieres.xls (Drive) → `cp` (atomic full reload), no CSV
├── bc.py                   # YBONTEC.xlsx (Drive) → `bc` (date-scoped), no CSV
├── parc.py                 # Fullparcs.xls (Drive) → `parc` (atomic full reload), no CSV
├── lib/
│   ├── transform.py        # pure: clean_val(), format_date()
│   ├── validate.py         # pure: validate_columns()
│   ├── mongo.py             # I/O: get_mongo_db(), df_to_mongo_records(), atomic_reload(), date_scoped_reload(), log_refresh_counts()
│   ├── gdrive.py            # I/O: list_drive_files(), download_file_bytes(), get_latest_expected_files(), fetch_file()
│   └── pipeline_log.py      # I/O: PipelineLogger, persist_run() — durable step log → `pipeline_runs` collection
├── api/
│   └── index.py             # Vercel serverless HTTP wrapper around run.py's run_all() (token-gated trigger) — served at /api
├── test_gdrive_auth.py      # optional manual debugging tool — Drive auth + listing only
├── test_gdrive_download.py  # optional manual debugging tool — Drive download + signature check
├── vercel.json              # maxDuration override for api/index.py
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

## Where to find more detail

- **[`AGENTS.md`](./AGENTS.md)** — mandatory cross-assistant rules, imported at the top of this file.
- **[`GEMINI.md`](./GEMINI.md)** — the equivalent entry point for Gemini CLI / Antigravity sessions.
