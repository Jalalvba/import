# CLAUDE.md

@AGENTS.md

This file provides guidance to Claude Code (claude.ai/code) when working
with code in this repository — the rules imported above apply regardless
of which AI assistant is driving. Gemini CLI / Antigravity sessions use
[`GEMINI.md`](./GEMINI.md); if the two disagree, that's a doc bug, fix
both.

## Project Overview

**avis** is a Python ETL pipeline that converts raw fleet vehicle management data from Excel (XLSX/XLS) files into clean, standardized CSV files ready for MongoDB import, then pushes them to MongoDB Atlas.

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

### Pipelines: one file per source, Drive → CSV → Mongo in a single run

```bash
python ds.py    # YFACSCALDS.xlsx (Drive)             → output/ds.csv   → `ds` collection   (date-scoped partial refresh)
python cp.py    # ConditionParticulieres.xls (Drive)  → output/cp.csv   → `cp` collection   (atomic full reload)
python bc.py    # YBONTEC.xlsx (Drive)                → output/bc.csv   → `bc` collection   (date-scoped partial refresh)
python parc.py  # Fullparcs.xls (Drive)               → output/parc.csv → `parc` collection (atomic full reload)
```

Each script runs the complete pipeline for its source — there is no separate CSV-only step anymore; running any of these four scripts always fetches its file from Drive, writes `output/<name>.csv`, **and** pushes to MongoDB in one invocation. `ds.py`/`bc.py` accept an optional year argument (`python ds.py 2026`, defaults to the current year). Run standalone, each script fetches only its own file from Drive (one API call).

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
- **`lib/mongo.py`** (I/O) — `get_mongo_db()` (`.env` loading + DNS resolver workaround + connected db handle); `atomic_reload(db, collection_name, records, index_specs)` (staged insert into `<collection>_staging`, verified count, atomic `rename(dropTarget=True)` swap — never a bare drop-then-insert, so the live collection is never dropped before the replacement data is fully written and verified); `date_scoped_reload(db, collection_name, records, date_field, earliest_date, year)` (deletes only `[earliest_date, end-of-year)`, then inserts — records before `earliest_date` are untouched); `log_refresh_counts(before, after)` (before/after/diff print).
- **`lib/gdrive.py`** (I/O) — `list_drive_files(folder_id)` (Drive API v3, `drive.readonly` scope, returns `[{id, name, mimeType, modifiedTime}, ...]`); `download_file_bytes(file_id)` (in-memory `files().get_media()` download, no temp files); `get_latest_expected_files(folder_id)` (fetches all four known pipeline filenames in one listing pass, deduping by latest `modifiedTime` when a name repeats — used by `run.py`); `fetch_file(folder_id, filename)` (single-file fetch — used when a pipeline script is run standalone). Auth loads `GOOGLE_SERVICE_ACCOUNT_KEY_B64` from `.env`, base64-decodes + JSON-parses it, and builds `Credentials` in memory — the decoded key is never written to disk.

### Pipeline scripts (`ds.py`, `cp.py`, `bc.py`, `parc.py`)

Each is a standalone, independently-runnable module with the same shape:

1. **Fetch** the source file's bytes from the Google Drive folder (`GOOGLE_DRIVE_FOLDER_ID` in `.env`) via `lib.gdrive` — run standalone, a script fetches only its own file; run via `run.py`, bytes are fetched once upfront and passed into each script's `main(file_bytes)`
2. **Read** Excel from those in-memory bytes (`pd.read_excel(io.BytesIO(file_bytes), ...)`) — XLSX files use openpyxl engine, legacy XLS files use calamine engine
3. **Validate** required columns exist via `lib.validate.validate_columns` (fail loudly if missing)
4. **Transform** — apply `lib.transform.format_date()`/`clean_val()`
5. **Filter** rows based on business rules (drop records missing key identifiers), and for `cp.py`, deduplicate by WW identifier (preferring a valid `IMM` code, keeping the latest contract end date) — this dedup logic is CP-specific and stays inline rather than living in `lib/`
6. **Write** UTF-8 BOM-encoded CSV to `output/<output>.csv` (creates `output/` if missing) — kept as an intermediate artifact for debugging/audit even though it's no longer a separate CLI step
7. **Push** to MongoDB via `lib.mongo`:
   - `ds.py`/`bc.py` — date-scoped partial refresh (`date_scoped_reload`), scoped on `Date DS`/`Date BC` respectively
   - `cp.py`/`parc.py` — atomic full reload (`atomic_reload`)

`parc.py` and `cp.py` read legacy XLS with `engine="calamine"`.

Each script's `extract_transform()`/`main()` take `file_bytes: bytes` as a parameter rather than reading a hardcoded local path — there is no `INPUT_FILE`/`INPUT_DIR` constant in any of the four scripts anymore, and the local `input/` folder has been deleted entirely (it's gone from disk, not just unreferenced).

## Data & File Paths

All paths below are relative to the repo root (wherever this project is checked out) — nothing is hardcoded to a fixed home-directory location, though older docstrings in some scripts still reference a prior `~/avis/` path from before the project was renamed.

```
./
├── run.py                  # orchestrator: fetches all input files from Drive once, runs the matching pipeline per file
├── ds.py                   # YFACSCALDS.xlsx (Drive) → ds.csv → `ds` (date-scoped)
├── cp.py                   # ConditionParticulieres.xls (Drive) → cp.csv → `cp` (atomic full reload)
├── bc.py                   # YBONTEC.xlsx (Drive) → bc.csv → `bc` (date-scoped)
├── parc.py                 # Fullparcs.xls (Drive) → parc.csv → `parc` (atomic full reload)
├── lib/
│   ├── transform.py        # pure: clean_val(), format_date()
│   ├── validate.py         # pure: validate_columns()
│   ├── mongo.py             # I/O: get_mongo_db(), atomic_reload(), date_scoped_reload(), log_refresh_counts()
│   └── gdrive.py            # I/O: list_drive_files(), download_file_bytes(), get_latest_expected_files(), fetch_file()
├── test_gdrive_auth.py      # optional manual debugging tool — Drive auth + listing only
├── test_gdrive_download.py  # optional manual debugging tool — Drive download + signature check
├── output/                 # gitignored — generated CSVs land here
│   ├── .gitkeep
│   ├── ds.csv
│   ├── cp.csv
│   ├── parc.csv
│   └── bc.csv
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
