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

Place raw Excel files in `input/` (relative to the repo root — all path handling is `Path(__file__).parent`-based, not a hardcoded home-directory path, despite what some in-file docstrings say) before running.

### Pipelines: one file per source, Excel → CSV → Mongo in a single run

```bash
python ds.py    # input/YFACSCALDS.xlsx             → output/ds.csv   → `ds` collection   (date-scoped partial refresh)
python cp.py    # input/ConditionParticulieres.xls  → output/cp.csv   → `cp` collection   (atomic full reload)
python bc.py    # input/YBONTEC.xlsx                → output/bc.csv   → `bc` collection   (date-scoped partial refresh)
python parc.py  # input/Fullparcs.xls               → output/parc.csv → `parc` collection (atomic full reload)
```

Each script runs the complete pipeline for its source — there is no separate CSV-only step anymore; running any of these four scripts always writes `output/<name>.csv` **and** pushes to MongoDB in one invocation. `ds.py`/`bc.py` accept an optional year argument (`python ds.py 2026`, defaults to the current year).

### Orchestrator

```bash
python run.py
```

Scans `input/` for any of the four known filenames and runs the matching single-file pipeline for each one found, skipping pipelines whose input file is absent. Continues to the next pipeline if one fails.

There are no tests, linters, or CI configured.

## Architecture

### `lib/` — shared logic, used by all four pipelines

- **`lib/transform.py`** (pure, no I/O) — `format_date(val, formats)` normalizes dates to ISO 8601 (`YYYY-MM-DDTHH:MM:SS.000Z`, returns `""` for invalid/null); `clean_val(val)` strips Excel encoding artifacts (`_x000a_`, `_x000d_`, `_x[hex]_`) and collapses whitespace. `format_date` takes an explicit `formats` tuple rather than a merged list — `BC_DS_FORMATS` and `CP_PARC_FORMATS` are separate named constants because the two source systems have historically needed different tried-format orders (`BC_DS_FORMATS` uniquely tries `%m/%d/%Y`).
- **`lib/validate.py`** (pure, no I/O) — `validate_columns(df, required_columns)` raises `ValueError` (never a silent return) if any required column is missing.
- **`lib/mongo.py`** (I/O) — `get_mongo_db()` (`.env` loading + DNS resolver workaround + connected db handle); `atomic_reload(db, collection_name, records, index_specs)` (staged insert into `<collection>_staging`, verified count, atomic `rename(dropTarget=True)` swap — never a bare drop-then-insert, so the live collection is never dropped before the replacement data is fully written and verified); `date_scoped_reload(db, collection_name, records, date_field, earliest_date, year)` (deletes only `[earliest_date, end-of-year)`, then inserts — records before `earliest_date` are untouched); `log_refresh_counts(before, after)` (before/after/diff print).

### Pipeline scripts (`ds.py`, `cp.py`, `bc.py`, `parc.py`)

Each is a standalone, independently-runnable module with the same shape:

1. **Read** Excel from `input/<InputFile>` — XLSX files use openpyxl engine, legacy XLS files use calamine engine
2. **Validate** required columns exist via `lib.validate.validate_columns` (fail loudly if missing)
3. **Transform** — apply `lib.transform.format_date()`/`clean_val()`
4. **Filter** rows based on business rules (drop records missing key identifiers), and for `cp.py`, deduplicate by WW identifier (preferring a valid `IMM` code, keeping the latest contract end date) — this dedup logic is CP-specific and stays inline rather than living in `lib/`
5. **Write** UTF-8 BOM-encoded CSV to `output/<output>.csv` (creates `output/` if missing) — kept as an intermediate artifact for debugging/audit even though it's no longer a separate CLI step
6. **Push** to MongoDB via `lib.mongo`:
   - `ds.py`/`bc.py` — date-scoped partial refresh (`date_scoped_reload`), scoped on `Date DS`/`Date BC` respectively
   - `cp.py`/`parc.py` — atomic full reload (`atomic_reload`)

`parc.py` and `cp.py` read legacy XLS with `engine="calamine"`.

## Data & File Paths

All paths below are relative to the repo root (wherever this project is checked out) — nothing is hardcoded to a fixed home-directory location, though older docstrings in some scripts still reference a prior `~/avis/` path from before the project was renamed.

```
./
├── run.py                  # orchestrator: detects input files, runs the matching pipeline per file
├── ds.py                   # YFACSCALDS.xlsx → ds.csv → `ds` (date-scoped)
├── cp.py                   # ConditionParticulieres.xls → cp.csv → `cp` (atomic full reload)
├── bc.py                   # YBONTEC.xlsx → bc.csv → `bc` (date-scoped)
├── parc.py                 # Fullparcs.xls → parc.csv → `parc` (atomic full reload)
├── lib/
│   ├── transform.py        # pure: clean_val(), format_date()
│   ├── validate.py         # pure: validate_columns()
│   └── mongo.py            # I/O: get_mongo_db(), atomic_reload(), date_scoped_reload(), log_refresh_counts()
├── input/                  # gitignored — place raw Excel files here
│   ├── .gitkeep
│   ├── YFACSCALDS.xlsx
│   ├── ConditionParticulieres.xls
│   ├── Fullparcs.xls
│   └── YBONTEC.xlsx
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
```

## Where to find more detail

- **[`AGENTS.md`](./AGENTS.md)** — mandatory cross-assistant rules, imported at the top of this file.
- **[`GEMINI.md`](./GEMINI.md)** — the equivalent entry point for Gemini CLI / Antigravity sessions.
