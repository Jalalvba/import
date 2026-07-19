# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**avis** is a Python ETL pipeline that converts raw fleet vehicle management data from Excel (XLSX/XLS) files into clean, standardized CSV files ready for MongoDB import, then pushes them to MongoDB Atlas.

## Setup & Running

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Place raw Excel files in `input/` (relative to the repo root — all path handling is `Path(__file__).parent`-based, not a hardcoded home-directory path, despite what some in-file docstrings say) before running.

### Step 1 — Convert Excel → CSV

```bash
python bc_csv.py    # input/YBONTEC.xlsx               → output/bc.csv
python cp_csv.py    # input/ConditionParticulieres.xls  → output/cp.csv
python ds_csv.py    # input/YFACSCALDS.xlsx             → output/ds.csv
python parc_csv.py  # input/Fullparcs.xls               → output/parc.csv
```

### Step 2 — Push CSV → MongoDB Atlas

```bash
python ds_refresh.py    # Partial refresh: deletes from earliest date in CSV → end of year, reinserts
python cp_refresh.py    # Full refresh: drops cp collection, reloads entirely
python parc_refresh.py  # Full refresh: drops parc collection, reloads entirely
```

Full pipeline for a given collection:

```bash
python ds_csv.py && python ds_refresh.py
python cp_csv.py && python cp_refresh.py
python parc_csv.py && python parc_refresh.py
```

`bc_csv.py` has no corresponding `bc_refresh.py` yet — its output is generated but not currently pushed to MongoDB.

### Orchestrator

```bash
python run.py
```

Scans `input/` for any of the four known filenames and runs the matching csv → refresh pipeline for each one found, skipping pipelines whose input file is absent (and skipping the refresh step for `bc_csv.py`, since none exists). Stops a given pipeline's remaining steps if one script fails, but continues with other pipelines.

There are no tests, linters, or CI configured.

## Architecture

### CSV scripts (ds_csv, cp_csv, parc_csv, bc_csv)

Four standalone ETL modules share the same structure:

1. **Read** Excel from `input/<InputFile>` — XLSX files use openpyxl engine, legacy XLS files use calamine engine
2. **Validate** required columns exist (fail loudly if missing)
3. **Transform** — apply `format_date()` and `clean_val()` helpers
4. **Filter** rows based on business rules (drop records missing key identifiers)
5. **Write** UTF-8 BOM-encoded CSV to `output/<output>.csv` (creates `output/` if missing)

### Refresh scripts (ds_refresh, cp_refresh, parc_refresh)

Read the clean CSV from `output/`, connect to Atlas via `.env`, and sync to MongoDB:

- **ds_refresh.py** — partial year refresh: detects earliest `Date DS` in CSV, deletes Atlas records from that date to end of year, inserts fresh records. Records before the earliest CSV date are untouched.
- **cp_refresh.py** — full drop + reload of `cp` collection
- **parc_refresh.py** — full drop + reload of `parc` collection

All refresh scripts print before/after record counts and the diff.

### Shared Helpers (replicated in each CSV module)

- **`format_date(val)`** — normalizes dates to ISO 8601 (`YYYY-MM-DDTHH:MM:SS.000Z`); returns `""` for invalid/null
- **`clean_val(val)`** — strips Excel encoding artifacts (`_x000a_`, `_x000d_`, `_x[hex]_`), collapses whitespace

### Module-specific logic

- **cp_csv.py** additionally deduplicates rows by WW identifier, preferring records with a valid `IMM` code and keeping the latest contract end date
- **parc_csv.py** and **cp_csv.py** read legacy XLS with `engine="calamine"`

## Data & File Paths

All paths below are relative to the repo root (wherever this project is checked out) — nothing is hardcoded to a fixed home-directory location, though older docstrings in some scripts still reference a prior `~/avis/` path from before the project was renamed.

```
./
├── run.py                  # orchestrator: detects input files, runs full pipeline per file
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
├── ds_csv.py
├── cp_csv.py
├── parc_csv.py
├── bc_csv.py
├── ds_refresh.py
├── cp_refresh.py
├── parc_refresh.py
└── requirements.txt
```

## Environment Variables (.env)

```
MONGODB_URI=mongodb+srv://...
MONGODB_DB=avis
```
