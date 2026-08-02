#!/usr/bin/env python3
"""
ds.py
-----
End-to-end DS (consumption sheet) pipeline: reads YFACSCALDS.xlsx fetched
from the Google Drive folder (GOOGLE_DRIVE_FOLDER_ID in .env), cleans and
normalizes all fields, writes output/ds.csv, then pushes a date-scoped
partial refresh to the `ds` MongoDB collection.

Only [earliest Date DS in the CSV, end of year) is touched in Atlas —
records before that date are left untouched.

Usage:
    python ds.py           # fetches from Drive, defaults to current year
    python ds.py 2026      # explicit year

Requirements:
    pip install pandas openpyxl pymongo python-dotenv google-api-python-client google-auth
"""

import io
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from lib.transform import BC_DS_FORMATS, clean_val, format_date
from lib.validate import validate_columns
from lib.mongo import csv_to_mongo_records, date_scoped_reload, get_mongo_db, log_refresh_counts

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_CSV = OUTPUT_DIR / "ds.csv"
COLLECTION = "ds"
FILENAME   = "YFACSCALDS.xlsx"

COLUMNS_NEEDED = [
    "Date DS",
    "N°DS",
    "Code art",
    "Désignation Consomation ",
    "Qté",
    "Immatriculation",
    "KM",
    "CMD Num",
    "Founisseur",
    "ENTITE",
    "Description",
    "Technicein",
]

DATE_COLUMNS = ["Date DS"]


# ── Excel → CSV ───────────────────────────────────────────────────────────────
def extract_transform(file_bytes: bytes) -> pd.DataFrame:
    if not file_bytes:
        raise ValueError(f"❌ No bytes provided for {FILENAME}")

    print(f"  Reading: {FILENAME} ({len(file_bytes):,} bytes)", flush=True)
    df = pd.read_excel(io.BytesIO(file_bytes), header=1)
    df.columns = [c.strip() for c in df.columns]

    needed_stripped = [c.strip() for c in COLUMNS_NEEDED]
    validate_columns(df, needed_stripped)

    df = df[needed_stripped].copy()

    for col in [c.strip() for c in DATE_COLUMNS]:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: format_date(v, BC_DS_FORMATS))

    for col in df.columns:
        if col not in [c.strip() for c in DATE_COLUMNS]:
            df[col] = df[col].apply(clean_val)

    key = "N°DS"
    df = df[df[key].notna() & (df[key].str.strip() != "")]

    return df


def write_csv(df: pd.DataFrame) -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ {len(df)} rows → {OUTPUT_CSV}")


# ── CSV → Mongo ───────────────────────────────────────────────────────────────
def extract_mongo_records(year: int) -> tuple[list[dict], object]:
    if not OUTPUT_CSV.exists():
        raise FileNotFoundError(f"❌ File not found: {OUTPUT_CSV}\n   CSV step must run first.")

    records = csv_to_mongo_records(OUTPUT_CSV, DATE_COLUMNS)
    year_records = [r for r in records if r.get("Date DS") is not None and r["Date DS"].year == year]

    if not year_records:
        print(f"  ⚠️  No records found for year {year} in ds.csv")
        return [], None

    earliest_date = min(r["Date DS"] for r in year_records)
    print(f"  📅 Earliest date in CSV: {earliest_date.date()}", flush=True)

    print(f"  → {len(year_records)} records in CSV for year {year}", flush=True)
    return year_records, earliest_date


def push_to_mongo(year: int) -> None:
    records, earliest_date = extract_mongo_records(year)
    if not records:
        return

    db = get_mongo_db()

    before_count = db[COLLECTION].count_documents({})
    print(f"  📊 Records in Atlas before refresh: {before_count}", flush=True)

    date_scoped_reload(db, COLLECTION, records, "Date DS", earliest_date, year)

    after_count = db[COLLECTION].count_documents({})
    log_refresh_counts(before_count, after_count)

    db.client.close()


# ── Main ──────────────────────────────────────────────────────────────────────
def main(file_bytes: bytes, year: int | None = None):
    year = year if year is not None else datetime.now().year

    df = extract_transform(file_bytes)
    write_csv(df)
    push_to_mongo(year)


if __name__ == "__main__":
    from lib.gdrive import fetch_file

    load_dotenv(dotenv_path=Path(__file__).parent / ".env")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not folder_id:
        raise EnvironmentError("❌ GOOGLE_DRIVE_FOLDER_ID not set in .env")

    arg_year = int(sys.argv[1]) if len(sys.argv) > 1 else None
    main(fetch_file(folder_id, FILENAME), arg_year)
