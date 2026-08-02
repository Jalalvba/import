#!/usr/bin/env python3
"""
bc.py
-----
End-to-end BC (purchase order) pipeline: reads YBONTEC.xlsx fetched from
the Google Drive folder (GOOGLE_DRIVE_FOLDER_ID in .env), extracts needed
columns, cleans and normalizes all fields, renames N° BC → CMD Num,
writes output/bc.csv, then pushes a date-scoped partial refresh to the
`bc` MongoDB collection.

Only [earliest Date BC in the CSV, end of year) is touched in Atlas —
records before that date are left untouched. (New in this pipeline —
bc previously had no Mongo push at all.)

Usage:
    python bc.py           # fetches from Drive, defaults to current year
    python bc.py 2026      # explicit year

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
OUTPUT_CSV = OUTPUT_DIR / "bc.csv"
COLLECTION = "bc"
FILENAME   = "YBONTEC.xlsx"

# Exact header names as they appear in the xlsx (will be stripped during match)
COLUMNS_NEEDED = [
    "N° BC",              # trailing spaces stripped at match time → renamed to CMD Num
    "Immatriculation",
    "Date BC",
    "Fournisseurs",
    "Code article",
    "Description article",
    "PU",
    "Qté",
    "N° DS",
    "Cree par",
]

# Rename map: stripped source name → output name
RENAME = {
    "N° BC": "CMD Num",
}

DATE_COLUMNS = ["Date BC"]
DATE_FIELD = "Date BC"


# ── Excel → CSV ───────────────────────────────────────────────────────────────
def extract_transform(file_bytes: bytes) -> pd.DataFrame:
    if not file_bytes:
        raise ValueError(f"❌ No bytes provided for {FILENAME}")

    print(f"  Reading: {FILENAME} ({len(file_bytes):,} bytes)", flush=True)
    # header=0 → row 1 is the header
    df = pd.read_excel(io.BytesIO(file_bytes), header=0)

    # Strip all column names for safe matching
    df.columns = [c.strip() for c in df.columns]

    needed_stripped = [c.strip() for c in COLUMNS_NEEDED]
    validate_columns(df, needed_stripped)

    df = df[needed_stripped].copy()

    # Date columns → ISO string
    for col in [c.strip() for c in DATE_COLUMNS]:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: format_date(v, BC_DS_FORMATS))

    # All other columns → clean string
    for col in df.columns:
        if col not in [c.strip() for c in DATE_COLUMNS]:
            df[col] = df[col].apply(clean_val)

    # Rename
    df.rename(columns=RENAME, inplace=True)

    # Drop rows where CMD Num is empty
    df = df[df["CMD Num"].notna() & (df["CMD Num"].str.strip() != "")]

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
    year_records = [r for r in records if r.get(DATE_FIELD) is not None and r[DATE_FIELD].year == year]

    if not year_records:
        print(f"  ⚠️  No records found for year {year} in bc.csv")
        return [], None

    earliest_date = min(r[DATE_FIELD] for r in year_records)
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

    date_scoped_reload(db, COLLECTION, records, DATE_FIELD, earliest_date, year)

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
