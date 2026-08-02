#!/usr/bin/env python3
"""
bc.py
-----
End-to-end BC (purchase order) pipeline: reads input/YBONTEC.xlsx
(relative to this file's directory, via Path(__file__).parent),
extracts needed columns, cleans and normalizes all fields, renames
N° BC → CMD Num, writes output/bc.csv, then pushes a date-scoped
partial refresh to the `bc` MongoDB collection.

Only [earliest Date BC in the CSV, end of year) is touched in Atlas —
records before that date are left untouched. (New in this pipeline —
bc previously had no Mongo push at all.)

Usage:
    python bc.py           # defaults to current year
    python bc.py 2026      # explicit year

Requirements:
    pip install pandas openpyxl pymongo python-dotenv
"""

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

from lib.transform import BC_DS_FORMATS, clean_val, format_date
from lib.validate import validate_columns
from lib.mongo import date_scoped_reload, get_mongo_db, log_refresh_counts

INPUT_DIR  = Path(__file__).parent / "input"
OUTPUT_DIR = Path(__file__).parent / "output"
INPUT_FILE = INPUT_DIR / "YBONTEC.xlsx"
OUTPUT_CSV = OUTPUT_DIR / "bc.csv"
COLLECTION = "bc"

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
def extract_transform() -> pd.DataFrame:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"❌ File not found: {INPUT_FILE}")

    print(f"  Reading: {INPUT_FILE.name}", flush=True)
    # header=0 → row 1 is the header
    df = pd.read_excel(INPUT_FILE, header=0)

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

    df = pd.read_csv(OUTPUT_CSV, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    df[DATE_FIELD] = pd.to_datetime(df[DATE_FIELD], utc=True, errors="coerce")
    df = df[df[DATE_FIELD].dt.year == year]

    if df.empty:
        print(f"  ⚠️  No records found for year {year} in bc.csv")
        return [], None

    earliest_date = df[DATE_FIELD].min()
    print(f"  📅 Earliest date in CSV: {earliest_date.date()}", flush=True)

    df = df[df["CMD Num"].notna() & (df["CMD Num"].str.strip() != "")]

    records = df.to_dict(orient="records")

    clean_records = []
    for rec in records:
        doc = {}
        for k, v in rec.items():
            if k == DATE_FIELD:
                doc[k] = v.to_pydatetime() if pd.notna(v) else None
            else:
                if pd.isna(v) or str(v).strip() == "":
                    continue
                doc[k] = str(v).strip()
        clean_records.append(doc)

    print(f"  → {len(clean_records)} records in CSV for year {year}", flush=True)
    return clean_records, earliest_date


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
def main():
    year = int(sys.argv[1]) if len(sys.argv) > 1 else datetime.now().year

    df = extract_transform()
    write_csv(df)
    push_to_mongo(year)


if __name__ == "__main__":
    main()
