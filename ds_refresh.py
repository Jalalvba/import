#!/usr/bin/env python3
"""
ds_refresh.py
-------------
Reads ~/avis/output/ds.csv  (output of ds_csv.py — already clean)
Filters to the target year, detects the earliest Date DS,
deletes that window from Atlas, inserts fresh records.

Flow:
    Excel → ds_csv.py → ds.csv → ds_refresh.py → Atlas

Example: ds.csv starts at 19/02/2026
  → deletes Atlas records from 2026-02-19 to end of 2026
  → inserts fresh records from ds.csv
  → records before 19/02/2026 in Atlas are untouched

Usage:
    python ds_refresh.py           # defaults to current year
    python ds_refresh.py 2026      # explicit year

Requirements:
    pip install pandas pymongo python-dotenv
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
import os
from pymongo import MongoClient

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_CSV = Path(__file__).parent / "output" / "ds.csv"


# ── Extract ───────────────────────────────────────────────────────────────────
def extract(year: int) -> tuple[list[dict], datetime]:
    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"❌ File not found: {INPUT_CSV}\n   Run ds_csv.py first.")

    print(f"  Reading: {INPUT_CSV.name}", flush=True)

    # ds.csv dates are ISO strings: 2026-02-19T00:00:00.000Z
    df = pd.read_csv(INPUT_CSV, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    # Parse Date DS → datetime for filtering
    df["Date DS"] = pd.to_datetime(df["Date DS"], utc=True, errors="coerce")

    # Filter to target year
    df = df[df["Date DS"].dt.year == year]

    if df.empty:
        print(f"  ⚠️  No records found for year {year} in ds.csv")
        return [], None

    # Earliest date → delete cutoff
    earliest_date = df["Date DS"].min()
    print(f"  📅 Earliest date in CSV: {earliest_date.date()}", flush=True)

    # Drop rows where N°DS is empty
    df = df[df["N°DS"].notna() & (df["N°DS"].str.strip() != "")]

    # Convert to list of dicts
    records = df.to_dict(orient="records")

    # Clean each record: Date DS → Python datetime, empty fields dropped
    clean_records = []
    for rec in records:
        doc = {}
        for k, v in rec.items():
            if k == "Date DS":
                doc[k] = v.to_pydatetime() if pd.notna(v) else None
            else:
                if pd.isna(v) or str(v).strip() == "":
                    continue
                doc[k] = str(v).strip()
        clean_records.append(doc)

    print(f"  → {len(clean_records)} records in CSV for year {year}", flush=True)
    return clean_records, earliest_date


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    year = int(sys.argv[1]) if len(sys.argv) > 1 else datetime.now().year

    # Load .env from ~/avis/.env
    env_path = Path(__file__).parent / ".env"
    load_dotenv(dotenv_path=env_path)

    uri     = os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGODB_DB")

    if not uri or not db_name:
        raise EnvironmentError("❌ MONGODB_URI or MONGODB_DB not set in .env")

    # Extract from CSV
    records, earliest_date = extract(year)
    if not records:
        return

    # Connect
    client = MongoClient(uri)
    col = client[db_name]["ds"]

    before_count = col.count_documents({})
    print(f"  📊 Records in Atlas before refresh: {before_count}", flush=True)

    # Delete from earliest_date → end of year (records before are untouched)
    end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)

    result = col.delete_many({"Date DS": {"$gte": earliest_date, "$lt": end}})
    print(f"  🗑️  Deleted {result.deleted_count} records from {earliest_date.date()} to end of {year}", flush=True)

    # Insert fresh records
    inserted = col.insert_many(records)
    print(f"  ✅ Inserted {len(inserted.inserted_ids)} records for {year}", flush=True)

    after_count = col.count_documents({})
    diff = after_count - before_count
    diff_str = f"+{diff}" if diff > 0 else str(diff)
    print(f"  📈 Before: {before_count}  |  After: {after_count}  |  Diff: {diff_str}")

    client.close()


if __name__ == "__main__":
    main()