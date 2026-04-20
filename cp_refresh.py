#!/usr/bin/env python3
"""
cp_refresh.py
-------------
Reads ~/avis/output/cp.csv  (output of cp_csv.py — already clean)
Drops the cp collection in Atlas and reloads it entirely.

Flow:
    ConditionParticulieres.xls → cp_csv.py → cp.csv → cp_refresh.py → Atlas

Usage:
    python cp_refresh.py

Requirements:
    pip install pandas pymongo python-dotenv
"""

from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
import os
from pymongo import MongoClient

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_CSV  = Path(__file__).parent / "output" / "cp.csv"
COLLECTION = "cp"

DATE_COLUMNS = ["Date MCE", "Date début contrat", "Date fin contrat"]


# ── Extract ───────────────────────────────────────────────────────────────────
def extract() -> list[dict]:
    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"❌ File not found: {INPUT_CSV}\n   Run cp_csv.py first.")

    print(f"  Reading: {INPUT_CSV.name}", flush=True)

    df = pd.read_csv(INPUT_CSV, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    # Parse date columns → Python datetime
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    records = df.to_dict(orient="records")

    clean_records = []
    for rec in records:
        doc = {}
        for k, v in rec.items():
            if k in DATE_COLUMNS:
                doc[k] = v.to_pydatetime() if pd.notna(v) else None
            else:
                if pd.isna(v) or str(v).strip() == "":
                    continue
                doc[k] = str(v).strip()
        clean_records.append(doc)

    print(f"  → {len(clean_records)} records", flush=True)
    return clean_records


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    env_path = Path.home() / "avis" / ".env"
    load_dotenv(dotenv_path=env_path)

    uri     = os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGODB_DB")

    if not uri or not db_name:
        raise EnvironmentError("❌ MONGODB_URI or MONGODB_DB not set in .env")

    records = extract()
    if not records:
        return

    client = MongoClient(uri)
    db = client[db_name]

    before_count = db[COLLECTION].count_documents({})
    print(f"  📊 Records in Atlas before refresh: {before_count}", flush=True)

    db[COLLECTION].drop()
    print(f"  🗑️  Dropped collection: {COLLECTION}", flush=True)

    db[COLLECTION].insert_many(records)
    print(f"  ✅ Inserted {len(records)} records into {COLLECTION}", flush=True)

    after_count = db[COLLECTION].count_documents({})
    diff = after_count - before_count
    diff_str = f"+{diff}" if diff > 0 else str(diff)
    print(f"  📈 Before: {before_count}  |  After: {after_count}  |  Diff: {diff_str}")

    client.close()


if __name__ == "__main__":
    main()
