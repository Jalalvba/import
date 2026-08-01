#!/usr/bin/env python3
"""
parc_refresh.py
---------------
Reads output/parc.csv (relative to this file's directory, via
Path(__file__).parent — output of parc_csv.py, already clean).
Reloads the parc collection in Atlas entirely, via a staged insert +
atomic rename so the live collection is never dropped before the
replacement data is fully written and verified.

Flow:
    Fullparcs.xls → parc_csv.py → parc.csv → parc_refresh.py → Atlas

Usage:
    python parc_refresh.py

Requirements:
    pip install pandas pymongo python-dotenv
"""

import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
import os
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_CSV  = Path(__file__).parent / "output" / "parc.csv"
COLLECTION = "parc"

DATE_COLUMNS = ["Date MCE"]

INDEX_SPECS = [
    ([("Immatriculation", 1)], "immatriculation"),
    ([("Numéro WW", 1)], "numero_ww"),
    ([("N° de chassis", 1)], "n_de_chassis"),
]


# ── Extract ───────────────────────────────────────────────────────────────────
def extract() -> list[dict]:
    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"❌ File not found: {INPUT_CSV}\n   Run parc_csv.py first.")

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


# ── Load ──────────────────────────────────────────────────────────────────────
def atomic_reload(db, collection_name: str, records: list[dict], index_specs) -> None:
    """Insert records into a staging collection, verify the count, then
    atomically rename staging → collection_name (dropTarget=True). The live
    collection is never dropped before the replacement data is fully
    written and verified. On any failure the staging collection is dropped
    and the live collection is left untouched."""
    staging_name = f"{collection_name}_staging"
    db[staging_name].drop()
    try:
        if records:
            db[staging_name].insert_many(records)
        staged_count = db[staging_name].count_documents({})
        if staged_count != len(records):
            raise RuntimeError(
                f"staging insert count mismatch: expected {len(records)}, got {staged_count}"
            )
        for keys, name in index_specs:
            db[staging_name].create_index(keys, name=name)
        db[staging_name].rename(collection_name, dropTarget=True)
    except Exception:
        db[staging_name].drop()
        raise


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    env_path = Path(__file__).parent / ".env"
    load_dotenv(dotenv_path=env_path)

    uri     = os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGODB_DB")

    if not uri or not db_name:
        raise EnvironmentError("❌ MONGODB_URI or MONGODB_DB not set in .env")

    records = extract()
    if not records:
        return

    import dns.resolver
    dns.resolver.default_resolver = dns.resolver.Resolver(configure=False)
    dns.resolver.default_resolver.nameservers = ['8.8.8.8']
    client = MongoClient(uri)
    db = client[db_name]

    before_count = db[COLLECTION].count_documents({})
    print(f"  📊 Records in Atlas before refresh: {before_count}", flush=True)

    try:
        atomic_reload(db, COLLECTION, records, INDEX_SPECS)
    except (PyMongoError, RuntimeError) as e:
        print(f"  ❌ Refresh failed — '{COLLECTION}' left untouched: {e}", flush=True)
        client.close()
        sys.exit(1)

    print(f"  ✅ Inserted {len(records)} records into {COLLECTION} (staged + swapped)", flush=True)
    print(f"  🔧 Recreated indexes on {COLLECTION}: immatriculation, numero_ww, n_de_chassis", flush=True)

    after_count = db[COLLECTION].count_documents({})
    diff = after_count - before_count
    diff_str = f"+{diff}" if diff > 0 else str(diff)
    print(f"  📈 Before: {before_count}  |  After: {after_count}  |  Diff: {diff_str}")

    client.close()


if __name__ == "__main__":
    main()
