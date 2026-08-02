#!/usr/bin/env python3
"""
parc.py
-------
End-to-end PARC (fleet park) pipeline: reads input/Fullparcs.xls
(relative to this file's directory, via Path(__file__).parent),
extracts needed columns, cleans and normalizes all fields, writes
output/parc.csv, then pushes a full atomic reload to the `parc`
MongoDB collection.

Usage:
    python parc.py

Requirements:
    pip install pandas openpyxl python-calamine pymongo python-dotenv
"""

import sys
from pathlib import Path

import pandas as pd
from pymongo.errors import PyMongoError

from lib.transform import CP_PARC_FORMATS, clean_val, format_date
from lib.validate import validate_columns
from lib.mongo import atomic_reload, csv_to_mongo_records, get_mongo_db, log_refresh_counts

INPUT_DIR  = Path(__file__).parent / "input"
OUTPUT_DIR = Path(__file__).parent / "output"
INPUT_FILE = INPUT_DIR / "Fullparcs.xls"
OUTPUT_CSV = OUTPUT_DIR / "parc.csv"
COLLECTION = "parc"

HEADER_ROW = 7  # row 8 → 0-indexed = 7

COLUMNS_NEEDED = [
    "Client",
    "Marque",
    "Modèle",
    "Immatriculation",
    "Numéro WW",
    "N° de chassis",
    "Etat véhicule",
    "Date MCE",
    "Type location",
    "Locataire",
]

DATE_COLUMNS = ["Date MCE"]

INDEX_SPECS = [
    ([("Immatriculation", 1)], "immatriculation"),
    ([("Numéro WW", 1)], "numero_ww"),
    ([("N° de chassis", 1)], "n_de_chassis"),
]


# ── Excel → CSV ───────────────────────────────────────────────────────────────
def extract_transform() -> pd.DataFrame:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"❌ File not found: {INPUT_FILE}")

    print(f"  Reading: {INPUT_FILE.name}", flush=True)
    df = pd.read_excel(INPUT_FILE, engine="calamine", header=HEADER_ROW)
    df.columns = [c.strip() for c in df.columns]

    needed_stripped = [c.strip() for c in COLUMNS_NEEDED]
    validate_columns(df, needed_stripped)

    df = df[needed_stripped].copy()

    for col in [c.strip() for c in DATE_COLUMNS]:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: format_date(v, CP_PARC_FORMATS))

    for col in df.columns:
        if col not in [c.strip() for c in DATE_COLUMNS]:
            df[col] = df[col].apply(clean_val)

    # Drop rows where Immatriculation and Numéro WW are both empty
    df = df[
        df["Immatriculation"].notna() & (df["Immatriculation"].str.strip() != "") |
        df["Numéro WW"].notna() & (df["Numéro WW"].str.strip() != "")
    ]

    return df


def write_csv(df: pd.DataFrame) -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ {len(df)} rows → {OUTPUT_CSV}")


# ── CSV → Mongo ───────────────────────────────────────────────────────────────
def extract_mongo_records() -> list[dict]:
    if not OUTPUT_CSV.exists():
        raise FileNotFoundError(f"❌ File not found: {OUTPUT_CSV}\n   CSV step must run first.")

    records = csv_to_mongo_records(OUTPUT_CSV, DATE_COLUMNS)
    print(f"  → {len(records)} records", flush=True)
    return records


def push_to_mongo() -> None:
    records = extract_mongo_records()
    if not records:
        return

    db = get_mongo_db()

    before_count = db[COLLECTION].count_documents({})
    print(f"  📊 Records in Atlas before refresh: {before_count}", flush=True)

    try:
        atomic_reload(db, COLLECTION, records, INDEX_SPECS)
    except (PyMongoError, RuntimeError) as e:
        print(f"  ❌ Refresh failed — '{COLLECTION}' left untouched: {e}", flush=True)
        db.client.close()
        sys.exit(1)

    print(f"  ✅ Inserted {len(records)} records into {COLLECTION} (staged + swapped)", flush=True)
    print(f"  🔧 Recreated indexes on {COLLECTION}: immatriculation, numero_ww, n_de_chassis", flush=True)

    after_count = db[COLLECTION].count_documents({})
    log_refresh_counts(before_count, after_count)

    db.client.close()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    df = extract_transform()
    write_csv(df)
    push_to_mongo()


if __name__ == "__main__":
    main()
