#!/usr/bin/env python3
"""
cp.py
-----
End-to-end CP (contract particulars) pipeline: reads
input/ConditionParticulieres.xls (relative to this file's directory,
via Path(__file__).parent), extracts needed columns, cleans and
normalizes all fields, deduplicates by IMM keeping the row with the
latest Date fin contrat, writes output/cp.csv, then pushes a full
atomic reload to the `cp` MongoDB collection.

Usage:
    python cp.py

Requirements:
    pip install pandas openpyxl python-calamine pymongo python-dotenv
"""

import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from pymongo.errors import PyMongoError

from lib.transform import CP_PARC_FORMATS, clean_val, format_date
from lib.validate import validate_columns
from lib.mongo import atomic_reload, get_mongo_db, log_refresh_counts

INPUT_DIR  = Path(__file__).parent / "input"
OUTPUT_DIR = Path(__file__).parent / "output"
INPUT_FILE = INPUT_DIR / "ConditionParticulieres.xls"
OUTPUT_CSV = OUTPUT_DIR / "cp.csv"
COLLECTION = "cp"

HEADER_ROW = 7

COLUMNS_NEEDED = [
    "Gestionnaire",
    "WW",
    "IMM",
    "NUM chassis",
    "Marque",
    "Modèle",
    "Libellé version long",
    "Type location",
    "Date MCE",
    "Date début contrat",
    "Date fin contrat",
    "Type",
    "Jockey",
]

DATE_COLUMNS = ["Date MCE", "Date début contrat", "Date fin contrat"]

INDEX_SPECS = [
    ([("IMM", 1)], "imm"),
    ([("WW", 1)], "ww"),
]


def parse_date_for_sort(val):
    """Return a sortable datetime for dedup, or datetime.min if unparseable."""
    if val is None:
        return datetime.min
    try:
        if isinstance(val, datetime):
            return val
        if pd.isnull(val):
            return datetime.min
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    for fmt in CP_PARC_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return datetime.min


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

    # ── Dedup: group by WW, pick real IMM, use latest Date fin contrat ───────
    def is_real_imm(imm: str) -> bool:
        imm = imm.strip()
        if imm in ("", "nan"):
            return False
        if re.search(r"WW", imm, re.IGNORECASE):
            return False
        return True

    df["_ww_key"]    = df["WW"].apply(lambda x: str(x).strip())
    df["_sort_date"] = df["Date fin contrat"].apply(parse_date_for_sort)
    df["_real_imm"]  = df["IMM"].apply(lambda x: 1 if is_real_imm(str(x)) else 0)

    # Drop rows where WW is empty
    df = df[~df["_ww_key"].isin(["", "nan"])]

    # For each WW group: get the latest Date fin contrat
    latest_date = (
        df.groupby("_ww_key")["Date fin contrat"]
        .apply(lambda s: s.iloc[s.apply(parse_date_for_sort).argmax()])
        .reset_index()
        .rename(columns={"Date fin contrat": "_latest_fin"})
    )

    # Pick the best representative row per WW: real IMM first, then latest date
    df = df.sort_values(["_ww_key", "_real_imm", "_sort_date"], ascending=[True, False, False])
    df = df.drop_duplicates(subset=["_ww_key"], keep="first")

    # Merge latest Date fin contrat back in
    df = df.merge(latest_date, on="_ww_key", how="left")
    df["Date fin contrat"] = df["_latest_fin"]

    df = df.drop(columns=["_sort_date", "_real_imm", "_ww_key", "_latest_fin"])

    print(f"  → {len(df)} unique IMM rows after dedup", flush=True)

    for col in [c.strip() for c in DATE_COLUMNS]:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: format_date(v, CP_PARC_FORMATS))

    for col in df.columns:
        if col not in [c.strip() for c in DATE_COLUMNS]:
            df[col] = df[col].apply(clean_val)

    return df


def write_csv(df: pd.DataFrame) -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ {len(df)} rows → {OUTPUT_CSV}")


# ── CSV → Mongo ───────────────────────────────────────────────────────────────
def extract_mongo_records() -> list[dict]:
    if not OUTPUT_CSV.exists():
        raise FileNotFoundError(f"❌ File not found: {OUTPUT_CSV}\n   CSV step must run first.")

    df = pd.read_csv(OUTPUT_CSV, dtype=str)
    df.columns = [c.strip() for c in df.columns]

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
    print(f"  🔧 Recreated indexes on {COLLECTION}: imm, ww", flush=True)

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
