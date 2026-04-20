#!/usr/bin/env python3
"""
cp_csv.py
---------
Reads ~/avis/input/ConditionParticulieres.xls,
extracts needed columns, cleans and normalizes all fields,
deduplicates by IMM keeping the row with the latest Date fin contrat,
outputs ~/avis/output/cp.csv ready for MongoDB Compass.

Usage:
    python cp_csv.py

Requirements:
    pip install pandas openpyxl python-calamine
"""

import math
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

INPUT_DIR  = Path(__file__).parent / "input"
OUTPUT_DIR = Path(__file__).parent / "output"
INPUT_FILE = INPUT_DIR / "ConditionParticulieres.xls"
OUTPUT_CSV = OUTPUT_DIR / "cp.csv"

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


def format_date(val):
    if val is None:
        return ""
    try:
        if isinstance(val, datetime):
            return val.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        if pd.isnull(val):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    if not s:
        return ""
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        except ValueError:
            continue
    return s


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
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return datetime.min


def clean_val(val):
    if val is None:
        return ""
    try:
        if pd.isnull(val):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return ""
        if val == int(val):
            return str(int(val))
        return str(val)
    if isinstance(val, int):
        return str(val)
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    s = str(val)
    s = s.replace("_x000a_", " ").replace("_x000d_", " ")
    s = s.replace("_x000A_", " ").replace("_x000D_", " ")
    s = re.sub(r"_x[0-9A-Fa-f]{4}_", " ", s)
    s = s.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    s = s.replace("\xa0", " ")
    s = re.sub(r" +", " ", s)
    return s.strip()


def main():
    if not INPUT_FILE.exists():
        print(f"❌ File not found: {INPUT_FILE}")
        return

    print(f"  Reading: {INPUT_FILE.name}", flush=True)
    df = pd.read_excel(INPUT_FILE, engine="calamine", header=HEADER_ROW)
    df.columns = [c.strip() for c in df.columns]

    needed_stripped = [c.strip() for c in COLUMNS_NEEDED]
    missing = [c for c in needed_stripped if c not in df.columns]
    if missing:
        print(f"❌ Missing columns: {missing}")
        return

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

    # Format date columns → ISO string
    for col in [c.strip() for c in DATE_COLUMNS]:
        if col in df.columns:
            df[col] = df[col].apply(format_date)

    # Clean all other columns
    for col in df.columns:
        if col not in [c.strip() for c in DATE_COLUMNS]:
            df[col] = df[col].apply(clean_val)

    OUTPUT_DIR.mkdir(exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ {len(df)} rows → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
