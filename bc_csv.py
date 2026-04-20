#!/usr/bin/env python3
"""
bc_csv.py
---------
Reads ~/avis/input/YBONTEC.xlsx,
extracts needed columns, cleans and normalizes all fields,
renames N° BC → CMD Num,
outputs ~/avis/output/bc.csv ready for MongoDB Compass.

Usage:
    python bc_csv.py

Requirements:
    pip install pandas openpyxl
"""

import math
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

INPUT_DIR  = Path(__file__).parent / "input"
OUTPUT_DIR = Path(__file__).parent / "output"
INPUT_FILE = INPUT_DIR / "YBONTEC.xlsx"
OUTPUT_CSV = OUTPUT_DIR / "bc.csv"

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
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        except ValueError:
            continue
    return ""


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
    # header=0 → row 1 is the header
    df = pd.read_excel(INPUT_FILE, header=0)

    # Strip all column names for safe matching
    df.columns = [c.strip() for c in df.columns]

    needed_stripped = [c.strip() for c in COLUMNS_NEEDED]
    missing = [c for c in needed_stripped if c not in df.columns]
    if missing:
        print(f"❌ Missing columns: {missing}")
        return

    df = df[needed_stripped].copy()

    # Date columns → ISO string
    for col in [c.strip() for c in DATE_COLUMNS]:
        if col in df.columns:
            df[col] = df[col].apply(format_date)

    # All other columns → clean string
    for col in df.columns:
        if col not in [c.strip() for c in DATE_COLUMNS]:
            df[col] = df[col].apply(clean_val)

    # Rename
    df.rename(columns=RENAME, inplace=True)

    # Drop rows where CMD Num is empty
    df = df[df["CMD Num"].notna() & (df["CMD Num"].str.strip() != "")]

    OUTPUT_DIR.mkdir(exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ {len(df)} rows → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
