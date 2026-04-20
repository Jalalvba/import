#!/usr/bin/env python3
"""
ds_csv.py
---------
Reads ~/avis/input/YFACSCALDS.xlsx,
cleans and normalizes all fields,
outputs ~/avis/output/ds.csv ready for MongoDB Compass.

Usage:
    python ds_csv.py

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
INPUT_FILE = INPUT_DIR / "YFACSCALDS.xlsx"
OUTPUT_CSV = OUTPUT_DIR / "ds.csv"

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
    df = pd.read_excel(INPUT_FILE, header=1)
    df.columns = [c.strip() for c in df.columns]

    needed_stripped = [c.strip() for c in COLUMNS_NEEDED]
    missing = [c for c in needed_stripped if c not in df.columns]
    if missing:
        print(f"❌ Missing columns: {missing}")
        return

    df = df[needed_stripped].copy()

    for col in [c.strip() for c in DATE_COLUMNS]:
        if col in df.columns:
            df[col] = df[col].apply(format_date)

    for col in df.columns:
        if col not in [c.strip() for c in DATE_COLUMNS]:
            df[col] = df[col].apply(clean_val)

    key = "N°DS"
    df = df[df[key].notna() & (df[key].str.strip() != "")]

    OUTPUT_DIR.mkdir(exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print(f"✅ {len(df)} rows → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
