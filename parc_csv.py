#!/usr/bin/env python3
"""
parc_csv.py
-----------
Reads ~/avis/input/Fullparcs.xls,
extracts needed columns, cleans and normalizes all fields,
outputs ~/avis/output/parc.csv ready for MongoDB Compass.

Usage:
    python parc_csv.py

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
INPUT_FILE = INPUT_DIR / "Fullparcs.xls"
OUTPUT_CSV = OUTPUT_DIR / "parc.csv"

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

    for col in [c.strip() for c in DATE_COLUMNS]:
        if col in df.columns:
            df[col] = df[col].apply(format_date)

    for col in df.columns:
        if col not in [c.strip() for c in DATE_COLUMNS]:
            df[col] = df[col].apply(clean_val)

    # Drop rows where Immatriculation and Numéro WW are both empty
    df = df[
        df["Immatriculation"].notna() & (df["Immatriculation"].str.strip() != "") |
        df["Numéro WW"].notna() & (df["Numéro WW"].str.strip() != "")
    ]

    OUTPUT_DIR.mkdir(exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ {len(df)} rows → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
