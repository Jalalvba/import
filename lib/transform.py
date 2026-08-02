"""
lib/transform.py
-----------------
Pure per-value transforms shared by every avis ETL pipeline. No I/O.

format_date() takes an explicit `formats` tuple rather than a single
merged list — bc.py/ds.py and cp.py/parc.py read different source
systems (YBONTEC.xlsx/YFACSCALDS.xlsx vs ConditionParticulieres.xls/
Fullparcs.xls) whose text-formatted dates have historically needed
different tried-format orders (BC_DS_FORMATS uniquely tries %m/%d/%Y).
Merging them silently would risk one source's dates being misparsed by
a format intended for the other.
"""

import math
import re
from datetime import datetime

import pandas as pd

BC_DS_FORMATS = ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y %H:%M:%S")
CP_PARC_FORMATS = ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y %H:%M:%S")


def format_date(val, formats):
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
    for fmt in formats:
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
