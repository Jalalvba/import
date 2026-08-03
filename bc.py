#!/usr/bin/env python3
"""
bc.py
-----
End-to-end BC (purchase order) pipeline: reads YBONTEC.xlsx fetched from
the Google Drive folder (GOOGLE_DRIVE_FOLDER_ID in .env), extracts needed
columns, cleans and normalizes all fields, renames N° BC → CMD Num, then
pushes a date-scoped partial refresh directly to the `bc` MongoDB
collection -- no CSV intermediate. Every run's full step-by-step
breakdown is persisted as one document in the `pipeline_runs` collection
(see lib/pipeline_log.py).

Only [earliest Date BC in the CSV, end of year) is touched in Atlas —
records before that date are left untouched. (New in this pipeline —
bc previously had no Mongo push at all.)

Usage:
    python bc.py           # fetches from Drive, defaults to current year
    python bc.py 2026      # explicit year

Requirements:
    pip install pandas openpyxl pymongo python-dotenv google-api-python-client google-auth
"""

import io
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from lib.transform import BC_DS_FORMATS, clean_val, format_date
from lib.validate import validate_columns
from lib.mongo import df_to_mongo_records, date_scoped_reload, get_mongo_db, log_refresh_counts
from lib.pipeline_log import PipelineLogger, persist_run

COLLECTION = "bc"
FILENAME   = "YBONTEC.xlsx"
PIPELINE_NAME = "bc"

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
DATE_FIELD = "Date BC"


# ── Excel → Mongo records ──────────────────────────────────────────────────────
def extract_transform(file_bytes: bytes, logger: PipelineLogger) -> pd.DataFrame:
    if not file_bytes:
        raise ValueError(f"❌ No bytes provided for {FILENAME}")

    logger.log("excel_parse", "started", f"parsing {FILENAME}")
    # header=0 → row 1 is the header
    df = pd.read_excel(io.BytesIO(file_bytes), header=0)

    # Strip all column names for safe matching
    df.columns = [c.strip() for c in df.columns]
    logger.log("excel_parse", "success", f"read {len(df)} rows, {len(df.columns)} columns")

    needed_stripped = [c.strip() for c in COLUMNS_NEEDED]
    logger.log("column_validation", "started")
    validate_columns(df, needed_stripped)
    logger.log("column_validation", "success", f"all {len(needed_stripped)} required columns present")

    df = df[needed_stripped].copy()
    rows_in = len(df)

    logger.log("transform_filter", "started")

    # Date columns → ISO string
    for col in [c.strip() for c in DATE_COLUMNS]:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: format_date(v, BC_DS_FORMATS))

    # All other columns → clean string
    for col in df.columns:
        if col not in [c.strip() for c in DATE_COLUMNS]:
            df[col] = df[col].apply(clean_val)

    # Rename
    df.rename(columns=RENAME, inplace=True)

    # Drop rows where CMD Num is empty
    df = df[df["CMD Num"].notna() & (df["CMD Num"].str.strip() != "")]
    rows_out = len(df)
    logger.log(
        "transform_filter", "success",
        f"{rows_in} rows in, {rows_out} rows out, {rows_in - rows_out} dropped (missing CMD Num)",
    )

    return df


def extract_mongo_records(df: pd.DataFrame, year: int) -> tuple[list[dict], object]:
    records = df_to_mongo_records(df, DATE_COLUMNS)
    year_records = [r for r in records if r.get(DATE_FIELD) is not None and r[DATE_FIELD].year == year]

    if not year_records:
        return [], None

    earliest_date = min(r[DATE_FIELD] for r in year_records)
    return year_records, earliest_date


def push_to_mongo(df: pd.DataFrame, year: int, logger: PipelineLogger) -> None:
    records, earliest_date = extract_mongo_records(df, year)
    if not records:
        logger.log("mongo_push", "skipped", f"no records for year {year}")
        return

    logger.log("mongo_connect", "started")
    db = get_mongo_db()
    logger.log("mongo_connect", "success")

    before_count = db[COLLECTION].count_documents({})

    logger.log("mongo_push", "started", f"{len(records)} records, earliest={earliest_date.date()}")
    date_scoped_reload(db, COLLECTION, records, DATE_FIELD, earliest_date, year)

    after_count = db[COLLECTION].count_documents({})
    log_refresh_counts(before_count, after_count)
    logger.log(
        "mongo_push", "success",
        f"before={before_count} after={after_count} diff={after_count - before_count}",
    )

    db.client.close()


# ── Main ──────────────────────────────────────────────────────────────────────
def main(file_bytes: bytes, year: int | None = None, logger: PipelineLogger | None = None):
    year = year if year is not None else datetime.now().year
    if logger is None:
        logger = PipelineLogger(PIPELINE_NAME)

    try:
        df = extract_transform(file_bytes, logger)
        push_to_mongo(df, year, logger)
        logger.finish("success")
    except BaseException as e:
        logger.log("pipeline_error", "failed", str(e))
        logger.finish("failed")
        persist_run(logger)
        raise
    else:
        persist_run(logger)


if __name__ == "__main__":
    from lib.gdrive import fetch_file

    load_dotenv(dotenv_path=Path(__file__).parent / ".env")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not folder_id:
        raise EnvironmentError("❌ GOOGLE_DRIVE_FOLDER_ID not set in .env")

    arg_year = int(sys.argv[1]) if len(sys.argv) > 1 else None

    run_logger = PipelineLogger(PIPELINE_NAME)
    run_logger.log("drive_auth", "started")
    try:
        fetched_bytes = fetch_file(folder_id, FILENAME)
    except Exception as e:
        run_logger.log("drive_auth", "failed", str(e))
        run_logger.finish("failed")
        persist_run(run_logger)
        raise
    run_logger.log("drive_auth", "success")
    run_logger.log("drive_listing", "success", f"located {FILENAME} in Drive folder")
    run_logger.log("file_download", "success", f"{FILENAME}: {len(fetched_bytes):,} bytes")

    main(fetched_bytes, arg_year, logger=run_logger)
