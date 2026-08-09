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

Only [earliest Date BC in the source file, end of year) is touched in Atlas —
records before that date are left untouched. (New in this pipeline —
bc previously had no Mongo push at all.)

Usage:
    python bc.py           # fetches from Drive, defaults to current year
    python bc.py 2026      # explicit year

Requirements:
    pip install pandas python-calamine pymongo python-dotenv google-api-python-client google-auth
"""

import io
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from pymongo.errors import PyMongoError

from lib.transform import BC_DS_FORMATS, clean_numeric, clean_val, format_date
from lib.validate import validate_columns, validate_non_empty
from lib.mongo import df_to_mongo_records, date_scoped_reload, ensure_indexes, get_mongo_db, log_refresh_counts
from lib.pipeline_log import PipelineLogger
from lib.field_mapping import apply_field_mapping, validate_against_registry

COLLECTION = "bc"
FILENAME   = "YBONTEC.xlsx"
PIPELINE_NAME = "bc"

COLUMNS_NEEDED = [
    "cmd_num",
    "immatriculation",
    "date_bc",
    "fournisseurs",
    "code_article",
    "description_article",
    "pu",
    "qte",
    "n_ds",
    "cree_par",
]

DATE_COLUMNS = ["date_bc"]
DATE_FIELD = "date_bc"
# pu/qte are stored as native BSON numbers, not strings -- see
# lib.transform.clean_numeric(). jalal's own aggregation code already
# tolerates either representation defensively ($toString/$convert), so
# this is a safe cutover; see the field-mapping deploy plan for the
# Phase 2 audit that confirmed it.
NUMERIC_COLUMNS = ["pu", "qte"]

# "cmd_num_code_article" already covers cmd_num (as its leftmost field) --
# only the immatriculation+date_bc compound is missing.
INDEX_SPECS = [
    ([("immatriculation", 1), ("date_bc", -1)], "immatriculation_date_bc"),
]


# ── Excel → Mongo records ──────────────────────────────────────────────────────
def extract_transform(file_bytes: bytes, logger: PipelineLogger) -> pd.DataFrame:
    if not file_bytes:
        raise ValueError(f"❌ No bytes provided for {FILENAME}")

    logger.log("excel_parse", "started", f"parsing {FILENAME}")
    # header=0 → row 1 is the header
    df = pd.read_excel(io.BytesIO(file_bytes), engine="calamine", header=0)
    logger.log("excel_parse", "success", f"read {len(df)} rows, {len(df.columns)} columns")

    df = apply_field_mapping(df, COLLECTION)
    validate_against_registry(COLLECTION, COLUMNS_NEEDED)

    logger.log("column_validation", "started")
    validate_columns(df, COLUMNS_NEEDED)
    validate_non_empty(df, "cmd_num")
    logger.log("column_validation", "success", f"all {len(COLUMNS_NEEDED)} required columns present, 'cmd_num' not empty")

    df = df[COLUMNS_NEEDED].copy()
    rows_in = len(df)

    logger.log("transform_filter", "started")

    # Date columns → ISO string
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: format_date(v, BC_DS_FORMATS))

    # Numeric columns → native int/float (or None if blank/malformed)
    raw_numeric = {col: df[col].copy() for col in NUMERIC_COLUMNS if col in df.columns}
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(clean_numeric)

    # All other columns → clean string
    for col in df.columns:
        if col not in DATE_COLUMNS and col not in NUMERIC_COLUMNS:
            df[col] = df[col].apply(clean_val)

    # A malformed (non-blank) numeric cell is set to null, same as a
    # blank one, but logged as a warning rather than passing silently --
    # never crashes the run, never silently coerces to 0.
    for col, raw in raw_numeric.items():
        blank = raw.apply(lambda v: v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == "")
        malformed = df[col].isna() & ~blank
        if malformed.any():
            samples = raw[malformed].astype(str).head(5).tolist()
            logger.log(
                "transform_filter", "warning",
                f"{int(malformed.sum())} malformed '{col}' value(s) set to null, e.g. {samples}",
            )

    # Drop rows where cmd_num is empty
    df = df[df["cmd_num"].notna() & (df["cmd_num"].str.strip() != "")]
    rows_out = len(df)
    logger.log(
        "transform_filter", "success",
        f"{rows_in} rows in, {rows_out} rows out, {rows_in - rows_out} dropped (missing cmd_num)",
    )

    return df


def extract_mongo_records(df: pd.DataFrame, year: int) -> tuple[list[dict], object]:
    records = df_to_mongo_records(df, DATE_COLUMNS, NUMERIC_COLUMNS)
    year_records = [r for r in records if r.get(DATE_FIELD) is not None and r[DATE_FIELD].year == year]

    if not year_records:
        return [], None

    earliest_date = min(r[DATE_FIELD] for r in year_records)
    return year_records, earliest_date


def push_to_mongo(df: pd.DataFrame, year: int, logger: PipelineLogger) -> bool:
    """Returns True if records were actually pushed, False if the run was
    skipped for having zero records -- callers must not treat a False
    return as a success (see main())."""
    records, earliest_date = extract_mongo_records(df, year)
    if not records:
        logger.log("mongo_push", "skipped", f"no records for year {year}")
        return False

    logger.log("mongo_connect", "started")
    db = get_mongo_db()
    logger.log("mongo_connect", "success")

    before_count = db[COLLECTION].count_documents({})

    logger.log("mongo_push", "started", f"{len(records)} records, earliest={earliest_date.date()}")
    try:
        date_scoped_reload(db, COLLECTION, records, DATE_FIELD, earliest_date, year)
    except (PyMongoError, RuntimeError) as e:
        logger.log("mongo_push", "failed", f"'{COLLECTION}' window [{earliest_date.date()}, {year}] left untouched: {e}")
        db.client.close()
        sys.exit(1)

    after_count = db[COLLECTION].count_documents({})
    log_refresh_counts(before_count, after_count)
    logger.log(
        "mongo_push", "success",
        f"before={before_count} after={after_count} diff={after_count - before_count}",
    )

    logger.log("index_ensure", "started", f"{len(INDEX_SPECS)} index(es) on '{COLLECTION}'")
    ensure_indexes(db, COLLECTION, INDEX_SPECS)
    logger.log("index_ensure", "success")

    db.client.close()
    return True


# ── Main ──────────────────────────────────────────────────────────────────────
def main(file_bytes: bytes, year: int | None = None, logger: PipelineLogger | None = None) -> str:
    """Returns the run's finish status: "success" (pushed), "skipped"
    (zero usable records -- pipeline_state must NOT be updated for this,
    so the next trigger genuinely retries instead of trusting a false
    "already processed" state), or raises on failure."""
    year = year if year is not None else datetime.now().year
    if logger is None:
        logger = PipelineLogger(PIPELINE_NAME)

    try:
        df = extract_transform(file_bytes, logger)
        pushed = push_to_mongo(df, year, logger)
        status = "success" if pushed else "skipped"
        logger.finish(status)
        return status
    except BaseException as e:
        logger.log("pipeline_error", "failed", str(e))
        logger.finish("failed")
        raise


if __name__ == "__main__":
    from lib.gdrive import download_file_bytes, find_file_metadata, verify_download_size
    from lib.pipeline_state import force_requested, release_lock, resolve_pipeline_run, update_state

    load_dotenv(dotenv_path=Path(__file__).parent / ".env")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not folder_id:
        raise EnvironmentError("❌ GOOGLE_DRIVE_FOLDER_ID not set in .env")

    arg_year = int(sys.argv[1]) if len(sys.argv) > 1 else None

    run_logger = PipelineLogger(PIPELINE_NAME)
    run_logger.log("drive_auth", "started")
    try:
        file_meta = find_file_metadata(folder_id, FILENAME)
    except Exception as e:
        run_logger.log("drive_auth", "failed", str(e))
        run_logger.finish("failed")
        raise
    run_logger.log("drive_auth", "success")

    if file_meta is None:
        run_logger.log("drive_listing", "failed", f"{FILENAME} not found in Drive folder")
        run_logger.finish("failed")
        raise FileNotFoundError(f"❌ '{FILENAME}' not found in Drive folder {folder_id}")
    run_logger.log("drive_listing", "success", f"located {FILENAME} in Drive folder")

    decision = resolve_pipeline_run(PIPELINE_NAME, file_meta, run_logger, force=force_requested())
    if decision in ("skip_unchanged", "already_running"):
        sys.exit(0)
    if decision == "hard_fail":
        sys.exit(1)

    try:
        fetched_bytes = download_file_bytes(file_meta["id"])
        try:
            verify_download_size(fetched_bytes, file_meta)
        except ValueError as e:
            run_logger.log("file_download", "failed", str(e))
            run_logger.finish("failed")
            raise
        run_logger.log("file_download", "success", f"{FILENAME}: {len(fetched_bytes):,} bytes")

        try:
            status = main(fetched_bytes, arg_year, logger=run_logger)
        except BaseException:
            raise
        else:
            if status == "success":
                update_state(PIPELINE_NAME, FILENAME, file_meta["id"], file_meta["modifiedTime"], run_logger.run_id)
    finally:
        release_lock(PIPELINE_NAME, run_logger.run_id)
