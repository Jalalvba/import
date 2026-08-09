#!/usr/bin/env python3
"""
ds.py
-----
End-to-end DS (consumption sheet) pipeline: reads YFACSCALDS.xlsx fetched
from the Google Drive folder (GOOGLE_DRIVE_FOLDER_ID in .env), cleans and
normalizes all fields, then pushes a date-scoped partial refresh directly
to the `ds` MongoDB collection -- no CSV intermediate. Every run's full
step-by-step breakdown is persisted as one document in the
`pipeline_runs` collection (see lib/pipeline_log.py), since there's no
more output/*.csv file to inspect afterward if something goes wrong.

Only [earliest Date DS in the source file, end of year) is touched in Atlas —
records before that date are left untouched.

Usage:
    python ds.py           # fetches from Drive, defaults to current year
    python ds.py 2026      # explicit year

Requirements:
    pip install pandas python-calamine pymongo python-dotenv google-api-python-client google-auth
"""

import io
import sys
from datetime import datetime

import pandas as pd

from pymongo.errors import PyMongoError

from lib.transform import BC_DS_FORMATS, clean_numeric, clean_val, format_date
from lib.validate import validate_columns, validate_non_empty
from lib.mongo import df_to_mongo_records, date_scoped_reload, ensure_indexes, get_mongo_db, log_refresh_counts
from lib.pipeline_log import PipelineLogger
from lib.field_mapping import apply_field_mapping, validate_against_registry

COLLECTION = "ds"
FILENAME   = "YFACSCALDS.xlsx"
PIPELINE_NAME = "ds"

# "immatriculation_date_ds" already exists on the live collection.
# "technicien" is deliberately not indexed -- the field is still
# extracted/stored (see COLUMNS_NEEDED below), but it's no longer
# queried by the frontend, so no index is needed for it.
INDEX_SPECS = [
    ([("cmd_num", 1)], "cmd_num"),
    ([("entite_nom", 1), ("date_ds", -1)], "entite_nom_date_ds"),
]

COLUMNS_NEEDED = [
    "date_ds",
    "n_ds",
    "code_art",
    "designation_consommation",
    "qte",
    "immatriculation",
    "km",
    "cmd_num",
    "fournisseur",
    "entite_nom",
    "description",
    "technicien",
]

DATE_COLUMNS = ["date_ds"]
# km/qte are stored as native BSON numbers, not strings -- see
# lib.transform.clean_numeric(). jalal's own aggregation code already
# tolerates either representation defensively ($toString/$convert), so
# this is a safe cutover; see the field-mapping deploy plan for the
# Phase 2 audit that confirmed it.
NUMERIC_COLUMNS = ["km", "qte"]


# ── Excel → Mongo records ──────────────────────────────────────────────────────
def extract_transform(file_bytes: bytes, logger: PipelineLogger) -> pd.DataFrame:
    if not file_bytes:
        raise ValueError(f"❌ No bytes provided for {FILENAME}")

    logger.log("excel_parse", "started", f"parsing {FILENAME}")
    df = pd.read_excel(io.BytesIO(file_bytes), engine="calamine", header=1)
    logger.log("excel_parse", "success", f"read {len(df)} rows, {len(df.columns)} columns")

    df = apply_field_mapping(df, COLLECTION)
    validate_against_registry(COLLECTION, COLUMNS_NEEDED)

    logger.log("column_validation", "started")
    validate_columns(df, COLUMNS_NEEDED)
    key = "n_ds"
    validate_non_empty(df, key)
    logger.log("column_validation", "success", f"all {len(COLUMNS_NEEDED)} required columns present, '{key}' not empty")

    df = df[COLUMNS_NEEDED].copy()
    rows_in = len(df)

    logger.log("transform_filter", "started")

    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: format_date(v, BC_DS_FORMATS))

    raw_numeric = {col: df[col].copy() for col in NUMERIC_COLUMNS if col in df.columns}
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(clean_numeric)

    for col in df.columns:
        if col not in DATE_COLUMNS and col not in NUMERIC_COLUMNS:
            df[col] = df[col].apply(clean_val)

    for col, raw in raw_numeric.items():
        blank = raw.apply(lambda v: v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == "")
        malformed = df[col].isna() & ~blank
        if malformed.any():
            samples = raw[malformed].astype(str).head(5).tolist()
            logger.log(
                "transform_filter", "warning",
                f"{int(malformed.sum())} malformed '{col}' value(s) set to null, e.g. {samples}",
            )

    key = "n_ds"
    df = df[df[key].notna() & (df[key].str.strip() != "")]
    rows_out = len(df)
    logger.log(
        "transform_filter", "success",
        f"{rows_in} rows in, {rows_out} rows out, {rows_in - rows_out} dropped (missing {key})",
    )

    return df


def extract_mongo_records(df: pd.DataFrame, year: int) -> tuple[list[dict], object]:
    records = df_to_mongo_records(df, DATE_COLUMNS, NUMERIC_COLUMNS)
    year_records = [r for r in records if r.get("date_ds") is not None and r["date_ds"].year == year]

    if not year_records:
        return [], None

    earliest_date = min(r["date_ds"] for r in year_records)
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

    before_count = db[COLLECTION].estimated_document_count()

    logger.log("mongo_push", "started", f"{len(records)} records, earliest={earliest_date.date()}")
    try:
        date_scoped_reload(db, COLLECTION, records, "date_ds", earliest_date, year)
    except (PyMongoError, RuntimeError) as e:
        logger.log("mongo_push", "failed", f"'{COLLECTION}' window [{earliest_date.date()}, {year}] left untouched: {e}")
        db.client.close()
        sys.exit(1)

    after_count = db[COLLECTION].estimated_document_count()
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
    from lib.cli_run import run_standalone_pipeline

    arg_year = int(sys.argv[1]) if len(sys.argv) > 1 else None
    run_standalone_pipeline(
        PIPELINE_NAME, FILENAME, __file__,
        lambda file_bytes, run_logger: main(file_bytes, arg_year, logger=run_logger),
    )
