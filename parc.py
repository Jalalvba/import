#!/usr/bin/env python3
"""
parc.py
-------
End-to-end PARC (fleet park) pipeline: reads Fullparcs.xls fetched from
the Google Drive folder (GOOGLE_DRIVE_FOLDER_ID in .env), extracts
needed columns, cleans and normalizes all fields, then pushes a full
atomic reload directly to the `parc` MongoDB collection -- no CSV
intermediate. Every run's full step-by-step breakdown is persisted as
one document in the `pipeline_runs` collection (see lib/pipeline_log.py).

Usage:
    python parc.py

Requirements:
    pip install pandas openpyxl python-calamine pymongo python-dotenv google-api-python-client google-auth
"""

import io
import sys

import pandas as pd
from pymongo.errors import PyMongoError

from lib.transform import CP_PARC_FORMATS, clean_val, format_date
from lib.validate import validate_any_non_empty, validate_columns
from lib.mongo import atomic_reload, df_to_mongo_records, get_mongo_db, log_refresh_counts
from lib.pipeline_log import PipelineLogger
from lib.field_mapping import apply_field_mapping, validate_against_registry

COLLECTION = "parc"
FILENAME   = "Fullparcs.xls"
PIPELINE_NAME = "parc"

HEADER_ROW = 7  # row 8 → 0-indexed = 7

COLUMNS_NEEDED = [
    "client",
    "marque",
    "modele",
    "immatriculation",
    "numero_ww",
    "n_de_chassis",
    "etat_vehicule",
    "date_mce",
    "type_location",
    "locataire",
]

DATE_COLUMNS = ["date_mce"]

INDEX_SPECS = [
    ([("immatriculation", 1)], "immatriculation"),
    ([("numero_ww", 1)], "numero_ww"),
    ([("n_de_chassis", 1)], "n_de_chassis"),
]


# ── Excel → Mongo records ──────────────────────────────────────────────────────
def extract_transform(file_bytes: bytes, logger: PipelineLogger) -> pd.DataFrame:
    if not file_bytes:
        raise ValueError(f"❌ No bytes provided for {FILENAME}")

    logger.log("excel_parse", "started", f"parsing {FILENAME}")
    df = pd.read_excel(io.BytesIO(file_bytes), engine="calamine", header=HEADER_ROW)
    logger.log("excel_parse", "success", f"read {len(df)} rows, {len(df.columns)} columns")

    df = apply_field_mapping(df, COLLECTION)
    validate_against_registry(COLLECTION, COLUMNS_NEEDED)

    logger.log("column_validation", "started")
    validate_columns(df, COLUMNS_NEEDED)
    validate_any_non_empty(df, ["immatriculation", "numero_ww"])
    logger.log(
        "column_validation", "success",
        f"all {len(COLUMNS_NEEDED)} required columns present, "
        f"'immatriculation'/'numero_ww' not both empty",
    )

    df = df[COLUMNS_NEEDED].copy()
    rows_in = len(df)

    logger.log("transform_filter", "started")

    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: format_date(v, CP_PARC_FORMATS))

    for col in df.columns:
        if col not in DATE_COLUMNS:
            df[col] = df[col].apply(clean_val)

    # Drop rows where immatriculation and numero_ww are both empty
    df = df[
        df["immatriculation"].notna() & (df["immatriculation"].str.strip() != "") |
        df["numero_ww"].notna() & (df["numero_ww"].str.strip() != "")
    ]
    rows_out = len(df)
    logger.log(
        "transform_filter", "success",
        f"{rows_in} rows in, {rows_out} rows out, {rows_in - rows_out} dropped "
        f"(missing both immatriculation and numero_ww)",
    )

    return df


def push_to_mongo(df: pd.DataFrame, logger: PipelineLogger) -> bool:
    """Returns True if records were actually pushed, False if the run was
    skipped for having zero records -- callers must not treat a False
    return as a success (see main())."""
    records = df_to_mongo_records(df, DATE_COLUMNS)
    if not records:
        logger.log("mongo_push", "skipped", "no records to push")
        return False

    logger.log("mongo_connect", "started")
    db = get_mongo_db()
    logger.log("mongo_connect", "success")

    before_count = db[COLLECTION].estimated_document_count()

    logger.log("mongo_push", "started", f"{len(records)} records")
    try:
        atomic_reload(db, COLLECTION, records, INDEX_SPECS)
    except (PyMongoError, RuntimeError) as e:
        logger.log("mongo_push", "failed", f"'{COLLECTION}' left untouched: {e}")
        db.client.close()
        sys.exit(1)

    after_count = db[COLLECTION].estimated_document_count()
    log_refresh_counts(before_count, after_count)
    logger.log(
        "mongo_push", "success",
        f"inserted {len(records)} records into {COLLECTION} (staged + swapped, "
        f"indexes recreated: immatriculation, numero_ww, n_de_chassis); "
        f"before={before_count} after={after_count} diff={after_count - before_count}",
    )

    db.client.close()
    return True


# ── Main ──────────────────────────────────────────────────────────────────────
def main(file_bytes: bytes, logger: PipelineLogger | None = None) -> str:
    """Returns the run's finish status: "success" (pushed), "skipped"
    (zero usable records -- pipeline_state must NOT be updated for this,
    so the next trigger genuinely retries instead of trusting a false
    "already processed" state), or raises on failure."""
    if logger is None:
        logger = PipelineLogger(PIPELINE_NAME)

    try:
        df = extract_transform(file_bytes, logger)
        pushed = push_to_mongo(df, logger)
        status = "success" if pushed else "skipped"
        logger.finish(status)
        return status
    except BaseException as e:
        logger.log("pipeline_error", "failed", str(e))
        logger.finish("failed")
        raise


if __name__ == "__main__":
    from lib.cli_run import run_standalone_pipeline

    run_standalone_pipeline(
        PIPELINE_NAME, FILENAME, __file__,
        lambda file_bytes, run_logger: main(file_bytes, logger=run_logger),
    )
