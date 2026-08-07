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
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from pymongo.errors import PyMongoError

from lib.transform import CP_PARC_FORMATS, clean_val, format_date
from lib.validate import validate_any_non_empty, validate_columns
from lib.mongo import atomic_reload, df_to_mongo_records, get_mongo_db, log_refresh_counts
from lib.pipeline_log import PipelineLogger

COLLECTION = "parc"
FILENAME   = "Fullparcs.xls"
PIPELINE_NAME = "parc"

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

INDEX_SPECS = [
    ([("Immatriculation", 1)], "immatriculation"),
    ([("Numéro WW", 1)], "numero_ww"),
    ([("N° de chassis", 1)], "n_de_chassis"),
]


# ── Excel → Mongo records ──────────────────────────────────────────────────────
def extract_transform(file_bytes: bytes, logger: PipelineLogger) -> pd.DataFrame:
    if not file_bytes:
        raise ValueError(f"❌ No bytes provided for {FILENAME}")

    logger.log("excel_parse", "started", f"parsing {FILENAME}")
    df = pd.read_excel(io.BytesIO(file_bytes), engine="calamine", header=HEADER_ROW)
    df.columns = [c.strip() for c in df.columns]
    logger.log("excel_parse", "success", f"read {len(df)} rows, {len(df.columns)} columns")

    needed_stripped = [c.strip() for c in COLUMNS_NEEDED]
    logger.log("column_validation", "started")
    validate_columns(df, needed_stripped)
    validate_any_non_empty(df, ["Immatriculation", "Numéro WW"])
    logger.log(
        "column_validation", "success",
        f"all {len(needed_stripped)} required columns present, "
        f"'Immatriculation'/'Numéro WW' not both empty",
    )

    df = df[needed_stripped].copy()
    rows_in = len(df)

    logger.log("transform_filter", "started")

    for col in [c.strip() for c in DATE_COLUMNS]:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: format_date(v, CP_PARC_FORMATS))

    for col in df.columns:
        if col not in [c.strip() for c in DATE_COLUMNS]:
            df[col] = df[col].apply(clean_val)

    # Drop rows where Immatriculation and Numéro WW are both empty
    df = df[
        df["Immatriculation"].notna() & (df["Immatriculation"].str.strip() != "") |
        df["Numéro WW"].notna() & (df["Numéro WW"].str.strip() != "")
    ]
    rows_out = len(df)
    logger.log(
        "transform_filter", "success",
        f"{rows_in} rows in, {rows_out} rows out, {rows_in - rows_out} dropped "
        f"(missing both Immatriculation and Numéro WW)",
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

    before_count = db[COLLECTION].count_documents({})

    logger.log("mongo_push", "started", f"{len(records)} records")
    try:
        atomic_reload(db, COLLECTION, records, INDEX_SPECS)
    except (PyMongoError, RuntimeError) as e:
        logger.log("mongo_push", "failed", f"'{COLLECTION}' left untouched: {e}")
        db.client.close()
        sys.exit(1)

    after_count = db[COLLECTION].count_documents({})
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
    from lib.gdrive import download_file_bytes, find_file_metadata, verify_download_size
    from lib.pipeline_state import force_requested, release_lock, resolve_pipeline_run, update_state

    load_dotenv(dotenv_path=Path(__file__).parent / ".env")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not folder_id:
        raise EnvironmentError("❌ GOOGLE_DRIVE_FOLDER_ID not set in .env")

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
            status = main(fetched_bytes, logger=run_logger)
        except BaseException:
            raise
        else:
            if status == "success":
                update_state(PIPELINE_NAME, FILENAME, file_meta["id"], file_meta["modifiedTime"], run_logger.run_id)
    finally:
        release_lock(PIPELINE_NAME, run_logger.run_id)
