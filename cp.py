#!/usr/bin/env python3
"""
cp.py
-----
End-to-end CP (contract particulars) pipeline: reads
ConditionParticulieres.xls fetched from the Google Drive folder
(GOOGLE_DRIVE_FOLDER_ID in .env), extracts needed columns, cleans and
normalizes all fields, deduplicates by WW keeping ONE whole representative
row per WW (real IMM preferred over a WW-style placeholder, then latest
Date fin contrat as a tiebreak) -- every kept field comes from that same
row, never mixed across rows in the group -- then pushes a full atomic
reload directly to the `cp` MongoDB collection -- no CSV intermediate.
Every run's full step-by-step breakdown is persisted as one document in
the `pipeline_runs` collection (see lib/pipeline_log.py).

Usage:
    python cp.py

Requirements:
    pip install pandas openpyxl python-calamine pymongo python-dotenv google-api-python-client google-auth
"""

import io
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from pymongo.errors import PyMongoError

from lib.transform import CP_PARC_FORMATS, clean_val, format_date
from lib.validate import validate_columns, validate_non_empty
from lib.mongo import atomic_reload, df_to_mongo_records, get_mongo_db, log_refresh_counts
from lib.pipeline_log import PipelineLogger
from lib.field_mapping import apply_field_mapping, validate_against_registry

COLLECTION = "cp"
FILENAME   = "ConditionParticulieres.xls"
PIPELINE_NAME = "cp"

HEADER_ROW = 7

COLUMNS_NEEDED = [
    "gestionnaire",
    "ww",
    "imm",
    "num_chassis",
    "marque",
    "modele",
    "libelle_version_long",
    "type_location",
    "date_mce",
    "date_debut_contrat",
    "date_fin_contrat",
    "type_vh_relais",
    "jockey",
]

DATE_COLUMNS = ["date_mce", "date_debut_contrat", "date_fin_contrat"]

INDEX_SPECS = [
    ([("imm", 1)], "imm"),
    ([("ww", 1)], "ww"),
]


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
    for fmt in CP_PARC_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return datetime.min


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
    validate_non_empty(df, "ww")
    logger.log("column_validation", "success", f"all {len(COLUMNS_NEEDED)} required columns present, 'ww' not empty")

    df = df[COLUMNS_NEEDED].copy()
    rows_in = len(df)

    logger.log("transform_filter", "started")

    # ── Dedup: group by WW, pick real IMM, use latest Date fin contrat ───────
    def is_real_imm(imm: str) -> bool:
        imm = imm.strip()
        if imm in ("", "nan"):
            return False
        if re.search(r"WW", imm, re.IGNORECASE):
            return False
        return True

    df["_ww_key"]    = df["ww"].apply(lambda x: str(x).strip())
    df["_sort_date"] = df["date_fin_contrat"].apply(parse_date_for_sort)
    df["_real_imm"]  = df["imm"].apply(lambda x: 1 if is_real_imm(str(x)) else 0)

    # Drop rows where WW is empty
    df = df[~df["_ww_key"].isin(["", "nan"])]

    # Pick ONE representative row per WW: real IMM first, then latest
    # Date fin contrat as a tiebreak -- and keep ALL of that row's fields
    # together (start date, end date, vehicle type, ...), never hoisting a
    # field from a different row in the same WW group. Mixing an end date
    # from one row with the start date/vehicle details of another produced
    # "chimera" records describing a contract that occurred in no single
    # source row. "NUM chassis" is a final tiebreak so the pick is
    # deterministic independent of source row order -- without it, a tie on
    # both real-IMM and end date (e.g. two rows neither with a real IMM,
    # same end date) fell back on pandas' stable sort preserving whatever
    # order Excel happened to export rows in, which isn't guaranteed to
    # stay consistent across re-exports of the same underlying data.
    df["_chassis_key"] = df["num_chassis"].apply(lambda x: str(x).strip())
    df = df.sort_values(
        ["_ww_key", "_real_imm", "_sort_date", "_chassis_key"],
        ascending=[True, False, False, True],
    )
    df = df.drop_duplicates(subset=["_ww_key"], keep="first")

    df = df.drop(columns=["_sort_date", "_real_imm", "_ww_key", "_chassis_key"])

    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: format_date(v, CP_PARC_FORMATS))

    for col in df.columns:
        if col not in DATE_COLUMNS:
            df[col] = df[col].apply(clean_val)

    rows_out = len(df)
    logger.log(
        "transform_filter", "success",
        f"{rows_in} rows in, {rows_out} unique-WW rows out, "
        f"{rows_in - rows_out} consolidated/dropped (dedup by WW, empty-WW rows removed)",
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
        f"indexes recreated: imm, ww); before={before_count} after={after_count} "
        f"diff={after_count - before_count}",
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
