#!/usr/bin/env python3
"""
cp.py
-----
End-to-end CP (contract particulars) pipeline: reads
ConditionParticulieres.xls fetched from the Google Drive folder
(GOOGLE_DRIVE_FOLDER_ID in .env), extracts needed columns, cleans and
normalizes all fields, deduplicates by IMM keeping the row with the
latest Date fin contrat, then pushes a full atomic reload directly to
the `cp` MongoDB collection -- no CSV intermediate. Every run's full
step-by-step breakdown is persisted as one document in the
`pipeline_runs` collection (see lib/pipeline_log.py).

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
from lib.validate import validate_columns
from lib.mongo import atomic_reload, df_to_mongo_records, get_mongo_db, log_refresh_counts
from lib.pipeline_log import PipelineLogger

COLLECTION = "cp"
FILENAME   = "ConditionParticulieres.xls"
PIPELINE_NAME = "cp"

HEADER_ROW = 7

COLUMNS_NEEDED = [
    "Gestionnaire",
    "WW",
    "IMM",
    "NUM chassis",
    "Marque",
    "Modèle",
    "Libellé version long",
    "Type location",
    "Date MCE",
    "Date début contrat",
    "Date fin contrat",
    "Type",
    "Jockey",
]

DATE_COLUMNS = ["Date MCE", "Date début contrat", "Date fin contrat"]

INDEX_SPECS = [
    ([("IMM", 1)], "imm"),
    ([("WW", 1)], "ww"),
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
    df.columns = [c.strip() for c in df.columns]
    logger.log("excel_parse", "success", f"read {len(df)} rows, {len(df.columns)} columns")

    needed_stripped = [c.strip() for c in COLUMNS_NEEDED]
    logger.log("column_validation", "started")
    validate_columns(df, needed_stripped)
    logger.log("column_validation", "success", f"all {len(needed_stripped)} required columns present")

    df = df[needed_stripped].copy()
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

    df["_ww_key"]    = df["WW"].apply(lambda x: str(x).strip())
    df["_sort_date"] = df["Date fin contrat"].apply(parse_date_for_sort)
    df["_real_imm"]  = df["IMM"].apply(lambda x: 1 if is_real_imm(str(x)) else 0)

    # Drop rows where WW is empty
    df = df[~df["_ww_key"].isin(["", "nan"])]

    # For each WW group: get the latest Date fin contrat
    latest_date = (
        df.groupby("_ww_key")["Date fin contrat"]
        .apply(lambda s: s.iloc[s.apply(parse_date_for_sort).argmax()])
        .reset_index()
        .rename(columns={"Date fin contrat": "_latest_fin"})
    )

    # Pick the best representative row per WW: real IMM first, then latest date
    df = df.sort_values(["_ww_key", "_real_imm", "_sort_date"], ascending=[True, False, False])
    df = df.drop_duplicates(subset=["_ww_key"], keep="first")

    # Merge latest Date fin contrat back in
    df = df.merge(latest_date, on="_ww_key", how="left")
    df["Date fin contrat"] = df["_latest_fin"]

    df = df.drop(columns=["_sort_date", "_real_imm", "_ww_key", "_latest_fin"])

    for col in [c.strip() for c in DATE_COLUMNS]:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: format_date(v, CP_PARC_FORMATS))

    for col in df.columns:
        if col not in [c.strip() for c in DATE_COLUMNS]:
            df[col] = df[col].apply(clean_val)

    rows_out = len(df)
    logger.log(
        "transform_filter", "success",
        f"{rows_in} rows in, {rows_out} unique-WW rows out, "
        f"{rows_in - rows_out} consolidated/dropped (dedup by WW, empty-WW rows removed)",
    )

    return df


def push_to_mongo(df: pd.DataFrame, logger: PipelineLogger) -> None:
    records = df_to_mongo_records(df, DATE_COLUMNS)
    if not records:
        logger.log("mongo_push", "skipped", "no records to push")
        return

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
        f"indexes recreated: imm, ww); before={before_count} after={after_count} "
        f"diff={after_count - before_count}",
    )

    db.client.close()


# ── Main ──────────────────────────────────────────────────────────────────────
def main(file_bytes: bytes, logger: PipelineLogger | None = None):
    if logger is None:
        logger = PipelineLogger(PIPELINE_NAME)

    try:
        df = extract_transform(file_bytes, logger)
        push_to_mongo(df, logger)
        logger.finish("success")
    except BaseException as e:
        logger.log("pipeline_error", "failed", str(e))
        logger.finish("failed")
        raise


if __name__ == "__main__":
    from lib.gdrive import download_file_bytes, find_file_metadata
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
        run_logger.log("file_download", "success", f"{FILENAME}: {len(fetched_bytes):,} bytes")

        try:
            main(fetched_bytes, logger=run_logger)
        except BaseException:
            raise
        else:
            update_state(PIPELINE_NAME, FILENAME, file_meta["id"], file_meta["modifiedTime"], run_logger.run_id)
    finally:
        release_lock(PIPELINE_NAME, run_logger.run_id)
