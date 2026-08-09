"""
lib/cli_run.py
---------------
Shared standalone-CLI runner for the four pipeline scripts' `if __name__ ==
"__main__"` blocks: Drive auth/listing, skip-if-unchanged + size-tier gate,
download + size verification, invoking the pipeline's own main(), and
updating pipeline_state on success. Each script's `main(file_bytes, ...,
logger=...)` signature differs only in whether it takes a `year` -- that
one difference is handled by each caller passing its own zero-arg-or-year
closure in, rather than this helper special-casing it.
"""

import os
import sys
from pathlib import Path
from typing import Callable

from dotenv import load_dotenv

from lib.gdrive import download_file_bytes, find_file_metadata, verify_download_size
from lib.pipeline_log import PipelineLogger
from lib.pipeline_state import force_requested, release_lock, resolve_pipeline_run, update_state


def run_standalone_pipeline(
    pipeline_name: str,
    filename: str,
    script_path: str,
    run_main: Callable[[bytes, PipelineLogger], str],
) -> None:
    """Runs one pipeline's standalone CLI invocation end to end.

    `run_main(file_bytes, run_logger)` must call the pipeline's own
    `main()` (passing through any extra args like `year`) and return its
    status string.
    """
    load_dotenv(dotenv_path=Path(script_path).parent / ".env")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not folder_id:
        raise EnvironmentError("❌ GOOGLE_DRIVE_FOLDER_ID not set in .env")

    run_logger = PipelineLogger(pipeline_name)
    run_logger.log("drive_auth", "started")
    try:
        file_meta = find_file_metadata(folder_id, filename)
    except Exception as e:
        run_logger.log("drive_auth", "failed", str(e))
        run_logger.finish("failed")
        raise
    run_logger.log("drive_auth", "success")

    if file_meta is None:
        run_logger.log("drive_listing", "failed", f"{filename} not found in Drive folder")
        run_logger.finish("failed")
        raise FileNotFoundError(f"❌ '{filename}' not found in Drive folder {folder_id}")
    run_logger.log("drive_listing", "success", f"located {filename} in Drive folder")

    decision = resolve_pipeline_run(pipeline_name, file_meta, run_logger, force=force_requested())
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
        run_logger.log("file_download", "success", f"{filename}: {len(fetched_bytes):,} bytes")

        try:
            status = run_main(fetched_bytes, run_logger)
        except BaseException:
            raise
        else:
            if status == "success":
                update_state(pipeline_name, filename, file_meta["id"], file_meta["modifiedTime"], run_logger.run_id)
    finally:
        release_lock(pipeline_name, run_logger.run_id)
