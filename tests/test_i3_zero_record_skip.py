"""Regression tests for I3: a zero-record pipeline run must finish as
"skipped", not "success", and must not update pipeline_state -- otherwise
skip-if-unchanged permanently stops retrying a file that never actually
ingested anything. See AGENTS.md / CLAUDE.md."""
from unittest.mock import patch

import pandas as pd

import bc
import cp
import ds
import parc
import run as pipeline_run
from lib.pipeline_log import PipelineLogger


def _empty_df(columns):
    return pd.DataFrame({c: [] for c in columns})


def test_ds_zero_records_finishes_skipped_not_success():
    logger = PipelineLogger(ds.PIPELINE_NAME)
    with patch.object(ds, "extract_transform", return_value=_empty_df(["N°DS", "Date DS"])):
        status = ds.main(b"irrelevant", 2026, logger=logger)
    assert status == "skipped"
    assert logger.status == "skipped"


def test_bc_zero_records_finishes_skipped_not_success():
    logger = PipelineLogger(bc.PIPELINE_NAME)
    with patch.object(bc, "extract_transform", return_value=_empty_df(["CMD Num", bc.DATE_FIELD])):
        status = bc.main(b"irrelevant", 2026, logger=logger)
    assert status == "skipped"
    assert logger.status == "skipped"


def test_cp_zero_records_finishes_skipped_not_success():
    logger = PipelineLogger(cp.PIPELINE_NAME)
    with patch.object(cp, "extract_transform", return_value=_empty_df(["WW"])):
        status = cp.main(b"irrelevant", logger=logger)
    assert status == "skipped"
    assert logger.status == "skipped"


def test_parc_zero_records_finishes_skipped_not_success():
    logger = PipelineLogger(parc.PIPELINE_NAME)
    with patch.object(parc, "extract_transform", return_value=_empty_df(["Immatriculation"])):
        status = parc.main(b"irrelevant", logger=logger)
    assert status == "skipped"
    assert logger.status == "skipped"


def test_run_pipeline_maps_skipped_to_skipped_empty():
    logger = PipelineLogger("ds")
    with patch.object(ds, "extract_transform", return_value=_empty_df(["N°DS", "Date DS"])):
        result_status = pipeline_run.run_pipeline({"module": ds}, b"irrelevant", logger)
    assert result_status == "skipped_empty"
