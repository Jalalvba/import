"""Regression test for lib.pipeline_state's skip-if-unchanged comparison:
Opus's mutation testing proved this exact check could be silently broken
with zero detection -- either always skipping (stale data ships forever)
or never skipping (every trigger re-downloads/re-pushes unnecessarily)."""
from unittest.mock import patch

import mongomock

import lib.pipeline_state as ps
from lib.pipeline_log import PipelineLogger


def _scratch_db():
    client = mongomock.MongoClient()
    db = client["scratch_db"]
    db.client.close = lambda: None
    return db


def test_unchanged_file_is_skipped():
    db = _scratch_db()
    db["pipeline_state"].insert_one({
        "pipeline": "ds",
        "drive_file_id": "file-abc",
        "modified_time": "2026-01-01T00:00:00.000Z",
    })
    logger = PipelineLogger("ds")
    file_meta = {"id": "file-abc", "modifiedTime": "2026-01-01T00:00:00.000Z", "size": "1000"}

    with patch.object(ps, "get_mongo_db", return_value=db):
        decision = ps.resolve_pipeline_run("ds", file_meta, logger, force=False)

    assert decision == "skip_unchanged"
    assert logger.status == "skipped"


def test_changed_modified_time_is_not_skipped():
    db = _scratch_db()
    db["pipeline_state"].insert_one({
        "pipeline": "ds",
        "drive_file_id": "file-abc",
        "modified_time": "2026-01-01T00:00:00.000Z",
    })
    logger = PipelineLogger("ds")
    file_meta = {"id": "file-abc", "modifiedTime": "2026-02-01T00:00:00.000Z", "size": "1000"}

    with patch.object(ps, "get_mongo_db", return_value=db):
        decision = ps.resolve_pipeline_run("ds", file_meta, logger, force=False)

    assert decision == "proceed"


def test_changed_file_id_is_not_skipped():
    db = _scratch_db()
    db["pipeline_state"].insert_one({
        "pipeline": "ds",
        "drive_file_id": "file-abc",
        "modified_time": "2026-01-01T00:00:00.000Z",
    })
    logger = PipelineLogger("ds")
    file_meta = {"id": "file-XYZ", "modifiedTime": "2026-01-01T00:00:00.000Z", "size": "1000"}

    with patch.object(ps, "get_mongo_db", return_value=db):
        decision = ps.resolve_pipeline_run("ds", file_meta, logger, force=False)

    assert decision == "proceed"


def test_no_prior_state_is_not_skipped():
    db = _scratch_db()  # empty pipeline_state -- first run ever
    logger = PipelineLogger("ds")
    file_meta = {"id": "file-abc", "modifiedTime": "2026-01-01T00:00:00.000Z", "size": "1000"}

    with patch.object(ps, "get_mongo_db", return_value=db):
        decision = ps.resolve_pipeline_run("ds", file_meta, logger, force=False)

    assert decision == "proceed"


def test_force_bypasses_unchanged_skip():
    db = _scratch_db()
    db["pipeline_state"].insert_one({
        "pipeline": "ds",
        "drive_file_id": "file-abc",
        "modified_time": "2026-01-01T00:00:00.000Z",
    })
    logger = PipelineLogger("ds")
    file_meta = {"id": "file-abc", "modifiedTime": "2026-01-01T00:00:00.000Z", "size": "1000"}

    with patch.object(ps, "get_mongo_db", return_value=db):
        decision = ps.resolve_pipeline_run("ds", file_meta, logger, force=True)

    assert decision == "proceed"
