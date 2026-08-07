"""Regression test for I4: pipeline_runs needs a unique index on run_id
(so /api/status lookups aren't a full collection scan) and a TTL index on
started_at (so the collection doesn't grow unbounded)."""
from unittest.mock import patch

import mongomock

import lib.pipeline_log as pl


def test_pipeline_runs_gets_run_id_and_ttl_indexes():
    client = mongomock.MongoClient()
    db = client["scratch_db"]
    db.client.close = lambda: None

    with patch.object(pl, "get_mongo_db", return_value=db):
        logger = pl.PipelineLogger("ds")
        logger.log("step1", "started")
        logger.finish("success")

    indexes = db["pipeline_runs"].index_information()

    run_id_indexes = [v for v in indexes.values() if v.get("key") == [("run_id", 1)]]
    assert run_id_indexes and run_id_indexes[0].get("unique") is True

    ttl_indexes = [v for v in indexes.values() if "expireAfterSeconds" in v]
    assert ttl_indexes and ttl_indexes[0]["expireAfterSeconds"] == 90 * 24 * 3600
