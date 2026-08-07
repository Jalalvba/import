"""Regression tests for C2: overlapping pipeline runs must not corrupt live
data via a shared staging collection name, and must be serialized by the
pipeline_state run lock. See AGENTS.md."""
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import mongomock
import pytest

import lib.pipeline_state as ps
from lib.mongo import atomic_reload


@pytest.fixture
def scratch_db():
    client = mongomock.MongoClient()
    db = client["scratch_db"]
    db.client.close = lambda: None
    return db


def test_atomic_reload_staging_names_dont_collide(scratch_db):
    scratch_db["cp"].insert_many([{"WW": f"W{i}"} for i in range(100)])

    atomic_reload(scratch_db, "cp", [{"WW": "A1"}], [])
    atomic_reload(scratch_db, "cp", [{"WW": "B1"}, {"WW": "B2"}], [])

    # No leftover staging collections, and the final collection is one
    # complete run's data, not a partial mix of both.
    assert scratch_db["cp"].count_documents({}) == 2
    staging_leftovers = [n for n in scratch_db.list_collection_names() if "_staging_" in n]
    assert staging_leftovers == []


def test_acquire_lock_blocks_concurrent_run(scratch_db):
    with patch.object(ps, "get_mongo_db", return_value=scratch_db):
        assert ps.acquire_lock("ds", "run-A") is True
        assert ps.acquire_lock("ds", "run-B") is False, "a second run must not acquire an already-held lock"


def test_release_then_reacquire(scratch_db):
    with patch.object(ps, "get_mongo_db", return_value=scratch_db):
        ps.acquire_lock("ds", "run-A")
        ps.release_lock("ds", "run-A")
        assert ps.acquire_lock("ds", "run-B") is True, "lock must be acquirable again after release"


def test_expired_lease_is_reclaimable(scratch_db):
    with patch.object(ps, "get_mongo_db", return_value=scratch_db):
        ps.acquire_lock("ds", "run-A")
        scratch_db["pipeline_state"].update_one(
            {"pipeline": "ds"},
            {"$set": {"lock_acquired_at": datetime.now(timezone.utc) - timedelta(seconds=ps.LOCK_LEASE_SECONDS + 10)}},
        )
        assert ps.acquire_lock("ds", "run-C") is True, "a stale/expired lock must be reclaimable by a new run"


def test_fresh_lease_is_not_reclaimable(scratch_db):
    with patch.object(ps, "get_mongo_db", return_value=scratch_db):
        ps.acquire_lock("ds", "run-A")
        assert ps.acquire_lock("ds", "run-B") is False, "a fresh (non-expired) lock must not be reclaimable"
