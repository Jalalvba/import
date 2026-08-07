"""Regression test for atomic_reload()'s staged-count verification: Opus's
mutation testing proved this check could be silently broken with zero
detection, which would let a partial/corrupted insert get promoted to
live production data via rename(dropTarget=True) as if it had succeeded."""
import mongomock
import pytest

from lib.mongo import atomic_reload


def test_staging_count_mismatch_is_detected_and_live_collection_untouched():
    client = mongomock.MongoClient()
    db = client["scratch_db"]
    db["cp"].insert_many([{"WW": "EXISTING-1"}, {"WW": "EXISTING-2"}])

    records = [{"WW": f"NEW-{i}"} for i in range(50)]

    # Simulate a partial insert (e.g. a driver-level silent drop): only
    # some of the intended records actually land in staging.
    real_insert_many = mongomock.collection.Collection.insert_many

    def flaky_insert_many(self, docs, *args, **kwargs):
        docs = list(docs)
        return real_insert_many(self, docs[:10], *args, **kwargs)  # drops 40

    from unittest.mock import patch
    with patch("mongomock.collection.Collection.insert_many", flaky_insert_many):
        with pytest.raises(RuntimeError, match="count mismatch"):
            atomic_reload(db, "cp", records, [])

    # Live collection must be completely untouched by the failed reload.
    assert db["cp"].count_documents({}) == 2
    live_ww = {d["WW"] for d in db["cp"].find({})}
    assert live_ww == {"EXISTING-1", "EXISTING-2"}
