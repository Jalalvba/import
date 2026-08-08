"""Phase 1 (corrected): ds/bc get the indexes missing from production that
the Phase 0 audit didn't already find in place -- ds.INDEX_SPECS/
bc.INDEX_SPECS deliberately exclude what's already live (see the comments
next to each constant), so this only proves the *new* ones get created,
idempotently, via lib.mongo.ensure_indexes()."""
import mongomock

import bc
import ds
from lib.mongo import ensure_indexes


def test_ds_index_specs_are_created():
    db = mongomock.MongoClient()["scratch_db"]

    ensure_indexes(db, "ds", ds.INDEX_SPECS)

    indexes = db["ds"].index_information()
    assert indexes["cmd_num"]["key"] == [("cmd_num", 1)]
    assert indexes["technicien"]["key"] == [("technicien", 1)]


def test_bc_index_specs_are_created():
    db = mongomock.MongoClient()["scratch_db"]

    ensure_indexes(db, "bc", bc.INDEX_SPECS)

    indexes = db["bc"].index_information()
    assert indexes["immatriculation_date_bc"]["key"] == [("immatriculation", 1), ("date_bc", -1)]


def test_ensure_indexes_is_idempotent():
    db = mongomock.MongoClient()["scratch_db"]

    ensure_indexes(db, "ds", ds.INDEX_SPECS)
    ensure_indexes(db, "ds", ds.INDEX_SPECS)  # second call must not error or duplicate

    indexes = db["ds"].index_information()
    assert len(indexes) == 1 + len(ds.INDEX_SPECS)  # _id_ plus the two new ones
