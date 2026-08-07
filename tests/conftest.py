"""Shared fakes for testing lib/mongo.py without a real Mongo connection.

mongomock is used for simple collection state (atomic_reload tests), but it
has no session/transaction support at all, so date_scoped_reload's
transaction-wrapped tests use a small hand-rolled in-memory
collection/session double that implements real Mongo transaction
semantics: session.with_transaction() snapshots state, runs the callback,
and restores the snapshot if the callback raises -- exactly what an Atlas
replica-set transaction does. This proves date_scoped_reload is wired
correctly to abort-on-failure without needing a live Mongo cluster.
"""
import pytest


class FakeDeleteResult:
    def __init__(self, deleted_count):
        self.deleted_count = deleted_count


class FakeCollection:
    def __init__(self, initial_docs=None):
        self.docs = list(initial_docs or [])
        self.fail_insert = False

    def delete_many(self, filt, session=None):
        field, cond = next(iter(filt.items()))
        gte, lt = cond["$gte"], cond["$lt"]
        before = len(self.docs)
        self.docs = [d for d in self.docs if not (gte <= d[field] < lt)]
        return FakeDeleteResult(before - len(self.docs))

    def insert_many(self, records, session=None):
        if self.fail_insert:
            raise RuntimeError("boom - simulated insert failure")
        self.docs.extend(records)

    def count_documents(self, filt=None):
        return len(self.docs)

    def find(self, filt=None):
        return list(self.docs)


class FakeSession:
    def __init__(self, col):
        self.col = col

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def with_transaction(self, callback):
        snapshot = list(self.col.docs)
        try:
            return callback(self)
        except Exception:
            self.col.docs = snapshot
            raise


class FakeClient:
    def __init__(self, col):
        self.col = col

    def start_session(self):
        return FakeSession(self.col)


class FakeDB(dict):
    """Minimal stand-in for a pymongo Database: db["name"] indexing plus
    a .client with .start_session(), scoped to a single collection since
    that's all date_scoped_reload touches."""

    def __init__(self, collection_name, col):
        super().__init__()
        self[collection_name] = col
        self.client = FakeClient(col)


@pytest.fixture
def fake_transactional_db():
    """Factory: fake_transactional_db(collection_name, initial_docs) -> (db, col)"""

    def _make(collection_name, initial_docs=None):
        col = FakeCollection(initial_docs)
        db = FakeDB(collection_name, col)
        return db, col

    return _make
