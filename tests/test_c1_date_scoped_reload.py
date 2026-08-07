"""Regression tests for C1: date_scoped_reload must not lose data when
insert_many fails after delete_many. See AGENTS.md rule 3."""
from datetime import datetime, timezone

import pytest

from lib.mongo import date_scoped_reload

EARLIEST = datetime(2026, 1, 1, tzinfo=timezone.utc)
YEAR = 2026
EXISTING = [{"Date DS": datetime(2026, 3, 1, tzinfo=timezone.utc), "N°DS": f"DS{i}"} for i in range(50)]
NEW_RECORDS = [{"Date DS": datetime(2026, 4, 1, tzinfo=timezone.utc), "N°DS": f"NEW{i}"} for i in range(10)]


def test_insert_failure_rolls_back_delete(fake_transactional_db):
    db, col = fake_transactional_db("ds", EXISTING)
    col.fail_insert = True

    with pytest.raises(RuntimeError):
        date_scoped_reload(db, "ds", NEW_RECORDS, "Date DS", EARLIEST, YEAR)

    assert col.count_documents() == 50, "delete_many must be rolled back when insert_many fails"


def test_successful_reload_replaces_window(fake_transactional_db):
    db, col = fake_transactional_db("ds", EXISTING)

    date_scoped_reload(db, "ds", NEW_RECORDS, "Date DS", EARLIEST, YEAR)

    assert col.count_documents() == 10
    assert all(d["N°DS"].startswith("NEW") for d in col.docs)
