"""Regression tests for the minor (M1-M6) audit fixes."""
from unittest.mock import patch

import mongomock
import pandas as pd
import pytest

import cp
import lib.gdrive as gdrive
import lib.mongo as mongo_lib
from lib.mongo import atomic_reload
from lib.pipeline_log import _sanitize
from lib.pipeline_log import PipelineLogger


# M1: CP dedup tiebreaker is deterministic via NUM chassis, not row order.
def test_cp_dedup_tiebreak_is_deterministic_via_chassis():
    # Same WW, neither row has a real IMM, same end date -- the only
    # remaining tiebreak should be NUM chassis (lexicographically first).
    df = pd.DataFrame({
        "Gestionnaire": ["G1", "G1"],
        "WW": ["WW999", "WW999"],
        "IMM": ["WW999", "WW999"],  # neither is a "real" IMM
        "NUM chassis": ["ZZZ-LAST", "AAA-FIRST"],
        "Marque": ["Renault", "Peugeot"],
        "Modèle": ["Clio", "208"],
        "Libellé version long": ["Clio V", "208 II"],
        "Type location": ["LLD", "LLD"],
        "Date MCE": ["01/01/2024", "01/01/2024"],
        "Date début contrat": ["01/01/2024", "01/01/2024"],
        "Date fin contrat": ["01/06/2025", "01/06/2025"],  # tied end date
        "Type": ["VP", "VP"],
        "Jockey": ["", ""],
    })
    logger = PipelineLogger("cp")
    with patch("cp.pd.read_excel", return_value=df):
        out = cp.extract_transform(b"fake", logger)
    assert out.iloc[0]["num_chassis"] == "AAA-FIRST"

    # Reversing source row order must not change the outcome.
    df_reversed = df.iloc[::-1].reset_index(drop=True)
    logger2 = PipelineLogger("cp")
    with patch("cp.pd.read_excel", return_value=df_reversed):
        out2 = cp.extract_transform(b"fake", logger2)
    assert out2.iloc[0]["num_chassis"] == "AAA-FIRST"


# M3: atomic_reload() refuses to run with empty records, even if a caller
# forgot to guard against it.
def test_atomic_reload_refuses_empty_records():
    client = mongomock.MongoClient()
    db = client["scratch_db"]
    db["cp"].insert_many([{"WW": "W1"}, {"WW": "W2"}])

    with pytest.raises(ValueError, match="empty"):
        atomic_reload(db, "cp", [], [])

    # Live collection must be untouched.
    assert db["cp"].count_documents({}) == 2


# M4: DNS resolver workaround only applies under Termux, not unconditionally.
def test_dns_workaround_only_applies_on_termux(monkeypatch):
    monkeypatch.delenv("PREFIX", raising=False)
    assert mongo_lib._is_termux() is False

    monkeypatch.setenv("PREFIX", "/data/data/com.termux/files/usr")
    assert mongo_lib._is_termux() is True

    monkeypatch.setenv("PREFIX", "/usr")
    assert mongo_lib._is_termux() is False


# M5: Drive service client is built once and reused, not once per call.
def test_drive_service_client_is_cached():
    gdrive._service_cache = None
    build_calls = []

    def fake_build(*args, **kwargs):
        build_calls.append(1)
        return object()

    with patch.object(gdrive, "_load_service_account_credentials", return_value="creds"), \
         patch.object(gdrive, "build", side_effect=fake_build):
        gdrive._get_drive_service()
        gdrive._get_drive_service()
        gdrive._get_drive_service()

    assert len(build_calls) == 1
    gdrive._service_cache = None


# M6: exception detail is sanitized before being logged/persisted.
def test_connection_string_is_sanitized_from_log_detail():
    leaked = "connection failed: mongodb+srv://user:pass@cluster0.mongodb.net/avis"
    sanitized = _sanitize(leaked)
    assert "pass" not in sanitized
    assert "mongodb://[REDACTED]" in sanitized
