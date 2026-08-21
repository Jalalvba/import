"""PIPELINE_ALLOW_NEW_FIELDS: the narrow escape hatch that lets a field be
added to a pipeline's COLUMNS_NEEDED for the first time.

field_registry.json is a live scan of fields real documents actually have,
so a brand-new field can't be in it until the pipeline that writes it has
run once -- and validate_against_registry() would otherwise block exactly
that run. These tests pin the two properties that keep the hatch safe: it
excuses only the names spelled out in it, and it stops mattering as soon
as the registry is regenerated.
"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from lib import field_mapping
from lib.field_mapping import validate_against_registry


@pytest.fixture
def registry(tmp_path, monkeypatch):
    """A registry where cp has only 'ww' -- 'statut' is unknown to it."""
    path = tmp_path / "field_registry.json"
    path.write_text(json.dumps({"collections": {"cp": ["ww"]}}), encoding="utf-8")
    monkeypatch.setattr(field_mapping, "_REGISTRY_PATH", Path(path))
    monkeypatch.delenv("PIPELINE_ALLOW_NEW_FIELDS", raising=False)
    return path


def test_new_field_raises_without_the_escape_hatch(registry):
    with pytest.raises(ValueError, match="field_registry.json drift"):
        validate_against_registry("cp", ["ww", "statut"])


def test_new_field_passes_when_named_in_allow_new_fields(registry, monkeypatch):
    monkeypatch.setenv("PIPELINE_ALLOW_NEW_FIELDS", "statut,client")
    validate_against_registry("cp", ["ww", "statut"])  # must not raise


def test_hatch_excuses_only_the_names_it_lists(registry, monkeypatch):
    """A typo elsewhere in COLUMNS_NEEDED must still be caught during the
    same bootstrap run -- the hatch is not a blanket disable."""
    monkeypatch.setenv("PIPELINE_ALLOW_NEW_FIELDS", "statut")
    with pytest.raises(ValueError, match="marque"):
        validate_against_registry("cp", ["ww", "statut", "marque"])


def test_field_verifies_normally_once_the_registry_is_regenerated(registry):
    """After scripts/export_field_registry.py re-runs, the field is in the
    registry and needs no hatch -- the env var stays unset here."""
    registry.write_text(
        json.dumps({"collections": {"cp": ["ww", "statut"]}}), encoding="utf-8"
    )
    validate_against_registry("cp", ["ww", "statut"])  # must not raise
