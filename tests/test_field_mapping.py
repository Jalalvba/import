"""Coverage for lib/field_mapping.py: apply_field_mapping() renames raw
dirty Excel columns to clean Mongo keys (or raises on an unmapped one),
and the four *_FIELD_MAP dicts must never produce colliding clean keys."""
import pandas as pd
import pytest

from lib.field_mapping import (
    FIELD_MAPS,
    DROPPED_COLUMNS,
    DS_FIELD_MAP,
    apply_field_mapping,
)


@pytest.mark.parametrize("collection", list(FIELD_MAPS))
def test_apply_field_mapping_renames_all_raw_columns(collection):
    field_map = FIELD_MAPS[collection]
    raw_columns = list(field_map.keys())
    df = pd.DataFrame({col: [f"val-{i}"] for i, col in enumerate(raw_columns)})

    out = apply_field_mapping(df, collection)

    assert set(out.columns) == set(field_map.values())
    for raw, clean in field_map.items():
        assert out[clean].iloc[0] == df[raw].iloc[0]


@pytest.mark.parametrize("collection", list(FIELD_MAPS))
def test_apply_field_mapping_raises_on_unmapped_column(collection):
    field_map = FIELD_MAPS[collection]
    raw_columns = list(field_map.keys())
    df = pd.DataFrame({col: ["x"] for col in raw_columns})
    df["Some Unexpected Column"] = ["x"]

    with pytest.raises(ValueError, match="unmapped"):
        apply_field_mapping(df, collection)


def test_entite_is_dropped_from_ds():
    raw_columns = list(DS_FIELD_MAP.keys()) + ["Entité"]
    df = pd.DataFrame({col: ["x"] for col in raw_columns})

    out = apply_field_mapping(df, "ds")

    assert "Entité" not in out.columns
    assert "entite" not in out.columns
    assert set(out.columns) == set(DS_FIELD_MAP.values())


def test_dropped_columns_are_ignored_not_treated_as_unmapped():
    for collection, dropped in DROPPED_COLUMNS.items():
        for col in dropped:
            df = pd.DataFrame({col: ["x"]})
            apply_field_mapping(df, collection)  # must not raise


@pytest.mark.parametrize("collection", list(FIELD_MAPS))
def test_no_clean_key_collisions_within_each_map(collection):
    clean_keys = list(FIELD_MAPS[collection].values())
    duplicates = {k for k in clean_keys if clean_keys.count(k) > 1}
    assert not duplicates, f"{collection}: duplicate clean keys {duplicates}"
