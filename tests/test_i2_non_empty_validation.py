"""Regression tests for I2: a required key column that's present but
(almost) entirely empty must fail validation loudly, not silently pass
through validate_columns() and produce documents missing that key
entirely once lib.mongo drops blank fields."""
import pandas as pd
import pytest

from lib.validate import validate_any_non_empty, validate_non_empty

# ds: N°DS, bc: N° BC (pre-rename), cp: WW, parc: Immatriculation/Numéro WW


def test_mostly_populated_column_passes():
    df = pd.DataFrame({"N°DS": [f"DS{i}" for i in range(100)]})
    validate_non_empty(df, "N°DS")  # must not raise


def test_all_empty_column_raises():
    df = pd.DataFrame({"WW": [""] * 100})
    with pytest.raises(ValueError, match="empty"):
        validate_non_empty(df, "WW")


def test_mostly_empty_column_over_threshold_raises():
    df = pd.DataFrame({"CMD Num": ["X"] * 4 + [""] * 96})  # 96% empty
    with pytest.raises(ValueError, match="empty"):
        validate_non_empty(df, "CMD Num")


def test_just_under_threshold_passes():
    df = pd.DataFrame({"CMD Num": ["X"] * 6 + [""] * 94})  # 94% empty, threshold 95%
    validate_non_empty(df, "CMD Num")  # must not raise


def test_missing_column_is_noop_not_validate_columns_job():
    df = pd.DataFrame({"other": [1, 2, 3]})
    validate_non_empty(df, "N°DS")  # must not raise -- validate_columns() catches this


def test_empty_dataframe_is_noop():
    df = pd.DataFrame({"N°DS": []})
    validate_non_empty(df, "N°DS")  # must not raise -- nothing to be a fraction of


def test_any_non_empty_passes_when_one_column_populated():
    df = pd.DataFrame({
        "Immatriculation": ["A1"] * 100,
        "Numéro WW": [""] * 100,
    })
    validate_any_non_empty(df, ["Immatriculation", "Numéro WW"])  # must not raise


def test_any_non_empty_raises_when_both_columns_empty():
    df = pd.DataFrame({
        "Immatriculation": [""] * 100,
        "Numéro WW": [""] * 100,
    })
    with pytest.raises(ValueError, match="blank"):
        validate_any_non_empty(df, ["Immatriculation", "Numéro WW"])
