"""Regression tests for I2: a required key column that's present but
(almost) entirely empty must fail validation loudly, not silently pass
through validate_columns() and produce documents missing that key
entirely once lib.mongo drops blank fields."""
import pandas as pd
import pytest

from lib.validate import validate_any_non_empty, validate_non_empty

# validate_non_empty()/validate_any_non_empty() run after apply_field_mapping()
# in every pipeline, so they see clean keys, not raw Excel headers:
# ds: n_ds, bc: cmd_num (post-rename), cp: ww, parc: immatriculation/numero_ww


def test_mostly_populated_column_passes():
    df = pd.DataFrame({"n_ds": [f"DS{i}" for i in range(100)]})
    validate_non_empty(df, "n_ds")  # must not raise


def test_all_empty_column_raises():
    df = pd.DataFrame({"ww": [""] * 100})
    with pytest.raises(ValueError, match="empty"):
        validate_non_empty(df, "ww")


def test_mostly_empty_column_over_threshold_raises():
    df = pd.DataFrame({"cmd_num": ["X"] * 4 + [""] * 96})  # 96% empty
    with pytest.raises(ValueError, match="empty"):
        validate_non_empty(df, "cmd_num")


def test_just_under_threshold_passes():
    df = pd.DataFrame({"cmd_num": ["X"] * 6 + [""] * 94})  # 94% empty, threshold 95%
    validate_non_empty(df, "cmd_num")  # must not raise


def test_missing_column_is_noop_not_validate_columns_job():
    df = pd.DataFrame({"other": [1, 2, 3]})
    validate_non_empty(df, "n_ds")  # must not raise -- validate_columns() catches this


def test_empty_dataframe_is_noop():
    df = pd.DataFrame({"n_ds": []})
    validate_non_empty(df, "n_ds")  # must not raise -- nothing to be a fraction of


def test_any_non_empty_passes_when_one_column_populated():
    df = pd.DataFrame({
        "immatriculation": ["A1"] * 100,
        "numero_ww": [""] * 100,
    })
    validate_any_non_empty(df, ["immatriculation", "numero_ww"])  # must not raise


def test_any_non_empty_raises_when_both_columns_empty():
    df = pd.DataFrame({
        "immatriculation": [""] * 100,
        "numero_ww": [""] * 100,
    })
    with pytest.raises(ValueError, match="blank"):
        validate_any_non_empty(df, ["immatriculation", "numero_ww"])
