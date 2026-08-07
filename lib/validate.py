"""
lib/validate.py
----------------
Pure validation logic shared by every avis ETL pipeline. No I/O.
"""


def validate_columns(df, required_columns):
    """Raise ValueError (never a silent return) if any required_columns
    are absent from df.columns."""
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")


DEFAULT_MAX_NULL_FRACTION = 0.95


def _is_blank(series):
    return series.isna() | (series.astype(str).str.strip() == "")


def validate_non_empty(df, column: str, max_null_fraction: float = DEFAULT_MAX_NULL_FRACTION) -> None:
    """Raise ValueError if `column` is more than max_null_fraction blank
    (null or empty-after-strip). validate_columns() only checks a column
    exists -- a column that's present but entirely (or almost entirely)
    empty passes that check silently, then lib.mongo's blank-field-dropping
    means affected documents end up with the key missing entirely. This
    catches that before it reaches Mongo. A no-op on an empty DataFrame
    (nothing to be a fraction of) or a genuinely absent column (that's
    validate_columns()'s job to catch, not this one's)."""
    if column not in df.columns or len(df) == 0:
        return
    blank_fraction = _is_blank(df[column]).mean()
    if blank_fraction > max_null_fraction:
        raise ValueError(
            f"column '{column}' is {blank_fraction:.1%} empty (threshold "
            f"{max_null_fraction:.0%}) -- likely a broken or renamed source column"
        )


def validate_any_non_empty(df, columns: list[str], max_null_fraction: float = DEFAULT_MAX_NULL_FRACTION) -> None:
    """Like validate_non_empty(), but for a set of columns where business
    logic only requires AT LEAST ONE to be populated per row (e.g. parc.py
    accepts a row with either Immatriculation or Numéro WW). Raises if the
    fraction of rows where ALL of `columns` are blank exceeds
    max_null_fraction. A no-op on an empty DataFrame or if none of
    `columns` are present (validate_columns()'s job to catch)."""
    present_columns = [c for c in columns if c in df.columns]
    if not present_columns or len(df) == 0:
        return
    all_blank = _is_blank(df[present_columns[0]])
    for c in present_columns[1:]:
        all_blank &= _is_blank(df[c])
    blank_fraction = all_blank.mean()
    if blank_fraction > max_null_fraction:
        raise ValueError(
            f"columns {present_columns} are ALL blank together in {blank_fraction:.1%} of rows "
            f"(threshold {max_null_fraction:.0%}) -- likely a broken or renamed source column"
        )
