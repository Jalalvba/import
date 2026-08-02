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
