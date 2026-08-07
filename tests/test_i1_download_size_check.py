"""Regression tests for I1: a truncated Drive download must be caught
before pandas silently parses it as fewer rows."""
import pytest

from lib.gdrive import verify_download_size


def test_matching_size_is_a_noop():
    verify_download_size(b"1234567890", {"name": "f.xlsx", "size": "10"})


def test_mismatched_size_raises():
    with pytest.raises(ValueError, match="truncated"):
        verify_download_size(b"12345", {"name": "f.xlsx", "size": "10"})


def test_missing_size_is_a_noop():
    # Drive omits `size` for native Google Docs types -- never true for
    # this project's .xlsx/.xls inputs, but tolerated rather than treated
    # as a mismatch.
    verify_download_size(b"12345", {"name": "f.xlsx"})


def test_size_zero_still_validated_and_raises_on_mismatch():
    # A reported size of "0" is falsy-looking but a real (if unusual)
    # Drive-reported value -- it must still be validated against actual
    # bytes, not silently skipped like a genuinely absent `size` key.
    with pytest.raises(ValueError, match="truncated"):
        verify_download_size(b"12345", {"name": "f.xlsx", "size": "0"})
