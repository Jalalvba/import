"""
lib/size_check.py
------------------
Pure size-tier thresholds for Drive input files, checked against Drive's
own reported `size` metadata (see lib.gdrive.list_expected_files /
find_file_metadata) before any bytes are downloaded.

WARN_BYTES is set well above the largest file seen in production so far
(cp.py's ConditionParticulieres.xls, ~11MB) but low enough to catch a
genuinely bigger export (e.g. a full year of data) early. HARD_FAIL_BYTES
is the point past which processing on Vercel risks a silent OOM kill or
maxDuration timeout instead of a clear error -- see lib.pipeline_state
for how the two tiers are actually applied (hard-fail only blocks
Vercel-triggered runs; a local run just gets a warning and proceeds).
"""

WARN_BYTES = 20 * 1024 * 1024        # 20MB -- ~2x the largest file seen in production to date
HARD_FAIL_BYTES = 100 * 1024 * 1024  # 100MB -- well beyond a realistic single-year export


def classify(size_bytes: int | None) -> str:
    """Returns "unknown" (Drive reported no size), "normal", "warn", or
    "hard_fail"."""
    if size_bytes is None:
        return "unknown"
    if size_bytes > HARD_FAIL_BYTES:
        return "hard_fail"
    if size_bytes > WARN_BYTES:
        return "warn"
    return "normal"
