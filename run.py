#!/usr/bin/env python3
"""
run.py
------
Auto-detects which Excel files are present in ~/avis/input/
and runs the full pipeline (csv + refresh) for each one found.

Pipelines:
    YFACSCALDS.xlsx          → ds_csv.py  → ds_refresh.py
    ConditionParticulieres.xls → cp_csv.py  → cp_refresh.py
    Fullparcs.xls            → parc_csv.py → parc_refresh.py
    YBONTEC.xlsx             → bc_csv.py   (no refresh script yet)

Usage:
    python run.py
"""

import subprocess
import sys
from pathlib import Path

# ── Pipeline map: input file → [csv script, refresh script or None] ──────────
PIPELINES = [
    {
        "label":   "DS (Consumption sheets)",
        "input":   "YFACSCALDS.xlsx",
        "scripts": ["ds_csv.py", "ds_refresh.py"],
    },
    {
        "label":   "CP (Contract particulars)",
        "input":   "ConditionParticulieres.xls",
        "scripts": ["cp_csv.py", "cp_refresh.py"],
    },
    {
        "label":   "PARC (Fleet parks)",
        "input":   "Fullparcs.xls",
        "scripts": ["parc_csv.py", "parc_refresh.py"],
    },
    {
        "label":   "BC (Purchase orders)",
        "input":   "YBONTEC.xlsx",
        "scripts": ["bc_csv.py"],   # no refresh script yet
    },
]

ROOT      = Path(__file__).parent
INPUT_DIR = ROOT / "input"
PYTHON    = sys.executable  # use the same venv python that launched run.py


def run_script(script: str) -> bool:
    """Run a script and stream its output. Returns True if successful."""
    path = ROOT / script
    if not path.exists():
        print(f"  ⚠️  Script not found: {script} — skipping")
        return False

    result = subprocess.run(
        [PYTHON, str(path)],
        cwd=str(ROOT),
    )
    return result.returncode == 0


def main():
    print("🔍 Scanning input/ for Excel files...\n")

    found = []
    for pipeline in PIPELINES:
        if (INPUT_DIR / pipeline["input"]).exists():
            found.append(pipeline)

    if not found:
        print("❌ No Excel files found in input/")
        print("   Place one or more of these files in ~/avis/input/:")
        for p in PIPELINES:
            print(f"   - {p['input']}")
        return

    print(f"✅ Found {len(found)} file(s) to process:\n")
    for p in found:
        print(f"   • {p['input']}  →  {p['label']}")
    print()

    errors = []

    for pipeline in found:
        label   = pipeline["label"]
        scripts = pipeline["scripts"]

        print(f"{'─' * 60}")
        print(f"▶  {label}")
        print(f"{'─' * 60}")

        for script in scripts:
            print(f"\n  ⚙️  Running {script} ...")
            ok = run_script(script)
            if not ok:
                print(f"  ❌ {script} failed — stopping pipeline for {label}")
                errors.append(f"{label} / {script}")
                break  # don't run refresh if csv step failed
        print()

    print(f"{'═' * 60}")
    if errors:
        print(f"⚠️  Completed with errors:")
        for e in errors:
            print(f"   - {e}")
    else:
        print(f"✅ All pipelines completed successfully.")
    print(f"{'═' * 60}")


if __name__ == "__main__":
    main()
