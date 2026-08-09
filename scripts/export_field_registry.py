#!/usr/bin/env python3
"""
scripts/export_field_registry.py
---------------------------------
Read-only ground-truth export: connects to the real MongoDB and, for each
collection (ds/cp/parc/bc), scans every document and records the exact,
distinct field names actually present -- not what field_mapping.py claims
should be there. Writes field_registry.json at the repo root.

This is the single source of truth both this repo (lib.field_mapping, via
its own self-check) and jalal (scripts/verify-field-names.cjs) verify
against. It must be regenerated -- see "when to regenerate" below -- any
time the real field set on any collection changes, or the two checks will
just be agreeing with a stale snapshot instead of live reality.

Safe to re-run anytime: read-only (aggregate + $group), no writes.

When to regenerate (see also CLAUDE.md "Field registry" section):
    - after scripts/migrate_field_names.py --execute (a real backfill ran)
    - after any change to lib/field_mapping.py's FIELD_MAPS
    - after any other manual migration/write that adds, renames, or
      removes a field on ds/cp/parc/bc

Usage:
    python3 scripts/export_field_registry.py
    python3 scripts/export_field_registry.py --out /path/to/field_registry.json
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.mongo import get_mongo_db
from lib.field_mapping import FIELD_MAPS

DEFAULT_OUT = Path(__file__).resolve().parent.parent / "field_registry.json"


def distinct_fields(db, collection: str) -> list[str]:
    pipeline = [
        {"$project": {"arr": {"$objectToArray": "$$ROOT"}}},
        {"$unwind": "$arr"},
        {"$group": {"_id": None, "keys": {"$addToSet": "$arr.k"}}},
    ]
    result = list(db[collection].aggregate(pipeline, allowDiskUse=True))
    keys = result[0]["keys"] if result else []
    return sorted(k for k in keys if k != "_id")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help=f"Output path (default {DEFAULT_OUT})")
    args = parser.parse_args()

    db = get_mongo_db()
    try:
        collections = {}
        for name in FIELD_MAPS:
            fields = distinct_fields(db, name)
            collections[name] = fields
            print(f"{name}: {len(fields)} distinct field(s)")
    finally:
        db.client.close()

    registry = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "live MongoDB scan, not field_mapping.py's intended mappings",
        "collections": collections,
    }

    args.out.write_text(json.dumps(registry, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()
