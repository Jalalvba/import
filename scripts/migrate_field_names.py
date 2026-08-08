#!/usr/bin/env python3
"""
scripts/migrate_field_names.py
-------------------------------
One-time backfill: renames existing MongoDB documents' dirty Excel-derived
field keys to the clean names defined in lib.field_mapping, across
ds/cp/parc/bc.

Idempotent by construction, not by bookkeeping: each collection's filter
query matches only documents that still have at least one old-style key
present (every key in that collection's *_FIELD_MAP, plus any dropped
column like ds's "Entité"). A document with none of those keys -- whether
already migrated, or freshly written by a pipeline run using the new
mapping layer -- never matches the filter, on any run. Re-running this
script after a partial failure (crash, kill, network blip) picks up
exactly where it left off: no bookmark or offset is tracked anywhere,
because the filter query itself re-derives "what's left to do" from the
data every time it runs. A single-document update ($rename + $unset
together) is applied atomically by MongoDB, so no document can end up
half-migrated; and $rename on a field that doesn't exist is a documented
no-op, so there's no risk of double-renaming even in edge cases.

"Entité" (ds only) is $unset, not renamed -- confirmed redundant with
"Code entité" and confirmed to have zero jalal references, including no
blank-as-signal logic, before this script was written.

Default is a DRY RUN: reports what would change per collection, with
sample before/after documents, and writes nothing. Pass --execute to
actually perform the migration.

Usage:
    python3 scripts/migrate_field_names.py                     # dry run (default)
    python3 scripts/migrate_field_names.py --execute            # actually writes
    python3 scripts/migrate_field_names.py --execute --batch-size 500
"""

import argparse
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pymongo import UpdateOne

from lib.mongo import get_mongo_db
from lib.field_mapping import FIELD_MAPS, DROPPED_COLUMNS

SAMPLE_SIZE = 5
DEFAULT_BATCH_SIZE = 500


def _old_keys(collection: str) -> list[str]:
    return list(FIELD_MAPS[collection].keys()) + list(DROPPED_COLUMNS[collection])


def _old_key_filter(collection: str) -> dict:
    return {"$or": [{k: {"$exists": True}} for k in _old_keys(collection)]}


def _doc_hash(doc: dict) -> str:
    """Stable hash of a document's content (excluding _id, which is never
    touched by this migration and just adds noise to the comparison)."""
    d = {k: v for k, v in doc.items() if k != "_id"}
    return hashlib.sha256(json.dumps(d, default=str, sort_keys=True).encode()).hexdigest()


def _planned_changes(doc: dict, collection: str) -> dict:
    field_map = FIELD_MAPS[collection]
    dropped = DROPPED_COLUMNS[collection]
    renames = {old: new for old, new in field_map.items() if old in doc}
    unsets = [old for old in dropped if old in doc]
    return {"renames": renames, "unsets": unsets}


def _report_collection(db, collection: str) -> int:
    filt = _old_key_filter(collection)
    count = db[collection].count_documents(filt)
    print(f"\n=== {collection} ===")
    print(f"documents matching old-key filter: {count}")

    if count == 0:
        print("  nothing to migrate.")
        return count

    samples = list(db[collection].find(filt).limit(SAMPLE_SIZE))
    for doc in samples:
        changes = _planned_changes(doc, collection)
        print(f"\n  --- sample document _id={doc.get('_id')} ---")
        for old, new in changes["renames"].items():
            print(f"    rename: {old!r:45s} -> {new!r}")
        for old in changes["unsets"]:
            print(f"    drop:   {old!r}")

    return count


def _execute_collection(db, collection: str, batch_size: int) -> int:
    field_map = FIELD_MAPS[collection]
    dropped = DROPPED_COLUMNS[collection]
    filt = _old_key_filter(collection)
    col = db[collection]

    total_migrated = 0
    while True:
        batch = list(col.find(filt).limit(batch_size))
        if not batch:
            break

        ops = []
        for doc in batch:
            renames = {old: new for old, new in field_map.items() if old in doc}
            unsets = {old: "" for old in dropped if old in doc}
            update = {}
            if renames:
                update["$rename"] = renames
            if unsets:
                update["$unset"] = unsets
            if update:
                ops.append(UpdateOne({"_id": doc["_id"]}, update))

        if not ops:
            break

        result = col.bulk_write(ops, ordered=False)
        total_migrated += result.modified_count
        print(f"  [{collection}] migrated batch of {len(ops)} (running total: {total_migrated})")

    return total_migrated


def dry_run(db) -> None:
    print("=" * 72)
    print("DRY RUN -- no documents will be modified")
    print("=" * 72)

    before = {}
    for collection in FIELD_MAPS:
        before[collection] = {
            "total_count": db[collection].count_documents({}),
            "sample_hashes": [
                _doc_hash(d) for d in db[collection].find().sort("_id", 1).limit(SAMPLE_SIZE)
            ],
        }

    counts = {}
    for collection in FIELD_MAPS:
        counts[collection] = _report_collection(db, collection)

    print("\n" + "=" * 72)
    print("verifying zero writes occurred during this dry run...")
    ok = True
    for collection in FIELD_MAPS:
        after_total = db[collection].count_documents({})
        after_hashes = [
            _doc_hash(d) for d in db[collection].find().sort("_id", 1).limit(SAMPLE_SIZE)
        ]
        b = before[collection]
        if after_total != b["total_count"] or after_hashes != b["sample_hashes"]:
            print(f"  ❌ {collection}: state changed during dry run! "
                  f"before_count={b['total_count']} after_count={after_total}")
            ok = False
        else:
            print(f"  ✅ {collection}: unchanged (count={after_total}, "
                  f"{len(after_hashes)} sample doc(s) hash-verified identical)")

    print("\n" + "=" * 72)
    print("SUMMARY (dry run -- nothing written)")
    for collection, count in counts.items():
        print(f"  {collection}: {count} document(s) would be migrated")
    print("=" * 72)

    if not ok:
        print("\n⚠️  Dry run integrity check FAILED -- see above. Do not trust this report.")
        sys.exit(1)


def execute(db, batch_size: int) -> None:
    print("=" * 72)
    print("EXECUTE -- documents WILL be modified")
    print("=" * 72)

    totals = {}
    for collection in FIELD_MAPS:
        print(f"\n=== migrating {collection} ===")
        totals[collection] = _execute_collection(db, collection, batch_size)

    print("\n" + "=" * 72)
    print("SUMMARY (executed)")
    for collection, count in totals.items():
        print(f"  {collection}: {count} document(s) migrated")
    print("=" * 72)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--execute", action="store_true", help="Actually write changes. Default is dry-run (report only).")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help=f"Batch size for --execute (default {DEFAULT_BATCH_SIZE}).")
    args = parser.parse_args()

    db = get_mongo_db()
    try:
        if args.execute:
            execute(db, args.batch_size)
        else:
            dry_run(db)
    finally:
        db.client.close()


if __name__ == "__main__":
    main()
