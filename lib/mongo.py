"""
lib/mongo.py
------------
Shared MongoDB I/O for the avis ETL pipelines.
"""

import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import PyMongoError


def _is_termux() -> bool:
    """Termux (Android) sets $PREFIX to its own /data/data/com.termux/...
    sandbox path -- a reliable, environment-specific signal, unlike
    checking for the absence of some other variable."""
    return os.environ.get("PREFIX", "").startswith("/data/data/com.termux")


def get_mongo_db(env_path: Path | None = None):
    """Load .env and return a connected pymongo Database handle. Callers
    close the connection via db.client.close()."""
    if env_path is None:
        env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)

    uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGODB_DB")
    if not uri or not db_name:
        raise EnvironmentError("❌ MONGODB_URI or MONGODB_DB not set in .env")

    if _is_termux():
        # Termux's bundled resolver can't resolve the SRV/TXT records
        # mongodb+srv:// URIs need; forcing 8.8.8.8 works around that.
        # Scoped to Termux only -- applying this unconditionally (as it
        # used to be) meant Vercel's runtime got its process-global DNS
        # resolver silently overridden for no reason it ever needed.
        import dns.resolver
        dns.resolver.default_resolver = dns.resolver.Resolver(configure=False)
        dns.resolver.default_resolver.nameservers = ["8.8.8.8"]

    client = MongoClient(uri)
    return client[db_name]


def df_to_mongo_records(df: pd.DataFrame, date_columns: list[str]) -> list[dict]:
    """Convert a pipeline's transformed DataFrame directly into Mongo-ready
    dicts: date_columns (already ISO 8601 strings from format_date()) are
    parsed to Python datetime (or None if blank/unparseable), every other
    blank field is dropped, and non-empty fields are kept as their
    already-cleaned string value."""
    df = df.copy()

    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    records = df.to_dict(orient="records")

    clean_records = []
    for rec in records:
        doc = {}
        for k, v in rec.items():
            if k in date_columns:
                doc[k] = v.to_pydatetime() if pd.notna(v) else None
            else:
                if pd.isna(v) or str(v).strip() == "":
                    continue
                doc[k] = str(v).strip()
        clean_records.append(doc)

    return clean_records


def atomic_reload(db, collection_name: str, records: list[dict], index_specs) -> None:
    """Insert records into a staging collection, verify the count, then
    atomically rename staging → collection_name (dropTarget=True). The live
    collection is never dropped before the replacement data is fully
    written and verified. On any failure the staging collection is dropped
    and the live collection is left untouched.

    The staging collection name is unique per call (uuid4 suffix), not a
    fixed f"{collection_name}_staging" -- two overlapping runs against the
    same collection (e.g. a manual trigger + a concurrent Vercel trigger)
    each get their own staging collection and can't stomp on each other's
    in-progress staged data. See lib/pipeline_state.py's run lock for the
    complementary guard against overlapping runs actually racing to the
    final rename.

    Refuses to run at all if records is empty -- every caller already
    guards this individually before calling (checking for zero records
    and logging mongo_push:skipped instead), but this is defense-in-depth
    at the primitive itself: a caller that ever forgot that guard would
    otherwise have this rename an empty collection over live production
    data, verified successfully (0 == 0) the whole way."""
    if not records:
        raise ValueError(
            f"atomic_reload() refused to run for '{collection_name}': records is empty -- "
            f"this would replace the live collection with nothing. Callers must check for "
            f"zero records before calling."
        )

    staging_name = f"{collection_name}_staging_{uuid.uuid4().hex[:8]}"
    db[staging_name].drop()
    try:
        db[staging_name].insert_many(records)
        staged_count = db[staging_name].count_documents({})
        if staged_count != len(records):
            raise RuntimeError(
                f"staging insert count mismatch: expected {len(records)}, got {staged_count}"
            )
        for keys, name in index_specs:
            db[staging_name].create_index(keys, name=name)
        db[staging_name].rename(collection_name, dropTarget=True)
    except Exception:
        db[staging_name].drop()
        raise


def _run_date_scoped_reload(col, records: list[dict], date_field: str, earliest_date, end, session=None) -> int:
    """The delete+insert pair, factored out so it can run either inside a
    real Mongo transaction (session set) or be exercised directly by tests
    against a fake transactional double. Not called directly outside this
    module."""
    result = col.delete_many({date_field: {"$gte": earliest_date, "$lt": end}}, session=session)
    if records:
        col.insert_many(records, session=session)
    return result.deleted_count


def date_scoped_reload(db, collection_name: str, records: list[dict], date_field: str, earliest_date, year: int) -> None:
    """Delete [earliest_date, end-of-year) from collection_name, then
    insert records, as a single Mongo multi-document transaction — Atlas
    clusters always run as a replica set, so transactions are supported.
    If insert_many fails after delete_many, the transaction aborts and the
    delete is rolled back, instead of permanently losing the date window.
    Records outside that window (i.e. before earliest_date) are
    intentionally left untouched — this is a partial, date-bounded
    refresh, not a full reload."""
    col = db[collection_name]
    end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)

    try:
        with db.client.start_session() as session:
            deleted_count = session.with_transaction(
                lambda s: _run_date_scoped_reload(col, records, date_field, earliest_date, end, session=s)
            )
    except PyMongoError as e:
        raise RuntimeError(f"date_scoped_reload failed for '{collection_name}': {e}") from e

    print(f"  🗑️  Deleted {deleted_count} records from {earliest_date.date()} to end of {year}", flush=True)
    print(f"  ✅ Inserted {len(records)} records for {year}", flush=True)


def log_refresh_counts(before_count: int, after_count: int) -> None:
    diff = after_count - before_count
    diff_str = f"+{diff}" if diff > 0 else str(diff)
    print(f"  📈 Before: {before_count}  |  After: {after_count}  |  Diff: {diff_str}")
