# AGENTS.md — universal AI assistant standards

This is the shared rulebook for every AI coding assistant working in this
repo (Claude Code, Gemini CLI, Antigravity, or any other agent). It holds
only the rules that must never be violated regardless of which assistant
is driving — not feature docs, not architecture, not history. The
assistant-specific entry points — [`CLAUDE.md`](./CLAUDE.md) (Claude Code)
and [`GEMINI.md`](./GEMINI.md) (Gemini CLI / Antigravity) — link to this
file rather than restating it; follow those for full setup and pipeline
detail — this file states the rule, not the case for it.

## Rules

1. **Credentials never hardcoded, never committed.** `MONGODB_URI` and
   `MONGODB_DB` live only in `.env`, loaded via `python-dotenv`. `.env` is
   git-ignored; `.env.example` (placeholder values only) is the only
   variant ever committed. No connection string, password, or key is
   ever written directly into a `.py` file, a commit, or a doc.

2. **`cp.py`/`parc.py` do a full reload of their collection via
   `lib/mongo.py`'s `atomic_reload()`** — a staged insert into a
   per-call, uuid4-suffixed staging collection (`<collection>_staging_<8
   hex chars>`, not a fixed name), a verified row count, then an atomic
   `rename(dropTarget=True)` swap. The live collection is never dropped
   before the replacement data is fully written and verified, so a failed
   insert leaves the original collection untouched rather than empty. The
   staging name being unique per call (not a fixed `<collection>_staging`)
   is deliberate, not incidental — two overlapping runs against the same
   collection used to be able to stomp on each other's in-progress staged
   data via that fixed name; each call now gets its own. Overlapping runs
   are additionally prevented altogether by the run lock in
   `lib/pipeline_state.py` (`acquire_lock()`/`release_lock()`, backed by a
   unique index on `pipeline_state.pipeline` and a `LOCK_LEASE_SECONDS`
   lease so a crashed run can't deadlock future ones) — `resolve_pipeline_run()`
   acquires it before returning `"proceed"`, and every caller (`run.py`,
   each script's `__main__`) releases it in a `finally` block regardless of
   outcome. That said: never run either of these without a current fetch of
   the Drive input file — a stale or partial in-memory transform faithfully
   replaces the entire collection with stale or partial data. Always
   check the printed before/after record counts before treating the run
   as successful.

3. **`ds.py`/`bc.py` do a partial, date-bounded write, not a full
   reload**, via `lib/mongo.py`'s `date_scoped_reload()`. Each deletes and
   reinserts only from the earliest date in the source file (`Date DS`/`Date BC`)
   through end-of-year — records before that date are intentionally left
   untouched. Never "fix" this into a full drop+reload without an
   explicit decision to do so; it's a deliberate scope limit, not a gap.
   **Safety guarantee, precisely stated:** `atomic_reload()` (rule 2) and
   `date_scoped_reload()` protect the live collection by two different
   mechanisms, not the same one — don't assume either implies the other's
   properties. `atomic_reload()` never touches the live collection until
   the full replacement set is staged and verified elsewhere, then swaps
   in via `rename()`. `date_scoped_reload()` instead deletes and inserts
   directly against the live collection, wrapped in a single Mongo
   multi-document transaction (`session.with_transaction()`, viable
   because Atlas clusters always run as a replica set) — if the insert
   fails after the delete, the whole transaction aborts and the delete is
   rolled back, so the window is never left empty. This was **not always
   true**: before the fix landed, `date_scoped_reload()` ran a bare
   `delete_many()` then `insert_many()` with no transaction and no
   rollback, so an `insert_many()` failure after a successful
   `delete_many()` permanently lost that date window. If you're touching
   `date_scoped_reload()`, keep the delete+insert pair inside the
   transaction — pulling either call out of the `session.with_transaction`
   callback reintroduces the original bug.

4. **`bc.py`'s Mongo push is new** — until the ETL restructuring, `bc`
   had no Mongo path at all (`bc_csv.py` only generated a CSV). It now
   follows the same date-scoped partial-refresh pattern as `ds.py`
   (`Date BC` → `bc` collection), confirmed with Jalal beforehand. It was
   verified end-to-end against real data for the first time on
   2026-08-03, via a `python3 run.py` run with `YBONTEC.xlsx` present in
   the Drive folder: 11959 rows written, before/after record counts
   64219/64219 (diff 0). Prior runs had only ever exercised it against
   synthetic data. `run.py` still skips `bc.py` cleanly (no error) when
   `YBONTEC.xlsx` is absent from the Drive folder, matching its old
   local-`input/`-absent skip behavior. **Sharper caution, per a later
   production `pipeline_runs` review:** as of that review, `bc.py` had
   only ever successfully *ingested* that one distinct `YBONTEC.xlsx`
   export on 2026-08-03 — every run since had been a `skip_unchanged`
   against that same file, not a fresh ingest. That means
   `date_scoped_reload()`'s delete+insert path for `bc` (now
   transaction-wrapped, see rule 3) has never actually been exercised
   against a second, *genuinely different or overlapping* `YBONTEC.xlsx`
   export — only against the one file, once. Treat any future `bc.py` run
   that processes a materially different export as effectively
   unverified in practice until it succeeds and its before/after counts
   are spot-checked, regardless of how much testing the code path itself
   has had via `ds.py` (same function, different collection/data
   patterns). Re-confirm this claim against `pipeline_runs` directly
   (`db.pipeline_runs.find({ pipeline: "bc" }).sort({ started_at: -1 })`)
   before relying on it, since it will go stale the next time `bc.py`
   actually ingests a changed file.

5. **All four pipeline scripts (`ds.py`, `cp.py`, `bc.py`, `parc.py`) fail
   loudly on missing required columns**, via `lib/validate.py`'s
   `validate_columns()` (raises `ValueError`). Never silently drop a
   validation check or paper over a missing/renamed Excel column with a
   default value — surface the error so the input file mismatch gets
   caught before bad data reaches Mongo.

6. **All paths are relative to the repo root via
   `Path(__file__).parent`.** Never introduce a hardcoded absolute path
   (home directory or otherwise) into any script — including "fixing" a
   stale docstring reference by making the code match the docstring
   instead of the other way around. If a docstring is stale, fix the
   docstring.

Do not restate or fork these rules elsewhere. If a rule needs to change,
edit it here once — every other doc links in rather than duplicating it.

## Multi-Tool Workflow: Gemini/Antigravity is Read-Only

This project uses two AI tools with strictly separated roles:

- **Claude Code**: the ONLY tool permitted to write, edit, or execute
  changes in this repository. All code changes, file edits, commits, and
  test runs — including any run of `ds.py`/`cp.py`/`bc.py`/`parc.py`
  against the live MongoDB Atlas cluster — happen exclusively through
  Claude Code.
- **Antigravity CLI / Gemini**: READ-ONLY audit and research tool. No
  exceptions.

### Hard rules for Gemini/Antigravity sessions in this repo

1. **Never write, edit, delete, or modify any file, and never run
   `ds.py`, `cp.py`, `bc.py`, or `parc.py`.** Since the Drive migration,
   each of these always fetches from Drive and pushes directly to
   MongoDB in a single invocation, with nothing written to disk at any
   point — there is no CSV-only, Mongo-safe way to run them, and (unlike
   before the CSV intermediate was removed entirely) no `output/*.csv`
   left behind afterward to inspect either. Read-only access to the
   filesystem and to MongoDB, always. If you need to preview what a
   pipeline would produce, read the code and query MongoDB directly
   instead of executing anything.
2. **Never claim something is true without citing where it was verified**
   (a specific file/line, a specific MongoDB query result). Gemini/
   Antigravity has no Drive API access and there is no `output/*.csv`
   left to inspect, so it cannot verify a source Excel file's contents
   directly at all — any claim about an Excel file's structure must be
   attributed to reading the pipeline's own validation/transform code
   (`lib/validate.py`'s `validate_columns()` call, `lib/transform.py`),
   not to having "seen" the file. Any claim about a collection's record
   count must be based on an actual MongoDB read performed during that
   session — not assumed, not remembered from a prior session, not
   inferred from a filename.
3. **Every Gemini/Antigravity session must end by producing a single,
   ready-to-use prompt** — written for Claude Code to execute —
   summarizing the audit findings and the exact recommended action. This
   prompt is the ONLY output the human should need to copy; Gemini/
   Antigravity itself makes zero changes to the project directly.
4. **If a task requires making an actual change** — triggering a fresh
   Mongo push, running a refresh script, editing a `.py` file —
   **Gemini/Antigravity must decline and say so explicitly**, e.g. "This
   requires running a write operation, which I cannot make. Here is the
   prompt to give Claude Code instead:" — rather than attempting it under
   any circumstance.
5. **Claude Code must independently verify any finding from a Gemini/
   Antigravity-sourced prompt against the real, live repo/data before
   acting on it** — same as any other unverified claim, per the Mandatory
   Verification Protocol below. A read-only audit tool can still be wrong
   or working from stale context; verification stays required regardless
   of source.

### Mandatory Verification Protocol (applies to all findings, any source)

- Factual claims about input file structure, MongoDB collection state,
  or pipeline failure causes must be checked against real, live data
  (`git diff`, a direct read of the source Excel — Claude Code has Drive
  access via `lib/gdrive.py`/`scripts/gdrive_download_check.py`, unlike Gemini/
  Antigravity — a direct MongoDB query, or an actual script run) before
  Claude Code acts on them.
- Neither tool's assertions override direct, observable output.

### Handoff Checklist (Claude Code only, since it's the sole writer)

- Before starting work from a Gemini-sourced prompt: verify its claims
  against the real repo/data first, per the protocol above.
- Before running any of the four pipeline scripts: confirm the Drive
  folder (`GOOGLE_DRIVE_FOLDER_ID`) has the current source Excel file —
  running one always both fetches it fresh from Drive and pushes to
  Mongo in the same invocation, with nothing written to disk in between,
  so there's no separate "regenerate first, then check" step to run
  beforehand. Note the printed before/after record counts once it runs.
- Commit small and often so any mistaken change is trivially revertible
  (`git revert`).
