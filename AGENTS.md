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
   `lib/mongo.py`'s `atomic_reload()`** — a staged insert into
   `<collection>_staging`, a verified row count, then an atomic
   `rename(dropTarget=True)` swap. The live collection is never dropped
   before the replacement data is fully written and verified, so a failed
   insert leaves the original collection untouched rather than empty.
   That said: never run either of these against a CSV you haven't just
   regenerated from a current fetch of the Drive input file — a stale or
   partial CSV faithfully replaces the entire collection with stale or
   partial data. Always check the printed before/after record counts
   before treating the run as successful.

3. **`ds.py`/`bc.py` do a partial, date-bounded write, not a full
   reload**, via `lib/mongo.py`'s `date_scoped_reload()`. Each deletes and
   reinserts only from the earliest date in the CSV (`Date DS`/`Date BC`)
   through end-of-year — records before that date are intentionally left
   untouched. Never "fix" this into a full drop+reload without an
   explicit decision to do so; it's a deliberate scope limit, not a gap.

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
   local-`input/`-absent skip behavior.

5. **All four pipeline scripts (`ds.py`, `cp.py`, `bc.py`, `parc.py`) fail
   loudly on missing required columns**, via `lib/validate.py`'s
   `validate_columns()` (raises `ValueError`). Never silently drop a
   validation check or paper over a missing/renamed Excel column with a
   default value — surface the error so the input file mismatch gets
   caught before bad data reaches `output/` or Mongo.

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
   `ds.py`, `cp.py`, `bc.py`, or `parc.py`.** Since the ETL restructuring,
   each of these always both writes `output/<name>.csv` *and* pushes to
   MongoDB in a single invocation — there is no longer a CSV-only,
   Mongo-safe way to run them. (Before the restructuring, a `*_csv.py`
   converter could be run in isolation to preview output; that safe
   subset no longer exists as a CLI entrypoint.) Read-only access to the
   filesystem and to MongoDB, always. If you need to preview what a
   pipeline would produce, read the code and the existing `output/*.csv`
   instead of executing anything.
2. **Never claim something is true without citing where it was verified**
   (a specific file/line, a specific live read of the input Excel/CSV, a
   specific MongoDB query result). Any claim about a collection's record
   count, a CSV's column contents, or an Excel file's structure must be
   based on an actual read performed during that session — not assumed,
   not remembered from a prior session, not inferred from a filename.
3. **Every Gemini/Antigravity session must end by producing a single,
   ready-to-use prompt** — written for Claude Code to execute —
   summarizing the audit findings and the exact recommended action. This
   prompt is the ONLY output the human should need to copy; Gemini/
   Antigravity itself makes zero changes to the project directly.
4. **If a task requires making an actual change** — regenerating a CSV,
   running a refresh script, editing a `.py` file — **Gemini/Antigravity
   must decline and say so explicitly**, e.g. "This requires running a
   write operation, which I cannot make. Here is the prompt to give
   Claude Code instead:" — rather than attempting it under any
   circumstance.
5. **Claude Code must independently verify any finding from a Gemini/
   Antigravity-sourced prompt against the real, live repo/data before
   acting on it** — same as any other unverified claim, per the Mandatory
   Verification Protocol below. A read-only audit tool can still be wrong
   or working from stale context; verification stays required regardless
   of source.

### Mandatory Verification Protocol (applies to all findings, any source)

- Factual claims about input file structure, CSV output contents,
  MongoDB collection state, or pipeline failure causes must be checked
  against real, live data (`git diff`, a direct read of the Excel/CSV, a
  direct MongoDB query, or an actual script run) before Claude Code acts
  on them.
- Neither tool's assertions override direct, observable output.

### Handoff Checklist (Claude Code only, since it's the sole writer)

- Before starting work from a Gemini-sourced prompt: verify its claims
  against the real repo/data first, per the protocol above.
- Before running any of the four pipeline scripts: confirm the Drive
  folder (`GOOGLE_DRIVE_FOLDER_ID`) has the current source Excel file —
  running one always both fetches it fresh from Drive, regenerates the
  CSV, and pushes to Mongo in the same invocation, there's no separate
  "just regenerate the CSV" step to check freshness against beforehand.
  Note the printed before/after record counts once it runs.
- Commit small and often so any mistaken change is trivially revertible
  (`git revert`).
