# CHANGELOG

Narrative summary of how `avis` got to its current shape, for a future
session that only has the code + commit messages in front of it. Not a
full commit-by-commit log — see `git log` for that — just the "why" that
ties the commits together. Newest first.

## 2026-08-03 — Vercel HTTP trigger, incremental logging, skip/size safety, routing fix

**CSV intermediate removed entirely; Drive → Mongo direct.** The
previous day's Drive migration (below) still had each pipeline write a
CSV to `output/` before reading it back to push to Mongo. Vercel's
function filesystem is read-only outside `/tmp`, so that round trip
couldn't survive being deployed there. `lib/mongo.py` gained
`df_to_mongo_records()` to convert a pipeline's transformed DataFrame
straight into Mongo-ready dicts in memory; `output/`, `OUTPUT_CSV`, and
`lib/paths.py` were deleted, not just made conditional.

**`api/index.py` added** — a token-gated Vercel Python function reusing
`run.py`'s `run_all()` so a run can be triggered from a phone browser or
`curl`, not just `python run.py` locally. `PIPELINE_TRIGGER_SECRET` is
checked with a constant-time comparison before Drive/Mongo are touched
at all.

**`pipeline_runs` durable run log added**, since there was no more
`output/*.csv` to inspect after a run. Later the same day, logging
became *incremental*: `lib/pipeline_log.py`'s `PipelineLogger.log()`
writes each step to Mongo as it happens (an atomic `$push`) rather than
batching the whole run into one document at the end — so a run killed
mid-flight (a `maxDuration` timeout) still leaves a partial step history
instead of losing the log entirely. This is what makes live-progress
polling meaningful rather than just an end-of-run snapshot.

**Skip-if-unchanged + file-size safety added.** `lib/pipeline_state.py`
compares a Drive file's `id`+`modifiedTime` against the last
successfully-processed one before downloading anything, so an unchanged
file costs one metadata listing call and nothing else.
`lib/size_check.py` classifies a file's Drive-reported size into
normal/warn/hard_fail tiers *before* any download — hard_fail only
blocks on Vercel (never locally, where the documented escape hatch is
just running the script directly). `?force=true` /
`PIPELINE_FORCE_RUN=1` bypass the unchanged-skip but never the
hard-fail size check.

**Two real production incidents shaped the current limits:**
- A live trigger hit the initial `maxDuration: 60` and was killed
  mid-run (DS/CP/PARC finished, BC never started — confirmed safe only
  because `bc`'s collection count matched its last known-good state).
  `maxDuration` is now `180`.
- `vercel.json` requested `memory: 3009` (the documented absolute max)
  for headroom against a larger-than-tested file, but a deploy on this
  project's Hobby plan clamped/rejected it. It's now pinned to `2048`,
  confirmed as the actual usable ceiling on Hobby — bumping it back
  toward 3009 needs a plan upgrade first, not just a config edit.

**Bug #1 — `api/status.py` silently never worked.** It was added as a
second file under `api/` to serve a read-only `GET
/api/status?run_id=...` progress-poll route with no token requirement.
Vercel's Python runtime, however, only recognizes a fixed set of
entrypoint filenames (`index.py`, `app.py`, `server.py`, `main.py`,
`wsgi.py`, `asgi.py`) and treats whichever one exists as the sole
handler for the *entire* `/api/*` path — a second file is accepted at
deploy time without error but is simply never routed to. Every request
to `/api/status` was landing on `api/index.py`'s handler instead, which
has no route for it and, worse, unconditionally applies the trigger
route's token check — so `/api/status` calls returned the same `401
unauthorized` as a bad token on the trigger route. This was caught by
`curl`ing `/api/status`, `/api`, and a nonexistent path and observing
all three return the identical `401`. Fixed by merging both routes into
one `handler` class in `api/index.py`, dispatched by parsing
`urlparse(self.path).path` (`/api/status` → no-token status branch,
`/api`/`/api/index` → token-gated trigger branch, anything else →
`404`), and deleting `api/status.py`. Verified against production:
nonexistent path → `404`, wrong token → `401` (trigger auth intact),
valid `run_id` → `200` with real step data. **This is the one bug this
session actually found and fixed** — an initial framing of "two bugs
(routing/serialization)" going into the wrap-up pass didn't hold up:
`run_all()`'s result dicts are plain strings/lists (no `datetime`
objects), so the trigger route's original `json.dumps()` without
`default=str` was never actually at risk of a serialization crash. The
merged handler uses `default=str` uniformly now because the status
route's `pipeline_runs` documents *do* contain raw `datetime` values,
not because a second bug was found.

## 2026-08-02 — Drive migration

Pipeline input moved from a local `input/` folder to a Google Drive
folder (`GOOGLE_DRIVE_FOLDER_ID`), fetched via a service account
(`lib/gdrive.py`). The local `input/` folder was deleted from disk
entirely once every script was wired to `download_file_bytes()` instead
of a hardcoded path. `extract_transform()`/`main()` in all four pipeline
scripts now take `file_bytes: bytes` as a parameter rather than reading
`INPUT_FILE`/`INPUT_DIR` constants, which no longer exist.
