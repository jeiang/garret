# 23 — `garret-admin fsck`: DB↔S3 consistency audit and repair

Status: implemented (2026-08; spec docs updated).

Evidence: [survey](../research/similar-projects-survey.md).

## Problem

The invariant is "row exists ⇔ blob exists" (spec 02/03), maintained by
ordering and the orphan sweep. Nothing *verifies* it after the fact: a
manually deleted S3 object, an interrupted restore, or bucket drift leaves
narinfos that 302 to 404s, and the first detection is a failed
substitution.

## Evidence

- ncps ships `fsck` with check-only / repair / dry-run modes for exactly
  this (database↔storage inconsistencies).
- attic's FAQ concedes it *cannot* detect corrupt or missing chunks and
  hand-waves manual deletion — the cautionary tale.

## Proposed shape

`garret-admin fsck` (admin socket, so the Pusher's writer connection does
the deleting), **online by default** — the same two guards ticket 11's
orphan sweep already uses to make its mirror-image check (orphan blobs,
not dangling rows) safe against in-flight pushes, without a maintenance
window:

- **Live in-flight set**: the admin socket asks the Pusher for its
  in-memory in-flight-upload set (the one the orphan sweep already
  maintains); any "dangling" row that's in it is a push in progress, not
  a defect, and is excluded before it's ever reported.
- **Age threshold**: belt-and-braces for when the in-flight set isn't
  trustworthy (Pusher just restarted, admin socket briefly down) — same
  default window as the orphan sweep's.
- List S3 keys and DB rows; report **dangling rows** (row, no blob, past
  both guards) and **orphan blobs** (blob, no row — the existing orphan
  sweep already covers these on its schedule).
- `--repair` deletes dangling rows (restoring "row ⇒ blob"); dry-run is
  the default. Optionally `--verify-sizes` compares `file_size` against
  S3 object sizes for cheap corruption smoke. `--repair` uses the same
  two guards and is online by default too.
- Offline fallback, not a default requirement: `--repair --quiesce` (or
  equivalent read-only maintenance mode — reject pushes, keep serving)
  for the degraded case where the live in-flight signal isn't available
  and the age threshold alone isn't trust enough before deleting rows —
  sccache treats read-only deployment as first-class for exactly this
  kind of situation.

## Score

Speed **low** · Ops **high** (the recovery tool a single-host cache
actually needs, runnable anytime without a maintenance window) · UX
**low**.
