# 23 — `garret-admin fsck`: DB↔S3 consistency audit and repair

Status: proposed (2026-08 review). Evidence:
[survey](../research/similar-projects-survey.md).

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
the deleting):

- List S3 keys and DB rows; report **dangling rows** (row, no blob) and
  **orphan blobs** (blob, no row — the existing orphan sweep already
  covers these on its schedule).
- `--repair` deletes dangling rows (restoring "row ⇒ blob"); dry-run is
  the default. Optionally `--verify-sizes` compares `file_size` against
  S3 object sizes for cheap corruption smoke.
- Companion knob worth bundling: a read-only maintenance mode (reject
  pushes, keep serving) so fsck/restore can run without a write race —
  sccache treats read-only deployment as first-class.

## Score

Speed **low** · Ops **high** (the recovery tool a single-host cache
actually needs) · UX **low**.
