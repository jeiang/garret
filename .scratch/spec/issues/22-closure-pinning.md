# 22 — Closure pinning: GC-exempt roots

Status: proposed (2026-08 review). Evidence:
[survey](../research/similar-projects-survey.md).

## Problem

Eviction is LRU under quota (spec 05). Nothing marks "the current system
closure" or "the release toolchain" as must-keep: a burst of pushes can
evict paths that are expensive or impossible to rebuild, and the only
defence is a roomy quota.

## Evidence

- ncps: `POST/DELETE /pin/{hash}.narinfo`, `GET /pins` — pinning walks the
  references and protects the entire closure; pinned paths still count
  toward max-size; idempotent.
- cachix: named pins with `--keep-days`/`--keep-revisions` retention,
  positioned as release protection.

## Proposed shape

- `pins` table: name → `store_path_hash` (validated to exist in `objects`
  at pin time — pinning a hash never pushed is a hard error, not a
  no-op) + optional `expires_at`. Eviction candidate query excludes the
  closure of every pin whose `expires_at` is null or in the future
  (garret already walks references for the closure-completeness
  invariant, so the machinery exists).
- Pins count toward quota (ncps semantics): a quota made entirely of live
  pins stops evicting and surfaces in metrics rather than silently
  thrashing — ticket 11's existing "candidates exhausted" alarm, not new
  machinery.

**Retention: permanent by default, opt-in expiry.** ncps's model
(pin/unpin, no lifetime) over cachix's `--keep-days`/`--keep-revisions`
— that's SaaS hygiene for many forgetful tenants, and garret is
single-tenant (one operator, their own release cadence); the "forgot to
unpin, quota fills" failure is already the alarm case above, not a new
one. But permanent-only means every temporary pin ("protect this while I
test a migration") needs a remembered follow-up `unpin`, so
`--expires <duration>` is worth including as a plain nullable
`expires_at` on the row: an expired pin just stops matching the
eviction-exclusion query on its own — no sweep, no revision tracking,
evaluated inline in the same query as the null case.

**CLI placement.** `pin`/`unpin` are GC-exemption control — the same
category as `garret-admin gc run`/`fsck`/`resign`, not a routine action
— so they live on `garret-admin` (Pusher writer path), consistent with
ticket 13's locked split ("admin operations live in the separate
garret-admin binary"), not on the main `garret` client:

- `garret-admin pin <name> <hash> [--expires <duration>]`,
  `garret-admin unpin <name>` — mutations.
- `garret pins` (main client, read-only) — lists current pins via the
  browse API's `GET /api/v1/pins` (ticket 14's Puller, same OIDC gate as
  `list`/`tree`); `garret-admin` reads the same endpoint rather than a
  second Pusher-side listing.

## Score

Speed **low** · Ops **med** (protects against rebuild emergencies,
self-cleaning for temporary pins) · UX **high** (one command guards a
release).
