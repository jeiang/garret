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

- `pins` table keyed by name → root hash; eviction candidate query excludes
  the closure of every pin (garret already walks references for the
  closure-completeness invariant, so the machinery exists).
- `garret pin <name> <path>`, `garret unpin <name>`, `garret pins` via the
  Pusher (writer) — the browse API lists them read-only.
- Pins count toward quota (ncps semantics): a quota made entirely of pins
  stops evicting and surfaces in metrics rather than silently thrashing.

## Score

Speed **low** · Ops **med** (protects against rebuild emergencies) ·
UX **high** (one command guards a release).
