# 25 — Bounded budgets + degrade-to-miss on the pull path

Status: proposed (2026-08 review). Evidence:
[survey](../research/similar-projects-survey.md),
[baseline](../research/benchmark-baseline-2026-08.md).

## Problem

A substituter's contract is *bounded latency and harmless failure*: nix
tolerates a miss natively (builds locally / tries the next substituter)
but a hang stalls every build fleet-wide. Today the Puller's narinfo path
holds no deadline around the SQLite read, and the NAR path none around
presigning; a wedged disk or S3 hiccup turns into indefinitely queued
requests rather than misses.

## Evidence

- sccache: hard 60s budget on cache lookup; timeout → `MissType::TimedOut`
  → local compile. Every read-path failure (read error, decompress error)
  degrades to a miss, and **each degradation has a named counter** so the
  behaviour is observable, not silent.
- magic-nix-cache's operating rule: cache errors never fail the build.
- bazel-remote: only the expensive request class is throttled; cheap GETs
  never queue behind it.

## Proposed shape

- Wrap the narinfo DB read and the presign call in `tokio::time::timeout`
  with modest budgets (hundreds of ms; measured p99s are 1.6 ms and
  2.4 ms, so any trip means something is genuinely wrong).
- On timeout/error: return **404** (a miss nix handles) rather than 500
  where protocol-safe, and increment
  `garret_degraded_total{reason="db_timeout"|"presign_timeout"|...}`.
- Keep 503 for the not-yet-ready DB case (`/ready` already models that).

Small change, but it converts the worst failure mode (hang) into the one
failure mode clients are built for (miss) — and makes it countable.

## Score

Speed **med** (tail behaviour under stress) · Ops **high** (outages become
misses + a counter) · UX **high** (builds never wedge on the cache).
