# 26 — Puller read-connection pool to reclaim the pull-path p99

Status: proposed (2026-08, follow-up to [25](25-pull-path-budgets.md)).
Later investigation — the numbers do not justify building it today.

## Problem

Ticket 25 moved the pull path's sync SQLite reads onto the blocking pool
so its budgets can actually trip on a wedged read. Measured cost (A/B,
3 runs per side, macos-aarch64, 10 000 pull requests per run at
concurrency 20):

- narinfo/redirect p50: +~25 µs (the `spawn_blocking` handoff)
- narinfo p99: 1.20 ms → 2.01 ms; redirect p99: 1.24 ms → 2.11 ms
- closed-loop throughput: 35.2k → 32.4k req/s (−7.8%)

Acceptable — p99 stays ~120× under the 250 ms budget and the throughput
ceiling is synthetic — but the tail cost has a named cause: every pull
request serializes on the single `Mutex<rusqlite::Connection>` and pays
two cross-thread wakeups, so queueing on the one connection compounds
the blocking pool's scheduling jitter.

## Proposed shape

A small pool of read-only SQLite connections (WAL readers don't block
each other), so concurrent pull requests stop serializing on one
connection. Keep the ticket-25 budgets exactly as they are — the pool
addresses queueing, the budget addresses wedging; neither replaces the
other.

## Score

Speed **low-med** (tail only; medians are already ~0.5 ms) · Ops **low**
(one more knob: pool size) · UX **low** (no client-visible change at
today's load). Revisit if real pull concurrency ever makes the p99
approach the budget, or if the degraded counter shows budget trips under
healthy disks.
