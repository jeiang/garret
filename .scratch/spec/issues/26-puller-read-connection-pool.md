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

## Measured (2026-11): built, A/B tested, not adopted

A pool of one read-only connection per CPU (try-lock round robin, the
ticket-25 budgets unchanged) against the single `Mutex<Connection>`.
4-vCPU Xeon container, bench client on the same box, corpus seeded once,
Puller binary swapped between runs, 3 interleaved rounds, 30 000 requests
per step, keep-alive on (narinfo p99, mean of 3 rounds):

| Concurrency | Single connection | Pool of 4 |
|---|---|---|
| 20 | 14.7k req/s, p99 3.95 ms | 14.9k req/s, p99 3.00 ms |
| 100 | 17.7k req/s, p99 12.2 ms | 17.3k req/s, p99 12.2 ms |
| 300 | 18.0k req/s, p99 34.9 ms | 16.6k req/s, p99 43.0 ms |

Keep-alive off was within noise at every step. The pool lowers the tail
at light load and raises it under overload, where four blocking threads
compete with the Tokio workers for the same CPUs. Throughput is flat:
the box is CPU-bound, not lock-bound. Neither design wins across the
range, so the single connection stays. Revisit on a host with more
cores than the request path can use.
