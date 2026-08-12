# 28 — Stepped-concurrency pull sweep in `garret-bench`

Status: proposed (2026-08, follow-up to the refreshed baseline). Evidence:
[baseline](../research/benchmark-baseline-2026-08.md) ("Deferred bench
extensions"), [ticket 26](26-puller-read-connection-pool.md).

## Problem

Ticket 26 (Puller read-connection pool) was shelved because the pull
scenario's numbers stayed far under the 250 ms budget (ticket 25) — and
the refreshed 2026-08-12 baseline reaffirms that: worst case tested is
narinfo p99 3.59 ms (artemis, 1 CPU), ~70× under budget. But every one of
those numbers, ticket 26's own included, comes from a **single fixed
concurrency: c=20**. `garret-bench pull --pull-concurrency` is a real
knob, but nothing checked in ever varies it, so "not justified today" is
only as good as c=20 being representative — and it isn't the failure
shape ticket 26 is worried about.

Ticket 26's own revisit condition is explicit: "revisit if real pull
concurrency ever makes the p99 approach the budget." That's a curve, not
a point, and the harness has never measured the curve. The research doc
already names this as the gap, twice: finding 4 ("the interesting
regression lives at a concurrency this harness doesn't step to yet") and
under "Deferred bench extensions" ("Stepped-concurrency pull sweep (c =
20/100/300, keep-alive on and off) per bazel-remote issue #280 — add when
the Puller ever fronts more than this household's machines") — recorded,
not built.

## Evidence

- bazel-remote issue #280: mean latency 123 ms → 2122 ms between c=20 and
  c=300 on a hot blob, from connection/lock contention alone at fixed
  throughput per request — the exact mechanism ticket 26 anticipates for
  garret's single `Mutex<rusqlite::Connection>`.
- benchmark-baseline-2026-08.md finding 4: c=20 keeps the Puller "nowhere
  near saturation" despite full serialization on the mutex — silent on
  where that stops being true.
- Every checked-in `benchmarks/*.json` pull result: `"concurrency": 20`,
  no exceptions.

## Proposed shape

- Extend `garret-bench pull`'s `--pull-concurrency` into a sweep mode:
  run the same passes over the same pushed corpus at c = 20 / 100 / 300
  (bazel-remote's own steps), back to back, same process.
- Report p50/p99 **per step**, not one number — the sweep's output is a
  curve (4-5 points), so a regression or an approach-to-budget shows up
  as a shape, not a single comparison.
- `--pull-keepalive`/`--no-pull-keepalive` toggle: bazel-remote's finding
  was sensitive to connection reuse, and the Puller's own behavior here
  is currently unmeasured either way.
- No new pass/fail gate — stays reported-only, consistent with spec 09's
  existing pull scenario ("latency... reported, not gated"). The 250 ms
  budget remains the only hard number; this scenario exists to show
  whether the curve approaches it, which is what would reopen ticket 26.
- Opt-in extra pass (e.g. `garret-bench pull --sweep`, wired into
  `just bench-local` as a separate `just bench-pull-sweep` rather than
  the default run) — c=300 against a throwaway local Garage is a
  materially heavier run than the current headline scenario and doesn't
  belong in every `bench-local` invocation.

## Non-goals

- Building ticket 26's connection pool — this ticket only produces the
  evidence to decide that question; a rising curve approaching 250 ms is
  what reopens 26, not this ticket itself.
- Real-S4 WAN validation, Perfetto/chrome-trace export — separate
  deferred items in the same research doc, unrelated to this gap.

## Score

Speed n/a (harness-only, no runtime change) · Ops low (one optional bench
flag + one just recipe) · UX none (no client-visible change) · unblocks:
an evidence-based revisit of ticket 26 instead of a guess from a single
data point.
