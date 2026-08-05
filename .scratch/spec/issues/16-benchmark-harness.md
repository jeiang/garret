# Benchmark harness and load targets

Type: grilling
Status: resolved
Blocked by: 06

## Question

Design the benchmark that proves the target: N concurrent pushers with
flat/bounded memory and no timeouts. Decide: the workload set (drawn from
real closure sizes on this infrastructure), N and the pass/fail criteria
(memory ceiling, p99 latencies, zero failed pushes), tooling (custom driver
using the garret client vs oha/vegeta-style generators), the environment
(against Garage or a local S3 stand-in), what the harness measures on the
pull side too, and how benchmarks stay runnable for regression tracking
during implementation.

## Answer

**Corpus** — a seeded synthetic generator reproducing artemis's measured
NAR-size distribution (ticket 03 data): many KB–MB NARs, a fat tail, one
multi-GB blob, and a realistic compressibility mix including an
incompressible "model weights" slice. Fixed seed ⇒ identical corpus on
any machine; no real store contents anywhere.

**Driver** — a `garret-bench` binary reusing the client's push library:
real protocol path (negotiation, preamble framing, zstd, 429 backoff),
controlled concurrency, per-NAR latency capture.

**Scenarios and pass/fail (locked)**

1. *Headline*: 20 concurrent pushers × the corpus against default caps.
   PASS = zero failed pushes (429-retries fine); Pusher RSS < 2× its
   configured in-flight byte cap throughout and returns to baseline
   after; p99 per-NAR time ≤ 3× the uncontended median. Memory criteria
   are tied to configured caps, not absolute MB — provable and
   machine-independent.
2. *Large-body streaming*: single-stream 1 MiB / 100 MiB / 2 GiB pushes
   and pulls (the HTTP-research regression gate for flow-control tuning).
3. *Pull side*: concurrent cold downloads (post-restart, no page cache);
   flat Puller memory, saturating the test link.

**Environment** — the harness provisions a throwaway local Garage + both
services from the flake (devshell/NixOS test); also pointable at real
infra for validation runs.

**Regression tracking** — results emit JSON; a justfile target compares
against a checked-in baseline. Rerun before merging
performance-relevant changes; no CI gating in v1.
