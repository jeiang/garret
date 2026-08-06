# Benchmarks

Sources: [ticket 16](../../.scratch/spec/issues/16-benchmark-harness.md),
[ticket 03 measurement](../../.scratch/spec/research/dedup-measurement.md).

## Corpus

A seeded synthetic generator reproduces the measured NAR-size
distribution from real system generations (many KB–MB NARs, a fat tail,
one multi-GB blob) with a realistic compressibility mix including an
incompressible "model weights" slice. Fixed seed ⇒ identical corpus on
any machine; no real store contents in the repo.

## Driver

`garret-bench` reuses the client's push library — real protocol path
(negotiation, preamble framing, zstd, 429 backoff), controlled
concurrency, per-NAR latency capture.

## Scenarios and pass/fail

1. **Headline — 20 concurrent pushers** × the corpus against default
   caps. PASS =
   - zero failed pushes (429-retries are fine);
   - Pusher RSS < **2× its configured in-flight byte cap** throughout,
     returning to baseline after (cap-relative: provable and
     machine-independent);
   - p99 per-NAR time ≤ 3× the uncontended median.
2. **Large-body streaming** — single-stream 1 MiB / 100 MiB / 2 GiB
   pushes and pulls; the regression gate for HTTP/2 flow-control tuning.
3. **Pull side** — concurrent cold downloads (post-restart, no page
   cache): flat Puller memory, saturating the test link.

## Environment & regression tracking

The harness provisions a throwaway local Garage + both services from the
flake (devshell/NixOS test); also pointable at real infra for validation
runs. Results emit JSON; a justfile target compares against a checked-in
baseline. Rerun before merging performance-relevant changes. No CI
gating in v1.
