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
   - latency: **reported, not gated** — see below.

**The p99 latency budget does not survive this corpus** (found at M5).
The original rule was "p99 per-NAR time ≤ 3× the uncontended median", and
no reading of it works:

- *Absolute p99 against a median* measures the corpus's size spread, not
  contention. This corpus is deliberately fat-tailed, so the largest NAR
  takes far more than 3× the median NAR at **any** concurrency, including
  none. The rule fails on an idle server.
- *Each NAR against its own uncontended time* removes size, but then
  measures small NARs queueing behind large ones — which bounded upload
  concurrency guarantees by construction. A 4 KiB path behind three
  16 MiB uploads shows a ~60× slowdown on a server behaving exactly as
  designed.

So `garret-bench` reports both (`p99_ms`, `p99_slowdown`) and gates only
on **zero failures**, which is unambiguous. Latency is tracked by
comparing against the checked-in baseline, where a regression shows up as
a change; `--max-p99-slowdown` sets a budget for anyone who wants a hard
gate. Picking a defensible fixed threshold needs numbers from real
hardware, which is a decision for after the first production runs.
2. **Large-body streaming** — single-stream 1 MiB / 100 MiB / 2 GiB
   pushes; the regression gate for HTTP/2 flow-control tuning. (Pulls no
   longer stream through us — see below.)
3. **Pull side** — concurrent cold narinfo + redirect issuance: flat
   Puller memory and redirect latency under load. Since the Puller
   redirects rather than proxies
   ([ADR-0005](../adr/0005-remote-object-store-presigned-reads.md)), this
   scenario measures metadata and presigning, not throughput; download
   speed is S4's, not ours, and is not a garret pass/fail.

## Environment & regression tracking

The harness provisions a throwaway **local Garage** + both services from
the flake (devshell/NixOS test) — a LAN stand-in keeps memory and
backpressure results machine-independent and costs no S4 egress. Also
pointable at real infra (S4) for validation runs, which is the only way
to see true WAN upload behaviour. Results emit JSON; a justfile target compares against a checked-in
baseline. Rerun before merging performance-relevant changes. No CI
gating in v1.
