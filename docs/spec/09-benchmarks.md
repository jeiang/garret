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

`garret-bench` authenticates exactly as the client does (its config and
OIDC flow) and speaks the real protocol — preamble framing, zstd, 429
backoff — with controlled concurrency and per-NAR latency capture.
Connection drops mid-body are retried on the same schedule as a 429 and
reported separately as `dropped_retries`: they are usually the server's
early `exists`/`429` reply closing the socket before the body finished
(spec 01), not a network fault, and neither retry counts as a failure.
One subcommand per scenario (`push`, `stream`, `pull`) plus `all`,
writing a single JSON report.

Every report carries a `meta` block: os, arch, cpu count, binary
version, seed, and a `label` (default `os-arch`, `--label` /
`GARRET_BENCH_LABEL` to override). The label is the comparison key —
the diff tool refuses to compare runs across labels, so a laptop
baseline can never silently masquerade as the cgroup-limited sandbox's.
A limited environment encodes its caps in the label
(e.g. `linux-x86_64-2cpu-2g`), and applies them via `GARRET_WRAP` — a
command prefix (`taskset -c 0`, a systemd-run scope, …) that
`bench-local` puts on the two garret services and nothing else. Garage
and the bench client are both stand-ins for things outside the sandbox
(the upstream S3-compatible service and the machines pushing to the
cache), and the builds are setup — so the limits measure garret, not
the harness. (Benchmarking a fully local setup with Garage under its own
constraints is recorded as a possible future scenario, not built.)

## Scenarios and pass/fail

1. **Headline — 20 concurrent pushers** (`garret-bench push`) × the
   corpus against default caps. PASS =
   - zero failed pushes (429-retries are fine);
   - Pusher RSS < **2× its configured in-flight byte cap** throughout,
     returning to baseline after (cap-relative: provable and
     machine-independent). The harness cannot see the server's memory;
     `just bench-local` samples the Pusher's RSS alongside the run and
     enforces this.
   - latency: **reported, not gated** — see below. Aggregate throughput
     is reported both as uncompressed NAR MiB/s (what a user
     experiences) and wire MiB/s.

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

So `garret-bench` reports both (`p99_ms`, `p99_slowdown`) — plus `max_ms`,
because with 200 entries p99 is only the ~2nd-worst sample and a single
hung request can crater `wall_seconds` while every percentile stays
normal — and gates only on **zero failures**, which is unambiguous. Latency is tracked by
comparing against the checked-in baseline, where a regression shows up as
a change; `--max-p99-slowdown` sets a budget for anyone who wants a hard
gate. Picking a defensible fixed threshold needs numbers from real
hardware, which is a decision for after the first production runs.
2. **Large-body streaming** (`garret-bench stream`) — single-stream
   1 MiB / 100 MiB / 2 GiB pushes; the regression gate for HTTP/2
   flow-control tuning. Bodies are generated chunk by chunk and
   streamed, never held whole, and are **not** zstd-compressed on the
   way out: the server never decompresses — everything after the
   preamble streams to S3 as-is — so compressing random filler would
   only measure the bench client's CPU. Reported as median wall time
   and payload MiB/s per size; PASS = zero failures. (Pulls no longer
   stream through us — see below.)
3. **Pull side** (`garret-bench pull`) — concurrent cold narinfo + NAR
   requests up to the redirect (never following it): flat Puller memory
   and redirect latency under load. Since the Puller redirects rather
   than proxies
   ([ADR-0005](../adr/0005-remote-object-store-presigned-reads.md)), this
   scenario measures metadata and presigning, not throughput; download
   speed is S4's, not ours, and is not a garret pass/fail. Reported as
   p50/p99 per request class plus requests/s; PASS = zero failures
   (narinfo 200s, NAR redirects). Requires the corpus pushed first —
   `all` sequences that automatically.

   `--pull-concurrency` takes a comma list (e.g. `20,100,300`): each
   step runs back to back over the same corpus and is reported
   separately, so the output is a latency curve rather than one point.
   `--no-pull-keepalive` disables HTTP connection reuse for the run.
   `just bench-pull-sweep` provisions locally and runs the
   c=20/100/300 sweep with keep-alive on and off. Sweep results stay
   reported-only — zero failures is still the only PASS rule, and the
   250 ms pull budget remains the sole hard latency number; a curve
   approaching it is the evidence that would reopen the Puller
   read-connection-pool question.

## Micro-benchmarks

`just microbench` (criterion, in `crates/garret-bench/benches/`) probes
the per-byte hot-path costs in-process, with no server: zstd at the
corpus's compressibility classes and level ladder (the client's cost),
SHA-256 over stored bytes (the Pusher's only per-byte CPU cost), and
preamble framing. These are the "maximum speed this CPU can reach"
numbers — the first thing to compare when a memory/cpu-limited sandbox
underperforms the load scenarios.

## Environment & regression tracking

`just bench-local` provisions a throwaway **local Garage** + both
services from the flake (release builds), runs all three scenarios, and
samples Pusher RSS for the scenario-1 memory criterion
(`GARRET_IN_FLIGHT_CAP` overrides the 256 MiB in-flight cap the criterion
is measured against, so a memory-limited sandbox can size the Pusher to
its box) — a LAN stand-in
keeps memory and backpressure results machine-independent and costs no
S4 egress. Also pointable at real infra (S4) via `just bench`, which is
the only way to see true WAN upload behaviour. Load scenarios assume a
fresh server (an `exists` ack skips the body and would fake an instant
push); `bench-local` guarantees that, and the streaming scenario
additionally salts its keys so repeat runs against a long-lived server
stay honest.

Results emit JSON (`bench-results.json`); `just bench-compare` diffs
against the checked-in baseline (`benchmarks/baseline.json`, promoted
via `just bench-baseline`), failing only on failed requests or an
environment-label mismatch — latency and throughput judgement stays
with a human. Rerun before merging performance-relevant changes. No CI
gating in v1.
