# Benchmark baseline and measured performance review (2026-08)

Date: 2026-08-12. Machine: Apple aarch64 (11 cores), macOS — label
`macos-aarch64` in `benchmarks/baseline.json`. Backend: local throwaway
Garage (LAN stand-in per spec 09); real-S4 WAN validation deliberately not
run here. Method agreed in review session: **measure first**, propose
optimizations only from numbers.

## Load results (release builds, `just bench-local`)

| Scenario | Result |
|---|---|
| Push, 20 concurrent, 200 entries (638 MiB NAR) | **641 MiB/s NAR** (149 MiB/s wire), wall 0.99 s, median 47 ms, p99 296 ms, p99_slowdown 112x, zero failures, zero sheds |
| Stream 1 MiB ×3 | 70 MiB/s (14 ms median) |
| Stream 100 MiB ×3 | 145 MiB/s |
| Stream 2 GiB ×1 | 200 MiB/s |
| Pull, c=20, 1200 requests | **28.8k req/s**; narinfo p50 0.61 ms / p99 1.61 ms; redirect p50 0.65 ms / p99 2.35 ms; zero failures |
| Pusher RSS | peak 146 MiB against the 512 MiB budget (2× the configured 256 MiB in-flight cap) — **criterion holds with 3.5× headroom** |

The p99_slowdown of 112x is the corpus's queueing-by-construction effect
documented in spec 09, not contention pathology: zero sheds and a 296 ms
absolute p99 on a fat-tailed corpus.

## Micro-benchmarks (`just microbench`, 4 MiB bodies, single core)

| Path | Throughput |
|---|---|
| zstd level 3, incompressible slice | 2.62 GiB/s |
| zstd level 3, mixed (70) | 1.16 GiB/s |
| zstd level 3, trivially compressible | 5.05 GiB/s |
| zstd level 1, mixed | 1.63 GiB/s |
| zstd level 9, mixed | 159 MiB/s |
| zstd level 19, mixed | 6.7 MiB/s |
| **SHA-256 (sha2 0.10, default features)** | **315 MiB/s** |
| SHA-256 with `asm` feature (experiment) | **1.61 GiB/s (5.2×)** |
| Preamble frame+parse | 2.45 µs/op — noise |

## Findings

1. **SHA-256 is the Pusher's only per-byte CPU cost and it runs at
   pure-Rust fallback speed.** `sha2 = "0.10"` without the `asm` feature
   ignores the SHA extensions on both Apple Silicon and modern x86. Measured
   5.2× with the feature flag. At 315 MiB/s it already shadows the 200 MiB/s
   single-stream result and would become the first wall in a cpu-limited
   sandbox. → ticket 20, one line.
2. **Client zstd is not a bottleneck at level 3** (1.16 GiB/s mixed,
   single-core) and the level ladder quantifies the cliff: level 9 costs 7×,
   level 19 costs 170×. Keep 3 as the default; nothing to do.
3. **Small-push overhead dominates below ~1 MiB**: 70 MiB/s at 1 MiB
   single-stream vs 200 MiB/s at 2 GiB means ~10 ms fixed cost per PUT on
   this stack (localhost). Consistent with the 2 ms uncontended median for
   KB-scale NARs. On WAN this fixed cost is dwarfed by RTT; not worth
   optimizing until a real-S4 run says otherwise.
4. **The Puller is nowhere near saturation at c=20** (28.8k req/s, sub-ms
   medians) despite every request serializing on the single
   `Mutex<Connection>`. The mutex is therefore the *right* simplicity today.
   The bazel-remote lesson (mean latency 123 ms → 2122 ms between c=20 and
   c=300 on a hot blob) says the interesting regression lives at a
   concurrency this harness doesn't step to yet — noted below as the one
   bench extension worth adding when it matters.
5. **Memory bounding works as designed**: peak RSS stayed at 28% of budget
   through 20-way concurrent pushes including a 2 GiB stream, and the
   backpressure path never shed. To make the shed path itself a measured
   scenario, run with tighter caps via
   `GARRET_BENCH_ARGS="--concurrency 40" just bench-local` variants — the
   knobs exist; no code needed.

## NixOS x86_64 runs, unlimited and resource-limited (2026-08-12)

Machine: `artemis`, NixOS x86_64, 8 cores, 96 GB RAM (shared box — one
anomalous 1-cpu run was discarded after a clean re-run reproduced the
combined-limit numbers instead; interference, not signal). Limits applied
to the **whole stack** (Garage + Pusher + Puller + bench client) via
`taskset -c 0` for the CPU pin and a systemd user scope with
`MemoryMax=896M MemorySwapMax=0` for the memory cap. Result files:
`benchmarks/bench-nixos-*.json`; labels encode the caps.

| Config | Push (NAR MiB/s) | Stream 1M/100M/2G (MiB/s) | Pull (req/s) | narinfo p99 | Peak RSS |
|---|---|---|---|---|---|
| unlimited | **974** | 111 / 298 / **472** | 27.5k | 1.4 ms | 411 MiB |
| 896 MiB | 960 | 105 / 295 / 237 | 26.6k | 1.7 ms | 354 MiB |
| 1 cpu | 279 | 84 / 98 / 95 | 6.9k | 6.3 ms | 136 MiB |
| 1 cpu + 896 MiB | 279 | 89 / 97 / 46 | 6.3k | 6.4 ms | 148 MiB |

**Zero failures and the RSS criterion held in every configuration** — the
backpressure design does what spec 01 promises even on one core under a
hard 896 MiB ceiling.

Findings:

1. **Push throughput is CPU-bound, not memory-bound**: 974 → 279 MiB/s
   when pinned to one core, while the 896 MiB cap alone costs ~1%. The
   sandbox's binding constraint is its CPU allocation.
2. **The memory cap taxes exactly one thing: large single-stream writes**
   (472 → 237 MiB/s unlimited-cpu, 95 → 46 MiB/s at 1 cpu) — page-cache
   / writeback pressure on Garage, not garret. Small and medium bodies
   are unaffected.
3. **The Puller degrades gracefully with CPU**: 27.5k → 6.9k req/s and
   p99 stays under 7 ms even with every component sharing one core.
4. Peak RSS *fell* with tighter limits (411 → 136 MiB) — the kernel
   reclaiming harder, plus slower ingest naturally holding fewer parts in
   flight.

Micro-benches on artemis (x86_64, default features): zstd level 3 mixed
1.47 GiB/s; **SHA-256 2.21 GiB/s with stock `sha2 = "0.10"`** — sha2
runtime-detects SHA-NI on x86_64, so the 315 MiB/s fallback measured on
the Mac is an **aarch64-only** gap (`asm` feature required there). Ticket
20 updated accordingly: production x86 hardware is already fast; the flag
matters for aarch64 dev machines and any future ARM deployment.

## 1-CPU noise investigation and methodology change (2026-08-12, later)

Re-running the matrix on latest main produced bimodal 1-CPU push numbers
(~275 vs ~36 vs once 4 MiB/s) that survived a quiet box (load <0.25).
Chased and pinned down:

- **Not disk**: reproduced with the whole bench root on tmpfs
  (`TMPDIR=/dev/shm`); bees CPU-time and NVMe deltas flat during runs.
- **Not TCP**: zero `ListenOverflows`/`ListenDrops`/`TCPSynRetrans`/
  `RetransSegs` deltas across slow runs.
- **Not client retries**: `shed_retries` and `dropped_retries` both 0.
- **Mechanism (confirmed via the new `max_ms` field)**: exactly one push
  per slow run was held open for the whole stall (`max_ms` ≈
  `wall_seconds`: 8.6 s of an 8.9 s wall, 141.9 s of 142 s — the 142.0 s
  figure recurred in three independent runs, so some deterministic
  timeout is involved), while every percentile stayed normal — with 200
  entries, p99 is only the ~2nd-worst sample, which is why the old
  results hid it. Garage logs show nothing; the held PUT sat in
  `store_upload` waiting on Garage (the aws-sdk default config has no
  operation timeout, so a stalled part upload waits indefinitely).

Verdict, refined after re-running with Garage unpinned: a **Garage
artifact under concurrent multipart load**, not (only) starvation. With
the limits on the garret services alone (`GARRET_WRAP`, spec 09) and
Garage free on 7 idle cores, ~half of 1-CPU runs still caught a stall
(`max_ms` 1.9 s / 8.6 s), and one run wedged **permanently**: Garage
stopped reading its S3 socket mid-part-upload (7.7 MB stuck in Recv-Q
for over an hour, all processes idle) — nothing in the chain times out
(the bench client's reqwest and the pusher's aws-sdk both default to no
operation timeout), so the run hung until killed. Pinning Garage made
the stall worse (it always self-resolved ≤142 s, but hit more runs);
unpinning made garret's own numbers honest.

Outcome:

- Methodology (spec 09): limits wrap `garret-pusher`/`garret-puller`
  only. Garage (upstream S3 stand-in) and the bench client (remote
  pushers stand-in) run free; builds too. 1-CPU push went 275 → ~890
  MiB/s — the old number was mostly the stand-ins' CPU bill.
- `garret-bench` now reports `max_ms`; re-run protocol: a run with
  `max_ms` out of line with `p99_ms` caught the stand-in stall — re-run
  it rather than checking it in (benchmarks/README.md documents this).
- Deferred: Garage under its own separate constraints (fully-local
  setup benchmark), and a production gap worth a ticket — the pusher's
  S3 calls have **no operation timeout**, so a stalled upstream holds an
  upload slot and its in-flight bytes indefinitely (observed live here).

Final numbers (garret-only limits) are in benchmarks/README.md; the
whole-stack table above is kept for history but is not comparable.

## Deferred bench extensions (recorded, not built)

- **Stepped-concurrency pull sweep** (c = 20/100/300, keep-alive on and
  off) per bazel-remote issue #280 — add when the Puller ever fronts more
  than this household's machines.
- **Real-S4 WAN validation run** — the only way to see true upload
  behaviour; run `just bench` pointed at production infra when convenient.
  Baselines from it must carry a distinct `--label`.
- **Perfetto/chrome-trace export of bench runs** (snix practice) — visual
  diffing of runs; revisit if regression hunts get non-obvious.

## Sandbox portability

Results JSON carries `meta.label` (here `macos-aarch64`);
`scripts/bench-diff.py` hard-refuses cross-label comparisons. The future
memory/cpu-limited Linux sandbox gets its own label encoding its caps
(e.g. `linux-x86_64-2cpu-2g`) and its own checked-in baseline; the RSS
criterion is cap-relative so it transfers unchanged. The wrapper itself
(systemd-run/cgroups) is a few lines to add when that machine exists.
