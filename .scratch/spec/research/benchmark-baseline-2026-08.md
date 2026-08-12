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
