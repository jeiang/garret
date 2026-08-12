# Benchmark results

Human-readable summary of the checked-in result JSONs in this directory,
last refreshed 2026-08-12. Produced by `just bench-local` (all spec-09
scenarios: 20-way concurrent push of a 200-entry / 638 MiB seeded corpus,
single-stream pushes of 1 MiB / 100 MiB / 2 GiB, and a 1200-request pull
scenario) against a throwaway local Garage. `just bench-compare` diffs a
fresh run against `baseline.json`; the diff refuses to compare across
labels, so each environment keeps its own file.

## Systems

| Host | Hardware | OS | Notes |
|---|---|---|---|
| MacBook Pro | Apple M3 Pro, 11 cores, 18 GiB RAM | macOS 26.6 | primary dev machine; `benchmarks/baseline.json` |
| artemis | AMD Ryzen 7 7800X3D, 8 cores, 96 GB RAM | NixOS 26.11 (Zokor), kernel 7.1.3-cachyos-lto, x86_64 | shared home-lab box; limits apply to the **garret services only** (`GARRET_WRAP`: `taskset -c 0` for the CPU pin, per-service systemd user scopes with `MemoryMax=896M MemorySwapMax=0` for the cap). Garage and the bench client stand in for out-of-sandbox components (the upstream S3 service, the pushing machines) and run unconstrained |

## Results

| Label | Environment | Push NAR MiB/s | Push median/p99/max ms | Stream 1M/100M/2G MiB/s | Pull req/s | narinfo p50/p99 ms | Peak pusher RSS | Zero failures |
|---|---|---|---|---|---|---|---|---|
| `macos-aarch64` | MacBook Pro (dev baseline) | 712 | 45 / 217 / 276 | 91 / 240 / 377 | 27.0k | 0.60 / 1.92 | 147 MiB | yes |
| `nixos-x86_64` | artemis, no limits | 803 | 33 / 243 / 269 | 107 / 292 / 366 | 22.5k | 0.70 / 3.63 | 411 MiB | yes |
| `nixos-x86_64-896m` | artemis, 896 MiB memory cap | 935 | 31 / 211 / 257 | 106 / 298 / 407 | 28.2k | 0.62 / 1.81 | 366 MiB | yes |
| `nixos-x86_64-1cpu` | artemis, pinned to 1 CPU | 892 | 31 / 226 / 308 | 117 / 278 / 457 | 11.8k | 1.66 / 3.59 | 162 MiB | yes |
| `nixos-x86_64-1cpu-896m` | artemis, 1 CPU + 896 MiB | 886 | 31 / 287 / 441 | 111 / 281 / 456 | 12.3k | 1.64 / 2.67 | 160 MiB | yes |

The RSS criterion (peak pusher RSS < 2x the 256 MiB in-flight cap, i.e.
512 MiB) held in every configuration, with zero failed requests and zero
shed retries throughout.

**Why `max_ms` is reported, and the re-run protocol.** Roughly half of
1-CPU push runs catch a Garage artifact: a single push held open for
seconds (occasionally: forever — one run sat an hour with 7.7 MB unread
in Garage's socket receive queue) while every percentile stays normal,
because with 200 entries p99 is only the ~2nd-worst sample. It is a
stand-in artifact, not garret behavior — it reproduces with Garage
unpinned on 7 free cores and on tmpfs, with zero TCP retransmits and
zero client retries. `max_ms` is the tell: a run with `max_ms` out of
line with `p99_ms` (or `wall_seconds` far above `sum(latencies)/
concurrency`) caught the stall and should be re-run, not checked in.
An earlier whole-stack-limited methodology made this much worse and
also charged the stand-ins' CPU to garret (1-CPU push measured 275
MiB/s then; garret alone does ~890). The full chase is in the research
notes below.

Interpretation of the limit matrix lives in the research notes:
[.scratch/spec/research/benchmark-baseline-2026-08.md](../.scratch/spec/research/benchmark-baseline-2026-08.md).
