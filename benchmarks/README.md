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
| artemis | AMD Ryzen 7 7800X3D, 8 cores, 96 GB RAM | NixOS 26.11 (Zokor), kernel 7.1.3-cachyos-lto, x86_64 | shared home-lab box; resource limits applied to the whole stack (Garage + Pusher + Puller + bench client) via `taskset -c 0` and a systemd user scope with `MemoryMax=896M MemorySwapMax=0` |

## Results

| Label | Environment | Push NAR MiB/s | Push median/p99 ms | Stream 1M/100M/2G MiB/s | Pull req/s | narinfo p50/p99 ms | Peak pusher RSS | Zero failures |
|---|---|---|---|---|---|---|---|---|
| `macos-aarch64` | MacBook Pro (dev baseline) | 677 | 47 / 238 | 88 / 234 / 370 | 26.8k | 0.62 / 2.12 | 149 MiB | yes |
| `nixos-x86_64` | artemis, no limits | 931 | 33 / 191 | 109 / 291 / 452 | 28.3k | 0.58 / 1.71 | 352 MiB | yes |
| `nixos-x86_64-896m` | artemis, 896 MiB memory cap | 875 | 34 / 296 | 110 / 297 / 161 | 26.0k | 0.64 / 3.01 | 348 MiB | yes |
| `nixos-x86_64-1cpu` | artemis, pinned to 1 CPU | 275 | 150 / 896 | 90 / 98 / 95 | 6.3k | 2.94 / 7.61 | 133 MiB | yes |
| `nixos-x86_64-1cpu-896m` | artemis, 1 CPU + 896 MiB | 36 | 116 / 809 | 86 / 94 / 56 | 5.7k | 3.51 / 11.07 | 154 MiB | yes |

The RSS criterion (peak pusher RSS < 2x the 256 MiB in-flight cap, i.e.
512 MiB) held in every configuration, with zero failed requests and zero
shed retries throughout.

**Known noise in the 1-CPU rows.** Push throughput under a 1-CPU pin is
bimodal across repeated runs on artemis (~275 vs ~36 MiB/s), even with the
box otherwise idle; the checked-in `1cpu-896m` run caught the slow mode
(the same config also measured 275 MiB/s earlier the same day). Pull and
small/medium stream numbers were stable across re-runs. Suspected causes —
btrfs writeback plus the box's `bees` dedup daemon sharing the NVMe, and
default IRQ routing to core 0 (the pinned core) — are under investigation;
treat the 1-CPU push cells as a range, not a point.

Interpretation of the limit matrix lives in the research notes:
[.scratch/spec/research/benchmark-baseline-2026-08.md](../.scratch/spec/research/benchmark-baseline-2026-08.md).
