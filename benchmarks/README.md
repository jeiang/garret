# Benchmark results

Human-readable summary of the checked-in result JSONs in this directory,
last refreshed 2026-09-24 on `c83cd2a`. Produced by `just bench-local` (all spec-09
scenarios: 20-way concurrent push of a 200-entry / 638 MiB seeded corpus,
single-stream pushes of 1 MiB / 100 MiB / 2 GiB, and a 1200-request pull
scenario) against a throwaway local Garage. `just bench-compare` diffs a
fresh run against `baseline.json`; the diff refuses to compare across
labels, so each environment keeps its own file.

## Systems

| Host | Hardware | OS | Notes |
|---|---|---|---|
| MacBook Pro | Apple M3 Pro, 11 cores, 18 GiB RAM | macOS 27.0 | primary dev machine; `benchmarks/baseline.json` |
| artemis | AMD Ryzen 7 7800X3D, 8 cores, 96 GB RAM | NixOS 26.11 (Zokor), kernel 7.2.0-cachyos-lto, x86_64 | shared home-lab box; limits apply to the **garret services only** (`GARRET_WRAP`: `taskset -c 0` for the CPU pin, per-service systemd user scopes with `MemoryMax=896M MemorySwapMax=0` for the cap). Garage and the bench client stand in for out-of-sandbox components (the upstream S3 service, the pushing machines) and run unconstrained |

## Results

| Label | Environment | Push NAR MiB/s | Push median/p99/max ms | Stream 1M/100M/2G MiB/s | Pull req/s | narinfo p50/p99 ms | Peak pusher RSS | Zero failures |
|---|---|---|---|---|---|---|---|---|
| `macos-aarch64` | MacBook Pro (dev baseline) | 477 | 72 / 340 / 427 | 78 / 263 / 358 | 36.3k | 0.43 / 1.52 | 217 MiB | yes |
| `nixos-x86_64` | artemis, no limits | 673 | 50 / 210 / 277 | 101 / 342 / 464 | 37.1k | 0.42 / 1.74 | 399 MiB | yes |
| `nixos-x86_64-896m` | artemis, 896 MiB memory cap | 672 | 50 / 227 / 299 | 106 / 344 / 461 | 34.2k | 0.48 / 2.10 | 379 MiB | yes |
| `nixos-x86_64-1cpu` | artemis, pinned to 1 CPU | 615 | 54 / 344 / 411 | 105 / 328 / 453 | 13.4k | 1.48 / 5.89 | 218 MiB | yes |
| `nixos-x86_64-1cpu-896m` | artemis, 1 CPU + 896 MiB | 609 | 53 / 258 / 419 | 103 / 288 / 346 | 13.3k | 1.46 / 6.29 | 206 MiB | yes |

The RSS criterion (peak pusher RSS < 2x the 256 MiB in-flight cap, i.e.
512 MiB) held in every configuration, with zero failed requests and zero
shed retries throughout. A second pass of the artemis matrix and of the
Mac run agreed within 5%.

**What changed since 2026-08-12.** Concurrent push throughput fell by
about 28% (Mac 712 → 477, artemis 938 → 673, 1 CPU 892 → 615 MiB/s of
NAR). The cost is the server-side NarHash check (ADR-0010, #41): the
Pusher now decompresses every upload and hashes the NAR, about 1.6
CPU-seconds per GiB. For the 638 MiB corpus that is about 1 CPU-second,
which matches the 1-CPU wall time. An interleaved A/B on the Mac
confirms the cause: the commit before #41 (`e8bcdb9`) pushed 656 and
672 MiB/s, and #41 itself (`34867a9`) pushed 478 and 475. On the Mac,
nothing merged after #41 changed push throughput. Pull throughput rose
about 35% (27k → 36–37k req/s) and narinfo p50 fell to about 0.43 ms.
The likely cause is #34, which moved access-time updates off the request
path; this was not A/B tested. Peak Pusher RSS rose 50–70 MiB on the Mac
and in the 1-CPU runs, and is flat in the other artemis runs. All
values stay well inside the budget. The 100 MiB and 2 GiB streams are
unchanged within noise.

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
