# Benchmark results

Human-readable summary of the checked-in result JSONs in this directory,
last refreshed 2026-11-07: the Mac row and `baseline.json` on `78b44d0`,
the artemis rows on `6ddb102` (main plus the pull-lock fix below).
Produced by `just bench-local` (all spec-09
scenarios: 20-way concurrent push of a 200-entry / 638 MiB seeded corpus,
single-stream pushes of 1 MiB / 100 MiB / 2 GiB, and a 1200-request pull
scenario) against a throwaway local Garage. `just bench-compare` diffs a
fresh run against `baseline.json`; the diff refuses to compare across
labels, so each environment keeps its own file.

Every checked-in JSON is corpus v2 and carries the current report shape;
`just bench-compare` refuses a diff across corpus versions or labels.

## Systems

| Host | Hardware | OS | Notes |
|---|---|---|---|
| MacBook Pro | Apple M3 Pro, 11 cores, 18 GiB RAM | macOS 27.0.1 | primary dev machine; `benchmarks/baseline.json` |
| artemis | AMD Ryzen 7 7800X3D, 8 cores, 96 GB RAM | NixOS 26.11 (Zokor), kernel 7.3.0-cachyos-lto, x86_64 | shared home-lab box; limits apply to the **garret services only** (`GARRET_WRAP`: `taskset -c 0` for the CPU pin, per-service systemd user scopes with `MemoryMax=896M MemorySwapMax=0` for the cap). Garage and the bench client stand in for out-of-sandbox components (the upstream S3 service, the pushing machines) and run unconstrained |

## Results

| Label | Environment | Push NAR MiB/s | Push median/p99/max ms | Stream 1M/100M/2G MiB/s | Pull req/s | narinfo p50/p99 ms | Peak pusher RSS | Zero failures |
|---|---|---|---|---|---|---|---|---|
| `macos-aarch64` | MacBook Pro (dev baseline) | 734 | 46 / 170 / 209 | 240 / 852 / 870 | 45.7k | 0.35 / 1.52 | 154 MiB | yes |
| `nixos-x86_64` | artemis, no limits | 731 | 49 / 171 / 221 | 220 / 896 / 1025 | 44.7k | 0.32 / 1.69 | 377 MiB | yes |
| `nixos-x86_64-896m` | artemis, 896 MiB memory cap | 743 | 48 / 187 / 213 | 214 / 859 / 1023 | 46.9k | 0.32 / 1.13 | 382 MiB | yes |
| `nixos-x86_64-1cpu` | artemis, pinned to 1 CPU | 699 | 52 / 258 / 282 | 220 / 595 / 677 | 16.6k | 1.17 / 2.07 | 175 MiB | yes |
| `nixos-x86_64-1cpu-896m` | artemis, 1 CPU + 896 MiB | 715 | 49 / 219 / 313 | 221 / 598 / 699 | 16.7k | 1.16 / 2.01 | 145 MiB | yes |

The Pusher runs the default limits (256 MiB in-flight cap, 16 MiB parts, 4 parts
in flight) except `max_concurrent_uploads`, raised from the default 16 to 32 so
the 20 pushers are not shed.

The RSS criterion (peak pusher RSS < 2x the 256 MiB in-flight cap, i.e.
512 MiB) held in every configuration, with zero failed requests and zero
shed retries throughout. A second pass on the Mac agreed within 2% on push,
the 1 MiB and 100 MiB streams, pull and RSS; the 2 GiB stream came in 5%
higher and push p99 8% higher. On artemis, a second pass of the matrix
agreed within 9% on push, streams and RSS, but not on pull: the
1200-request scenario lasts about 30 ms on 8 cores, and the two unpinned
configs came in 26–29% lower on the second pass (the 1-CPU configs agreed
within 1%). Treat the unpinned artemis pull column as indicative only; for
pull comparisons, run
`GARRET_BENCH_ARGS="--passes 100 --no-giant" just bench-local` (40,000
requests). Most such runs on artemis agree within about 2%, but about one
in five came in 4–18% low, so compare the best of three.

**What changed since 2026-09-24.** Push rose about 9% on artemis
(673 → 731 MiB/s) and about 54% on the Mac (477 → 734). The streams rose
two- to threefold (artemis 100 MiB 342 → 896, 2 GiB 464 → 1025; Mac
263 → 852 and 358 → 870 MiB/s). A first-parent bisect over the 2 GiB
stream puts this on #72 (`9fd5381` 458 MiB/s, `77e7515` 1023): the Pusher's
NarHash check for NARs of 4 MiB or more now runs on its own blocking
thread and overlaps the relay instead of serializing with it (spec 01).
`c83cd2a` rerun on the same day still measured the old artemis numbers
(670 / 467 MiB/s), so the gain is not the kernel update. Pull rose on both
(Mac 36.3k → 45.7k req/s, artemis 1 CPU 13.4k → 16.6k); not bisected.
Peak Pusher RSS fell on the Mac (217 → 154 MiB) and on artemis 1 CPU
(218 → 175 MiB).

**A pull regression, found and fixed in this refresh.** The untangle
refactor (#98, its Puller step `690b847`) made pull reads await the
connection's async lock and then hand off to the blocking pool while
holding it, serializing the hand-off. In 40,000-request runs with no
limits, pull fell from 64.2k to 54.7k req/s (narinfo p50 0.234 → 0.31 ms).
The fix takes the lock on the blocking thread again (ADR 0007): 60.6k to
61.6k req/s, against 55.8k to 63.4k for the pre-refactor `ffa297d` in the
same interleaved session (one of its three runs was a low outlier).

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
