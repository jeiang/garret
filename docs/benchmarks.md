# Benchmarks

Measured on garret itself, against a throwaway local Garage standing in for the
object store. The numbers describe the cache, not the bucket.

## What is measured

- **Push**: 20 concurrent pushers over a seeded corpus of 200 entries totalling
  638 MiB, each entry 4 KiB to 16 MiB.
- **Stream**: single-stream pushes of 1 MiB, 100 MiB and 2 GiB bodies.
- **Pull**: 1200 concurrent narinfo and redirect requests.

The corpus is generated from a seed, so a run is reproducible without holding
real store contents.

## Systems

| Host | Hardware | OS | Notes |
|---|---|---|---|
| MacBook Pro | Apple M3 Pro, 11 cores, 18 GiB RAM | macOS 27.0.1 | primary development machine |
| Ryzen server | AMD Ryzen 7 7800X3D, 8 cores, 96 GB RAM | NixOS 26.11, kernel 7.3.0-cachyos-lto, x86_64 | shared home-lab box |

The Ryzen server limits apply to the garret services only: `taskset -c 0` for the CPU
pin, and per-service systemd user scopes with `MemoryMax=896M MemorySwapMax=0`
for the memory cap. Garage and the bench client stand in for out-of-sandbox
components — the upstream S3 service and the pushing machines — and run
unconstrained.

## Results

| Label | Environment | Push NAR MiB/s | Push median/p99/max ms | Stream 1M/100M/2G MiB/s | Pull req/s | narinfo p50/p99 ms | Peak pusher RSS | Zero failures |
|---|---|---|---|---|---|---|---|---|
| `macos-aarch64` | MacBook Pro | 734 | 46 / 170 / 209 | 240 / 852 / 870 | 45.7k | 0.35 / 1.52 | 154 MiB | yes |
| `nixos-x86_64` | Ryzen server, no limits | 731 | 49 / 171 / 221 | 220 / 896 / 1025 | 44.7k | 0.32 / 1.69 | 377 MiB | yes |
| `nixos-x86_64-896m` | Ryzen server, 896 MiB memory cap | 743 | 48 / 187 / 213 | 214 / 859 / 1023 | 46.9k | 0.32 / 1.13 | 382 MiB | yes |
| `nixos-x86_64-1cpu` | Ryzen server, pinned to 1 CPU | 699 | 52 / 258 / 282 | 220 / 595 / 677 | 16.6k | 1.17 / 2.07 | 175 MiB | yes |
| `nixos-x86_64-1cpu-896m` | Ryzen server, 1 CPU + 896 MiB | 715 | 49 / 219 / 313 | 221 / 598 / 699 | 16.7k | 1.16 / 2.01 | 145 MiB | yes |

The Pusher runs the default limits — a 256 MiB in-flight cap, 16 MiB parts, 4
parts in flight — except `max_concurrent_uploads`, raised from the default 16 to
32 so the 20 pushers are not shed.

## Pass criteria

A run passes when it has zero failed requests and zero shed retries. The memory
criterion is that peak Pusher RSS stays below twice the 256 MiB in-flight cap,
that is 512 MiB. It held in every configuration above.

`just bench-local` enforces the peak. Final RSS is recorded in the JSON for
comparison and is not gated.

## Run-to-run variance

A second pass on the Mac agreed within 2% on push, the 1 MiB and 100 MiB
streams, pull and RSS; the 2 GiB stream came in 5% higher and push p99 8%
higher. On the Ryzen server, a second pass of the matrix agreed within 9% on push,
streams and RSS, but not on pull: the 1200-request scenario lasts about 30 ms on
8 cores, and the two unpinned configurations came in 26–29% lower on the second
pass, while the 1-CPU configurations agreed within 1%.

Treat the unpinned Ryzen pull column as indicative only. For pull comparisons,
run `GARRET_BENCH_ARGS="--passes 100 --no-giant" just bench-local` for 40,000
requests. Most such runs agree within about 2%, but about one in five came in
4–18% low, so compare the best of three.

## Why max is reported

Roughly half of the 1-CPU push runs catch a Garage artifact: a single push held
open for seconds — occasionally forever, with megabytes unread in Garage's
socket receive queue — while every percentile stays normal, because with 200
entries p99 is only the second-worst sample. It reproduces with Garage unpinned
on free cores and on tmpfs, with zero TCP retransmits and zero client retries,
so it is a stand-in artifact rather than garret behaviour.

`max_ms` is the tell. A run whose `max_ms` is out of line with `p99_ms`, or
whose wall time is far above the sum of latencies divided by concurrency, caught
the stall and should be re-run rather than checked in.

## Reproducing

`just bench-local` provisions a throwaway Garage and runs every scenario against
it. `just bench-compare` diffs a fresh run against `baseline.json` and refuses
to compare across labels or corpus versions; each environment keeps its own
file. The checked-in result JSONs live in `benchmarks/`.
