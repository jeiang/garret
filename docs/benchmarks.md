# Benchmarks

Measured on garret itself, against a throwaway local S3 server standing in for
the object store. The numbers describe the cache, not the bucket.

## What is measured

- **Push**: 20 concurrent pushers over a seeded corpus of 200 entries totalling
  638 MiB, each entry 4 KiB to 16 MiB. Every body is generated, SHA-256'd and
  zstd-compressed before the timer starts, so the numbers cover the upload and
  its retries, not the bench client's own CPU.
- **Stream**: single-stream pushes of 1 MiB, 100 MiB and 2 GiB bodies.
- **Pull**: 1200 concurrent narinfo and redirect requests.

The corpus is generated from a seed, so a run is reproducible without holding
real store contents.

The harness holds the prepared bodies for the load corpus and its baseline twin,
so it uses roughly 300 MiB of its own memory, or about 2.3 GiB with
`--with-giant`.

## Systems

| Host | Hardware | OS | Notes |
|---|---|---|---|
| MacBook Pro | Apple M3 Pro, 11 cores, 18 GiB RAM | macOS 27.0.1 | primary development machine |
| Ryzen server | AMD Ryzen 7 7800X3D, 8 cores, 96 GB RAM | NixOS 26.11, kernel 7.3.0-cachyos-lto, x86_64 | shared home-lab box |

The Ryzen server limits apply to the garret services only: `taskset -c 0` for the CPU
pin, and per-service systemd user scopes with `MemoryMax=896M MemorySwapMax=0`
for the memory cap. versitygw and the bench client stand in for out-of-sandbox
components — the upstream S3 service and the pushing machines — and run
unconstrained.

## Results

The table below was measured against versitygw, with the prepared-body harness
(v2). Each row is one run. The push phase moves 638 MiB in well under a second
on the unpinned configurations, so treat that column as indicative, like the
unpinned pull column (see [Run-to-run variance](#run-to-run-variance)).

| Label | Environment | Push NAR MiB/s | Push median/p99/max ms | Stream 1M/100M/2G MiB/s | Pull req/s | narinfo p50/p99 ms | Peak pusher RSS | Zero failures |
|---|---|---|---|---|---|---|---|---|
| `macos-aarch64` | MacBook Pro | 2934 | 10 / 116 / 142 | 237 / 583 / 697 | 57.2k | 0.29 / 1.76 | 182 MiB | yes |
| `nixos-x86_64` | Ryzen server, no limits | 3404 | 9 / 104 / 107 | 246 / 665 / 946 | 69.2k | 0.20 / 0.87 | 248 MiB | yes |
| `nixos-x86_64-896m` | Ryzen server, 896 MiB memory cap | 3728 | 8 / 95 / 101 | 264 / 668 / 945 | 72.0k | 0.20 / 1.06 | 247 MiB | yes |
| `nixos-x86_64-1cpu` | Ryzen server, pinned to 1 CPU | 1031 | 46 / 301 / 357 | 257 / 464 / 548 | 25.7k | 0.65 / 1.99 | 157 MiB | yes |
| `nixos-x86_64-1cpu-896m` | Ryzen server, 1 CPU + 896 MiB | 1013 | 46 / 288 / 386 | 249 / 461 / 544 | 25.4k | 0.65 / 1.78 | 153 MiB | yes |

`just bench-local` runs the Pusher with glibc's default malloc settings. The
NixOS module sets `MALLOC_ARENA_MAX=2` for the Pusher unit (see
[Deployment](deployment.md#process-behaviour)), which lowered peak RSS by about a third in
separate runs on the Ryzen server.

The Pusher runs the default limits — a 256 MiB in-flight cap, 16 MiB parts, 4
parts in flight — except `max_concurrent_uploads`, raised from the default 16 to
32 so the 20 pushers are not shed.

versitygw's `CompleteMultipartUpload` assembles the parts into the final object,
which Garage does not, so single-stream multipart results include that
server-side work. Summed over one full run, completion took 0.7–2.0 s against
Garage's 0.1–0.2 s on both hosts, and the 100 MiB stream came out 10–40% lower than
on Garage (macOS 485–582 vs 815–829 MiB/s, Ryzen server 756–768 vs 866–883
MiB/s). `copy_file_range` did not close the gap on the Ryzen server.

## Pass criteria

A run passes when it has zero failed requests and zero shed retries. The memory
criterion is that peak Pusher RSS stays below twice the 256 MiB in-flight cap,
that is 512 MiB. It held in every configuration above.

`just bench-local` enforces the peak. Final RSS is recorded in the JSON for
comparison and is not gated.

## Run-to-run variance

With versitygw and harness v2, repeated runs on the Ryzen server with a longer
workload (`--count 1000 --passes 50`, unpinned) agreed within 0.3% on pull
(63.5k–63.7k req/s over four sets of three runs). Push-only runs on that corpus
varied more: set medians spanned 4579–4965 NAR MiB/s, and single runs
4051–5091, so compare push throughput over several runs.

The figures below are from the Garage-era harness. A second pass on the Mac
agreed within 2% on push, the 1 MiB and 100 MiB
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

`max_ms` is the tell. A single push held open for seconds — occasionally
forever, with megabytes unread in the S3 server's socket receive queue — leaves
every percentile looking normal, because with 200 entries p99 is only the
second-worst sample. A run whose `max_ms` is out of line with `p99_ms`, or whose
wall time is far above the sum of latencies divided by concurrency, caught such
a stall and should be re-run rather than checked in. The Garage-era runs above
caught one in roughly half the 1-CPU passes; it reproduced with Garage unpinned
on free cores and on tmpfs, with zero TCP retransmits and zero client retries,
so it was a stand-in artifact rather than garret behaviour.

## Reproducing

`just bench-local` provisions a throwaway versitygw and runs every scenario
against it. `just bench-compare` diffs a fresh run against `baseline.json` and
refuses to compare across labels, corpus versions or harness versions; each
environment keeps its own file. The checked-in result JSONs live in
`benchmarks/`.
