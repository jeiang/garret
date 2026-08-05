# Chunk-dedup state of the art

Type: research
Status: resolved

## Question

What does the evidence say about content-defined chunking + dedup for a Nix
binary cache, versus storing whole compressed NARs? Gather: FastCDC parameter
practice (attic uses 256 KiB average — see `/Users/aidanp/Projects/attic`),
restic/borg/casync experience with dedup ratios and their costs, the
interaction between chunk-level compression and whole-file compression
(ratio loss from small chunks), read-path reassembly costs from object
storage, and metadata overhead per chunk (DB rows, GC refcounting).
Summarize the conditions under which chunking wins and loses for
CI-push-heavy workloads.

## Answer

Full findings: [research/chunking-state-of-the-art.md](../research/chunking-state-of-the-art.md).
Leaning captured for ticket 05: **baseline on whole compressed NARs +
NAR-hash dedup; adopt chunking only if ticket 03's measurements show a ratio
that pays for the read-path and metadata costs.**

- Premise corrected: attic's actual FastCDC defaults are **16/64/256 KiB**
  min/avg/max with a 64 KiB chunking threshold (casync's defaults), not
  64/256/512. Parameters are stamped at first run because changing them
  shifts cutpoints and wrecks existing dedup.
- CDC dedup gains are real on long-lived multi-version caches:
  nixbuild.net measured 6.55× (chunked) vs 2.69× (whole-NAR zstd), ~2.4×
  incremental; Replit's tvix-store cut 6 TB → 1.2 TB.
- But chunking is a **storage** optimization, not a push-throughput one:
  server-side chunking adds per-chunk hashing/compression/PUTs/DB rows on
  the hot push path, and multi-chunk downloads can't use presigned
  redirects — attic proxies reassembly with a 4-deep prefetch; at Garage's
  ~43 ms per-GET TTFB, serial 64 KiB fetches cap near 1.5 MB/s per stream
  without deep pipelining.
- Backup tools converged away from attic-sized chunks: borg targets 2 MiB
  (chunk-count RAM/index cost); restic packs blobs into multi-MiB pack
  files. CDC on internally-compressed payloads dedups ~0%.
- Per-chunk zstd loses ratio vs whole-file (no cross-chunk window); zstd
  dictionaries mitigate but add operational coupling.
- Metadata: ~16.4k chunkref rows/GiB at 64 KiB avg (4.1k at 256 KiB), a
  4-state chunk lifecycle, and two-phase orphan GC — vs one row and
  trivial GC per whole NAR.
