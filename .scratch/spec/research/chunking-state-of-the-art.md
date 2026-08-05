# Chunking state of the art: CDC + dedup vs whole compressed NARs

Research for garret (single-tenant, CI-push-heavy Nix binary cache; Garage S3 backend, SQLite metadata).
Companion empirical ticket: measure dedup on real closures (see "Unknowns" at the bottom).

## Verdict summary

Content-defined chunking is a **storage-cost optimization, not a push-throughput optimization**. With
server-side chunking (attic's model), the client still uploads the full NAR; the server pays extra CPU
(FastCDC + per-chunk SHA-256 + per-chunk compression), extra S3 PUTs, and extra DB rows on the hot push
path, and the read path degrades from "one ranged GET / presigned redirect" to "server-proxied reassembly
of thousands of small objects". The published Nix-specific numbers (nixbuild.net: 6.55x chunked vs 2.69x
whole-NAR zstd; Replit/tvix-store: 6 TB -> 1.2 TB) show dedup gains are real and large **when the cache
holds many successive versions of similar closures** — exactly what a long-lived CI cache accumulates.

### When chunking wins vs loses

| Condition | Chunking wins | Whole-NAR wins |
|---|---|---|
| Workload | Long-lived cache of many rebuilds of similar closures (version bumps, mass rebuilds, CI on every commit) | Short-retention cache, aggressive GC, mostly-unique artifacts |
| Cost driver | Storage $/GB dominates (large retained set) | Bandwidth/latency/req-count dominates; storage is cheap (Garage on own disks) |
| Content | Uncompressed NAR streams with cross-version similarity (ELF binaries, docs, headers) | NARs whose payload is already compressed/encrypted (tarballs, jars, wasm, media) — CDC finds nothing |
| Push path | Client-side chunk negotiation exists (skip chunks server already has -> less bytes on the wire) | Max single-stream push throughput: one multipart PUT, hash once, one DB row |
| Read path | Reads are rare or bulk (restore-style), latency-insensitive | Substitution latency matters; want presigned/ranged GET pass-through, zero server CPU per download |
| Ops | Willing to run refcount GC, tolerate chunk/chunkref row growth | Want O(1) rows per NAR, trivial GC (delete object + row) |
| Dedup already elsewhere | — | Nix store-path-level dedup (same NAR hash -> one object) already catches identical rebuilds for free |

For garret's stated goal (max push throughput, single tenant, Garage): the evidence favors **whole
compressed NAR objects with NAR-hash-level dedup as the baseline**, with chunking only if the empirical
ticket shows a dedup ratio (chunked vs whole-NAR-zstd) high enough to pay for the read-path and metadata
costs — the nixbuild.net data point (~2.4x incremental over plain zstd) says that is plausible for
long-retention caches.

---

## Evidence

### 1. Attic's implementation (local checkout, jeiang fork of zhaofengli/attic)

- Chunking: FastCDC (ronomon variant) over the **uncompressed** NAR stream —
  `/Users/aidanp/Projects/attic/attic/src/chunking/mod.rs` (`fastcdc::ronomon::FastCDC::with_eof`,
  streaming wrapper that re-buffers up to `max_size`).
- **Parameter correction to the ticket premise**: attic's shipped defaults are
  `nar-size-threshold = 64 KiB`, `min-size = 16 KiB`, `avg-size = 64 KiB`, `max-size = 256 KiB` —
  not 64/256/512 KiB. Verified in both the local checkout
  (`/Users/aidanp/Projects/attic/server/src/config-template.toml` lines 157-171) and upstream
  [zhaofengli/attic config-template.toml](https://raw.githubusercontent.com/zhaofengli/attic/main/server/src/config-template.toml).
  These match casync/desync defaults (16k/64k/256k). `ChunkingConfig` itself has **no serde defaults**
  (`/Users/aidanp/Projects/attic/server/src/config.rs` ~line 442): values are stamped at OOBE so upstream
  can change recommendations without breaking existing deployments, because (per the config comment)
  **changing any chunking parameter shifts cutpoints and wrecks dedup against existing chunks**.
  Larger deployments run coarser params (one reported deployment: 8 MiB threshold, 1 MiB average —
  [attic Discourse thread](https://discourse.nixos.org/t/introducing-attic-a-self-hostable-nix-binary-cache-server/24343)).
- Compression: per-chunk, default zstd (level defaulted). Each chunk row stores both
  `chunk_hash` (uncompressed, dedup key) and `file_hash`/`file_size` (compressed object) plus a
  per-chunk `compression` string — dedup keys on uncompressed content, so identical data compressed
  differently still dedups, but the same chunk can exist once per compression method.
- Read path (`/Users/aidanp/Projects/attic/server/src/api/binary_cache.rs` `get_nar`):
  - Single-chunk NAR: can redirect to a presigned URL (zero proxy bandwidth).
  - Multi-chunk NAR: **must be proxied through the server** ("URLs not supported for NAR reassembly");
    chunks are fetched with a prefetch window (`nar-reassembly-prefetch`, default 4 in this fork, each
    buffering up to `chunking.max-size`) and streamed concatenated. Serving compressed chunks
    back-to-back works because multi-frame concatenation is valid for zstd/xz stream decoders.
- Upload path (`/Users/aidanp/Projects/attic/server/src/api/v1/upload_path.rs`): NARs under the
  threshold go unchunked; otherwise the server chunks, hashes, compresses, and uploads chunks
  concurrently (fork adds a server-wide `max-concurrent-chunk-uploads`, default 10, and batched
  chunkref inserts). The fork also implements **negotiated (chunk-manifest) uploads** where the client
  sends chunk hashes and uploads only `inline` (missing) chunks — this is the only mode where chunking
  saves push bandwidth; it requires proof-of-possession to be disabled.
- Dedup/GC model (`/Users/aidanp/Projects/attic/server/src/database/entity/{chunk,chunkref}.rs`,
  `/Users/aidanp/Projects/attic/server/src/gc.rs`):
  - `chunk`: content-addressed row with a 4-state lifecycle (`PendingUpload -> Valid`,
    `PendingUpload -> ConfirmedDeduplicated` when a racing upload wins, `-> Deleted`) plus a
    `holders_count` to keep in-flight dedup targets alive against GC — i.e., correctness under
    concurrent upload/GC requires a small state machine, not just a refcount.
  - `chunkref`: one row per (nar, seq) -> chunk; `chunk_id` is nullable to represent chunks that
    went missing (with recovery logic on the upload path — "TODO: Fully kill chunk recovery").
  - GC is a two-phase orphan sweep: LEFT JOIN `chunk` x `chunkref` to find unreferenced Valid chunks
    with `holders_count = 0`, flip to Deleted, then batch-delete from storage (bounded batches; the
    fork caps SQLite batches at 65k rows and storage deletion concurrency at 20). Every NAR delete
    turns into thousands of chunkref deletes plus later orphan-chunk sweeps.

Metadata math at attic's defaults (64 KiB avg): ~16,400 chunkref rows per GiB of NAR; a 1 TiB
unique-data cache is ~16.8M chunk rows plus one chunkref row per (nar, chunk) — tens of millions of
rows in SQLite, all of which the GC join must scan. At the ticket's hypothetical 256 KiB average it is
~4,100 rows/GiB — 4x better, still 3-4 orders of magnitude more rows than one-row-per-NAR.

### 2. Nix-specific dedup numbers

- **nixbuild.net benchmark** (rickynils, via [flokli's nix-casync v0.5 post](https://flokli.de/posts/2022-02-21-nix-casync-update/)
  and [nix-casync intro](https://flokli.de/posts/2021-12-10-nix-casync-intro/)): naively chunking whole
  uncompressed NAR files gave a **6.55x** disk-saving ratio vs **2.69x** for plain zstd of whole NARs —
  i.e., CDC+dedup+compression stored ~2.4x less than whole-NAR zstd on a real build-farm dataset.
  Preliminary ("more testing needs to be done"); chunk params not published; project archived Jan 2023.
- **Replit / tvix-store** ([blog](https://replit.com/blog/tvix-store)): migrating a 6 TB Nix path cache
  into tvix-castore (FastCDC chunk-level content addressing, per
  [TVL blog](https://tvl.fyi/blog/tvix-update-february-24)) yielded **1.2 TB** (~80% size reduction,
  "90%" cost reduction), attributed to "different versions of the same package" sharing chunks.
  Chunk sizes/compression not disclosed; read path is FUSE with caching to mask latency.
- Both data points are archives of **many versions over time** — the regime where CDC shines. Neither
  measures a short-retention CI cache.

### 3. Backup-tool experience (restic / borg / casync / desync)

- **casync/desync**: defaults min 16 KiB / avg 64 KiB / max 256 KiB (attic copied these);
  desync parallelizes chunking for speed and supports S3 chunk stores
  ([desync README](https://github.com/folbricht/desync/blob/master/README.md)). No published
  small-object S3 pain numbers, but the store layout is one object per chunk.
- **borg**: default chunker `buzhash,19,23,21` = **min 512 KiB / target 2 MiB / max 8 MiB** —
  deliberately coarse because "resource usage (RAM and disk space) ... is determined by the total
  amount of chunks in the repository"; total RAM ~2.1x the repo index size; fine-grained params are
  recommended only for small repos with plenty of RAM
  ([borg chunker-params notes](https://github.com/borgbackup/borg/blob/master/docs/misc/create_chunker-params.txt),
  [borg usage notes](https://borgbackup.readthedocs.io/en/stable/usage/notes.html)). Borg moved its
  default *up* from 64 KiB-class chunks to 2 MiB precisely because chunk-count overhead beat marginal
  dedup for most users.
- **restic**: CDC blobs (~512 KiB-1 MiB average) but **never stores blobs as individual objects** —
  blobs are packed into multi-MiB pack files with an index kept under ~8 MiB per index file, and
  `prune` must **repack** (read + rewrite packs) to reclaim space from partially-dead packs
  ([restic design doc](https://restic.readthedocs.io/en/v0.2.0/Design/),
  [references](https://restic.readthedocs.io/en/stable/100_references.html)). The pack layer exists
  specifically because one-object-per-chunk on remote storage is too expensive; the price is repack
  I/O as an ongoing GC cost. Attic chose the opposite trade: no repack, but one S3 object per chunk.
- **Compressed inputs dedup terribly**: restic forum reports ~0.3% dedup on 100% duplicate data that
  was re-compressed per backup ([forum thread](https://forum.restic.net/t/dedup-only-0-3-efficient-on-100-duplicate-data/1952)).
  Same applies to NAR payloads that are internally compressed (jars, tarballs, .xz docs): CDC must see
  uncompressed, stable bytes. Chunking the uncompressed NAR (attic/tvix approach) is mandatory;
  chunking compressed NARs is pointless.

### 4. Compression interplay

- Small independent chunks lose compression ratio: zstd "learns from past data", so at the start of
  each chunk there is no history; gains from context are "mostly effective in the first few KB"
  ([zstd docs/wiki](https://github.com/facebook/zstd)). At 64 KiB chunks the window never exceeds
  64 KiB vs zstd level-3's 8 MiB default window on a whole NAR — expect a measurable ratio loss on
  large, self-similar NARs (worth quantifying empirically; no published Nix-specific number found).
- **zstd dictionaries** are the standard mitigation for small-payload compression: train on
  representative samples (>100 samples, ~100x dictionary size of training data)
  ([zstd manual](https://github.com/facebook/zstd/blob/dev/programs/zstd.1.md)). Operationally awkward
  for a cache: the dictionary becomes a versioned dependency of every stored chunk, must ship to any
  decompressing client, and retraining changes the dictionary ID (dedup keyed on uncompressed hash is
  unaffected, but stored bytes diverge). Attic does not use dictionaries.
- The chunked read path stays cheap only because **concatenated zstd frames are a valid zstd stream** —
  attic serves per-chunk-compressed frames back-to-back with no recompression. This constrains
  compression choice (zstd/xz multi-frame; no whole-stream brotli window sharing).
- Note the asymmetry: whole-NAR zstd compresses once at push; per-chunk compression at push costs
  roughly the same CPU total, but dedup hits skip both compression *and* upload of that chunk —
  chunking can *reduce* push CPU/bytes when the dedup ratio is high and negotiation is client-side.

### 5. Read-path costs and Garage specifics

- Reassembly fan-out: at attic's 64 KiB average, a 1 GiB NAR is ~16,000 GETs; at 256 KiB, ~4,000.
  Whole-NAR storage is **one** GET (or a presigned redirect, costing the server ~zero bandwidth and CPU).
  Attic's multi-chunk path cannot redirect — every chunked download transits the server, with a small
  prefetch window (default 4 x max-size = 1 MiB of buffer) to hide storage latency.
- Garage ([2022 performance post](https://garagehq.deuxfleurs.fr/blog/2022-perf/)): TTFB ~**43 ms**
  per GET with v0.8 block streaming (was 1.6-2 s in v0.7); object creation ~**5-20 ms** each; a
  million tiny objects on a 3-node cluster is fine but batch times grow with bucket size; objects
  **<3 KiB are inlined** into the metadata engine. So: per-request overhead is tens of ms — serial
  chunk fetches would cap a download at ~1.5-6 MB/s per stream (64 KiB / 43 ms); hiding it at, say,
  500 MB/s needs a prefetch depth of ~20+ in flight, i.e., real pipelining work.
- Garage's [known issues](https://garagehq.deuxfleurs.fr/documentation/reference-manual/known-issues/)
  note S3-style stores are "not designed for huge numbers of small objects" generally, and that very
  *large* objects have an O(n²) block-reference update cost on upload (mitigated by larger
  `block_size`) — which is instead an argument for bounding whole-NAR object sizes via multipart and
  reasonable `block_size`, not for chunking.
- Whole-NAR also preserves **HTTP range semantics for free** (S3 ranged GET on one object), which
  chunked storage must reimplement by mapping ranges to chunk sequences.
- Real-world attic perf anecdote: a localhost attic deployment measured *slower* than remote Cachix
  in most scenarios ([attic issue #151](https://github.com/zhaofengli/attic/issues/151)) — not a
  controlled study, but consistent with proxy-reassembly and per-chunk overhead on the serve path.

### 6. What NAR-level dedup already gives you for free

Nix store paths are content-addressed by NAR hash at upload (`nar` table keyed on hash in attic's
schema); identical rebuild outputs dedup at whole-NAR granularity with one DB row and zero chunking.
CDC only adds value on the *delta* between similar-but-not-identical NARs. For a CI cache where most
pushes are either identical (cache hit, not re-uploaded at all) or heavily changed, the incremental
CDC win must be measured, not assumed.

---

## Unknowns for the empirical ticket

1. **Actual dedup ratio on garret's real closures**: chunked-dedup+zstd vs whole-NAR zstd, on N
   successive CI pushes of the real project(s). This is the single decision-driving number
   (nixbuild.net saw ~2.4x incremental; a short-retention single-project cache may see far less).
   `atticadm test-chunking` exists for exactly this.
2. **Chunk-size sweep**: 16/64/256 KiB (attic default) vs 64/256/512 KiB vs 256K/1M/4M (borg-style
   coarse) — dedup ratio, chunk count, and compressed-size loss per setting.
3. **Per-chunk compression loss**: whole-NAR zstd-3 size vs sum of per-chunk zstd-3 sizes at each
   chunk size, on real NARs; and whether a trained dictionary recovers the gap (and is it worth the
   operational coupling).
4. **Garage throughput profile**: sustained GET/PUT throughput and p99 latency for 64 KiB / 256 KiB /
   1 MiB / whole-object workloads on the actual deployment hardware, including required prefetch
   depth to saturate a client link, and metadata-engine behavior at millions of objects.
5. **Push-path cost**: wall-clock and CPU for pushing a representative closure as (a) whole multipart
   NARs, (b) server-side chunked, (c) client-negotiated chunked (manifest) with warm server — does
   chunk-skip on re-push actually beat raw multipart throughput on a fast link?
6. **SQLite at scale**: chunk/chunkref row counts, DB file size, GC sweep duration, and write
   contention (busy_timeout pressure under concurrent chunked uploads — the attic fork already had to
   raise SQLite busy-timeout to 30 s for exactly this) at the projected 1-2 year cache size.
7. **Payload composition**: what fraction of the real closures' bytes are internally-compressed files
   (where CDC dedup will be ~nil) vs ELF/text (where it works).

## Sources

- Local: `/Users/aidanp/Projects/attic/attic/src/chunking/mod.rs`,
  `/Users/aidanp/Projects/attic/server/src/config.rs`,
  `/Users/aidanp/Projects/attic/server/src/config-template.toml`,
  `/Users/aidanp/Projects/attic/server/src/database/entity/chunk.rs`,
  `/Users/aidanp/Projects/attic/server/src/database/entity/chunkref.rs`,
  `/Users/aidanp/Projects/attic/server/src/gc.rs`,
  `/Users/aidanp/Projects/attic/server/src/api/binary_cache.rs`,
  `/Users/aidanp/Projects/attic/server/src/api/v1/upload_path.rs`
- [Upstream attic config template](https://raw.githubusercontent.com/zhaofengli/attic/main/server/src/config-template.toml)
- [flokli: Introducing nix-casync](https://flokli.de/posts/2021-12-10-nix-casync-intro/) /
  [nix-casync v0.5 update](https://flokli.de/posts/2022-02-21-nix-casync-update/)
- [Replit: Using Tvix Store to Reduce Nix Storage Costs by 90%](https://replit.com/blog/tvix-store)
- [TVL: Tvix Status February '24](https://tvl.fyi/blog/tvix-update-february-24)
- [Garage: Confronting theoretical design with observed performances](https://garagehq.deuxfleurs.fr/blog/2022-perf/) /
  [Known issues](https://garagehq.deuxfleurs.fr/documentation/reference-manual/known-issues/)
- [borg chunker-params notes](https://github.com/borgbackup/borg/blob/master/docs/misc/create_chunker-params.txt) /
  [borg usage notes](https://borgbackup.readthedocs.io/en/stable/usage/notes.html)
- [restic design document](https://restic.readthedocs.io/en/v0.2.0/Design/) /
  [restic references](https://restic.readthedocs.io/en/stable/100_references.html) /
  [restic forum: dedup on compressed data](https://forum.restic.net/t/dedup-only-0-3-efficient-on-100-duplicate-data/1952)
- [desync README](https://github.com/folbricht/desync/blob/master/README.md)
- [zstd manual (dictionary training)](https://github.com/facebook/zstd/blob/dev/programs/zstd.1.md)
- [FastCDC paper (USENIX ATC '16)](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf)
- [attic issue #151: Benchmarking Cachix vs Attic](https://github.com/zhaofengli/attic/issues/151)
- [attic Discourse announcement thread](https://discourse.nixos.org/t/introducing-attic-a-self-hostable-nix-binary-cache-server/24343)
