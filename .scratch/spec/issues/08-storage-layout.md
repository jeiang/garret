# S3 storage layout and interaction strategy

Type: grilling
Status: resolved
Blocked by: 05

## Question

Design the S3 side: object key naming scheme (ticket 05: one zstd-NAR blob
per object, keyed by store path hash), when to use multipart vs single PUT
(multi-GB model blobs are routine here), bounded part concurrency (attic's
OOM vector — see `OPTIMIZATION_PLAN.md` step 2), read-path streaming,
Garage-specific behavior worth designing around, failure/cleanup of partial
uploads, and whether the Puller redirects clients to presigned S3 URLs or
proxies bytes itself (ticket 05 guarantees one object per NAR, so redirects
are always possible).

## Answer

**Read path — proxy.** The Puller streams Garage→client itself: one public
endpoint, Garage stays internal. Large read buffers (256 KiB, the
OPTIMIZATIONS item-6 lesson — never ReaderStream's 4 KiB default),
HTTP Range passthrough, and connection reuse to Garage. Presigned
redirects are not shipped in v1; the one-blob-per-NAR guarantee keeps the
option open with trivial effort if a CDN or public Garage ever appears.

**Key scheme** — flat: `nar/<storePathHash>.nar.zst`. Garage needs no
prefix sharding; the key is derivable from the DB row and vice versa.

**Upload defaults (locked, all configurable)** — single PUT below
100 MiB; above it, multipart with 64 MiB parts and ≤4 parts in flight,
the permit acquired **before** reading each part (OPTIMIZATION_PLAN
step-2 lesson: never let the reader race ahead of S3). Worst-case buffer
is 4×64 MiB per NAR, and the push protocol's global in-flight byte cap
bounds the aggregate. A 5 GB model blob is ~80 parts. The benchmark
ticket validates these numbers.

**Failure/cleanup** — abort the multipart on any upload error so parts
free immediately; crash leftovers are covered by ticket 11's sweeps
(startup + weekly: abort multiparts not in the in-flight set older than
24 h, delete row-less blobs older than 24 h). Do not rely on S3 lifecycle
rules — garret's own sweep is the mechanism, keeping Garage-compatibility
assumptions minimal.

**Garage notes** — prefer fewer/larger parts (matches the 64 MiB
default); no lifecycle-rule dependence; DeleteObjects batching per ticket
11. Anything Garage-version-specific gets verified during implementation
benchmarks rather than assumed here.
