# Push protocol design

Type: grilling
Status: resolved
Blocked by: 05

## Question

Design the Pusher wire protocol for maximum throughput with minimum
round-trips: negotiation (get-missing-paths; ticket 05 removed chunk-level
negotiation — whole-NAR won), how parallel uploads are expressed
(concurrent requests vs multiplexed streams; HTTP/2? — per ticket 01,
HTTP/2 flow-control tuning is the real throughput lever), where zstd
compression happens (client-side vs server-side; codec is locked to zstd by
ticket 05, level decided here), upload validation (hash checking placement
— note client-side compression means the server can't cheaply verify the
NAR hash), error/retry semantics, and whether interrupted uploads are
resumable (multi-GB model blobs make this non-trivial).

## Answer

Two-step flow, zero avoidable round-trips: one batch negotiation, then
fully parallel self-contained uploads.

**Negotiation** — `POST /api/v1/missing-paths`: client sends the closure's
store path hashes (one batch), server returns the missing subset. This is
the only pre-upload round-trip.

**Upload** — `PUT /api/v1/nar/{storePathHash}`: a single self-contained
request per store path. Body = a length-prefixed JSON metadata preamble
(narinfo fields: NarHash, NarSize, references, deriver, CA) followed by the
zstd-compressed NAR stream. No header-size risk for long reference lists,
no multipart parsing, still pure streaming.

**Compression** — client-side zstd, single frame, default level 3
(client-configurable). The server hashes only the compressed bytes it
stores (FileHash, computed while relaying to S3 — nearly free) and
**trusts the client's claimed NarHash/NarSize**: defensible because every
pusher is OIDC-authenticated on single-tenant infra. A background
deep-verify pass stays an optional future add, not v1.

**Parallelism/transport** — parallelism is just concurrent PUTs from the
client's configurable worker pool. HTTP/2 with tuned flow control
(adaptive window; `max_send_buf_size` sized to bound per-stream server
memory — ticket 01's finding); the client may open additional connections
if one saturates. HTTP/1.1 works unmodified as a fallback. No custom
framing, no batch endpoint (revisit only if benchmarks show per-request
overhead dominating small-NAR pushes).

**Backpressure** — global semaphores cap concurrent uploads and in-flight
bytes (attic OOM lesson kept). Beyond the cap: fast `429` +
`Retry-After`; clients retry with jittered backoff. The queue lives in
the clients; server memory is provably bounded.

**Idempotency** — pushes are fully idempotent. Client sends `Expect:
100-continue`; the server checks the DB before requesting the body:
already-present → immediate success (`200 {"status":"exists"}`, no body
transfer); in-flight elsewhere → `200 {"status":"in-progress"}`, second
pusher treats it as success (first writer wins). Watcher/CI overlap and
timeout-retries are normal operation, never errors.

**Errors/retry** — 5xx and 429 are retryable, other 4xx are not; JSON
error bodies. Versioned under `/api/v1/`.

**Resume** — none in v1: a failed push restarts from zero (LAN drops are
rare; even 5 GB re-pushes in minutes). Protocol reserves an
`Upload-Offset`-style header so resume can arrive without a version break. Constraints:
configurable client-side multi-threading; bounded server memory under N
concurrent pushers (see attic OOM history in
`/Users/aidanp/Projects/attic/OPTIMIZATION_PLAN.md`); minimal per-path and
per-chunk round-trips (see `OPTIMIZATIONS.md` items 3–5).
