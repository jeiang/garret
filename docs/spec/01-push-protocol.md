# Push Protocol

Source: [ticket 06](../../.scratch/spec/issues/06-push-protocol.md).

Two-step flow, zero avoidable round-trips: one batch negotiation, then
fully parallel, self-contained, idempotent uploads. All endpoints are
versioned under `/api/v1` on the Pusher and require a valid OIDC bearer
token (see [04-auth.md](04-auth.md)).

## Negotiation

`POST /api/v1/missing-paths`

Request: JSON array of store path hashes (the closure, or any batch).
Response: the subset not present in the cache. This is the only
pre-upload round-trip.

## Upload

`PUT /api/v1/nar/{storePathHash}`

Body layout (streamed, never buffered whole):

1. A 4-byte little-endian length prefix.
2. A JSON metadata preamble of that length: `storePath`, `narHash`,
   `narSize`, `references` (**full store paths**), `deriver?`, `ca?`.
3. The zstd-compressed NAR stream (single frame) to EOF.

The length-prefixed preamble avoids header-size limits for long reference
lists while keeping the request a single streamed body.

References are full store paths, not hashes: narinfo's `References:` line
prints reference *names* (`<hash>-<name>`) and the signed fingerprint uses
their full paths, so a hash alone cannot be expanded back into either —
and references may point outside the cache, so the DB cannot resolve them.
Negotiation (`missing-paths`) still speaks hashes; only the upload
preamble needs the name. (Corrected at M1: the original spec said hashes
here, which made a valid narinfo impossible to produce.)

### Compression and verification

- The **client** compresses (zstd, default level 3, configurable).
- The server hashes the compressed bytes as it relays them to S3 — this
  becomes the stored `FileHash`/`FileSize` — and **trusts the client's
  claimed `narHash`/`narSize`** (every pusher is OIDC-authenticated on
  single-tenant infra; [ADR-0002](../adr/0002-whole-nar-storage.md)).
  A background deep-verify pass is an optional future addition, not v1.

### Idempotency

Pushes are fully idempotent; watcher/CI overlap and timeout-retries are
normal operation:

- Client sends `Expect: 100-continue`; the server checks the DB before
  requesting the body.
- Already present → `200 {"status":"exists"}` with no body transfer.
- In flight elsewhere → `200 {"status":"in-progress"}`; the second
  pusher treats it as success (first writer wins).
- Completed → `201 {"status":"created"}`.

## Parallelism and transport

Parallelism is expressed as concurrent PUTs from the client's
configurable worker pool — no custom framing, no batch endpoint (revisit
only if benchmarks show per-request overhead dominating small-NAR
pushes). HTTP/2 with explicitly tuned flow control: adaptive window
enabled; `max_send_buf_size` sized to bound per-stream server memory.
HTTP/1.1 works unmodified. The client may open additional connections if
one saturates.

## Backpressure

Global semaphores cap (a) concurrent uploads and (b) total in-flight
bytes. Past either cap the server sheds fast with `429` + `Retry-After`;
clients retry with jittered backoff. The queue lives in the clients;
server memory is provably bounded by configuration.

## Errors, retry, versioning

`5xx` and `429` are retryable; other `4xx` are not. Error bodies are
JSON. No upload resume in v1: a failed push restarts from zero. An
`Upload-Offset`-style header is reserved so resume can be added without a
version break.

**Connection drops mid-upload are retryable too.** The early replies
above (`exists`, `in-progress`, `429`) are all sent before the body is
read, and the server then closes the connection with request bytes still
unread — over HTTP/1.1 that close becomes a TCP RST. A client that is
not waiting on `100-continue` and is still writing the body races the
RST: usually it reads the reply first, but sometimes the write fails
(broken pipe / connection reset) and the reply is lost. Clients must
treat a connection-level error during an upload as retryable with the
same bounded backoff as a `429` — negotiation makes every push
idempotent, so the retry either lands the NAR or cheaply learns
`exists`. (Found at M5: the e2e bench flaked on exactly this race when
re-pushing an already-present corpus.)
