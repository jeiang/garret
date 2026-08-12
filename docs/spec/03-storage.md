# S3 Storage

Source: [ticket 08](../../.scratch/spec/issues/08-storage-layout.md),
revised at implementation start ([ADR-0005](../adr/0005-remote-object-store-presigned-reads.md)).

Backend: any S3-compatible store; **MEGA S4 is the deployment target**
(Garage remains the local dev/bench stand-in). The bucket is **remote**,
reached over WAN — not a colocated LAN service.

S4 supports everything garret uses: multipart (including
`AbortMultipartUpload` / `ListMultipartUploads`), `DeleteObjects`,
Range GET, `ListObjectsV2` continuation tokens, and presigned URLs.
It has no versioning, no server-side encryption, and no lifecycle rules
— none of which garret relies on.

## Layout

Flat keys: `nar/<storePathHash>.nar.zst` — one blob per object, derivable
from the DB row and vice versa. No prefix sharding.

## Uploads (Pusher → S3)

Defaults, all configurable:

- Single `PutObject` when the whole body fits in one part (**64 MiB**).
- Above: multipart with **64 MiB parts**, at most **4 parts in flight**,
  and the concurrency permit acquired **before** reading each part — the
  reader must never race ahead of S3. Worst-case buffering is 4×64 MiB
  per NAR; the protocol's global in-flight byte cap bounds the aggregate.
- On any upload error, abort the multipart immediately so parts free.
- Every S3 call carries an **overall operation deadline** —
  `[s3] operation_timeout_secs`, default **60** — covering connect,
  transfer, and any SDK-internal retries (ticket 27). Overall rather
  than per-attempt on purpose: a timed-out attempt would be retried,
  and part re-upload is forbidden below. A stalled upstream therefore
  costs one timeout and a loud failed push instead of holding an upload
  permit and its in-flight bytes forever.

The single-`PutObject` threshold is the part size, not a separate 100 MiB
knob as originally written (corrected at M3). `PutObject` needs the body
length up front, so the threshold is exactly how much must be buffered
before the shape of the upload is known — setting it above the part size
would buffer 100 MiB per upload and break the 4×64 MiB bound this section
promises. One knob, and the promise holds.

S4 constraints the defaults must keep satisfying: parts 1..N-1 must be
**identical in size** (uniform parts with a short final part — what we
already do), minimum part size **5 MiB**, and **no part re-upload** — a
failed part is unrecoverable, so the whole multipart aborts and the push
restarts. This matches the no-resume-in-v1 stance in
[01-push-protocol.md](01-push-protocol.md); part-level retry is not an
option on this backend.

Part concurrency is a WAN bandwidth-delay-product knob now, not a LAN
one: if uploads underfill the link, raise in-flight parts before
anything else.

## Read path (Puller): presigned redirect

The Puller **does not proxy NAR bytes**. `GET /nar/<hash>.nar.zst`
answers `302` with a presigned S4 URL (TTL configurable, default 1 h);
the client fetches bytes directly from S4, which serves Range requests
itself. Narinfo is still served by the Puller — it holds the signatures
— so the substituter surface is unchanged from a client's view.

Rationale: with a remote store, proxying crosses the host's uplink twice
per byte served. Redirecting removes the Puller from the byte path
entirely, which also makes its flat-memory property trivial rather than
engineered. ([ADR-0005](../adr/0005-remote-object-store-presigned-reads.md))

Consequence: the S4 endpoint and bucket name are publicly visible in
redirect URLs. Cache contents are already public; credentials are not
exposed (presigning is signature-only).

### Bounded budgets: degrade to a miss

A substituter's contract is bounded latency and harmless failure: nix
tolerates a miss natively (build locally, try the next substituter) but a
hang stalls builds fleet-wide. Both pull-path calls therefore carry
budgets — the narinfo/NAR database read (`db_read_budget_ms`) and the
presign call (`presign_budget_ms`), each defaulting to 250 ms; measured
p99s are ~1.6 ms and ~2.4 ms, so a trip means something is genuinely
wrong. The database read is synchronous rusqlite under a Mutex, so it
runs on the blocking pool — a wedged read (or one queued behind a wedged
lock holder) trips its budget instead of stalling the request.

On timeout or error the Puller answers **404** — a miss, which clients
handle — and increments `garret_degraded_total{reason}`
(spec [08-observability](08-observability.md)). The not-yet-created
database still answers **503** (`/ready` models that state); degradation
covers a database or object store that is present but wedged. The browse
API is outside this contract and keeps its 500s.
(Ticket 25; prior art: sccache's timed-out-lookup → local-compile miss.)

## Cleanup

No reliance on S3 lifecycle rules. GC owns cleanup — see
[05-gc.md](05-gc.md): startup + weekly sweeps abort stale multiparts and
delete row-less blobs older than 24 h.
