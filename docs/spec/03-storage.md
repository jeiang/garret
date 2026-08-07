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

## Cleanup

No reliance on S3 lifecycle rules. GC owns cleanup — see
[05-gc.md](05-gc.md): startup + weekly sweeps abort stale multiparts and
delete row-less blobs older than 24 h.
