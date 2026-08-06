# S3 Storage

Source: [ticket 08](../../.scratch/spec/issues/08-storage-layout.md).

Backend: any S3-compatible store; Garage is the deployment target. The
bucket is **internal** — only the two services talk to it.

## Layout

Flat keys: `nar/<storePathHash>.nar.zst` — one blob per object, derivable
from the DB row and vice versa. No prefix sharding.

## Uploads (Pusher → S3)

Defaults, all configurable:

- Single `PutObject` below **100 MiB**.
- Above: multipart with **64 MiB parts**, at most **4 parts in flight**,
  and the concurrency permit acquired **before** reading each part — the
  reader must never race ahead of S3. Worst-case buffering is 4×64 MiB
  per NAR; the protocol's global in-flight byte cap bounds the aggregate.
- On any upload error, abort the multipart immediately so parts free.

## Read path (Puller)

The Puller **proxies** bytes from Garage to clients: one public endpoint,
Garage stays internal. 256 KiB read buffers (never a 4 KiB default),
HTTP `Range` passthrough, connection reuse to Garage. Presigned
redirects are not shipped in v1; one-blob-per-NAR keeps that option
trivially available if a CDN or public Garage ever appears.

## Cleanup

No reliance on S3 lifecycle rules (keeps Garage-compat assumptions
minimal). GC owns cleanup — see [05-gc.md](05-gc.md): startup + weekly
sweeps abort stale multiparts and delete row-less blobs older than 24 h.
