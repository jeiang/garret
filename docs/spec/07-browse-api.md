# Browse API

Source: [ticket 14](../../.scratch/spec/issues/14-browse-api.md).

Hosted on the **Puller** (public endpoint — `garret list`/`tree` work
from anywhere you can log in). OIDC (Pocket ID) required on these routes
only; narinfo/NAR remain anonymous. Unmatched paths are a plain 404
and are never counted as browse auth failures. A token that fails
verification gets `401`; a verified token that an issuer policy gate refuses
gets `403` (spec 04). Both count as browse auth failures.

All JSON under `/api/v1`:

| Endpoint | Behavior |
|---|---|
| `GET /objects?q=&limit=&cursor=` | List/search by name (literal substring: `%` and `_` are not wildcards); keyset pagination; newest-first; `limit` defaults to 50 and is clamped to 1–500; `cursor` is a previous page's `next_cursor`, and a malformed one is a `400` |
| `GET /objects/{hash}` | Object detail: `store_path_hash`, `store_path`, `name`, the narinfo fields (`nar_hash`, `nar_size`, `file_hash`, `file_size`, `deriver`, `ca`, `references` as basenames, `sigs`) and `pushed_by`; no timestamps (the listing carries `created_at`) |
| `GET /objects/{hash}/tree` | Dependency tree — first occurrence expands, repeats truncate, self-references skipped, references missing from the cache shown but marked; expansion stops 64 levels below the root (nodes at that depth are listed without children) |
| `GET /objects/{hash}/referrers` | Reverse dependencies (reverse-ref index) |
| `GET /pins` | GC-exempt pins (spec 05), expired ones included, name-ordered |

Indices serve every endpoint: name, creation order (`objects_created`,
the listing's exact order, so a page never sorts the table), PK, and
reverse-ref. Exact response shapes are an implementation detail — keep
them stable once shipped.

The tree expands each node with one indexed query: the node's
`object_refs` primary-key range, left-joined to `objects` on the
generated `reference_hash` column referrers and GC also use, so each
child's hash and whether it is cached come back together, in reference
order, with self-edges filtered out. A malformed reference (too short,
or multibyte text at the hash boundary) matches no object and is shown
as missing; nothing on that path can panic while the browse connection
is held.

## Isolation from the pull path

A big closure tree or a full-scan search must never delay a narinfo
read, so browse shares neither the pull path's connection nor its async
workers, and takes at most one blocking thread:

- Queries run on the Puller's own browse connection, one at a time, on
  the blocking pool.
- A request waits for its turn on an async lock, so queued requests
  hold no threads.
- A request over 10 s, queueing included, answers 503. Its query still
  finishes in the background before the next one starts.
