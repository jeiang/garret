# Browse API

Source: [ticket 14](../../.scratch/spec/issues/14-browse-api.md).

Hosted on the **Puller** (public endpoint — `garret list`/`tree` work
from anywhere you can log in). OIDC (Pocket ID) required on these routes
only; narinfo/NAR remain anonymous.

All JSON under `/api/v1`:

| Endpoint | Behavior |
|---|---|
| `GET /objects?q=&limit=&cursor=` | List/search by name; keyset pagination; newest-first default |
| `GET /objects/{hash}` | Full object detail (narinfo fields, timestamps, pushed_by) |
| `GET /objects/{hash}/tree` | Dependency tree — first occurrence expands, repeats truncate, self-references skipped, references missing from the cache shown but marked |
| `GET /objects/{hash}/referrers` | Reverse dependencies (reverse-ref index) |
| `GET /pins` | GC-exempt pins (spec 05), expired ones included, name-ordered |

Indices serve every endpoint: name, creation order (`objects_created`,
the listing's exact order, so a page never sorts the table), PK, and
reverse-ref. Exact response shapes are an implementation detail — keep
them stable once shipped.

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
