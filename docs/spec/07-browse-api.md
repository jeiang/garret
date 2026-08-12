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

Ticket 07's indices (name, PK, reverse-ref) serve all four; trees are
recursive CTEs. Exact response shapes are an implementation detail —
keep them stable once shipped.
