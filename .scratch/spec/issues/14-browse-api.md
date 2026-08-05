# Listing and browse API

Type: grilling
Status: resolved
Blocked by: 07

## Question

Design the cache listing/browse surface: endpoints for listing objects,
searching by name, and the dependency-tree view (semantics carried from the
attic-era fork — see `/Users/aidanp/Projects/attic/CONTEXT.md`); pagination
and response shapes; which service serves it (Puller? Pusher? both?) and
whether it is public or OIDC-gated given the Puller is public for pulls
(cross-ref ticket 09); and what indices the schema needs to keep these
queries cheap (cross-ref ticket 07).

## Answer

**Hosted on the Puller.** Browse is read-only and the Puller is the
public endpoint, so `garret list`/`tree` work from anywhere you can log
in. OIDC (Pocket ID, per ticket 09) applies to the browse routes only;
narinfo/NAR remain anonymous. The Pusher keeps zero read surface.

**Surface (locked)** — JSON under `/api/v1` on the Puller:

- `GET /objects?q=&limit=&cursor=` — list/search by name, keyset
  pagination, newest-first default.
- `GET /objects/{hash}` — full object detail (narinfo fields + timestamps
  + pushed_by).
- `GET /objects/{hash}/tree` — dependency tree in the attic-glossary
  convention: first occurrence expands, repeats truncate, self-references
  skipped (consistent with the schema excluding them at insert),
  references missing from the cache shown but marked.
- `GET /objects/{hash}/referrers` — reverse deps, served by the schema's
  reverse index.

Ticket 07's indices (name, PK, reverse-ref) cover all four; recursive
CTEs implement the tree. Exact response shapes are spec-assembly detail.
