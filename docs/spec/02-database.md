# Database

Source: [ticket 07](../../.scratch/spec/issues/07-db-schema.md).

SQLite, one file, WAL mode, shared by both services on the same host.

## Concurrency discipline

- The **Pusher owns all schema writes**: object+refs inserts (one
  transaction per object), GC deletes, resign, stats.
- The **Puller** is read-only except debounced last-accessed bumps:
  performed off the request path (fire-and-forget, batched on a dedicated
  connection) and only when the stored value is >24 h stale. Day
  granularity is sufficient for LRU (see [05-gc.md](05-gc.md)).
- **Upload-in-progress state is not in the DB.** It lives in the Pusher's
  memory; the object row is inserted only after the S3 blob completes.

**Invariant: row exists ⇒ blob exists.** Crashes leave no dangling DB
state; orphaned blobs/multiparts are swept by GC.

## Schema

```sql
CREATE TABLE objects (
  store_path_hash   TEXT PRIMARY KEY,  -- 32-char base32, the object key
  store_path        TEXT NOT NULL,
  name              TEXT NOT NULL,     -- basename after the hash
  nar_hash          TEXT NOT NULL,     -- client-claimed
  nar_size          INTEGER NOT NULL,
  file_hash         TEXT NOT NULL,     -- server-computed over stored zstd
  file_size         INTEGER NOT NULL,
  deriver           TEXT,
  ca                TEXT,
  sigs              TEXT NOT NULL,     -- signed on write; multi-key JSON list
  pushed_by         TEXT,              -- OIDC subject (audit)
  created_at        INTEGER NOT NULL,
  last_accessed_at  INTEGER NOT NULL
);
CREATE INDEX objects_name          ON objects(name);
CREATE INDEX objects_last_accessed ON objects(last_accessed_at); -- LRU order

CREATE TABLE object_refs (
  referrer  TEXT NOT NULL REFERENCES objects ON DELETE CASCADE,
  reference TEXT NOT NULL,             -- basename `<hash>-<name>`; may not be in the cache
  reference_hash TEXT GENERATED ALWAYS AS (substr(reference, 1, 32)) VIRTUAL,
  PRIMARY KEY (referrer, reference)
) WITHOUT ROWID;
CREATE INDEX object_refs_reference ON object_refs(reference_hash); -- reverse deps

CREATE TABLE stats (                   -- single row
  id           INTEGER PRIMARY KEY CHECK (id = 1),
  total_bytes  INTEGER NOT NULL       -- maintained in insert/delete txs
);
```

Notes:

- References are normalized (recursive CTEs for dependency trees;
  referrer lookups via the reverse index). References may point outside
  the cache. **Self-references are excluded at insert time** (a
  self-referencing path would never be evictable under closure-safe GC).
- `reference` holds the **basename**, not the bare hash: narinfo prints
  reference names and the signed fingerprint needs full store paths, and
  neither can be reconstructed from a hash — least of all for references
  outside the cache. `reference_hash` is generated from it, so joins
  against `objects.store_path_hash` (GC's evictability check, the
  referrers endpoint) stay indexed with no second copy to keep in sync.
  (Corrected at M1; see [01-push-protocol.md](01-push-protocol.md).)
- Name search uses the indexed `name` column with LIKE; FTS5 only if
  scale ever demands it.
- `total_bytes` is reconciled against `SUM(file_size)` at each GC pass.

## Pragmas

WAL; `synchronous=NORMAL` (power-loss window acceptable for a cache);
`busy_timeout=5000`; `mmap_size=512MiB` (never attic's 28 GiB);
`foreign_keys=ON`. Short write transactions only; the Pusher runs
periodic checkpoint maintenance.
