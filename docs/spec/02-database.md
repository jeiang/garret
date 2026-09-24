# Database

Source: [ticket 07](../../.scratch/spec/issues/07-db-schema.md).

SQLite, one file, WAL mode, shared by both services on the same host.

## Concurrency discipline

- The **Pusher owns all schema writes**: object+refs inserts (one
  transaction per object), GC deletes, resign, stats.
- The **Puller** is read-only except debounced last-accessed bumps, and
  only when the stored value is >24 h stale. Day granularity is
  sufficient for LRU (see [05-gc.md](05-gc.md)).
  - The narinfo read already returns `last_accessed_at`, so a hit on a
    fresh row writes nothing. A no-op `UPDATE` would still take the WAL
    write lock, and every hit would contend with the Pusher.
  - Stale hashes are queued in memory as a set (a burst of hits on one
    hash is one write) and flushed every 5 s in one `BEGIN IMMEDIATE`
    transaction, on the blocking pool, on a dedicated connection with a
    1 s busy timeout. A flush never touches the pull-path connection.
  - A failed flush drops its batch. Those rows are still stale, so their
    next hit queues them again.
- **Upload-in-progress state is not in the DB.** It lives in the Pusher's
  memory, alongside the deletion claims GC, `delete` and prune hold from
  row delete to blob delete (spec 05); the object row is inserted only
  after the S3 blob completes.

**Invariant: row exists ⇒ blob exists.** Crashes leave no dangling DB
state; orphaned blobs/multiparts are swept by GC. `garret-admin fsck`
(spec [05-gc](05-gc.md#garret-admin-fsck)) audits this invariant against
the actual bucket contents and can repair drift the sweep never sees —
a manually deleted S3 object, an interrupted restore, bucket corruption.

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
  last_accessed_at  INTEGER NOT NULL,
  pushed_at         INTEGER NOT NULL   -- last upload or Negotiation hit
);
CREATE INDEX objects_name          ON objects(name);
CREATE INDEX objects_last_accessed ON objects(last_accessed_at); -- LRU order
CREATE INDEX objects_created       ON objects(created_at DESC, store_path_hash); -- browse listing order

CREATE TABLE object_refs (
  referrer  TEXT NOT NULL REFERENCES objects ON DELETE CASCADE,
  reference TEXT NOT NULL,             -- basename `<hash>-<name>`; may not be in the cache
  reference_hash TEXT GENERATED ALWAYS AS (substr(reference, 1, 32)) VIRTUAL,
  PRIMARY KEY (referrer, reference)
) WITHOUT ROWID;
CREATE INDEX object_refs_reference ON object_refs(reference_hash); -- reverse deps

CREATE TABLE pins (                  -- GC-exempt roots (ticket 22, spec 05)
  name             TEXT PRIMARY KEY,   -- operator-chosen
  store_path_hash  TEXT NOT NULL REFERENCES objects ON DELETE CASCADE,
  expires_at       INTEGER,            -- NULL = permanent
  created_at       INTEGER NOT NULL
);
CREATE INDEX pins_hash ON pins(store_path_hash);

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
- `pushed_at` is set on insert and refreshed by every Negotiation that
  finds the object present, debounced to one write per hour. It is the
  age `garret-admin prune` judges by, and GC leaves anything pushed in
  the last day alone (the push grace, spec 05). Databases from before
  the column existed are migrated with `pushed_at = created_at`.
- A re-push rewrites the object's row in place (an upsert), never
  delete-then-insert: `pins` cascade on delete, so `INSERT OR REPLACE`
  would silently unpin a re-pushed object.

## Pragmas

WAL; `synchronous=NORMAL` (power-loss window acceptable for a cache);
`busy_timeout=5000`; `mmap_size=512MiB` (never attic's 28 GiB);
`journal_size_limit=64MiB`; `foreign_keys=ON`. Short write transactions
only.

A write transaction that reads before it writes (insert and delete, for
their `stats` delta) begins `IMMEDIATE`. Begun deferred, it would take a
read lock first, and SQLite fails the later upgrade to the write lock
with `SQLITE_BUSY` at once, without consulting `busy_timeout`, whenever
another connection holds it — which the Puller's bumps do.

Checkpointing is SQLite's own; there is no periodic checkpoint task.
Auto-checkpoint (PASSIVE, every 1000 WAL pages, on commit on either
connection) keeps the WAL at a few MiB even under continuous pull reads,
because no read transaction is long-lived. It never shrinks the file,
though, so `journal_size_limit` trims it back after one large
transaction, and the Pusher truncates it at startup. A timed PASSIVE
checkpoint would repeat auto-checkpoint; a timed TRUNCATE would stall
writers behind readers, for up to `busy_timeout`, to reclaim what the
size limit already reclaims.
