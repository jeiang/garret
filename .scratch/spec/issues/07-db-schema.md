# SQLite schema and concurrency discipline

Type: grilling
Status: resolved
Blocked by: 05

## Question

Design the SQLite schema and the concurrency rules both services follow:
WAL-mode settings (mmap, synchronous, busy timeout), the single-writer
discipline (Pusher writes, Puller reads — does the Puller ever write, e.g.
last-accessed bumps?), write batching on the upload path (attic paid up to 4
statements per chunk — see `OPTIMIZATIONS.md` item 4), how last-accessed
tracking for LRU GC stays off the read critical path (item 2), and the
schema itself: objects (one row per object, keyed by store path hash — no
chunk tables; ticket 05 chose whole-NAR with no content dedup), narinfo
fields, upload in-progress state, and indices sized for the browse/listing
queries.

## Answer

**Concurrency discipline**

- The Pusher owns all schema writes: object+refs inserted in one
  transaction, GC deletes, admin operations.
- The Puller is read-only except **debounced last-accessed bumps**: updates
  happen off the request path (fire-and-forget, batched on a dedicated
  connection) and only when the stored value is >24 h stale — ~one tiny
  write per object per day maximum. Day-granularity LRU is sufficient for
  quota eviction. WAL + busy_timeout absorbs this second writer.
- **Upload-in-progress state is not in the DB.** It lives in the Pusher's
  memory (single process); the object row is inserted only after the S3
  blob completes. Invariant: **row exists ⇒ blob exists**. Crashes leave
  no dangling DB state; orphaned S3 multiparts are swept at startup/GC
  (ticket 08). Attic's pending-row state machine is gone entirely.

**Schema sketch**

```sql
CREATE TABLE objects (
  store_path_hash   TEXT PRIMARY KEY,  -- 32-char base32, the object key
  store_path        TEXT NOT NULL,
  name              TEXT NOT NULL,     -- basename after the hash
  nar_hash          TEXT NOT NULL,     -- client-claimed (ticket 06)
  nar_size          INTEGER NOT NULL,
  file_hash         TEXT NOT NULL,     -- server-computed over stored zstd
  file_size         INTEGER NOT NULL,
  deriver           TEXT,
  ca                TEXT,
  pushed_by         TEXT,              -- OIDC subject, audit trail
  created_at        INTEGER NOT NULL,
  last_accessed_at  INTEGER NOT NULL
);
CREATE INDEX objects_name ON objects(name);
CREATE INDEX objects_last_accessed ON objects(last_accessed_at); -- LRU order
CREATE TABLE object_refs (
  referrer  TEXT NOT NULL REFERENCES objects ON DELETE CASCADE,
  reference TEXT NOT NULL,             -- hash; may not be in the cache
  PRIMARY KEY (referrer, reference)
) WITHOUT ROWID;
CREATE INDEX object_refs_reference ON object_refs(reference); -- reverse deps
```

- References normalized (not JSON): dependency trees are recursive CTEs,
  referrer lookups come free, and refs may point outside the cache
  (attic-era glossary semantics preserved).
- Name search: indexed `name` column with LIKE; FTS5 only if scale ever
  demands it.
- A `sigs` column is added iff ticket 10 chooses sign-on-write.
- Quota accounting (SUM(file_size) vs maintained counter) is ticket 11's
  call; the schema supports either.

**Pragmas** — WAL, `synchronous=NORMAL` (power-loss window acceptable for
a cache), `busy_timeout=5000`, `mmap_size=512MiB` (the OPTIMIZATION_PLAN
lesson — never attic's 28 GiB), `foreign_keys=ON`. Short write
transactions only; the Pusher runs periodic checkpoint maintenance.
