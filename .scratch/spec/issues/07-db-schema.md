# SQLite schema and concurrency discipline

Type: grilling
Status: open
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
