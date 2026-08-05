# GC: quota + LRU design

Type: grilling
Status: resolved
Blocked by: 07

## Question

Design garbage collection under the decided policy (storage quota + LRU
eviction): how total usage is tracked cheaply, how last-accessed data feeds
eviction without hot-path writes (ties to the schema ticket's decision),
eviction granularity (whole objects — ticket 05 eliminated chunk
refcounting; an object's blob dies with its row), orphan/partial-upload
cleanup, closure-awareness (is evicting a path whose
referrers remain acceptable?), GC scheduling (in-process in the Pusher vs a
separate invocation), S3 delete batching (`OPTIMIZATIONS.md` item 10), and
the quota configuration surface.

## Answer

**Policy** — storage quota + LRU with **root-first, closure-safe
eviction**: an object is evictable only when no surviving referrer exists
in the cache (`NOT EXISTS` against `object_refs.reference` — ref rows
cascade-delete with their referrer, so any surviving row means a live
referrer). Candidates are evicted in last-accessed order. Old roots (stale
system generations) go first; deleting them frees their deps for the same
pass's next iteration — the pass loops until usage reaches the low
watermark or no candidates remain. Every surviving root's closure stays
complete. **Self-references are excluded at insert time** (a
self-referencing path would otherwise never be evictable).

**Trigger** — high/low watermarks, defaults 95%/85% of quota, all three
configurable. Usage is a maintained counter in a stats row (updated in the
same transaction as inserts and deletes), reconciled against
`SUM(file_size)` at each GC pass. If eviction exhausts candidates while
still above the low watermark (everything referenced), GC stops and raises
a metric/log alarm rather than breaking closures.

**Placement** — a background task inside the Pusher: single-writer
discipline holds, and the orphan sweep can consult in-memory in-flight
upload state directly. Interval configurable (each tick is a cheap counter
check; eviction work only happens past the high watermark). The admin CLI
can trigger a pass on demand via the Pusher's admin endpoint.

**Delete order** — DB row first, blob second (preserves the ticket-07
invariant row⇒blob; a failed blob delete leaves an orphan, which the sweep
catches). S3 deletes use `DeleteObjects` batches of 1,000 with bounded,
configurable concurrency (`OPTIMIZATIONS.md` item 10 honored).

**Orphan sweep** — at startup and on a slow timer (default weekly): list
the bucket against the DB; delete blobs with no row that are older than
24 h (age threshold as belt-and-braces even though in-flight state is
known), and abort multipart uploads not in the in-flight set past the same
threshold.

**Metrics fed to ticket 15** — quota usage gauge, evicted objects/bytes
per pass, pass duration, orphaned blobs found, candidates-exhausted alarm.
