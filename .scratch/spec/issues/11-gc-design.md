# GC: quota + LRU design

Type: grilling
Status: open
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
