# Chunking model decision

Type: grilling
Status: resolved
Blocked by: 02, 03

## Question

Does garret chunk NARs (content-defined dedup, attic-style), store whole
compressed NARs, or something hybrid (e.g. chunk only above a size
threshold)? This is the map's central fork: the answer drives the push
protocol shape (missing-chunk negotiation vs whole-NAR streaming), the DB
schema (chunk tables and refcounts vs one row per object), storage layout,
GC complexity, and read-path reassembly. Decide with ticket 02's literature
findings and ticket 03's measured dedup ratios on real closures in hand.

## Answer

**Whole-NAR storage. No chunking, no content dedup, zstd as the only codec.**

- Each object is stored as a single zstd-compressed NAR (one frame) in S3,
  **keyed by store path hash** — not NAR hash. One DB row per object; no
  refcounting anywhere; deleting an object deletes its blob.
- Rationale: chunking is a storage optimization and garret's binding goals
  are push throughput, minimal round-trips, and bounded memory (tickets
  02+03). Measured identical-NAR dedup is 1.009× — not worth refcounted
  blobs. The ~1.6× rebuild-churn storage cost is accepted; quota+LRU GC
  just evicts sooner.
- Consequences claimed by this decision: presigned-redirect downloads are
  always possible (one object per NAR); GC is trivial (no orphan-chunk
  lifecycle); the push protocol needs only path-level negotiation
  (get-missing-paths), no chunk negotiation.
- zstd level and where compression runs (client vs server) are ticket 06's
  to decide; multipart handling of multi-GB blobs is ticket 08's.
- Rejected: attic-style fine chunking (26× metadata for 23% more savings
  than coarse); coarse chunking (real 1.6× churn win, but pays hot-path
  and GC complexity against the stated priorities); defer-with-seams (the
  store-path-keyed schema is simple enough to migrate later if storage
  economics change).
