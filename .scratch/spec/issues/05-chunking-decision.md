# Chunking model decision

Type: grilling
Status: open
Blocked by: 02, 03

## Question

Does garret chunk NARs (content-defined dedup, attic-style), store whole
compressed NARs, or something hybrid (e.g. chunk only above a size
threshold)? This is the map's central fork: the answer drives the push
protocol shape (missing-chunk negotiation vs whole-NAR streaming), the DB
schema (chunk tables and refcounts vs one row per object), storage layout,
GC complexity, and read-path reassembly. Decide with ticket 02's literature
findings and ticket 03's measured dedup ratios on real closures in hand.
