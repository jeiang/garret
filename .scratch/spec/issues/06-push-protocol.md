# Push protocol design

Type: grilling
Status: open
Blocked by: 05

## Question

Design the Pusher wire protocol for maximum throughput with minimum
round-trips: negotiation (get-missing-paths; ticket 05 removed chunk-level
negotiation — whole-NAR won), how parallel uploads are expressed
(concurrent requests vs multiplexed streams; HTTP/2? — per ticket 01,
HTTP/2 flow-control tuning is the real throughput lever), where zstd
compression happens (client-side vs server-side; codec is locked to zstd by
ticket 05, level decided here), upload validation (hash checking placement
— note client-side compression means the server can't cheaply verify the
NAR hash), error/retry semantics, and whether interrupted uploads are
resumable (multi-GB model blobs make this non-trivial). Constraints:
configurable client-side multi-threading; bounded server memory under N
concurrent pushers (see attic OOM history in
`/Users/aidanp/Projects/attic/OPTIMIZATION_PLAN.md`); minimal per-path and
per-chunk round-trips (see `OPTIMIZATIONS.md` items 3–5).
