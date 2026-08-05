# Push protocol design

Type: grilling
Status: open
Blocked by: 05

## Question

Design the Pusher wire protocol for maximum throughput with minimum
round-trips: negotiation (get-missing-paths, and missing-chunks if chunking
won), how parallel uploads are expressed (concurrent requests vs multiplexed
streams; HTTP/2?), where compression happens (client-side vs server-side,
codec/level), upload validation (hash checking placement), error/retry
semantics, and whether interrupted uploads are resumable. Constraints:
configurable client-side multi-threading; bounded server memory under N
concurrent pushers (see attic OOM history in
`/Users/aidanp/Projects/attic/OPTIMIZATION_PLAN.md`); minimal per-path and
per-chunk round-trips (see `OPTIMIZATIONS.md` items 3–5).
