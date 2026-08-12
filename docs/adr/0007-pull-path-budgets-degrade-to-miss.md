# The pull path carries bounded budgets and degrades to a miss

A substituter's contract is bounded latency and harmless failure: nix
tolerates a miss natively (it builds locally or tries the next
substituter) but a hang stalls every build fleet-wide, and a 500 is noise
it isn't built for. The Puller's two pull-path calls therefore run under
`tokio::time::timeout` budgets — the narinfo/NAR database read
(`db_read_budget_ms`) and the presign call (`presign_budget_ms`), both
defaulting to 250 ms; measured p99s are ~1.6 ms and ~2.4 ms
([baseline](../../.scratch/spec/research/benchmark-baseline-2026-08.md)),
so a trip means something is genuinely wrong, and the budget's job is to
convert the worst failure mode (hang) into the one clients are built for
(miss). On timeout *or* error the route answers 404 and increments
`garret_degraded_total{reason="db_timeout"|"db_error"|"presign_timeout"|
"presign_error"}` — sccache's pattern: every read-path degradation is a
named counter, never silent. The database read is synchronous rusqlite
under the connection Mutex, so it moves to the blocking pool
(`spawn_blocking`) for the timeout to mean anything: a wedged read — or a
read queued behind a wedged lock holder — trips its budget while the
orphaned read keeps its blocking thread until it returns. Boundaries
kept: the not-yet-created database still answers 503 (`/ready` already
models that state, and it is a startup condition, not a wedge), and the
browse API is an authenticated JSON surface outside the substituter
contract, so it keeps its 500s. Consequence: a persistently wedged disk
now looks like a cold cache (miss storm + climbing degraded counter)
rather than a stalled fleet — alert on `garret_degraded_total`, since
builds will quietly stop hitting the cache instead of failing loudly.
(Ticket 25.)
