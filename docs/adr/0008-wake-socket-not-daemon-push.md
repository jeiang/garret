# Post-build-hook push is a wake socket on watch-store, not a daemon

Ticket 19 proposed reopening the v1 non-goal of post-build-hook
ingestion with a cachix-style shape: a new `garret daemon` owning a
unix socket, a queue, and a JSON command protocol, with `watch-store`
kept running alongside as the durability backstop. We rejected the
two-process shape: it means two long-lived services, two
`client_credentials` sessions and two systemd units running the same
push pipeline, plus a queue whose durability story ("enqueue is
fire-and-forget, the watcher catches what falls") only works *because*
the watcher exists — so the watcher's cursor was always the real queue.
Instead, `watch-store` binds a unix **datagram** socket
(`[watch] socket_path`, default `/run/garret/watch.sock`) and treats
any datagram as "poll the cursor now"; `garret enqueue`, registered as
nix's `post-build-hook` (paths arrive via `$OUT_PATHS` — the hook
passes no argv), sends one datagram and exits 0 unconditionally, since
a hook must never block or fail a build. The datagram payload is debug
text only: the cursor sweep decides what pushes, which keeps the
filters, skip-list and retry logic on the single existing code path and
dissolves the ticket's duplicate-push caveat — there is only one path.
Consequences: the socket carries no authority, so it is mode `0666`
(single-user installs run the hook as the building user) and a bind
failure merely warns — the watcher polls on, pushes trailing builds by
at most `poll_interval`; there is deliberately no response channel, no
framing and no versioning. If command-shaped traffic (`watch-exec`,
status queries) ever needs this socket, that is the day it graduates to
the admin socket's line-delimited-JSON protocol. (Ticket 19.)
