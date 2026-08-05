# Garret client CLI design

Type: grilling
Status: open
Blocked by: 06, 12

## Question

Design the garret client: command surface (push, watch-store, list — plus
what else?), configuration (file format, credential storage, endpoint
config), the configurable multi-threaded push implementation (worker model,
parallelism knobs, backpressure against the Pusher), watch-store behavior
built on ticket 12's chosen detection mechanism (queueing, dedup of
already-pushed paths, backlog handling when offline), retry/resume behavior
per the push protocol, and client-side observability (progress output,
optional metrics).
