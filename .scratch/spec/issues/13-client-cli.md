# Garret client CLI design

Type: grilling
Status: resolved
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

## Answer

**Command surface (locked)** — `garret login` (device flow), `garret push
<paths…|installable>` (full closure via one missing-paths query, parallel
workers), `garret watch-store` (daemon), `garret list` (search/filter),
`garret tree <path>` (dependency tree). Admin operations live in the
separate `garret-admin` binary. Config: TOML (endpoint, workers, zstd
level, filters, credentials reference) with env/flag overrides;
parallelism via `--jobs`.

**Watcher design**

- *Source of truth*: persisted `ValidPaths.id` cursor over
  `/nix/var/nix/db/db.sqlite` (schema checked at startup), with inotify on
  `*.lock` removals as a latency wakeup that triggers an immediate poll.
  The cursor doubles as the offline backlog — an unreachable Pusher just
  means an old cursor.
- *Privilege*: root systemd service managed by the NixOS module. No
  unprivileged mode in v1.
- *Scope*: the watcher pushes bare paths as they become valid (deps
  register before roots, so closures fill in); manual `garret push`
  expands closures. The server never requires closed closures.
- *Filtering*: skip `.drv` and paths signed by configured upstream keys
  (default cache.nixos.org — read from the DB's sigs, free with the
  cursor). Keep `-source` and fixed-output paths. Configurable
  exclude-pattern list.
- *Failure handling*: capped retries with backoff (default 5), then local
  skip-list + loud log + metrics counter; cursor always advances. Paths
  nix-GC'd before push are skipped silently.
- *Bootstrap*: cursor starts at `MAX(id)` — only new paths push.
  `--full-sync` opts into walking history from 0.

**Not in v1** — post-build-hook socket mode (cachix-daemon pattern):
ruled out of the v1 spec; the fleet is NixOS and the cursor covers
substitutions too. Recorded on the map as out of scope.

**Push behavior** — per the protocol: worker pool of concurrent PUTs,
client-side zstd (default level 3), jittered backoff on 429/5xx,
idempotent retries. Progress output per path + summary; no client
metrics endpoint in v1 (the server side measures; watcher failures
surface via logs and the skip-list).
