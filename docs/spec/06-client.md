# Client CLI & Store Watcher

Sources: [ticket 13](../../.scratch/spec/issues/13-client-cli.md),
[ticket 12 research](../../.scratch/spec/research/store-watcher-mechanisms.md).

## Commands

| Command | Behavior |
|---|---|
| `garret login` | Device flow against Pocket ID; stores rotating refresh token |
| `garret push <paths…\|installable>` | Full closure via one missing-paths query; parallel workers |
| `garret watch-store` | Watcher daemon (below) |
| `garret list` | Search/filter cache contents (browse API) |
| `garret tree <path>` | Dependency tree (browse API) |

Admin operations live in `garret-admin`
(see [10-packaging.md](10-packaging.md)).

Config: TOML — endpoint, workers, zstd level, filters, credentials
reference — with env/flag overrides. Parallelism via `--jobs`.

## Push behavior

Per the protocol: worker pool of concurrent PUTs, client-side zstd
(default level 3), jittered backoff on 429/5xx, idempotent retries.
Progress output per path plus a summary. No client metrics endpoint in
v1.

## Store watcher

- **Source of truth**: a persisted cursor over `ValidPaths.id` in
  `/nix/var/nix/db/db.sqlite` (AUTOINCREMENT — monotonic, never reused;
  schema checked at startup). Complete by construction: catch-up after
  downtime, initial scan, and offline backlog are all "the cursor is
  old". inotify on `*.lock` removals (race-free: Nix unlinks the lock
  only after `registerValidPath`) serves as a latency wakeup triggering
  an immediate poll.
- **Privilege**: root systemd service managed by the NixOS module. No
  unprivileged mode in v1.
- **Scope**: pushes bare paths as they become valid — dependencies
  register before roots, so closures self-assemble. The server never
  requires closed closures.
- **Filtering**: skip `.drv` paths and paths already signed by configured
  upstream keys (default `cache.nixos.org`, read from the DB's sigs).
  Keep `-source` and fixed-output paths. Configurable exclude patterns.
- **Failure handling**: capped retries with backoff (default 5), then
  local skip-list + loud log + metrics-visible counter; the cursor always
  advances — one poison path never wedges the pipeline. Paths nix-GC'd
  before push are skipped silently.
- **Bootstrap**: cursor starts at `MAX(id)` (only new paths push);
  `--full-sync` opts into walking history from id 0.
