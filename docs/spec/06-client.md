# Client CLI & Store Watcher

Sources: [ticket 13](../../.scratch/spec/issues/13-client-cli.md),
[ticket 18](../../.scratch/spec/issues/18-client-ux.md),
[ticket 12 research](../../.scratch/spec/research/store-watcher-mechanisms.md).

## Commands

| Command | Behavior |
|---|---|
| `garret login [pusher-url] [--force]` | Writes the config from server discovery, then device flow against Pocket ID; stores rotating refresh token |
| `garret logout` | Deletes the stored refresh token; the config stays |
| `garret whoami` | Subject, audience, expiry, and a live probe that the Pusher accepts the token |
| `garret doctor [path]` | Layer-by-layer diagnosis (below); with a path, a Negotiation round answers "is this cached" |
| `garret use [--print]` | Adds the Puller to the user's `nix.conf` as a substituter |
| `garret push <paths…\|installable> [--dry-run] [--no-upstream-filter]` | Full closure via one missing-paths query, minus upstream-signed paths; parallel workers |
| `garret watch-store` | Watcher daemon (below) |
| `garret enqueue [paths…] [--socket]` | Wake a running `watch-store` to poll now; paths default to `$OUT_PATHS`, for use as nix's `post-build-hook`. Exits 0 unconditionally |
| `garret list` | Search/filter cache contents (browse API) |
| `garret tree <path>` | Dependency tree (browse API) |
| `garret pins` | List GC-exempt pins (browse API; set with `garret-admin pin`) |
| `garret completions <shell>` | bash/zsh/fish completion script |

Admin operations live in `garret-admin`
(see [10-packaging.md](10-packaging.md)).

`login`, `logout`, `completions` and `enqueue` run without a config — `login`
is the only way to create one, so requiring one would be circular, and
`enqueue` runs inside the post-build-hook as whatever user nix chooses, so it
depends only on its socket-path default. Every other command reports the
missing config by naming `garret login <pusher-url>`.

## Config bootstrap

`garret login <pusher-url>` fetches `GET /api/v1/discovery` from the Pusher
(anonymous; [ADR-0006](../adr/0006-server-served-client-discovery.md)) and
writes `endpoint`, `puller_endpoint`, `public_keys` and the whole `[oidc]`
section, creating `~/.config/garret/` if absent. The URL argument is the
Pusher's, which is also what lands in `endpoint`, so the one thing the human
must know is the one thing they would have had to configure anyway.

- No config and no URL is an error naming the fix.
- A URL with no config bootstraps.
- No URL with a config re-authenticates and leaves the config alone.
- A URL *and* a config refuses without `--force`, printing both the current
  contents and what would replace them. `--force` overwrites wholesale rather
  than merging: the only fields a human hand-edits interactively are `jobs` and
  `zstd_level`, and `[watch]` lives on daemon hosts whose config the NixOS
  module writes at an explicit `--config` path.

The written file is a hand-rendered subset with comments, not a serialization
of the config struct — which would emit every default and the entire `[watch]`
section into a laptop's config.

## Doctor

Sources: [ticket 24](../../.scratch/spec/issues/24-doctor.md). Prior art:
`cachix doctor`, and sccache's startup check that names the failing backend
instead of 500ing later.

When a push or substitution misbehaves, every diagnosis step is one curl —
but only if you remember them all. `garret doctor [path]` runs them in layer
order, each printing one `pass`/`fail` line naming the layer, and exits
non-zero if any check failed:

| Check | What it probes |
|---|---|
| `discovery` | `GET /api/v1/discovery` on the Pusher — the server is reachable at all |
| `config` | The local config against the discovery document — drift in `puller_endpoint` or `[oidc]` since `garret login` wrote it. Fields the server does not advertise are sparse, not drifted |
| `keys` | Configured `public_keys` against discovery's — a configured key the server no longer signs with fails; *extra* server keys pass with a re-login nudge (rotation in progress) |
| `auth` | Token acquisition plus the empty-Negotiation liveness probe `whoami` uses — the token is not merely present but accepted |
| `pull` | The Puller's `/nix-cache-info`, then one narinfo probe. Hit and miss both pass: the question is whether the substituter protocol is served, not whether a given path is cached |
| `path` | Only with a path argument: one Negotiation round for its hash. Not cached is a `fail`, so the exit code carries the answer for scripts |

A check whose prerequisite failed reports `skip` rather than guessing:
no discovery document leaves `config` and `keys` nothing to compare
against, and without an accepted token `path` cannot be asked. Every probe
runs under a hard timeout — a diagnostic that hangs is a diagnostic that
failed.

Under `--json`, `doctor` emits one object (the checks are the data; the
failure summary goes to stderr):

```json
{"ok":false,"checks":[{"name":"discovery","status":"pass","detail":"…"},
                      {"name":"path","status":"fail","detail":"not cached — …"}]}
```

## Push behavior

Per the protocol: worker pool of concurrent PUTs, client-side zstd
(default level 3), jittered backoff on 429/5xx, idempotent retries.
No client metrics endpoint in v1.

**Upstream filter** (ticket 21; prior art: attic's
`--upstream-cache-key-name`, cachix's configurable upstreams). During closure
assembly, paths whose `nix path-info` signatures carry a configured upstream
key name are dropped before the Negotiation: an upstream cache already serves
them signed, so pushing them would spend bandwidth and Quota on bytes Eviction
would reclaim but the cache never needed to hold. The filter is a pure
signature-name comparison — no network probes, no server change; the
Negotiation batch just shrinks. Key names come from the top-level
`upstream_keys` config (default `["cache.nixos.org-1"]`); `--no-upstream-filter`
bypasses the filter for one push. Filtered paths are reported with status
`upstream` — distinct from `deduped`, whose bandwidth was actually spent — and
the `negotiated` counts stay honest: `closure` minus `upstream` is what was
negotiated.

Progress is a single bar over **uncompressed NAR bytes** — that total is known
exactly from the closure before a byte moves, while compressed bytes-on-wire
have no total until the upload ends, and a bar over path count lurches because
a closure holds 1 KB man-pages beside 400 MB toolchains. The reported rate is
therefore NAR bytes/s, not wire bytes/s. The bar draws to stderr and hides
itself when stderr is not a terminal, so CI needs no flag and takes the
per-path lines as its progress indication. There is no `--no-progress`.

A path reported `deduped` was uploaded in full and *then* found redundant
server-side; the bandwidth was spent either way. A failure does not abort the
run — every path is attempted and reported, the summary is emitted, and only
then does the process exit non-zero.

`--dry-run` negotiates, reports what would be uploaded, and stops.

## Machine-readable output

`--json` is global. It sends everything human to stderr, so stdout carries only
data.

- `list` and `tree` emit the browse API's JSON verbatim. The server already has
  a schema (spec 07); mirroring it client-side would create a second one.
- `whoami` and `doctor` each emit one object.
- `push` emits NDJSON, one event per line:

```
{"event":"negotiated","closure":312,"upstream":118,"missing":47,"nar_bytes":4021374976}
{"event":"path","path":"/nix/store/…","status":"upstream","nar_size":123456}
{"event":"path","path":"/nix/store/…","status":"pushed","nar_size":81920}
{"event":"path","path":"/nix/store/…","status":"deduped","nar_size":4096}
{"event":"path","path":"/nix/store/…","status":"failed","error":"upload rejected with 503: …"}
{"event":"done","pushed":40,"deduped":6,"failed":1,"nar_bytes":4021374976}
```

`negotiated` gives a consumer the denominator up front and `done` makes a
truncated stream detectable. `upstream`-filtered paths are reported first, then
uploads as they finish. `--dry-run --json` reuses the identical schema, with
`status:"would-push"` for what would upload (upstream paths keep their real
`upstream` status — the filter runs identically either way) and a zeroed
`done`, so a consumer needs one parser. The human summary line mentions the
upstream count only when it is non-zero.

## Pointing nix at the cache

`garret use` appends `extra-substituters` and `extra-trusted-public-keys` to
the user's `~/.config/nix/nix.conf` — never `/etc/nix/nix.conf`, never via
sudo. Idempotence is a substring match on the URL, not a nix.conf parse.

It then runs `nix config show substituters` and checks the URL comes back.
Substituters in a *user's* nix.conf are silently ignored unless that user is in
`trusted-users`; that one subprocess turns the classic silent failure into a
message carrying the `nix.settings` snippet to use instead. `--print` emits
both forms and writes nothing, for the declarative case.

`public_keys` is plural because the Pusher signs every object with every
configured key, so a rotation has several live at once.

## Store watcher

- **Source of truth**: a persisted cursor over `ValidPaths.id` in
  `/nix/var/nix/db/db.sqlite` (AUTOINCREMENT — monotonic, never reused;
  schema checked at startup). Complete by construction: catch-up after
  downtime, initial scan, and offline backlog are all "the cursor is
  old".
- **Wake socket** ([ADR-0008](../adr/0008-wake-socket-not-daemon-push.md)):
  a unix *datagram* socket at `[watch] socket_path` (default
  `/run/garret/watch.sock`). Any datagram means "poll the cursor now";
  bursts coalesce into one early poll. `garret enqueue`, registered as
  nix's `post-build-hook` (which passes paths via `$OUT_PATHS`, not
  argv), is the sender — so pushes start the moment a build finishes
  instead of up to `poll_interval_secs` later. The socket carries no
  authority (the cursor decides what pushes), so it is mode `0666` and
  a bind failure only warns: the watcher keeps polling, and `enqueue`
  exits 0 even when nothing listens — a hook must never block or fail
  a build. The cursor is the durability story; the socket is latency
  only.
- **Privilege**: root systemd service managed by the NixOS module. No
  unprivileged mode in v1.
- **Scope**: pushes bare paths as they become valid — dependencies
  register before roots, so closures self-assemble. The server never
  requires closed closures.
- **Filtering**: skip `.drv` paths and paths already signed by configured
  upstream keys — the same key list as push's upstream filter (top-level
  `upstream_keys`, default `cache.nixos.org-1`; `[watch] upstream_keys`
  overrides it per daemon), read from the DB's sigs rather than
  `nix path-info`. Keep `-source` and fixed-output paths. Configurable
  exclude patterns.
- **Failure handling**: capped retries with backoff (default 5), then
  local skip-list + loud log + metrics-visible counter; the cursor always
  advances — one poison path never wedges the pipeline. Paths nix-GC'd
  before push are skipped silently.
- **Bootstrap**: cursor starts at `MAX(id)` (only new paths push);
  `--full-sync` opts into walking history from id 0.

### Hook setup

`post-build-hook` takes a bare program, no arguments, so it cannot name
`garret enqueue` directly. The NixOS module wires a wrapper automatically
whenever the watcher is enabled. Elsewhere, write one by hand:

```sh
#!/bin/sh
exec garret enqueue
```

and set `post-build-hook = /path/to/that-script` in `nix.conf`.
