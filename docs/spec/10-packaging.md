# Packaging, Crate Layout, Admin CLI

Source: resolved at spec assembly
([ticket 17](../../.scratch/spec/issues/17-assemble-spec.md)).

## Workspace layout

```
Cargo.toml            # workspace
crates/
  garret-common/      # protocol types, narinfo/signing, NAR framing, config
  garret-server/      # shared server internals: DB, storage, metrics, auth
  garret-pusher/      # bin: push API + GC
  garret-puller/      # bin: substituter + browse
  garret-client/      # lib (push/watch logic) + bin `garret`
  garret-admin/       # bin: admin operations
  garret-bench/       # bin: load harness
docs/                 # this spec + ADRs
flake.nix
justfile
```

Separate binaries per service (not one multi-mode binary): systemd units,
resource accounting, and restarts stay independent.

## Admin CLI (`garret-admin`)

Key operations are offline file operations; everything touching the DB
goes through the Pusher's **admin API on an owner-only unix socket** (0600,
the Pusher's user: root and the Pusher can connect, the Puller cannot)
(single-writer discipline — garret-admin never opens the DB while the
Pusher runs).

| Command | Path |
|---|---|
| `key generate` | offline — writes nix-format keypair file |
| `key show` | offline — prints public key for nix.conf |
| `resign` | socket — backfill signatures after adding a key |
| `gc run` | socket — trigger a GC pass |
| `status` | socket — object count, usage vs quota, in-flight uploads |
| `fsck [--repair] [--verify-sizes] [--quiesce] [--json]` | socket — audit row⇔blob consistency, optionally repair |
| `pin <name> <hash> [--expires <duration>]` | socket — GC-exempt root, closure-protecting (spec 05) |
| `unpin <name>` | socket — remove a pin; unknown name is an error |
| `prune --before <YYYY-MM-DD\|age> [--apply]` | socket — delete closures last pushed before the cutoff, keeping what newer pushes and pins need (spec 05); dry-run unless `--apply` |

## NixOS modules & flake outputs

Flake outputs: `packages.{garret,garret-pusher,garret-puller,garret-admin,garret-bench}`,
`nixosModules.{pusher,puller,watcher}`, `devShells.default`, `checks`
(`build`; on Linux also `module`, a NixOS VM test that boots the pusher and
puller modules on one host and checks the service-user boundary below).

Module option sketch (all under `services.garret.*`):

- **pusher**: `enable`, `port`, `metricsPort`, `dbPath`, `s3.{endpointUrl,
  bucket, region, credentialsFile}`, `quota`, `watermarks.{high,low}`,
  `limits.{maxConcurrentUploads, maxInFlightBytes}`, `oidc.{pocketId.{issuer,
  audience}, github.{ownerId, refPatterns, refProtected}}`, `signingKeyFiles` (list —
  active + retiring), `adminSocketPath`, `gcInterval`.
- **puller**: `enable`, `port`, `metricsPort`, `dbPath`, `s3.*` (same),
  `presignTtl` (default 1 h), `browse.oidc.{issuer, audience}`,
  `bumpDebounce`, `dbReadBudgetMs` / `presignBudgetMs` (pull-path
  degrade-to-miss budgets, spec 03; default 250 ms).
- **watcher** (client machines): `enable`, `endpoint`,
  `credentialsFile` (client id/secret), `filters.{excludePatterns,
  upstreamKeys}`, `jobs`, `zstdLevel`, `fullSync`, `socketPath` (wake
  socket, spec 06). Enabling the watcher also sets
  `nix.settings.post-build-hook` to a wrapper running `garret enqueue`,
  so builds wake the watcher the moment they finish.

The pusher module also carries `pullerEndpoint` and a per-issuer `client_id`,
both advertised by `/api/v1/discovery` so `garret login <pusher-url>` can write
a whole client config (spec 06). Set `client_id` on the human issuer only.

Secrets (S3 credentials, signing keys, OIDC client secrets) are file
paths — agenix/sops-friendly, never in the nix store.

### Service users and file modes

The Pusher and the Puller run as different users
([ADR-0009](../adr/0009-puller-runs-as-its-own-user.md)), so a compromise
of the public-facing Puller reaches neither the signing keys, the admin
socket, nor bucket write:

| | Pusher | Puller |
|---|---|---|
| User / group | `garret` / `garret` | `garret-puller` / `garret` |
| Database directory, `garret:garret` 0750 | owner | enter only: cannot create or replace files |
| `garret.db`, `-wal`, `-shm`, 0660 | read/write | read/write: last-accessed bumps, and WAL readers write `-shm` |
| Signing keys, `garret`-only (0400) | read | none |
| Admin socket, `garret` 0600 | serves | cannot connect |
| S3 credentials | its key (bucket write) | its own key; GetObject suffices |

SQLite gives `-wal` and `-shm` the database file's mode but creates the
database itself 0644, so the pusher unit's `ExecStartPre` (as `garret`)
creates it 0660 before SQLite first opens it, and re-applies 0660 to all
three files on every start. The Puller cannot create the sidecars either,
so every connection runs in persistent-WAL mode (spec 02) and a last close
leaves them in place: a Pusher that exits with an error after opening the
database does not strand the Puller. The Puller can still write rows: its
bumps need the write lock, and SQLite has no finer permission.

Upgrading from modules without the split: with the default `dbPath`,
nothing to do — the first Pusher start makes the directory 0750 and the
database files 0660. A custom `dbPath` directory must already be
`garret:garret` 0750. A signing key readable by group
`garret` (say `root:garret` 0440) must become `garret`-owned 0400, or the
Puller can read it. Pointing `services.garret.puller.s3.credentialsFile` at
a GetObject-only key is optional, and takes bucket write away from the Puller.

## Shell completions

The workspace derivation's `postInstall` runs the freshly built `garret
completions <shell>` under `installShellFiles`, installing bash, zsh and fish
scripts under `$out/share`. It is guarded by
`stdenv.buildPlatform.canExecute stdenv.hostPlatform`: generating completions
means executing the binary just built, which a `pkgsCross` build cannot do, and
without the guard that fails with an exec-format error naming nothing useful.

The per-binary `only` wrappers symlink `share/` as well as `bin/`, or
`nix profile install .#garret` would install the binary and silently drop its
completions.
