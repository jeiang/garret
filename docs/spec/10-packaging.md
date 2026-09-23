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
| `backup <path>` | socket — online copy of the DB, mode 0600, never overwrites |

### Backup and restore

`garret-admin backup <path>` has the Pusher run `VACUUM INTO` on a
read-only connection of its own: one read transaction, so the copy is a
consistent snapshot (un-checkpointed WAL included) taken while both
services keep serving. The Pusher creates the file itself with
`O_CREAT|O_EXCL` and mode 0600 — it refuses any existing path, and the
copy is never readable by others, even briefly — then fsyncs it before
replying. The path is resolved by `garret-admin` and written by the Pusher,
so it must be writable by the Pusher and visible outside it: under the NixOS
module, inside the directory holding `dbPath` (the unit's `/tmp` is
private). Rotation is the caller's job: remove or rename the previous copy
first.

Restoring a copy that is older than the bucket:

1. Stop both services. Put the copy in place of `dbPath`, owned by the
   Pusher's user (`garret` under the NixOS module, whose Pusher start makes
   it group-writable for the Puller), and delete any `-wal`/`-shm` files
   beside it.
2. Start **only the Pusher**, with its push endpoint unreachable to
   clients (reverse-proxy route or firewall closed).
3. `garret-admin fsck --repair --verify-sizes --quiesce` deletes the rows
   whose blobs were evicted after the backup, and rows whose blob a later
   delete and re-push replaced with one of a different size.
4. Start the Puller.
5. Once every restored row is older than `orphan_grace_secs` (fsck skips
   younger rows), run step 3 again, then reopen the push endpoint.
6. Re-apply any pins set or removed since the backup.

Closures stay complete because GC and `prune` delete an object only once
nothing surviving references it, roots first (spec 05). Anything deleted
after the backup was therefore unreferenced by every object still present,
so dropping its row leaves no surviving object with a missing reference. The
exceptions are the ones that already break closures on the live cache:
`garret-admin delete`, which is unconditional by design, and an eviction
whose blob delete failed, whose orphaned blob makes the restored row look
intact. Blobs uploaded after the backup have no row; clients re-push them
through normal Negotiation, and the orphan sweep removes the rest. That
costs cache misses, never correctness.

The ordering protects that argument. Negotiation answers from rows alone,
so while any restored row is unchecked it can tell a client that a path
whose blob is gone is present, and the client then pushes a referrer
without it; hence pushes stay closed until fsck has covered every row. The
Puller only waits for the first fsck, which clears most of the rows that
would redirect to missing blobs.

## NixOS modules & flake outputs

Flake outputs: `packages.{garret,garret-pusher,garret-puller,garret-admin,garret-bench}`,
`nixosModules.{pusher,puller,watcher}`, `devShells.default`, `checks`
(`build`; on Linux also `module`, a NixOS VM test that boots the pusher and
puller modules on one host and checks the service-user boundary below).

Module option sketch (all under `services.garret.*`):

- **pusher**: `enable`, `port`, `metricsPort`, `dbPath`, `s3.{endpointUrl,
  bucket, region, credentialsFile}`, `quota`, `watermarks.{high,low}`,
  `limits.{maxConcurrentUploads, maxInFlightBytes}`, `oidc.{pocketId.{issuer,
  audience}, github.{ownerId, refPatterns, refProtected, repositoryIds,
  eventNames, jobWorkflowRefs}}`, `signingKeyFiles` (list — active +
  retiring), `adminSocketPath`, `gcInterval`.
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
bumps need the write lock, and SQLite has no finer permission. A database
put in place by hand must be owned by `garret`: the start step runs as
`garret` and fails on a file it cannot chmod.

Upgrading from modules without the split: with the default `dbPath`,
nothing to do — the first Pusher start makes the directory 0750 and the
database files 0660. A custom `dbPath` directory must already be
`garret:garret` 0750. A signing key readable by group
`garret` (say `root:garret` 0440) must become `garret`-owned 0400, or the
Puller can read it. Pointing `services.garret.puller.s3.credentialsFile` at
a GetObject-only key is optional, and takes bucket write away from the Puller.

## Prebuilt outputs

CI builds `garret-all` and the `garret`, `garret-pusher`, `garret-puller`
and `garret-admin` wrappers on x86_64-linux and aarch64-darwin, and for every
push to `main` or a `v*` tag pushes their closures (plus the devshell) to
garret's own deployment. The wrappers are pushed as well as the workspace:
each is its own store path, and a consumer installing
`#garret` or a NixOS module running the default `garret-pusher` would
otherwise still build it. Consumers pin arbitrary `main` revisions, so `main`
and tag runs are never cancelled by a later push; only pull request runs are.

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
