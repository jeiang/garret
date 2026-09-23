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
goes through the Pusher's **admin API on a root-only unix socket**
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
| `backup <path>` | socket — online copy of the DB, mode 0600, never overwrites |

### Backup and restore

`garret-admin backup <path>` has the Pusher run `VACUUM INTO` on a
read-only connection of its own: one read transaction, so the copy is a
consistent snapshot (un-checkpointed WAL included) taken while both
services keep serving. The Pusher creates the file itself with
`O_CREAT|O_EXCL` and mode 0600 — it refuses any existing path, and the
copy is never readable by others, even briefly — then fsyncs it before
replying. The path is resolved by `garret-admin` and written by the Pusher,
so it must be writable by the Pusher (under the NixOS module, inside the
directory holding `dbPath`). Rotation is the caller's job: remove or rename
the previous copy first.

Restoring a copy that is older than the bucket:

1. Stop both services and put the copy in place of `dbPath` (removing any
   `-wal`/`-shm` files beside the old database).
2. Start **only the Pusher**.
3. `garret-admin fsck --repair --quiesce` deletes the rows whose blobs were
   evicted after the backup was taken.
4. Start the Puller.

Closures stay complete because GC evicts an object only once nothing
surviving references it, roots first (spec 05). Anything evicted after the
backup was therefore unreferenced by every object still present, so
dropping its row leaves no surviving object with a missing reference. The
exceptions are the ones that already break closures on the live cache:
`garret-admin delete`, which is unconditional by design, and an eviction
whose blob delete failed, whose orphaned blob makes the restored row look
intact. Blobs uploaded after the backup have no row; clients re-push them
through normal Negotiation, and the orphan sweep removes the rest. That
costs cache misses, never correctness. fsck skips rows younger than
`orphan_grace_secs`, so a backup taken within that window of the restore
needs a second fsck once it has aged past it.

The Puller stays down until fsck has run because until then it would
redirect to blobs that no longer exist.

## NixOS modules & flake outputs

Flake outputs: `packages.{garret,garret-pusher,garret-puller,garret-admin,garret-bench}`,
`nixosModules.{pusher,puller,watcher}`, `devShells.default`, `checks`
(unit + NixOS integration test that pushes and pulls a closure).

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
