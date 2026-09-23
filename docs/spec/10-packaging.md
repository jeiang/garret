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
| `prune --before <YYYY-MM-DD\|age> [--apply]` | socket — delete closures last pushed before the cutoff, keeping what newer pushes and pins need (spec 05); dry-run unless `--apply` |

## NixOS modules & flake outputs

Flake outputs: `packages.{garret,garret-pusher,garret-puller,garret-admin,garret-bench}`,
`nixosModules.{pusher,puller,watcher}`, `devShells.default`, `checks`
(unit + NixOS integration test that pushes and pulls a closure).

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
