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

## NixOS modules & flake outputs

Flake outputs: `packages.{garret,garret-pusher,garret-puller,garret-admin,garret-bench}`,
`nixosModules.{pusher,puller,watcher}`, `devShells.default`, `checks`
(unit + NixOS integration test that pushes and pulls a closure).

Module option sketch (all under `services.garret.*`):

- **pusher**: `enable`, `port`, `metricsPort`, `dbPath`, `s3.{endpointUrl,
  bucket, region, credentialsFile}`, `quota`, `watermarks.{high,low}`,
  `limits.{maxConcurrentUploads, maxInFlightBytes}`, `oidc.{pocketId.{issuer,
  audience}, github.{ownerId, refPatterns}}`, `signingKeyFiles` (list —
  active + retiring), `adminSocketPath`, `gcInterval`.
- **puller**: `enable`, `port`, `metricsPort`, `dbPath`, `s3.*` (same),
  `presignTtl` (default 1 h), `browse.oidc.{issuer, audience}`,
  `bumpDebounce`.
- **watcher** (client machines): `enable`, `endpoint`,
  `credentialsFile` (client id/secret), `filters.{excludePatterns,
  upstreamKeys}`, `jobs`, `zstdLevel`, `fullSync`.

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
