# Deployment

Garret runs as two systemd services on one NixOS host, plus an optional store
watcher on build machines. The NixOS modules generate the service config from
typed options; a hand-written config works too, and the binaries check the same
ranges at startup.

## Modules

| Module | Runs |
|---|---|
| `services.garret-pusher` | The Pusher, and puts `garret-admin` on the system `PATH` |
| `services.garret-puller` | The Puller; requires the Pusher's module |
| `services.garret-watcher` | The store watcher, and wires `post-build-hook` to it |

The Puller has no database path, store directory or bucket options of its own:
it serves the Pusher's database and bucket on the same host, so it takes them
from `services.garret.pusher`. Enabling the Puller without the Pusher fails
evaluation.

## Pusher options

| Option | Type | Default |
|---|---|---|
| `enable` | bool | `false` |
| `package` | package | `garret-pusher` |
| `listen` | string or null | `null`: loopback |
| `metricsListen` | string or null | `null`: loopback |
| `dbPath` | string | `/var/lib/garret/garret.db` |
| `storeDir` | string or null | `null` |
| `s3.bucket` | string | required |
| `s3.endpointUrl` | string or null | `null` |
| `s3.region` | string or null | `null` |
| `s3.pathStyle` | bool or null | `null`: on |
| `s3.operationTimeoutSeconds` | positive int or null | `null` |
| `s3.operationAttemptTimeoutSeconds` | positive int or null | `null`: unset |
| `s3.credentialsFile` | path | required |
| `signingKeyFiles` | list of strings | required |
| `pullerEndpoint` | string or null | `null` |
| `quotaBytes` | positive int or null | `null`: no quota |
| `watermarks.high` | number or null | `null` |
| `watermarks.low` | number or null | `null` |
| `gcIntervalSeconds` | positive int or null | `null` |
| `orphanGraceSeconds` | int or null | `null` |
| `limits.maxConcurrentUploads` | positive int or null | `null` |
| `limits.maxInFlightBytes` | positive int or null | `null` |
| `limits.partSize` | positive int or null | `null` |
| `limits.maxPartsInFlight` | positive int or null | `null` |
| `limits.bodyIdleTimeoutSeconds` | positive int or null | `null` |
| `oidc` | list of issuer submodules | required |
| `adminSocketPath` | string | `/run/garret/admin.sock` |
| `fsck.calendar` | string or null | `null`: no scheduled fsck |
| `fsck.extraArgs` | list of strings | `[]` |
| `telemetry.enable` | bool | `false` |
| `telemetry.endpoint` | string or null | `null`: `http://127.0.0.1:4318` |
| `telemetry.serviceName` | string or null | `null`: `garret-pusher` |
| `telemetry.sampleRatio` | number or null | `null`: `1.0` |
| `extraSettings` | TOML | `{}` |

An option marked `null` is left out of the generated config, so the binary
applies its own default.

`pullerEndpoint` is the public URL of the Puller. The Pusher advertises it, with
the signing keys and the OIDC settings, through `/api/v1/discovery`, which is
how `garret login <pusher-url>` writes a whole client config.

`signingKeyFiles` takes several keys during a rotation: every object is signed
with all of them, and `garret-admin resign` backfills the ones added later.

### Issuer submodules

Each entry in `oidc` takes `issuer`, `audience`, `extra_audiences`, `client_id`,
`jwks_url`, `github_owner_id`, `ref_patterns`, `ref_protected`,
`repository_ids`, `event_names`, `job_workflow_refs` and `allowed_groups`.

## Puller options

| Option | Type | Default |
|---|---|---|
| `enable` | bool | `false` |
| `package` | package | `garret-puller` |
| `listen` | string or null | `null`: loopback; front it with TLS |
| `metricsListen` | string or null | `null`: loopback |
| `presignTtlSeconds` | int or null | `null` |
| `bumpDebounceSeconds` | int or null | `null` |
| `dbWaitTimeoutSeconds` | positive int or null | `null` |
| `dbReadBudgetMs` | positive int or null | `null` |
| `presignBudgetMs` | positive int or null | `null` |
| `s3.credentialsFile` | path | required |
| `browseOidc` | submodule or null | `null`: no browse API |
| `telemetry.enable` | bool | `false` |
| `telemetry.endpoint` | string or null | `null`: `http://127.0.0.1:4318` |
| `telemetry.serviceName` | string or null | `null`: `garret-puller` |
| `telemetry.sampleRatio` | number or null | `null`: `1.0` |
| `extraSettings` | TOML | `{}` |

`browseOidc` takes `issuer`, `audience`, `jwks_url` and `allowed_groups`. When
it is unset the browse API is not served at all: every browse path returns 404.
Any configured issuer is accepted there; it need not be Pocket ID.

Give the Puller its own S3 key limited to `GetObject`. A presigned URL carries
exactly the authority of the key that signed it, so the Pusher's key would hand
write access to the public-facing process.

`telemetry` is the same on both services and off by default:

```nix
services.garret.pusher.telemetry = {
  enable = true;
  endpoint = "http://127.0.0.1:4318";
  sampleRatio = 0.1;
};

services.garret.puller.telemetry = {
  enable = true;
  endpoint = "http://127.0.0.1:4318";
  sampleRatio = 0.1;
};
```

Spans go to `<endpoint>/v1/traces` over OTLP/HTTP. The service reaches the
collector over the network, which the unit sandbox already allows. See
[Operations](operations.md#tracing) for what is traced and what is not.

## Watcher options

| Option | Type | Default |
|---|---|---|
| `enable` | bool | `false` |
| `package` | package | `garret` |
| `endpoint` | string | required |
| `oidc.issuer` | string | required |
| `oidc.clientId` | string | required |
| `oidc.clientSecretFile` | path | required |
| `oidc.audience` | string | required |
| `oidc.resource` | string or null | `null`: not sent |
| `nixDb` | string or null | `null`: the system database |
| `cursorPath` | string | `/var/lib/garret/watcher-cursor` |
| `pollIntervalSeconds` | positive int or null | `null` |
| `jobs` | positive int or null | `null` |
| `zstdLevel` | int or null | `null` |
| `maxRetries` | int or null | `null` |
| `maxAttempts` | positive int or null | `null` |
| `socketPath` | string | `/run/garret/watch.sock` |
| `fullSync` | bool | `false` |
| `filters.upstreamKeys` | list or null | `null`: the client-wide list applies |
| `filters.excludePatterns` | list of strings | `[]` |
| `extraSettings` | TOML | `{}` |

Enabling the watcher also sets `nix.settings.post-build-hook` to a wrapper that
runs `garret enqueue`, so a build wakes the watcher the moment it finishes.

## Authentication

The Pusher validates bearer tokens from the issuers you configure, directly
against each issuer's published keys. There is no token-exchange service and no
garret-issued token, and no way to disable authentication.

- **Pocket ID** for interactive use. Register a garret API and audience there,
  and restrict the client to the right user group in Pocket ID. An optional
  `allowed_groups` list adds a second check.
- **GitHub Actions** for CI. Match the immutable `repository_owner_id` claim,
  never a renameable name. Owner-wide matching alone lets every workflow in
  every repository of that owner mint a push token, so narrow it with
  `repository_ids`, `ref_patterns`, `ref_protected`, `event_names` and
  `job_workflow_refs`. Each is off when unset; once set, a token missing the
  claim is refused. The Pusher refuses to start when a GitHub issuer sets
  neither `github_owner_id` nor `repository_ids`.

A token that fails verification gets 401, and the client may renew or retry. A
token that verifies but that a policy gate refuses gets 403, which is permanent
until the policy changes; clients never renew on it.

Refresh-token lifetime is entirely the issuer's business; garret neither sets
nor checks it.

## Service users and file modes

The Pusher and the Puller run as different users, so a compromise of the
public-facing Puller reaches neither the signing keys, the admin socket, nor
bucket write.

| | Pusher | Puller |
|---|---|---|
| User / group | `garret` / `garret` | `garret-puller` / `garret` |
| Database directory, `garret:garret` 0750 | owner | enter only |
| `garret.db`, `-wal`, `-shm`, 0660 | read/write | read/write |
| Signing keys | read | none |
| Admin socket, `garret` 0600 | serves | cannot connect |
| S3 credentials | bucket write | `GetObject` is enough |

The Puller is read-only except for debounced last-accessed bumps, which is why
it needs write access to the database files.

Signing keys must not be readable by group or others. The loader accepts any
mode without group or other bits — 0400 and 0600 both load — and refuses
anything else, naming the file and its mode.

The database directory and files are created with the modes above by the
Pusher's start step. A database put in place by hand must be owned by `garret`.

## Process behaviour

Both server units are `Type=notify`: the binaries send `READY=1` once every
listener is bound, so units ordered after them start only when the sockets
accept connections.

On `SIGTERM` or `SIGINT` both services stop accepting, let in-flight requests
finish, and only then exit — the Pusher after up to 60 seconds of uploads, the
Puller as soon as its open connections are idle. With tracing enabled, the
last batch of spans is flushed on the way out. The units set no `KillMode` or
`TimeoutStopSec`, so systemd's defaults apply: `SIGTERM` first, then `SIGKILL`
after 90 seconds, which is longer than either drain.

All three units share one sandbox: an empty capability bounding set,
`NoNewPrivileges`, `ProtectSystem=strict` with only the unit's own state
writable, `ProtectHome`, private `/tmp` and `/dev`, `MemoryDenyWriteExecute`,
and a system-call filter. The watcher runs as root with no capabilities,
because everything it touches is root-owned or world-accessible.

Size the Pusher's `MemoryMax` at no less than `2 × maxInFlightBytes +
maxConcurrentUploads × 8 MiB + ~1 MiB × maxConcurrentUploads`, plus headroom for
the process. The defaults need about 700 MiB.

The Pusher unit sets `MALLOC_ARENA_MAX=2`. Without it, glibc keeps freed upload
buffers in per-thread arenas, and peak RSS on an 8-core host was about 1.5×
higher at the same throughput. The Puller unit leaves the glibc default: the
same setting lowered its request rate by about 7%.

## Scheduled fsck

`fsck.calendar` takes a systemd `OnCalendar` expression and adds
`garret-fsck.service` and `garret-fsck.timer`. It requires `quotaBytes`, because
fsck reads the orphan grace from the `[gc]` section.

The unit runs `garret-admin fsck` with `fsck.extraArgs` appended verbatim. The
default is a read-only dry run, but the list is unconstrained:
`extraArgs = ["--repair"]` makes the scheduled unit delete rows, and
`["--repair", "--quiesce"]` also rejects pushes while it runs. Set them
deliberately.

The unit exits 1, and so shows as failed, when a run finds dangling rows or size
mismatches. Orphan blobs alone do not fail it.

## Validation

The module types reject most of what the binaries reject, at evaluation time:
positive integers for the quota, GC interval, S3 timeouts and limits, watermarks
between 0 and 1, and so on. They are not identical. Watermarks accept `0.0` and
`low == high`, and `operationAttemptTimeoutSeconds` may be greater than or equal
to `operationTimeoutSeconds`; the Pusher refuses to start in both cases. A
hand-written config gets the same startup checks.

## Raw settings

`extraSettings` is raw TOML merged recursively over the generated config, for
keys without a typed option. For example, extra audiences and a client id on the
browse API:

```nix
services.garret.puller.extraSettings.browse_oidc = {
  extra_audiences = [ "garret-browse" ];
  client_id = "garret-browse";
};
```

Secrets — S3 credentials, signing keys, OIDC client secrets — are file paths,
never values in the nix store.
