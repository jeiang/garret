# 18 — Client UX overhaul

Make `garret` self-configuring and legible: server-served discovery, a real
progress bar, `use`/`whoami`/`logout`/`completions`, `--json`/`--dry-run`,
and Nix-installed shell completions.

Decided in interview; every numbered decision below is settled, not open.

## Motivation

Today's client is correct but unfriendly. A fresh machine must hand-write
`~/.config/garret/config.toml` before *any* command runs — including
`garret login`, which loads config before it dispatches
([main.rs:52](../../../crates/garret-client/src/main.rs:52)) and so can never
bootstrap itself. Pushing a 4 GiB closure prints nothing until each path
finishes. Pointing Nix at the cache means hand-editing `nix.conf` with a
public key that is **not served on any route**. There are no completions.

Comparable tools (attic, cachix) solve all of this the same way: the server
tells the client its own configuration.

---

## 1. Discovery endpoint (server)

`GET /api/v1/discovery` on the **Pusher**, unauthenticated.

The Pusher, not the Puller, because it already holds everything discovery
returns: the signing keys ([pusher/main.rs:80](../../../crates/garret-pusher/src/main.rs:80))
and `oidc: Vec<IssuerConfig>` with issuer + audience. The Puller has neither
a signing key nor an audience, and hosting it there would mean duplicating
both into `PullerConfig` where they could drift from what the Pusher
actually validates against.

`axum`'s `Router::layer` wraps only routes registered before it, so the
route is public by placement — one line, no auth-bypass logic:

```rust
.route("/api/v1/missing-paths", post(missing_paths))
.route("/api/v1/nar/{hash}", put(upload))
.layer(middleware::from_fn_with_state(state.clone(), require_oidc))
.route("/api/v1/discovery", get(discovery))   // ← unauthenticated
```

Response:

```json
{
  "pusher_endpoint": "https://push.cache.example",
  "puller_endpoint": "https://cache.example",
  "public_key": "garret-1:abc…",
  "oidc": { "issuer": "https://id.example", "audience": "garret", "client_id": "garret" }
}
```

Everything here is public by construction: the signing key's public half, and
OIDC client metadata that ships in every device-flow request anyway.

### Config additions

`PusherConfig` gains one field; `IssuerConfig` gains one:

- `puller_endpoint: Option<String>` — top-level. Absent ⇒ omitted from
  discovery, and `use`/`list`/`tree` keep their existing "set
  `puller_endpoint`" error.
- `client_id: Option<String>` — on `IssuerConfig`, **not** top-level.
  Discovery advertises the **first issuer that has one**. Self-selecting: only
  the human device-flow issuer (Pocket ID) gets a `client_id`; the GitHub
  Actions issuer never would. Avoids a `[discovery]` section restating issuer
  and audience that could drift from the validated values.

`nix/pusher.nix`: `pullerEndpoint` option, and `client_id` inside the
existing `oidc` submodule. `stripNulls` already handles the null case.

**Consequence to accept:** client rollout now depends on a server deploy.
A new client against an old server gets a 404 from discovery and must be
told so plainly.

---

## 2. `garret login [URL]`

`URL` is the **Pusher** URL — the same value that lands in config as
`endpoint`, so the argument is the thing that had to be configured anyway.

| State | Behavior |
|---|---|
| No config, no URL | Error: pass a URL |
| No config, URL | Discover → device flow → write config + token |
| Config exists, no URL | Pure re-auth; config untouched |
| Config exists, URL | **Refuse** unless `--force`, printing the diff between current and discovered values |

`--force` clobbers wholesale rather than merging. The only fields a human
hand-edits interactively are `jobs` and `zstd_level`; losing those on an
explicit `--force` beats a `toml_edit` round-trip. `watch.*` lives on daemon
boxes whose config the NixOS module writes at an explicit `--config` path —
`login` never goes near it.

Config is written **after** the device flow succeeds, so a typo'd URL leaves
no broken config behind. Both config and token writes `create_dir_all` the
parent (`save_token` already does, [auth.rs:29](../../../crates/garret-client/src/auth.rs:29)).

### Writing the config

Hand-written `format!` template, **not** `#[derive(Serialize)]`. Serialize
would emit every default and the entire `[watch]` section — daemon settings —
into a laptop's config, with no comments.

```toml
# Written by `garret login`. Re-run with --force to regenerate.
endpoint = "https://push.cache.example"
puller_endpoint = "https://cache.example"
public_key = "garret-1:abc…"

[oidc]
issuer = "https://id.example"
client_id = "garret"
audience = "garret"

# Optional: jobs = 8, zstd_level = 3, max_retries = 5
```

Drift between template and struct is guarded by a unit test that round-trips
the rendered template through `toml::from_str::<Config>()` — which also
catches `deny_unknown_fields` violations. Rendering is a pure
`fn render_config(&Discovery) -> String` so the test needs no I/O.

**`login` is now the only way to create a config**, so every other command's
missing-config error becomes "run `garret login <url>`".

---

## 3. Progress bar

One overall bar over **uncompressed NAR bytes**. Total is
`missing.iter().map(|p| p.nar_size).sum()` — known exactly and for free before
a byte moves. The reader is wrapped *before* the `ZstdEncoder`
([push.rs:170](../../../crates/garret-client/src/push.rs:170)) so ticks are in
units the total is denominated in. Compressed bytes-on-wire are unknowable in
advance, and a path-count bar lurches because a closure is 1 KB man-pages next
to 400 MB toolchains.

Throughput is therefore **NAR bytes/s, not wire bytes/s**, and is labelled
as such.

- **Bar → stderr.** Per-path lines and JSON → stdout. So
  `garret push --json > events.ndjson` shows a live bar while writing clean
  JSON.
- **Non-TTY**: `ProgressDrawTarget::stderr()` auto-hides. In CI the per-path
  lines *are* the progress indication. No heartbeat — a second output path for
  a problem that doesn't exist.
- **Per-path lines print in both modes**, scrolling above the bar in a TTY.
- **`watch-store` gets no bar** by inheritance: the journal isn't a TTY, so
  indicatif hides itself with zero code.
- **No `--no-progress` flag.** TTY detection covers CI, `--json` covers
  scripting, `2>/dev/null` covers the rest.

**Rename `skipped` → `deduped`.** The current word is misleading: an
`exists`/`in-progress` ack means the path was fully uploaded and *then*
deduped server-side — the bandwidth was spent. Breaks `scripts/e2e.sh:230`,
which greps for it; fixing that grep is required, not optional.

### tracing collision

`tracing_subscriber::fmt::init()` writes to stderr and would shred the bar.
Use `tracing-indicatif`'s `IndicatifLayer` on the subscriber (~4 lines of
wiring) rather than hand-rolling an `io::Write` around `pb.suspend`.

---

## 4. `garret use`

Writes `~/.config/nix/nix.conf` only — never `/etc/nix/nix.conf`, never with
sudo. Appends `extra-substituters` (from **`puller_endpoint`**, never
`endpoint`) and `extra-trusted-public-keys` (from `public_key`). Idempotent by
substring match on the URL, not by parsing nix.conf.

**Then verifies**: runs `nix config show substituters` and checks the URL
actually appears. Substituters in a *user's* nix.conf are **silently ignored**
unless that user is in `trusted-users` — the single most confusing failure in
this space, and what `cachix use`'s whole `--mode` flag matrix exists to
manage. One subprocess call replaces all of it: if the URL is absent, print
the exact `nix.settings` snippet to fix it.

`--print` emits the nix.conf lines and a NixOS `nix.settings` snippet without
writing, for the declarative case where touching a user file is wrong.

A config written before `public_key` existed errors with "re-run
`garret login --force <url>`" — the only upgrade path.

---

## 5. `whoami` / `logout`

**`whoami`**: refresh the token, `POST /api/v1/missing-paths` with `[]` — a
valid, near-free, authenticated no-op that exercises the exact auth path
`push` uses — then decode the JWT payload (base64url the middle segment; **no
signature verification**, the client has no business verifying) for `sub`,
`aud`, `exp`. Prints endpoint, puller, issuer, audience, subject, expiry.

`base64 = "0.22"` is already in `Cargo.lock` and compiled for
`garret-admin`/`garret-server`, so this costs ~15 lines and no build time.

Four failure modes, four distinct messages: no config / no token / refresh
rejected / server rejected the token.

**`logout`**: delete `token.json`, leave `config.toml`, idempotent. No
`--all` — `rm` exists.

---

## 6. `--json` and `--dry-run`

`--json` is `#[arg(long, global = true)]`, matching the existing `--config`.
It implies **no bar and no per-path lines**; everything human goes to stderr.

| Command | Output |
|---|---|
| `list` / `tree` | **Server JSON passed through verbatim.** Reshaping client-side creates a second schema to keep in sync; pass-through is ~4 lines total. |
| `whoami` | `{endpoint, puller_endpoint, subject, audience, expires_at, issuer}` |
| `push` | NDJSON event stream (below) |
| `login` / `use` | Not supported — nothing to script |

### Push NDJSON

Type-tagged, one object per line on stdout:

```
{"event":"negotiated","closure":312,"missing":47,"nar_bytes":4021374976}
{"event":"path","path":"/nix/store/…","status":"pushed","nar_size":81920}
{"event":"path","path":"/nix/store/…","status":"deduped","nar_size":4096}
{"event":"path","path":"/nix/store/…","status":"failed","error":"upload rejected with 503: …"}
{"event":"done","pushed":40,"deduped":6,"failed":1,"nar_bytes":4021374976}
```

`negotiated` gives a consumer the denominator up front; `done` makes a
truncated stream detectable; `path` events arrive as uploads complete.

- **`failed` events don't abort the stream.** Every path gets an event, `done`
  reports the counts, and only then does the process exit non-zero — matching
  today's `push_all`, which already collects everything before bailing.
- **Exit code is unchanged by `--json`.**

### `--dry-run`

`push`-only, not global. Runs the negotiation, uploads nothing, exits 0.

- Human: the existing `N in closure, M missing` line, then the missing paths
  one per line, then a total size.
- `--json`: **identical schema** — `negotiated`, a `path` event per missing
  path with `status:"would-push"`, then `done`. One code path, one schema.

---

## 7. Completions

A real, visible `garret completions <shell>` subcommand using
`clap_complete::generate` (~6 lines), not a `build.rs` writing into a hashed
`OUT_DIR`. Also directly useful to anyone not on Nix. Static only — flags and
subcommand names, no dynamic completion of store paths.

Nix invokes it at build time in the main package's `postInstall`:

```nix
nativeBuildInputs = [ pkgs.installShellFiles ];
postInstall = lib.optionalString (stdenv.buildPlatform.canExecute stdenv.hostPlatform) ''
  installShellCompletion --cmd garret \
    --bash <($out/bin/garret completions bash) \
    --zsh  <($out/bin/garret completions zsh) \
    --fish <($out/bin/garret completions fish)
'';
```

The `canExecute` guard is required: running the built binary at build time
breaks any `pkgsCross` build. Not cross-compiling today, but the guard is one
line and the failure would be baffling.

**The `only` wrapper drops `share/` on the floor** — it symlinks just
`bin/${name}`, so completions installed by `postInstall` would be invisible to
`nix profile install .#garret`. Generalize `only` to also symlink `share/`
when present: three lines, and it benefits every wrapper rather than
special-casing `garret`.

Shells: bash, zsh, fish — exactly what `installShellCompletion` handles.

---

## 8. `main()` restructuring

`completions`, `login`, and `logout` skip config loading; every other command
still requires it. This is the smallest change to the current
load-then-dispatch ordering, and it's what makes `login` able to bootstrap at
all.

---

## 9. Dependencies

| Dep | Version | Note |
|---|---|---|
| `base64` | `0.22` | Already in `Cargo.lock`; matches workspace |
| `clap_complete` | `4` | Tracks `clap` |
| `indicatif` | `0.18` | New |
| `tracing-indicatif` | `0.3` | New |

All land in `Cargo.lock`, which `flake.nix` consumes via
`cargoLock.lockFile` — lockfile regen is part of the change; no flake edit
needed for the deps themselves.

---

## 10. Tests

### e2e (`scripts/e2e.sh` — already drives the client with `GARRET_TOKEN` and a hand-written `client.toml` at line 223)

- `curl` discovery **without auth**, assert 200 and the expected fields.
  Catches an accidental `.layer` reordering silently authenticating it.
- `garret whoami`.
- `garret use --print` — asserts the substituter line names `puller_endpoint`
  and the real public key.
- `garret push --dry-run --json` on a fresh path → `negotiated` +
  `would-push` events; then push for real and assert it reports `pushed`, not
  `deduped`. Catches a `--dry-run` that actually uploads.
- `garret push --json` → NDJSON parses line-by-line and terminates with a
  `done` whose counts match the `path` events.
- `garret completions fish` exits 0 with non-empty output **and no config
  present**. Guards the config-load-ordering regression.
- **Fix the `skipped` → `deduped` grep at line 230.**

### Unit

- Config template round-trips through `toml::from_str::<Config>()`.
- `login <url>` refuses to overwrite an existing config without `--force`.

---

## 11. Documentation

- **`docs/adr/0006-server-served-client-discovery.md`** — why the Pusher tells
  clients their own config, why that's an unauthenticated route on the write
  surface, the deploy-coupling cost, and when to revisit.
- **`docs/spec/06-client.md`** — commands table gains
  `use`/`whoami`/`logout`/`completions`; push section gains the bar,
  `--dry-run`, and the NDJSON schema; new config-bootstrap section.
- **`docs/spec/04-auth.md`** — one paragraph on the anonymous discovery route
  and what it does and does not reveal.
- **`docs/spec/10-packaging.md`** — completions via `installShellFiles`, the
  `canExecute` guard, the `only`-wrapper `share/` fix.
- **`README.md`** — status paragraph, and a quickstart:
  `garret login <url>` → `garret use` → `garret push`.

---

## Explicitly out of scope

- `watch-exec` — real work; `watch-store` covers the daemon case.
- Colors or styling beyond what indicatif brings.
- Dynamic completions (store paths, cache contents).
- Periodic byte-progress events in the NDJSON stream.
- A standalone user guide under `docs/` — would immediately duplicate spec 06.
- `toml_edit`-based config merging.
