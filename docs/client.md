# Client reference

`garret` is the client. It pushes store paths, watches the local Nix store,
lists what the cache holds, and diagnoses a broken setup.

## Commands

| Command | What it does |
|---|---|
| `garret login [pusher-url] [--force]` | Writes the config from server discovery, then signs you in; stores a rotating refresh token, or warns when the issuer returns none |
| `garret logout` | Deletes the stored refresh token; the config stays |
| `garret whoami` | Subject, audience, expiry, and a live probe that the Pusher accepts the token |
| `garret doctor [path]` | Layer-by-layer diagnosis; with a path, one negotiation answers "is this cached" |
| `garret use [--print]` | Adds the Puller to the user's `nix.conf` as a substituter; exits 1 if nix ignores that file |
| `garret push [paths…] [-j N] [--dry-run] [--no-upstream-filter]` | Pushes the full closure of each path, minus paths an upstream cache already signs. With no paths and a non-terminal stdin, reads whitespace-separated paths until EOF, so `nix build --print-out-paths \| garret push` works. A path that is not valid in the local store fails the push, naming it |
| `garret watch-store [--full-sync] [--drain] [--cursor <path>] [--socket <path>]` | Watcher daemon; `--drain` pushes what is left and exits; `--cursor` and `--socket` override `[watch] cursor_path` and `[watch] socket_path` |
| `garret enqueue [paths…] [--socket <path>]` | Wakes a running `watch-store` to poll now; paths default to `$OUT_PATHS`, for use as nix's `post-build-hook`. Always exits 0 |
| `garret list [query] [--limit N]` | Searches cache contents; prints full 32-character hashes. `--limit` defaults to 50; when more results remain the output says so, suggesting a larger limit only below the API's cap of 500 |
| `garret tree <hash\|path>` | Dependency tree for a store hash or store path |
| `garret pins` | Lists the GC-exempt pins set with `garret-admin pin` |
| `garret completions <shell>` | Prints a bash, zsh or fish completion script |

`--config <path>` is global and overrides the config file for every command that
loads one.

`list`, `tree` and `pins` read the Puller's browse API, which requires sign-in
and returns 404 on every path when `browse_oidc` is unset on the Puller.

## Configuration

The config lives at `$XDG_CONFIG_HOME/garret/config.toml`, falling back to
`~/.config/garret/config.toml` when `XDG_CONFIG_HOME` is unset. `garret use`
edits `$XDG_CONFIG_HOME/nix/nix.conf` with the same fallback.

| Key | Default |
|---|---|
| `endpoint` | required |
| `jobs` | 8 |
| `zstd_level` | 3 |
| `max_retries` | 5 |
| `upload_idle_secs` | 300 |
| `[oidc] audience` | required |
| `[oidc] issuer` | optional |
| `[oidc] client_id` | optional |
| `[oidc] client_secret_file` | optional |
| `[oidc] resource` | unset; an RFC 8707 resource indicator, sent only when set |
| `[watch] cursor_path` | `/var/lib/garret/watcher-cursor` |
| `[watch] socket_path` | `/run/garret/watch.sock` |
| `[watch] nix_db` | `/nix/var/nix/db/db.sqlite` |
| `[watch] poll_interval_secs` | 30 |
| `[watch] max_attempts` | 5 |
| `[watch] exclude_patterns` | `[]` |
| `[watch] upstream_keys` | `["cache.nixos.org-1"]` |

The `[oidc]` table and its `audience` key are required. Only `issuer` and
`client_id` are optional.

`garret login` writes `endpoint`, `puller_endpoint`, `public_keys` and the three
advertised `[oidc]` keys — `issuer`, `client_id`, `audience`. It does not write
the whole section, and it does not write `client_secret_file` or `resource`.

`login` refuses a discovery document whose URLs are not `https` (plain `http`
only to a loopback host), and whose values contain characters that would break
the file: URLs reject any non-graphic character plus `"`, `\`, `{`, `}` and
backtick; the audience and client id reject any non-graphic character plus `"`
and `\`. Every command re-checks the URLs and keys when it loads the config,
because the file may predate the check or be hand-edited.

## Authentication

Every command that talks to a server takes its token from the first source that
applies:

1. `GARRET_TOKEN`, used as given and never renewed.
2. `[oidc] client_secret_file`, a file holding only the client secret. The
   client id comes from `oidc.client_id`; both it and `oidc.issuer` are required
   alongside. The client-credentials grant is re-minted on expiry. A warning
   goes to stderr when the file is readable by group or others.
3. A GitHub Actions runner with both `ACTIONS_ID_TOKEN_REQUEST_URL` and
   `ACTIONS_ID_TOKEN_REQUEST_TOKEN` set.
4. The refresh token stored by `garret login`.

The token file is 0600 in a 0700 directory and is replaced whole, so no reader
sees it half-written. The token provider times out after 60 s, and a token is
re-minted 60 s before expiry. Concurrent requests share one mint, which rotating
refresh tokens require.

A non-2xx answer from the issuer or the cache is reported with the response
body, not just the status. A refresh refused with 400 or 401 reads "stored login
expired or revoked; run `garret login`". During the device flow, `slow_down`
adds 5 s to the poll interval.

## Push behaviour

Every request carries `User-Agent: garret/<version>`. The Pusher URL, the Puller
URL, the issuer, the token endpoint and the device-authorization endpoint must
follow the same `https` rule as discovery, and every configured key must be
`name:base64`; a config that breaks either is refused before any token is sent.
Trailing slashes on `endpoint` and `puller_endpoint` are dropped on load.

Retries run on two schedules:

- A `429` is the server's queue, not a fault. The path waits out `Retry-After`,
  held between 1 s and 15 minutes plus up to as much again in jitter, and tries
  again for up to 15 minutes from its first attempt. The deadline is per path,
  so a run fails after roughly 15 minutes per `jobs` paths. Sheds never spend
  `max_retries`.
- An `in-progress` answer means another pusher holds the path. The client waits
  it out against the same deadline, polling after jittered waits that double
  from 1 s to 30 s, and does not count the path until the server answers
  "exists" or takes this upload.
- `5xx` answers and dropped connections get `max_retries` jittered waits
  doubling from 250 ms. A shed or an `in-progress` wait resets that count, so it
  bounds consecutive faults.
- Other `4xx` answers fail at once. A 403 is a permanent policy refusal: it is
  never renewed or retried within the push. A 401 renews the token once and
  retries immediately; a second 401 is an ordinary failure.

Uploads start in references-first order. That is an order of starts, not of
completions: with `jobs` above 1 a referrer can finish before its references,
and a failed reference does not hold its referrers back.

After a run with no failures that uploaded something, `garret push` negotiates
the whole closure once more, because a path found present at the start can be
gone by the end. Whatever is missing is reported and pushed, and its results are
folded into the same summary. There is one re-check, not a loop. If the re-check
itself fails, `done` is still reported and the run exits non-zero.

A failure does not abort the run: every path is attempted and reported, the
summary is emitted, and only then does the process exit non-zero.

`--dry-run` negotiates, reports what would be uploaded, and stops.

`zstd_level` has no upper bound in the client. A level the server cannot accept
is rejected with 400 at upload time.

Progress is a single bar over uncompressed NAR bytes, drawn to stderr and hidden
when stderr is not a terminal. There is no `--no-progress`.

A path reported `deduped` was claimed by another writer first. Bandwidth may or
may not have been spent, because the server answers "already present" before
reading the body.

## JSON output

`--json` is accepted globally but honoured only by `list`, `tree`, `pins`,
`whoami`, `doctor` and `push`. `login`, `logout` and `use` ignore it and print
human text to stdout.

`list` and `tree` emit the browse API's JSON verbatim. `whoami` and `doctor`
each emit one object. `push` emits NDJSON, one event per line:

```
{"event":"negotiated","closure":312,"upstream":118,"missing":47,"nar_bytes":4021374976}
{"event":"path","path":"/nix/store/…","status":"upstream","nar_size":123456}
{"event":"path","path":"/nix/store/…","status":"pushed","nar_size":81920}
{"event":"path","path":"/nix/store/…","status":"deduped","nar_size":4096}
{"event":"path","path":"/nix/store/…","status":"failed","error":"upload rejected with 503: …"}
{"event":"done","pushed":40,"deduped":6,"failed":1,"nar_bytes":4021374976}
```

A run with no failures may end with a re-check before `done`:

```
{"event":"rechecked","missing":1,"nar_bytes":81920}
{"event":"path","path":"/nix/store/…","status":"pushed","nar_size":81920}
{"event":"done","pushed":48,"deduped":0,"failed":0,"nar_bytes":4021456896}
```

`negotiated` gives a consumer the denominator up front, and `done` makes a
truncated stream detectable. `--dry-run --json` reuses the same schema, with
`status:"would-push"` for what would upload and a `done` whose `pushed`,
`deduped` and `failed` are zero while `nar_bytes` is the total that would have
been uploaded.

## Store watcher

`garret watch-store` pushes store paths as they are built.

- **Cursor.** A persisted position in the local Nix database's history of
  validated paths. Catch-up after downtime, the initial scan and an offline
  backlog are all "the cursor is old". When no cursor file exists it starts at
  the newest path, so only new paths push; `--full-sync` walks history from the
  beginning instead.
- **Wake socket.** A unix datagram socket at `[watch] socket_path`, mode 0666.
  Any datagram means "poll now", and bursts coalesce into one early poll.
  `garret enqueue` is the sender, registered as nix's `post-build-hook`, so
  pushes start the moment a build finishes. A socket left by an earlier run is
  replaced; anything else at the path is refused rather than deleted. A bind
  failure only warns, and `enqueue` exits 0 even when nothing listens: a hook
  must never block or fail a build.
- **Batching.** Each poll, and each drain chunk of 500, is one batch: the paths
  that pass the filters go through a single non-recursive `nix path-info`, then
  a single negotiation naming the batch plus its direct references, and only the
  batch's own missing paths upload, `jobs` concurrent, references first.
- **Filtering.** Skips `.drv` paths, `exclude_patterns`, and paths already
  signed by a configured `upstream_keys` key or by one of the cache's own
  `public_keys`. Signatures are read from the local Nix database. `-source` and
  fixed-output paths are kept.
- **Failures.** The cursor always advances, so one bad path never wedges the
  pipeline. A path whose push fails goes on the failed list at
  `<cursor_path>.failed`, one store path per line, and the cursor is written
  once per batch after the failures are on the list. The daemon retries the list
  whenever a poll finds nothing new, up to `max_attempts` per path; the failure
  that spends the last attempt logs loudly and leaves the path for a restart or
  a drain. Paths removed by nix GC before the push are dropped silently. The
  cursor and the list are each replaced whole by a rename, so a kill mid-write
  cannot lose the backlog. A cursor or list that exists but cannot be read stops
  the watcher with an error naming it.
- **Drain.** `--drain` pushes every path validated after the cursor plus the
  failed list, in batches of 500, then exits — non-zero, naming the paths, if
  any failed. Run it after stopping the daemon, which shares the cursor. With no
  cursor it refuses rather than guessing where to start, unless `--full-sync`.

### Hook setup

`post-build-hook` takes a bare program with no arguments, so it cannot name
`garret enqueue` directly. The NixOS module wires a wrapper automatically
whenever the watcher is enabled. Elsewhere, write one:

```sh
#!/bin/sh
exec garret enqueue
```

and set `post-build-hook = /path/to/that-script` in `nix.conf`.

## Push as built in CI

A job that fails late should still leave behind everything it built, so the next
run substitutes it. The shape: the hook wakes a background `watch-store` as each
build finishes, and an always-run last step stops it and drains. No secret is
needed: on a GitHub Actions runner with `id-token: write`, garret mints its push
tokens from the runner, and `oidc.issuer` is not required.

```yaml
permissions:
  contents: read
  id-token: write # garret mints push tokens from the runner

steps:
  - uses: actions/checkout@v5

  # Before nix is installed: the hook must exist before the first build,
  # and must succeed while garret itself is not installed yet.
  - name: Write garret's post-build hook
    run: |
      cat > "$RUNNER_TEMP/garret-hook" <<EOF
      #!/bin/sh
      [ -x "$RUNNER_TEMP/garret/bin/garret" ] || exit 0
      exec "$RUNNER_TEMP/garret/bin/garret" enqueue --socket "$RUNNER_TEMP/garret.sock"
      EOF
      chmod +x "$RUNNER_TEMP/garret-hook"

  - uses: cachix/install-nix-action@v31
    with:
      extra_nix_config: |
        experimental-features = nix-command flakes
        post-build-hook = ${{ runner.temp }}/garret-hook

  - name: Start pushing as paths are built
    run: |
      nix build --out-link "$RUNNER_TEMP/garret" github:jeiang/garret#garret
      cat > "$RUNNER_TEMP/garret.toml" <<EOF
      endpoint = "https://push.cache.example"

      [oidc]
      issuer = "https://token.actions.githubusercontent.com"
      audience = "garret"

      [watch]
      cursor_path = "$RUNNER_TEMP/garret-cursor"
      socket_path = "$RUNNER_TEMP/garret.sock"
      EOF
      nohup "$RUNNER_TEMP/garret/bin/garret" --config "$RUNNER_TEMP/garret.toml" \
        watch-store > "$RUNNER_TEMP/garret-watch.log" 2>&1 &
      echo $! > "$RUNNER_TEMP/garret-watch.pid"
      # The cursor appears once it has authenticated and bootstrapped.
      for _ in $(seq 30); do [ -s "$RUNNER_TEMP/garret-cursor" ] && exit 0; sleep 1; done
      cat "$RUNNER_TEMP/garret-watch.log"; exit 1

  - run: nix build .#everything

  - name: Push whatever the watcher has not
    if: always()
    run: |
      kill "$(cat "$RUNNER_TEMP/garret-watch.pid")" || true
      "$RUNNER_TEMP/garret/bin/garret" --config "$RUNNER_TEMP/garret.toml" watch-store --drain
```

## Doctor

`garret doctor [path]` runs its checks in layer order, prints one `pass`, `fail`
or `skip` line per check, and exits non-zero if any check failed.

| Check | What it probes |
|---|---|
| `discovery` | The Pusher is reachable and its discovery document passes the checks `login` applies |
| `config` | The local config against the discovery document: drift in `puller_endpoint`, and, for a device-flow config, `[oidc]`. Fields the server does not advertise are sparse, not drifted |
| `keys` | The configured `public_keys` against the server's. A configured key the server no longer signs with fails; extra server keys pass with a re-login nudge |
| `auth` | Token acquisition plus the empty negotiation `whoami` also sends, through the same client `push` uses, so the token is not merely present but accepted |
| `pull` | The Puller's `/nix-cache-info`, then one narinfo probe. A hit and a miss both pass: the question is whether the substituter protocol is served |
| `path` | Only with a path argument: one negotiation for its hash. Not cached is a `fail`, so the exit code carries the answer for scripts |

A check whose prerequisite failed reports `skip` rather than guessing. The
`keys` check runs for every device-flow config and for any config with a
non-empty `public_keys`; only a machine config with no keys skips it. Its
messages suggest `garret login --force` even for a machine config — only the
`config` drift hint is device-flow-aware.

Timeouts: the `discovery` and `pull` probes are capped at 15 s, the `auth` check
may spend up to 60 s minting a token, and the two negotiations use 2 minutes.

Under `--json`, `doctor` emits one object; the failure summary goes to stderr:

```json
{"ok":false,"checks":[{"name":"discovery","status":"pass","detail":"…"},
                      {"name":"path","status":"fail","detail":"not cached — …"}]}
```
