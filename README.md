# garret

A single-tenant Nix binary cache — attic's successor for this
infrastructure. Split into an OIDC-protected **Pusher** (custom
high-throughput push protocol) and a public **Puller** (standard Nix
substituter), colocated on one host over SQLite + S3.

## Features

- Purpose-built push protocol: batch negotiation, minimal round-trips,
  provably bounded memory under concurrent load
- Standard substituter pull — any machine's `nix.conf` can use the cache,
  no special tooling (narinfo/NAR over HTTP, ed25519-signed narinfos)
- OIDC-protected push (multi-issuer: interactive device flow and CI tokens)
- Quota-driven GC: least-recently-accessed eviction that never breaks a
  surviving closure
- Store watcher: pushes newly built store paths automatically
- Online database backup (`garret-admin backup`) with a documented restore
  ([docs/spec/10-packaging.md](docs/spec/10-packaging.md#backup-and-restore))
- Browse API and extensive Prometheus metrics
- Nix packaging and NixOS modules for both services

## Install

CI builds every commit on `main` and every `v*` tag for x86_64-linux and
aarch64-darwin, then pushes the flake's packages (`garret`, `garret-pusher`,
`garret-puller`, `garret-admin`, `garret-all`) to garret's own cache. Trust it
in `nix.conf` (or `nix.settings` on NixOS; on a multi-user install
`--option` on the command line only works for trusted users):

```
extra-substituters = https://cache.jeiang.dev
extra-trusted-public-keys = cache.jeiang.dev-1:owXJK5/UX9NSf1lhmDDT3QTxMtbVk9YfHhjvOXyPhpA=
```

Then `nix profile install github:jeiang/garret#garret` (or pin a revision:
`github:jeiang/garret/<rev>#garret`) substitutes the prebuilt client instead
of compiling the workspace. The NixOS modules' default packages are the same
outputs, so a host that imports them substitutes too, as long as it does not
override garret's `nixpkgs` input with `follows`.

## Quickstart

```
garret login https://push.cache.example   # writes the config, then device flow
garret use                                # points nix at the cache
garret push ./result                      # uploads the closure
```

`login` takes the Pusher URL and fetches the rest — Puller URL, signing keys,
OIDC issuer and client id — from the server, creating `~/.config/garret/` if
it is not there. It is the only command that runs without a config, because it
is the one that writes it.

Also: `garret whoami`, `garret logout`, `garret push --dry-run`, `--json` on
`push` (NDJSON), `list`, `tree` and `whoami`, and `garret completions <shell>`
(installed automatically by the Nix package). Full reference:
[docs/spec/06-client.md](docs/spec/06-client.md).

## Push as built in CI

A job that fails late should still cache everything it built. Run
`garret watch-store` in the background with `garret enqueue` as nix's
post-build hook, then stop it and drain in an always-run last step:
`watch-store --drain` pushes whatever is left and fails the step if anything
would not push. No secret is needed: on a GitHub Actions runner with
`id-token: write`, garret mints its push tokens from the runner. For GitHub
Actions:

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

Details: [docs/spec/06-client.md](docs/spec/06-client.md#ci-push-as-built).

## Development

```
nix develop     # toolchain and dev dependencies
just build      # cargo build --workspace
just test       # unit tests
just check      # clippy (-D warnings) + rustfmt
just e2e        # end-to-end gate against a throwaway Garage
just bench-local # self-provisioned benchmark: all spec-09 scenarios + RSS check
just microbench  # in-process criterion benches (zstd, sha256, framing)
```

CI runs the same gates plus a push to the cache itself:
[.github/workflows/ci.yml](.github/workflows/ci.yml).

## Documentation

- Spec: [docs/spec/00-overview.md](docs/spec/00-overview.md)
- Decisions: [docs/adr/](docs/adr/)
- Glossary: [CONTEXT.md](CONTEXT.md)
- Agent instructions: [AGENTS.md](AGENTS.md)

## License

[MIT](LICENSE)
