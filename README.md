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
- Browse API and extensive Prometheus metrics
- Nix packaging and NixOS modules for both services

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

## Development

```
nix develop     # toolchain and dev dependencies
just build      # cargo build --workspace
just test       # unit tests
just check      # clippy (-D warnings) + rustfmt
just e2e        # end-to-end gate against a throwaway Garage
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
