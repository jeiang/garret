# garret

A single-tenant Nix binary cache — attic's successor for this
infrastructure. Split into an OIDC-protected **Pusher** (custom
high-throughput push protocol) and a public **Puller** (standard Nix
substituter), colocated on one host over SQLite + S3 (MEGA S4).

Design goals: maximum push throughput, minimal round-trips, provably
bounded memory under concurrent load, extensive Prometheus metrics.

## Status

**Implemented and passing its end-to-end gate; not yet deployed.**

Push, pull, auth, GC, browse, the store watcher, packaging and the NixOS
modules are all in. Nothing has run against MEGA S4 or a real OIDC issuer
yet — both are untested surfaces until the first deployment.

- Spec: [docs/spec/00-overview.md](docs/spec/00-overview.md)
- Decisions: [docs/adr/](docs/adr/)
- Glossary: [CONTEXT.md](CONTEXT.md)
- Design history (wayfinder map, tickets, research incl. the dedup
  measurements that shaped the storage model):
  [.scratch/spec/map.md](.scratch/spec/map.md)

## CI

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs three jobs:
**build** (`nix build .#garret-all`, which also runs the unit tests via the
package's check phase), **test** (lints, formatting, and the end-to-end gate
against a throwaway Garage), and **push to garret**.

The push job is inert until garret is running. Set these repository
variables to switch it on — no secrets involved, since CI authenticates with
a per-run GitHub OIDC token that garret validates by owner id:

| Variable | Effect |
|---|---|
| `GARRET_ENDPOINT` | Pusher URL. Setting it enables the push job. |
| `GARRET_PULLER` | Puller URL, added as a substituter so CI builds pull from the cache. |
| `GARRET_PUBLIC_KEY` | The signing key's public half, trusted for substitution. |
| `GARRET_AUDIENCE` | OIDC audience; defaults to `garret`. |

Until `GARRET_PULLER` is set, every run compiles from source. Once it is, CI
substitutes its own previous output — the cache caching its own dependencies.
