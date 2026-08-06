# garret

A single-tenant Nix binary cache — attic's successor for this
infrastructure. Split into an OIDC-protected **Pusher** (custom
high-throughput push protocol) and a public **Puller** (standard Nix
substituter), colocated on one host over SQLite + Garage.

Design goals: maximum push throughput, minimal round-trips, provably
bounded memory under concurrent load, extensive Prometheus metrics.

## Status

**Design complete, implementation not started.**

- Spec: [docs/spec/00-overview.md](docs/spec/00-overview.md)
- Decisions: [docs/adr/](docs/adr/)
- Glossary: [CONTEXT.md](CONTEXT.md)
- Design history (wayfinder map, tickets, research incl. the dedup
  measurements that shaped the storage model):
  [.scratch/spec/map.md](.scratch/spec/map.md)
