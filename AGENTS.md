# Agent instructions

Garret is a single-tenant Nix binary cache: an OIDC-protected **Pusher**
(custom push protocol), a public **Puller** (standard Nix substituter), and
the `garret` CLI, over SQLite + S3. Rust workspace under `crates/`.

## Commands

- `just build` / `just test` — build and unit tests
- `just check` — clippy (`-D warnings`, includes `missing_docs` on the lib
  crates) and rustfmt
- `just e2e` — end-to-end gate against a throwaway Garage (runs inside
  `nix develop`)

## Rules

- **Spec sync**: if you change behavior, update the matching `docs/spec/`
  file in the same change.
- **ADR discipline**: new architectural decisions get an ADR in `docs/adr/`.
- **Gate before done**: run `just check` and `just test` before declaring
  work finished. When nix is available, `just e2e` too.
- **Glossary terms**: use the terminology in [CONTEXT.md](CONTEXT.md)
  (Object, Blob, Negotiation, Watcher Cursor, Quota, Eviction) in code and
  docs.

## Reference

- Spec: [docs/spec/00-overview.md](docs/spec/00-overview.md)
- Decisions: [docs/adr/](docs/adr/)
- Glossary: [CONTEXT.md](CONTEXT.md)
- Design history (wayfinder map, tickets, research incl. the dedup
  measurements that shaped the storage model):
  [.scratch/spec/map.md](.scratch/spec/map.md)
