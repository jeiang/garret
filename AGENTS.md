# Agent instructions

Garret is a single-tenant Nix binary cache: an OIDC-protected **Pusher**
(custom push protocol), a public **Puller** (standard Nix substituter), and
the `garret` CLI, over SQLite + S3. Rust workspace under `crates/`.

## Commands

- `just build` / `just test` — build and unit tests
- `just check` — clippy (`-D warnings`, includes `missing_docs` on the lib
  crates) and rustfmt
- `just docs` — build the documentation book into `docs/book`
- `just e2e` — end-to-end gate against a throwaway Garage (runs inside
  `nix develop`)
- `just bench-local` — self-provisioned benchmark run (all scenarios + RSS
  check); `just bench-compare` diffs against `benchmarks/baseline.json`;
  `just microbench` for the criterion benches
- `just kani` / `just tla` — Kani proofs (preamble framing, upload budget) and the TLC
  model check of the push/GC protocol ([verification/tla](verification/tla/README.md))

## Rules

- **Docs**: `docs/` is the published book. If you change behavior, update the
  page that states it; `just docs` builds it and fails on a broken link.
- **Gate before done**: run `just check` and `just test` before declaring
  work finished. When nix is available, `just e2e` too.
- **Glossary terms**: use the terminology in [docs/glossary.md](docs/glossary.md)
  (Object, Blob, Negotiation, Watcher Cursor, Quota, Eviction) in code and
  docs.

## Reference

- Documentation book: [docs/](docs/), indexed by
  [docs/SUMMARY.md](docs/SUMMARY.md); published at
  <https://garret.docs.jeiang.dev>
- Glossary: [docs/glossary.md](docs/glossary.md)
