# Agent guide

Garret is a single-tenant Nix binary cache: an authenticated Pusher that accepts
store paths, and a public Puller that serves them. It is a Rust workspace of
separate service binaries over SQLite and an S3-compatible bucket.

## Where to look

| Question | Page |
|---|---|
| How do I install or use the cache? | [Installation](install.md) |
| What does a command or config key do? | [Client reference](client.md) |
| How do I run the server? | [Deployment](deployment.md) |
| How do I reclaim space, prune, back up, monitor? | [Operations](operations.md) |
| How does a push or a pull work end to end? | [How it works](how-it-works.md) |
| How fast is it, and under what limits? | [Benchmarks](benchmarks.md) |
| What does a term mean? | [Glossary](glossary.md) |

## Commands

Run from the repository root, inside the Nix development shell.

| Command | What it does |
|---|---|
| `just build` | Builds the workspace |
| `just test` | Runs the unit tests |
| `just check` | Clippy with warnings denied, and rustfmt |
| `just e2e` | End-to-end gate against a throwaway Garage |
| `just docs` | Builds this book into `docs/book` |
| `just bench-local` | Self-provisioned benchmark run |
| `just microbench` | In-process criterion benchmarks |
| `just kani` | Kani proofs |
| `just tla` | TLC model check of the push and GC protocol |

`just check` and `just test` both pass before work is called finished.

## Reading this documentation as an agent

- `https://garret.docs.jeiang.dev/llms.txt` — one line per page, with a
  description.
- `https://garret.docs.jeiang.dev/llms-full.txt` — the whole book in one
  Markdown file.

The repository's `AGENTS.md` carries the working agreements and the rules for
changing the code.

## Invariants

When reasoning about behaviour, these hold:

- The Pusher performs every metadata write. The Puller only records
  last-accessed times, in a debounced batch.
- An object's row implies its blob. The reverse is not true: an orphaned blob is
  an expected state that the orphan sweep removes.
- Eviction and prune never remove an object that a surviving object, a newer
  push, or a live pin still needs.
- A push is idempotent. Two clients pushing the same closure, or a client
  retrying, produce the same result.
