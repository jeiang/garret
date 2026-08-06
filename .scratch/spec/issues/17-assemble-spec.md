# Assemble the design spec

Type: task
Status: resolved
Blocked by: 05, 06, 07, 08, 09, 10, 11, 13, 14, 15, 16

## Question

Write the destination artifact: the garret design spec in this repo,
synthesized from every resolved ticket. Includes: architecture overview
(Pusher/Puller split, topology), the push protocol spec, DB schema, storage
layout, auth design, GC design, client/watcher design, browse API, metrics
catalog, benchmark plan, repo/workspace crate layout (decide here), NixOS
module option surface (decide here), admin CLI command surface (decide
here), ADRs for the hard-to-reverse calls (SQLite + same-host topology,
multi-issuer OIDC, the chunking outcome, HTTP framework), and CONTEXT.md
updates for any terms that crystallized late.

## Answer

Spec written: eleven documents under `docs/spec/` (overview, push
protocol, database, storage, auth, GC, client, browse API,
observability, benchmarks, packaging) and four ADRs under `docs/adr/`
(same-host split services over SQLite; whole-NAR storage; multi-issuer
OIDC with no token issuance; axum-on-hyper). README.md added.

Decisions made at assembly (as chartered):

- **Crate layout**: workspace with garret-common, garret-server (shared
  internals), and five bins (pusher, puller, client, admin, bench).
  Separate binaries per service, not one multi-mode binary.
- **Admin CLI**: key generate/show offline; resign, gc run, and status
  via a root-only unix-socket admin API on the Pusher (preserving
  single-writer discipline — garret-admin never opens the DB).
- **NixOS module surface**: `services.garret.{pusher,puller,watcher}`
  option sketch in `docs/spec/10-packaging.md`; all secrets are file
  paths, never in the nix store. Flake outputs: five packages, three
  modules, devshell, checks including a push/pull NixOS test.

CONTEXT.md gained: Blob, Negotiation, Watcher Cursor, Quota, Eviction.
