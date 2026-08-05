# Domain Glossary

## Garret

This project: a single-tenant Nix binary cache — attic's successor for this
infrastructure, with no compatibility with attic's protocol or tokens.

## Pusher

The service that accepts NARs from clients over a purpose-built
high-throughput protocol. Protected by OIDC (Pocket ID and GitHub Actions
issuers). Not exposed publicly.

## Puller

The public binary cache frontend. Speaks the standard Nix substituter
protocol (narinfo/NAR over HTTP, ed25519-signed narinfos) so any machine's
nix.conf can use it without special tooling. Read-only.

## Garret Client

The CLI that drives the Pusher: pushing store paths, watching the local Nix
store, and listing cache contents. Pull never requires it.

## Store Watcher

The garret client mode that observes the local Nix store and pushes newly
built store paths to the Pusher automatically.

## Object

The unit of content in the cache: one Nix store path mapped into the cache,
keyed by its store path hash. (Carried over from the attic-era glossary;
garret drops the multi-cache dimension.)
