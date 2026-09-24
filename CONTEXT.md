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

## Blob

The stored form of an object: a single compressed NAR. Exactly one blob
per object; a blob exists if and only if its object does.

## Negotiation

The pre-upload exchange in which a client asks which of a batch of store
paths the cache is missing, and pushes only those.

## Watcher Cursor

The store watcher's persisted position in the local Nix store's history of
validated paths. Everything after the cursor is yet to be pushed; an old
cursor is a backlog, not an error.

## Quota

The configured storage budget for the cache. Eviction reclaims space when
usage crosses it and deletes nothing while usage stays below it.

## Eviction

Removing an object (and its blob) to reclaim quota. Only objects no
surviving object references, no live pin holds, and no push used within
the push grace may be evicted, least-recently-accessed first — so every
closure the cache still serves, or a push is still relying on, remains
complete.

## Push Grace

The day after a push or Negotiation last used an object, during which
neither eviction nor prune removes it: the client was told the object is
present and uploads its referrers against that answer.

## Prune

Operator-requested removal of every closure last pushed before a cutoff,
independent of quota. Like eviction, it never removes an object that a
surviving object, a newer push, or a live pin still needs.

## Deletion Claim

The Pusher's in-memory hold on an object being removed — by eviction,
prune, or an operator delete — from before its row is deleted until its
blob is. An upload of that object meanwhile is told to retry, so it cannot
write a blob the pending delete would take.
