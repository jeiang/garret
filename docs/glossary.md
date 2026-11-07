# Glossary

## Garret

This project: a single-tenant Nix binary cache with an authenticated Pusher and
a public Puller.

## Pusher

The service that accepts NARs from clients over a purpose-built protocol.
Protected by OIDC (Pocket ID and GitHub Actions issuers). Not exposed publicly.

## Puller

The public binary cache frontend. Speaks the standard Nix substituter protocol
(narinfo and NAR over HTTP, ed25519-signed narinfos) so any machine's `nix.conf`
can use it without special tooling. Read-only except for debounced last-accessed
bumps, which eviction ordering needs.

## Garret Client

The CLI that drives the Pusher: pushing store paths, watching the local Nix
store, and listing cache contents. Pull never requires it.

## Store Watcher

The client mode that observes the local Nix store and pushes newly built store
paths to the Pusher automatically.

## Object

The unit of content in the cache: one Nix store path mapped into the cache,
keyed by its store path hash.

## Blob

The stored form of an object: a single compressed NAR. Exactly one blob per
object. The invariant is one-way — an object's row implies its blob, never the
reverse. An orphaned blob with no row is an expected state that the orphan sweep
removes.

## Negotiation

The pre-upload exchange in which a client asks which of a batch of store-path
hashes the cache is missing, and pushes only those.

## Watcher Cursor

The store watcher's persisted position in the local Nix store's history of
validated paths. Everything after the cursor is yet to be pushed; an old cursor
is a backlog, not an error. A path the cursor passed whose push failed is kept
on the **failed list** beside it until a retry, restart or drain pushes it.

## Quota

The configured storage budget for the cache. Eviction reclaims space when usage
reaches the high watermark, and deletes nothing below it.

## Eviction

Removing an object and its blob to reclaim quota. It starts at the high
watermark (default 95% of the quota) and stops at the low watermark (85%). Only
objects no surviving object references, no live pin holds, and no push used
within the push grace may be evicted, least-recently-accessed first — so every
closure the cache still serves, or a push is still relying on, remains complete.

## Push Grace

The day after a push or negotiation last used an object, during which neither
eviction nor prune removes it: the client was told the object is present and
uploads its referrers against that answer.

## Prune

Operator-requested removal of every closure last pushed before a cutoff,
independent of quota. Like eviction, it never removes an object that a surviving
object, a newer push, or a live pin still needs.

## Pin

A named GC-exempt root. Pinning protects the root's whole closure, because
eviction is root-first. Pinning a hash the cache does not hold is an error, and
an expired pin simply stops matching.

## Orphan Sweep

A periodic pass, at startup and weekly, that deletes blobs with no database row
and aborts abandoned multipart uploads. It is what makes the one-way blob
invariant safe: rows are deleted before blobs, so a failed blob delete leaves an
orphan for the sweep rather than a row pointing at nothing.

## Deletion Claim

The Pusher's in-memory hold on an object being removed — by eviction, prune, or
an operator delete — from before its row is deleted until its blob is. An upload
of that object meanwhile is told to retry, so it cannot write a blob the pending
delete would take.

## Discovery

The anonymous endpoint the Pusher serves so a client can configure itself: the
Puller URL, the signing keys' public halves, and the OIDC issuer, audience and
client id. It carries nothing secret and reveals no cache contents.

## Upstream Filter

The client-side rule that skips store paths an upstream cache already serves
signed, so pushing them would spend bandwidth and quota on bytes the cache never
needed to hold. It is a signature-name comparison, with no network probe.

## Upstream Keys

The key names the upstream filter recognises. Defaults to
`["cache.nixos.org-1"]`; the watcher can override the list per daemon.
