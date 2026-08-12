# 19 — Post-build-hook daemon push (reopens a v1 non-goal)

Status: implemented in a reduced shape (2026-08). No separate daemon: the
wake socket lives on `watch-store` itself and `garret enqueue` is the hook
stub — see [ADR-0008](../../../docs/adr/0008-wake-socket-not-daemon-push.md)
for why the two-process design below was rejected. Evidence:
[survey](../research/similar-projects-survey.md).

## Problem

`garret watch-store` pushes after the fact by polling the Nix DB: pushes
trail the build, and on a multi-user store the watcher sees everyone's
paths. v1 explicitly deferred "post-build-hook socket ingestion".

## Evidence to reopen

Two independent implementations converged on the same design after
shipping a watcher first:

- **cachix v1.7 daemon**: unix-socket daemon; `cachix daemon push`
  enqueues paths as builds finish; `watch-exec` reimplemented on Nix's
  `post-build-hook` because the watcher over-pushed on multi-user stores.
  cachix-action@v14 cut CI post-build push time "from tens of minutes to
  mere seconds" — pushes overlap the build instead of trailing it.
- **magic-nix-cache**: local daemon started per CI job, registered as
  substituter + post-build-hook target; everything built is pushed
  implicitly, and cache errors never fail the build.

## Proposed shape (smallest version)

Smaller than it looks — most of the daemon already exists:

- **Auth**: reuse `watch-store`'s session verbatim. It already runs as a
  long-lived process authenticating as itself via `client_credentials`
  against a per-machine confidential client
  ([`main.rs`](../../../crates/garret-client/src/main.rs):207,
  `cfg.watch.credentials_file`), wired by the NixOS module (spec 04). No
  new auth machinery.
- **Push pipeline**: negotiation, bounded concurrency, retries — already
  shared code in `push.rs`, used by both `push` and `watch-store` today.
- **Socket protocol**: reuse `garret-common::admin`'s existing shape —
  line-delimited JSON, one command per line — rather than a bespoke raw
  write. `garret daemon` owns the socket and the queue feeding the push
  pipeline above; `garret enqueue <path>` is the client stub, suitable
  for `post-build-hook = ` in nix.conf: hand the path to the daemon and
  exit immediately — the hook must never block or fail the build, so
  `enqueue` exits 0 unconditionally even on a connection failure, logging
  the failure (metrics counter, matching `watch-store`'s own "loud log +
  metrics counter" convention from ticket 13) rather than going silent.
- `watch-exec`-style convenience later, on the same mechanism.

**Durability**: the daemon has none of its own — enqueue is fire-and-forget,
matching the "never block or fail the build" requirement above, which means
a path built while the daemon is down or the hook write fails is simply
never pushed by this path. `watch-store` is what makes that safe, not an
edge case for hookless hosts: its cursor is already designed as "the
offline backlog — an unreachable Pusher just means an old cursor" (ticket
13), so it stays running on **every** host, hook or no hook, as the
daemon's backstop — the daemon makes the common case fast, watch-store
guarantees every path eventually lands. A path pushed by both paths is
harmless (idempotent; the second negotiation just reports it non-missing),
just a wasted round-trip.

Server sees no change — this is all client-side.

## Score (agreed axes)

Speed **high** (pushes overlap builds; the largest CI wall-clock win the
survey found) · Ops **med** (one more long-lived process, but systemd-unit
sized) · UX **high** (set the hook once, never think about pushing again).
