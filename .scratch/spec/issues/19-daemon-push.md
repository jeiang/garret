# 19 — Post-build-hook daemon push (reopens a v1 non-goal)

Status: proposed (2026-08 review). Evidence:
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

- `garret daemon` — long-lived process on a unix socket, owning one
  authenticated session and the existing push pipeline (negotiation,
  bounded concurrency, retries).
- A tiny `garret enqueue <path>` (or raw socket write) suitable for
  `post-build-hook = ` in nix.conf: hand the path to the daemon and exit
  immediately — the hook must never block or fail the build.
- `watch-exec`-style convenience later, on the same mechanism.

Keeps: watch-store (remote machines without hook access), one-shot push.
Server sees no change — this is all client-side.

## Score (agreed axes)

Speed **high** (pushes overlap builds; the largest CI wall-clock win the
survey found) · Ops **med** (one more long-lived process, but systemd-unit
sized) · UX **high** (set the hook once, never think about pushing again).
