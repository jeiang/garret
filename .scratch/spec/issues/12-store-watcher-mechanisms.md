# Store-path detection mechanisms for the watcher

Type: research
Status: resolved

## Question

What are the viable mechanisms for detecting newly built store paths on a
NixOS machine, and their trade-offs? Survey: attic's `watch-store`
implementation (see `/Users/aidanp/Projects/attic/client/`), Nix's
post-build-hook (reliability, blocking semantics, what it misses —
substituted paths, remote builds), filesystem watching of `/nix/store`
(fsnotify semantics on Linux, completeness, races with path finalization),
and any nix-daemon introspection options. Note how each behaves for
closures vs individual paths, and failure/backlog behavior when the Pusher
is unreachable.

## Answer

Full findings: [research/store-watcher-mechanisms.md](../research/store-watcher-mechanisms.md).
Recommendation leans **hybrid: a persisted-cursor poll of the Nix DB as
source of truth, with inotify as a latency wakeup** — the cursor doubles as
the durable backlog when the Pusher is unreachable. Seven open questions
are flagged in the findings for ticket 13.

- Attic's `watch-store`: notify crate, non-recursive, reacts only to Remove
  events on `*.lock` files — race-free (Nix unlinks the lock only after
  `registerValidPath`) and also fires for substitutions and `nix copy`;
  failed builds leave the lock. But a single plan-phase API error
  permanently kills the worker; no disk queue, no initial scan, no
  catch-up.
- `post-build-hook`: blocks the build loop, hook failure fails the build;
  misses substituted/copied paths; historically buggy with remote builders.
  Cachix makes it safe by having the hook only enqueue to an async daemon
  socket.
- Watching /nix/store directory creation is unsound (paths materialize
  before registration; validity is DB state; inotify drops events on
  overflow/downtime).
- Key finding: `ValidPaths.id` in `/nix/var/nix/db/db.sqlite` is
  AUTOINCREMENT — monotonic, never reused — so a persisted-cursor DB poll
  is the only *complete* mechanism: free catch-up, initial scan, and `sigs`
  for filtering. Costs: reading Nix's internal schema, root-level DB read
  access, no supported subscription API.
