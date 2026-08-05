# Store-path detection mechanisms for the watcher

Research for issue `12-store-watcher-mechanisms.md`. Question: how can the garret
client detect newly built (or otherwise newly *valid*) store paths on a NixOS
machine, and what are the trade-offs of each mechanism?

## Summary and recommendation

Four mechanism families are viable:

1. **Lock-file inotify watching** (`/nix/store/*.lock` removal) — what attic and
   cachix `watch-store` both do. Simple, unprivileged-ish, catches builds *and*
   substitutions *and* `nix copy` into the store, and is race-free with respect
   to path registration (the lock is unlinked only *after* `registerValidPath`).
   But it is lossy: no events while the watcher is down, no initial scan, inotify
   queue overflow drops events, and it depends on an undocumented Nix
   implementation detail (lock-file lifecycle).
2. **`post-build-hook`** — precise (fires exactly for locally coordinated builds,
   with `$OUT_PATHS` handed to you), but synchronous: it blocks the build loop
   and a hook failure fails the build. It misses substituted paths entirely and
   only covers builds coordinated by this machine's daemon. Production users
   (cachix daemon, nix.dev guide) always pair it with a local queue daemon so the
   hook itself is a fast, non-failing socket write.
3. **Nix DB cursor polling** (`/nix/var/nix/db/db.sqlite`, `ValidPaths.id` is
   `INTEGER PRIMARY KEY AUTOINCREMENT`) — the only mechanism that is *complete*:
   every path that becomes valid, from any source, appears as a new row with a
   monotonic, never-reused id. Persisting the cursor gives free catch-up after
   downtime and a natural initial scan. Downsides: unofficial schema, needs read
   access to the DB (root, effectively), and it's polling (latency), though the
   poll can be woken by inotify on `db.sqlite-wal`.
4. **nix-daemon introspection** — there is **no supported subscription API** in
   the daemon protocol; the structured (`internal-json`) log is per-invocation
   only. A protocol-proxy daemon is possible but heavyweight and
   version-coupled. Not recommended.

**Recommendation-leaning conclusion for garret:** use a **hybrid of (3) and
(1)**: a persistent cursor over `ValidPaths.id` as the source of truth
(completeness, restart catch-up, initial scan), with the lock-file/inotify
signal (or inotify on `/nix/var/nix/db`) used only as a low-latency *wakeup* for
the poller rather than as the event source itself. Optionally offer a
`post-build-hook`-fed unix socket (cachix-daemon style) as a "precise mode" for
users who only want their own builds pushed — but since garret is single-tenant
("push everything this machine builds"), the DB cursor matches the semantics
best. Decouple detection from pushing with a durable (on-disk) queue so an
unreachable server never loses paths and never breaks detection — this is
attic's weakest point today.

### Comparison table

| | Lock-file inotify (attic/cachix watch-store) | post-build-hook (+ local daemon) | DB cursor polling (`ValidPaths.id`) | Daemon introspection / proxy |
|---|---|---|---|---|
| Catches local builds | Yes | Yes | Yes | Only via proxied clients |
| Catches substitutions / `nix copy` in | Yes (same lock path in `addToStore`) | **No** | Yes | Partially |
| Catches remote-builder outputs copied back | Yes (copied back via `addToStore`) | Yes (hook runs on coordinating machine; historically buggy, #4245) | Yes | Partially |
| Race-free w.r.t. registration | Yes (unlink happens after `registerValidPath`) | Yes (runs after registration) | Yes (row = registration) | Yes |
| Missed-event window (restart/backlog) | Lost forever (no scan) | Lost if daemon socket down and hook doesn't queue; hook failure fails builds | None (cursor persists) | Lost |
| Latency | ~instant | ~instant | poll interval (or ~instant with WAL-file inotify wakeup) | ~instant |
| Privileges | read on `/nix/store` + inotify | root-owned nix.conf change; hook runs as root | read on `/nix/var/nix/db` (root in practice) | root; owns socket |
| Coupling to Nix internals | lock-file lifecycle (undocumented) | documented, supported | SQLite schema (undocumented but stable since 2010, `schema.sql`) | wire protocol (versioned, internal) |
| Build-time impact | none | blocks build loop; failing hook **fails the build** | none | none |
| False positives | failed builds: none (lock left in place); stale-lock cleanup could fake events | none | none | none |

## Mechanism details

### 1. Attic's `watch-store` (lock-file inotify)

Source: `/Users/aidanp/Projects/attic/client/src/command/watch_store.rs`,
`/Users/aidanp/Projects/attic/client/src/push.rs`;
upstream: <https://github.com/zhaofengli/attic/blob/main/client/src/command/watch_store.rs>.

- Uses the [`notify`](https://docs.rs/notify) crate v8 (`recommended_watcher`,
  built with `default-features = false, features = ["macos_fsevent"]` — inotify
  backend on Linux, FSEvents on macOS), watching the store dir
  **non-recursively** (`RecursiveMode::NonRecursive`, watch_store.rs:92).
- It reacts **only to `EventKind::Remove`** events whose path ends in `.lock`
  (watch_store.rs:105-113, `strip_lock_file` at :127). Rationale (comment at
  :103): "We watch the removals of lock files which signify store paths becoming
  valid". It filters out `*.drv.lock` and `*-source.lock` (the latter also skips
  any genuine `…-source` output — a minor false-negative class).
- There is **no debouncing at the watcher level**; raw events go into an
  unbounded mpsc channel. Debouncing/batching happens in `PushSession`
  (push.rs:96-128): paths accumulate and a batch is submitted after **2 s of
  queue silence or 10 s total**, then one closure computation
  (`compute_fs_closure_multi`) + one `get_missing_paths` API call covers the
  whole batch. A `known_paths` set suppresses re-pushing paths already handled
  in this session.
- **Closure behavior:** each detected path's full runtime closure is computed
  and pushed (unless `--no-closure`), then filtered by "already on server" and
  by upstream-cache signatures (`sigs` matching
  `upstream_cache_key_names`, push.rs:460-477) — this is how it avoids
  re-uploading cache.nixos.org contents even though substitutions also produce
  lock-removal events.
- **What it misses / failure handling:**
  - No initial scan and no persistence: anything built before start, while
    stopped, or during an inotify queue overflow is never pushed.
  - The notify callback does `tx.send(res).unwrap()` and the event loop does
    `session.queue_many(paths).unwrap()` (watch_store.rs:89, :116). If the
    session worker dies, the process panics.
  - The session worker propagates plan errors with `?` (push.rs:356-362): a
    single failed `get_missing_paths` call (server unreachable) or a
    `query_path_info` on a path that got GC'd/never became valid **terminates
    the worker permanently** — the watcher is then a zombie until the next
    `queue_many` panic. No retry, no on-disk queue. (Uploads themselves do have
    retry: `upload_path_with_retry`.)

### 2. Cachix: `watch-store`, `watch-exec`, and the daemon

Sources:
[`WatchStore.hs`](https://github.com/cachix/cachix/blob/master/cachix/src/Cachix/Client/WatchStore.hs),
[`Command/Watch.hs`](https://github.com/cachix/cachix/blob/master/cachix/src/Cachix/Client/Command/Watch.hs),
[`Daemon/PostBuildHook.hs`](https://github.com/cachix/cachix/blob/master/cachix/src/Cachix/Daemon/PostBuildHook.hs),
[Cachix v1.7 announcement](https://blog.cachix.org/posts/2024-01-12-cachix-v1-7/),
[docs.cachix.org/pushing](https://docs.cachix.org/pushing).

- `cachix watch-store` is the **same mechanism as attic** (attic borrowed it):
  watch `/nix/store` for `Removed` events on `*.lock`, skip `*.drv.lock`; the
  code comments "we queue store paths after their lock has been removed".
- `cachix daemon` (v1.7+): a standalone process listening on a **unix socket**;
  clients enqueue paths asynchronously (`cachix daemon push --socket …`), and a
  push manager with worker pool/queue does the uploads out-of-band.
- `watch-exec` in **auto mode**: "registers a post-build hook if the user is
  trusted. Otherwise, falls back to watching the entire Nix store." The
  post-build-hook variant generates a temp `post-build-hook.sh` containing
  `exec cachix daemon push --socket $SOCKET $OUT_PATHS` and injects it via
  `NIX_CONF`/`NIX_USER_CONF_FILES` — i.e. **the hook is only a cheap socket
  write; queueing, retries, and uploads live in the daemon**. This is the
  canonical pattern for making post-build-hook safe.
- Motivation for the hook mode over store watching: store watching "picks up
  things from unrelated builds"
  ([cachix#343](https://github.com/cachix/cachix/issues/343)) — a
  multi-tenant concern that mostly doesn't apply to single-tenant garret.

### 3. Nix's `post-build-hook`

Docs: [Nix manual, "Using the post-build-hook"](https://nix.dev/manual/nix/latest/advanced-topics/post-build-hook.html),
[nix.dev recipe](https://nix.dev/guides/recipes/post-build-hook.html).

- Runs after each successful build, with env `$OUT_PATHS` (space-separated
  output store paths, **outputs only, not the closure**) and `$DRV_PATH`. Must
  be root-executable; runs as root for daemon builds. Only trusted users can set
  it per-invocation; normally it's a system-wide `nix.conf` setting (NixOS:
  `nix.settings.post-build-hook` / `nix.extraOptions`).
- **Blocking semantics:** the manual is explicit — the hook "blocks the build
  loop. The build loop exits if the hook program fails", and a naive
  `nix copy`-in-hook setup "will make Nix slow or unusable when the internet is
  slow or unreliable". I.e. a slow hook serializes builds; a failing hook fails
  the user's `nix build` (the path itself stays valid — registration precedes
  the hook — but the overall command errors). The manual itself recommends
  passing paths "to a user-supplied daemon or queue".
- **What it misses:** substituted paths (no hook on substitution) and any path
  added via `nix copy`/`nix-store --import`. For **remote builders** coordinated
  by this machine, the *local* hook is supposed to run after outputs are copied
  back; this regressed in the 2.4 era
  ([NixOS/nix#4245](https://github.com/NixOS/nix/issues/4245), fixed by
  [#4342](https://github.com/NixOS/nix/pull/4342)) — treat it as
  works-but-historically-fragile. Builds performed *on* this machine on behalf
  of another coordinator do fire the local hook.
- Closure note: because references of a freshly built output are already valid
  locally but may never have been pushed (e.g. they were substituted from
  cache.nixos.org before the watcher existed), a hook-fed pusher still needs
  closure computation + server-side dedup/upstream filtering, same as attic's
  `PushSession`.

### 4. Filesystem watching of `/nix/store` directly (non-lock variants)

- inotify basics ([inotify(7)](https://man7.org/linux/man-pages/man7/inotify.7.html)):
  a non-recursive watch on `/nix/store` sees `IN_CREATE`/`IN_MOVED_TO`/
  `IN_DELETE` for direct children only. Events are queued per-instance;
  overflow (default `/proc/sys/fs/inotify/max_queued_events` = 16384) drops
  events and delivers `IN_Q_OVERFLOW` — the `notify` crate surfaces this as an
  error/rescan-needed, and a correct consumer must fall back to a full scan.
  No events for anything that happened while not watching. fanotify's
  directory-entry events (`FAN_CREATE`/`FAN_DELETE`, Linux 5.1+ with
  `FAN_REPORT_FID`) add nothing here except requiring `CAP_SYS_ADMIN`.
- **Why watching directory creation is wrong:** for input-addressed builds the
  output tree materializes at (or is renamed to) its final store path while the
  build/registration is still in progress — hash rewriting, `--check`
  processing, signing, and the SQLite `registerValidPath` all happen *after*
  the bytes appear. A `CREATE`/`MOVED_TO` watcher therefore fires while the
  path is **not yet valid** (and might never become valid, e.g. failed builds
  leave partial output that gets deleted). Validity is DB state, not FS state;
  a create-watcher would have to poll `isValidPath` per candidate.
- **Why lock-removal watching works:** Nix takes a sibling `<store-path>.lock`
  (per `lockPaths`: `lockPath += ".lock"` —
  [`unix/pathlocks.cc`](https://github.com/NixOS/nix/blob/master/src/libstore/unix/pathlocks.cc))
  before building or adding a path, and `unlock()` unlinks the lock **only when
  `setDeletion(true)` was called** — which `LocalStore::addToStore` /
  `addToStoreFromDump` do **immediately after `registerValidPath(info)`**
  ([`local-store.cc`](https://github.com/NixOS/nix/blob/master/src/libstore/local-store.cc));
  the builder goals do the same after `registerOutputs`. Substitution funnels
  through `copyStorePath → addToStore`
  ([`substitution-goal.cc`](https://github.com/NixOS/nix/blob/master/src/libstore/build/substitution-goal.cc)),
  so it produces the same signal. On **failure**, `deletePaths` stays false and
  the `.lock` is left on disk (stale locks are a known artifact,
  [NixOS/nix#10897](https://github.com/NixOS/nix/issues/10897)) — so lock
  removal is a *true* "path became valid" edge, with two caveats: (a) it's an
  undocumented internal that could change; (b) out-of-band deletion of stale
  lock files (tmpfiles, humans) forges events for paths that may not be valid —
  so consumers should still verify validity (attic effectively does via
  `query_path_info`, though it currently *dies* on the error instead of
  skipping).
- nix-daemon **temp build dirs** are not in `/nix/store` (they're under
  `/nix/var/nix/builds` or TMPDIR), so a non-recursive store watch doesn't see
  build churn; it does see scratch/rename traffic for outputs and the
  `.lock`/`.drv` noise, which is cheap to filter by suffix.
- Non-Linux/portability: attic's macOS FSEvents backend shows the same code
  works there; for garret (NixOS-targeted) inotify is sufficient. Network or
  overlay stores would not deliver events — out of scope for single-tenant
  NixOS.

### 5. nix-daemon introspection and the Nix DB

- **Daemon protocol:** strictly request/response worker ops; there is no
  subscribe/notify operation and no supported event stream. `nix store ping`
  only checks connectivity. Interposing a **proxy daemon socket**
  (`NIX_REMOTE=unix://…` pointing at a garret proxy that forwards to the real
  daemon and observes `AddToStore`/`BuildDerivation` results) would be
  complete *for clients using that socket*, but couples garret to the internal
  wire protocol and misses anything talking to the real socket. Rejected.
- **Structured logs:** `nix build --log-format internal-json` (`@nix …` lines)
  is per-invocation stderr, not a daemon-wide feed, and doesn't directly
  announce registered output paths (you'd map drv → outputs yourself). Only
  useful for a `watch-exec`-style wrapper. Rejected as the primary mechanism.
- **The local store DB** (`/nix/var/nix/db/db.sqlite`, WAL mode) is the actual
  registry of validity. Schema
  ([`schema.sql`](https://github.com/NixOS/nix/blob/master/src/libstore/schema.sql)):
  `ValidPaths(id INTEGER PRIMARY KEY AUTOINCREMENT, path, hash,
  registrationTime, deriver, narSize, ultimate, sigs, ca)` plus `Refs`.
  Because `id` is `AUTOINCREMENT`, ids are **monotonic and never reused** even
  across GC deletions — `SELECT id, path FROM ValidPaths WHERE id > :cursor
  ORDER BY id` is an exact, gap-tolerant change feed. This gives, uniquely:
  - completeness (builds, substitutions, `nix copy`, imports — anything valid);
  - trivial **initial scan** (start cursor at 0 or at `MAX(id)`);
  - crash-safe **catch-up** (persist the cursor; nothing is ever missed);
  - even `sigs` for upstream-cache filtering without a daemon round-trip.
  Caveats: the schema is internal (though unchanged in essentials since the
  2010 SQLite migration); open the DB read-only with a `busy_timeout` (Nix
  holds write locks briefly; WAL readers don't block writers); reading a
  WAL-mode DB requires access to the `-wal`/`-shm` files, i.e. effectively
  root — fine for a system service, a problem for a user-level watcher. Poll
  latency can be cut to ~0 by using inotify on `/nix/var/nix/db`
  (`db.sqlite-wal` modification) purely as a wakeup, with a slow timer as
  backstop — the DB remains the source of truth, so a missed wakeup only adds
  latency, never loss.

## Closures vs individual paths (all mechanisms)

Every mechanism yields *individual* newly valid paths (post-build-hook yields
output sets per derivation). During an active build, dependencies become valid
before dependents, so events arrive roughly in closure order — but only for
paths that became valid *now*; previously valid but never-pushed references
(pre-watcher substitutions, catch-up gaps) make per-event closure computation
necessary if the server requires closed caches. Attic's batching (collect roots
for 2–10 s, one `computeFSClosure` + one missing-paths query per batch,
remember known paths) is the right shape and worth replicating; with the DB
cursor, `Refs` could even supply references without daemon calls. Whether garret
pushes closures or bare paths (letting the server tolerate open sets) is a
server-contract question — flagged below.

## Failure/backlog handling when the push destination is unreachable

- **Attic today:** none — plan-phase API errors kill the session worker, then
  the watcher panics on next queue; no disk queue, no catch-up. Do not copy.
- **Cachix daemon:** in-memory queue + worker pool decoupled from the hook;
  survives slow uploads but the queue is not durable across daemon restarts.
- **Design implication for garret:** detection must never block on, or die
  from, push failures. A durable queue (or simply the persisted DB cursor — a
  path is "done" only when pushed, so the cursor *is* the queue) gives
  at-least-once semantics for free; server-side missing-paths dedup makes
  re-pushes cheap. On startup: resume cursor → enumerate missed rows → normal
  operation. With inotify-only detection, an equivalent would require a full
  `nix path-info --all` diff on every start.

## Open questions for the client-design ticket

1. **Cursor vs events as source of truth:** accept the recommendation to make
   the persisted `ValidPaths.id` cursor authoritative (with inotify as wakeup),
   or keep lock-file events primary with a scan-on-start? Decide how strongly
   garret wants to avoid depending on the DB schema vs. the lock-file behavior
   (both are internals; the DB one is more useful and arguably more stable).
2. **Privilege model:** does the watcher run as a root systemd service (needed
   for DB reads and for a cachix-style root post-build-hook anyway), or must an
   unprivileged mode exist (→ lock-file watching only)?
3. **Push scope:** whole closures per detected root, or bare paths assuming the
   cursor eventually covers everything valid? (Bare paths + complete cursor is
   self-consistent *going forward*, but the initial state and upstream-filtered
   paths punch holes — does the garret server require closed closures per
   upload, like attic's `get_missing_paths` model?)
4. **Filtering:** replicate attic's upstream-signature filter (skip paths
   signed by cache.nixos.org) — from `sigs` in the DB or via daemon
   `query_path_info`? Also: skip `.drv` paths only, or also `-source`/fixed-output
   fetches (attic skips `-source`; is that desirable)?
5. **Backpressure & durability:** cursor-as-queue means unbounded backlog is
   just an old cursor — but do we need per-path retry state (poison paths that
   repeatedly fail upload, GC'd-before-push races → skip-and-log)?
6. **post-build-hook mode:** offer an optional garret socket + generated hook
   (cachix-daemon pattern) for build-scoped pushing / non-NixOS Nix installs?
   If yes, the hook must be a non-failing enqueue (never fail the build) —
   spec the socket protocol in the client design.
7. **Initial scan policy:** on first install, start from `MAX(id)` (push only
   new things) or offer `--full-sync` from id 0?

## Source index

- Attic: `/Users/aidanp/Projects/attic/client/src/command/watch_store.rs`,
  `/Users/aidanp/Projects/attic/client/src/push.rs`,
  `/Users/aidanp/Projects/attic/client/Cargo.toml` (notify 8.1.0)
- Cachix: [WatchStore.hs](https://github.com/cachix/cachix/blob/master/cachix/src/Cachix/Client/WatchStore.hs),
  [Command/Watch.hs](https://github.com/cachix/cachix/blob/master/cachix/src/Cachix/Client/Command/Watch.hs),
  [Daemon/PostBuildHook.hs](https://github.com/cachix/cachix/blob/master/cachix/src/Cachix/Daemon/PostBuildHook.hs),
  [v1.7 blog](https://blog.cachix.org/posts/2024-01-12-cachix-v1-7/),
  [issue #343](https://github.com/cachix/cachix/issues/343)
- Nix: [post-build-hook manual](https://nix.dev/manual/nix/latest/advanced-topics/post-build-hook.html),
  [nix.dev post-build-hook recipe](https://nix.dev/guides/recipes/post-build-hook.html),
  [unix/pathlocks.cc](https://github.com/NixOS/nix/blob/master/src/libstore/unix/pathlocks.cc),
  [local-store.cc](https://github.com/NixOS/nix/blob/master/src/libstore/local-store.cc),
  [substitution-goal.cc](https://github.com/NixOS/nix/blob/master/src/libstore/build/substitution-goal.cc),
  [schema.sql](https://github.com/NixOS/nix/blob/master/src/libstore/schema.sql),
  [#4245 post-build-hook vs remote builders](https://github.com/NixOS/nix/issues/4245),
  [#10897 stale .lock files](https://github.com/NixOS/nix/issues/10897)
- [inotify(7)](https://man7.org/linux/man-pages/man7/inotify.7.html)
