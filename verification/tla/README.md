# TLA+ model of the push, Negotiation, eviction and delete protocol

`GarretPush.tla` models the concurrency protocol of the Pusher and Puller
and checks it with TLC (bounded model checking, not a proof). It follows
the design in [01-push-protocol](../../docs/spec/01-push-protocol.md),
[02-database](../../docs/spec/02-database.md),
[03-storage](../../docs/spec/03-storage.md) and
[05-gc](../../docs/spec/05-gc.md), and the code in
`crates/garret-server/src/{inflight,db}.rs` and
`crates/garret-pusher/src/{main,gc,admin,fsck}.rs`. Each action in the
spec cites the function it mirrors.

## Run

```
just tla
```

which is

```
cd verification/tla && nix develop --command bash -c 'tlc -workers auto -metadir "$(mktemp -d)" -config main.cfg GarretPush.tla'
```

Other runs (same directory, same command with another `-config`):

| Config | Purpose | Expected |
|---|---|---|
| `main.cfg` | All invariants, deadlock check, real protocol | pass |
| `deep.cfg` | Same, `Budget = 5` | pass (about 12 min) |
| `liveness.cfg` (add `-deadlock`) | `OrphanTransient` under fairness | pass |
| `fsck.cfg` | Real protocol plus S3 drift and fsck repairs, `Budget = 5` | pass (about 2 min) |
| `fsck_race.cfg` | Same, fsck before the fix (`FsckRecheck = FALSE`) | `FsckSafe` fails (finding 1) |
| `redirect_window.cfg` | `RedirectWindow`: a redirect whose row was deleted since still finds the blob | fails (finding 2, the window exists) |
| `mutant_no_delete_claims.cfg` | Deleters take no Delete claim | `RowImpliesBlob` fails |
| `mutant_no_evict_recheck.cfg` | `evict_object` trusts the candidate snapshot | `EvictionSafe` fails |
| `mutant_no_sweep_recheck.cfg` | Sweep does not re-check for a row | `RowImpliesBlob` fails |

The mutants show the invariants can fail: each removes one guard the
spec and code rely on, and TLC finds the race in under a minute.

## What is modeled

Processes (all interleave at the granularity of one atomic step):

- **Uploaders** (2): `PUT /nar/{hash}` as in `upload` and `store_upload`.
  First `already_stored`, claim (Upload kind; a held Upload claim means
  `in-progress`, a held Delete claim means 503), second `already_stored`,
  stream, atomic S3 put, `insert_object` transaction (upsert, refs
  replaced, `total_bytes` adjusted by the size delta), then the claim
  drops. Failure branches: client disconnect, stalled body or NarHash
  mismatch before the put (multipart aborted, no blob); a dead handler or
  failed insert after the put (orphan blob, claim dropped). A client
  disconnect after the claim has moved into the insert closure does not
  stop the insert, so it is the normal path. Each upload negotiates a
  subset of the other hash as its references (Negotiation refreshes their
  `pushed_at`) and stores them as the row's refs. Self references occur.
- **GC** (1): `Gc::evict`. Reconcile `total_bytes` against `SUM(file_size)`,
  loop while usage is above the low watermark: snapshot `evictable`
  candidates, claim each (Delete kind; skip if held), `evict_object`
  (re-check referrers, live pins and push grace in the transaction),
  delete blobs, drop claims. A failed blob delete drops the claims and
  leaves orphans.
- **Operator** (1): `garret-admin delete` (all paths claimed up front,
  unconditional row delete, then blob delete) and `prune` (mark from recent
  rows and live pins through references, claim all doomed paths or fail,
  delete rows, then blobs).
- **Puller reader** (1): redirect only if the row exists, fetch the blob
  later.
- **Orphan sweep** (1): snapshot rows and blobs, skip claimed and young
  blobs, claim each orphan, re-check for a row, delete, release.
- **fsck --repair** (1): list S3, then rows; dangling means no listed blob,
  no claim and past the grace; repair is `delete_object` after an `exists`
  check.
- Also: free Negotiation (`missing`), pin, unpin or expiry, two ageing
  actions (push grace and orphan grace), and, only in `fsck_race.cfg`, a
  manually deleted S3 object.

State: `rows`, `rsize` (`file_size`), `refs` (`object_refs`), `pins`,
`total` (`stats.total_bytes`), `blobs` (S3 keys), `claims` (the in-flight
map: none, Upload, Delete), `recent` (`pushed_at` inside the push grace),
`young` (younger than `orphan_grace_secs`).

## What is abstracted away

- Content, hashes, signatures, OIDC, HTTP status codes, `429` shedding,
  `Retry-After`. A refused or retried request is a return to idle.
- Multipart internals: put and complete are one atomic step per key,
  which matches S3. `abort_stale_multiparts` is not modeled.
- SQLite is a set of rows. Each transaction is one step. The Pusher's
  single writer connection mutex is implied by that atomicity. Prune holds
  the writer lock from mark to last delete, so it is one step here; its
  500-row batches and referrers-first order (which keep the cache closed
  between commits for the Puller) are covered by the unit tests in
  `db.rs`, not by this model. A scoped `delete` deletes its rows in one
  step instead of one transaction per hash.
- Time is two sets of flags and an ageing action. Assumptions: a push that
  negotiated a dependency finishes before the 24 h push grace ends (the
  spec's 23 h argument), so `AgeRecent` is disabled for a dependency an
  upload relies on; an fsck run is shorter than the orphan grace.
  `pushed_at` debouncing is dropped (a debounced path is still inside the
  grace). LRU order is dropped: any candidate order is explored.
- Size is 1 or 2 bytes; `High = 1`, `Low = 0`. Pin expiry is an unpin.
- Puller `last_accessed_at` bumps never change rows' existence and are not
  modeled. Resign, browse and quiesce are not modeled.
- Process crashes (the in-memory claim map is lost) are not modeled
  beyond the failure branches above: after a restart nothing is in
  flight, and the sweep and fsck handle the leftovers.
- Push order (a modeling assumption, not a system property): an upload
  starts only once its references are rows. The client does not promise
  this (with `jobs > 1` a referrer can finish before its references, and a
  failed reference does not hold back its referrers; `push.rs`), and
  `insert_object` does not check it. `ClosureIntact` therefore holds only
  for pushes in dependency order.
- fsck's `--verify-sizes` branch is not modeled: blobs have no sizes here,
  so `FsRows` never finds a size mismatch. That branch's guards (in flight,
  younger than the grace) are covered by the unit tests in `fsck.rs`.
- Bounds: `Budget` limits how many activities start (`Done` is the
  terminal state, which makes TLC's deadlock check meaningful),
  `MaxFaults` limits injected failures.

## Properties

Checked as state invariants unless noted. The statement comes from the
spec; the model's name is first.

| Name | Statement | Source |
|---|---|---|
| `RowImpliesBlob` | every row has its blob, in every state | spec 02 "Invariant: row exists => blob exists" |
| `BlobSurvivesClaim` | while an upload holds the claim after its put, the blob exists and the claim is still its own; at commit the row exists | spec 05 "Deletion claims" |
| `ClaimExclusive` | at most one upload per hash is past the claim | `inflight.rs` "one claim per path" |
| `QuotaCounter` | `total_bytes` equals `SUM(file_size)` after every step (every step is a committed transaction) | spec 02, 05 "Trigger and accounting" |
| `EvictionSafe` | GC never deletes a row that has a surviving referrer (self-edges aside), a live pin or a push inside the grace | spec 05 Eviction, glossary |
| `PruneSafe` | prune never deletes a recent row, a live pin, or anything a surviving row references | spec 05 Prune |
| `ClosureIntact` | every reference of a surviving row that is not a row was removed by an explicit operator `delete` (unconditional by design). Holds only under the modeling assumption that a referrer is pushed after its references are rows (see "What is abstracted away") | glossary Eviction, "closure stays complete" |
| `GcCountdown` | a pass never evicts at or below the low watermark (and starts only at or above the high one, as `gc.rs` `tick` does) | spec 05 Trigger |
| `FsckSafe` | fsck repair deletes a row only while its blob is missing | spec 05 fsck |
| `OrphanTransient` (liveness, `liveness.cfg`) | a blob with no row and no claim is eventually deleted or adopted by a row (`Orphan(h) ~> (h \notin blobs \/ h \in rows)`; being claimed is not enough) | spec 05 Orphan sweep |
| deadlock | no state with an unfinished activity and nothing enabled | TLC default check |

`TypeOK` and `DelClaimsHeld` check the model itself.

The "dangling rows are what fsck repairs" half of the orphan statement is
checked as an invariant: with no drift injected `RowImpliesBlob` holds, so
dangling rows arise only from outside the protocol.

## Result

`main.cfg` (2 hashes, 2 uploaders, 1 each of GC, operator, reader, sweep,
fsck; `Sizes = {1, 2}`, `High = 1`, `Low = 0`, `Budget = 4`,
`MaxFaults = 1`, symmetry on hashes and uploaders), TLC 2.19 on 4 workers:

```
Model checking completed. No error has been found.
9842399 states generated, 2031386 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 38.
Finished in 48s
```

All invariants above, plus the deadlock check, pass.

`deep.cfg` (`Budget = 5`): no error, 133,417,223 states generated,
23,488,699 distinct, depth 44, 11 min 54 s.

`liveness.cfg` (`-deadlock`, one uploader, `Sizes = {1}`, `Budget = 4`,
periodic sweep, weak fairness on started work, ageing and the sweep, no
symmetry): `OrphanTransient` holds; 6,804,190 states generated, 1,291,338
distinct, depth 38. With the sweep's blob delete made a no-op (a scratch
mutant), TLC reports the property violated, so it checks deletion and not
only the claim.

Mutants (each fails as expected, shortest trace by TLC):

- `mutant_no_delete_claims`: upload of h commits; operator `delete h`
  removes the row and takes no claim; a second upload of h puts the blob;
  the blob delete removes it; `RowImpliesBlob` fails after the second
  insert. This is the race spec 05 "Deletion claims" describes.
- `mutant_no_evict_recheck`: GC snapshots h as a candidate; an upload
  that negotiates h as a dependency starts; GC deletes h anyway;
  `EvictionSafe` fails.
- `mutant_no_sweep_recheck`: an upload fails after its put, leaving an
  orphan blob that ages; the sweep lists it; an upload of h completes
  (row and new blob); the sweep claims h without a row check and deletes
  the blob; `RowImpliesBlob` fails.

## Findings

Neither finding breaks `RowImpliesBlob`, `BlobSurvivesClaim`,
`QuotaCounter`, `EvictionSafe` or `PruneSafe`.

### 1. fsck --repair could delete a healthy, freshly pushed row (fixed)

Fixed: `repair_rows` now deletes a row only if its `created_at` is the
one fsck listed. A re-push rewrites `created_at`, so a row written since
the listing is skipped (regression test
`a_row_pushed_again_since_the_listing_is_not_repaired`). The model's
`FsckRecheck` constant is that check. `Budget = 5` is the smallest budget
at which the old code fails (`fsck_race.cfg`, 23 s); at that budget the
fixed model passes (`fsck.cfg`: 24,928,665 states generated, 4,742,194
distinct, depth 44, 1 min 27 s), and also at `Budget = 6` (180,924,482
generated, 30,127,737 distinct, depth 46, 9 min 28 s). The counterexample
below is the old code's.

Before the fix, in the code: `repair_rows` in `crates/garret-pusher/src/fsck.rs`
re-checks only `db::exists(hash)` and then calls `db::delete_object`,
without a claim and without repeating the age and blob checks. Spec 05
says "A row gone since the listing (GC evicted it, say) is skipped", but
a row that was deleted and then pushed again since the listing is not
gone, so it is repaired.

Counterexample (21 states, TLC, found by the original `Budget = 9` run; `u1` uploads, the operator delete stands
in for a GC eviction, which is equivalent for this race):

1. `u1` uploads h: blob and row exist.
2. The row ages past the orphan grace.
3. The S3 object for h is deleted manually (the drift fsck exists for).
4. fsck lists S3 (h absent), then lists rows: h is old, unclaimed, has no
   listed blob, so it is classified dangling.
5. `garret-admin delete h` (a GC eviction would do the same): claim, row
   delete, blob delete (`NoSuchKey`), claim released.
6. `u1` uploads h again: claim, recheck (no row), put, insert, release.
   Row and blob are healthy and young.
7. fsck repair: the row exists, so `delete_object(h)` removes it. The
   good blob stays.

Effect: a good object disappears from the cache until it is pushed again.
The blob is left for the orphan sweep (it waits one `orphan_grace_secs`
because it is young). No row without a blob results, the counter stays
correct, and a Negotiation after the repair reports h missing, so the next
push restores it. The window needed fsck to run at the same time as a delete and a re-push,
so it is narrow.

### 2. A Puller redirect can outlive the blob (`redirect_window.cfg`)

Not a violation of the stated invariant, and not acknowledged by the
specs: the Puller checks that the row exists and then presigns a URL
(default TTL 1 h). If GC, `delete` or prune removes the row and blob
after the check, the client's fetch from S3 returns 404.
`redirect_window.cfg` shows it: `RedirectWindow` fails in 11 steps. A
reader whose row survives always finds the blob (`RowImpliesBlob`).
Nix treats the miss as a cache miss (spec 03, "degrade to a miss").
Also, `last_accessed_at` is bumped only when 24 h stale, so a path read a
minute ago can still be the least recently accessed.

### Other notes

- `garret-admin delete` is unconditional by design (comment in `admin.rs`;
  not in the specs). It can leave a surviving row whose reference was
  removed, so `ClosureIntact` excludes hashes it removed.
- `MAX(0, total - freed)` in the debits never clamps in any explored
  state: `QuotaCounter` holds exactly.
