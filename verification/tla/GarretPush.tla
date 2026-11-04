------------------------------ MODULE GarretPush ------------------------------
(***************************************************************************)
(* Garret push / Negotiation / eviction / delete / prune / sweep / fsck.   *)
(*                                                                         *)
(* Models the concurrency protocol of the Pusher and Puller documented in *)
(* docs/spec/01-push-protocol.md, 02-database.md, 03-storage.md and        *)
(* 05-gc.md, as implemented in crates/garret-server/src/inflight.rs,       *)
(* crates/garret-pusher/src/{main,gc,admin,fsck}.rs and                    *)
(* crates/garret-server/src/db.rs. See README.md for what is abstracted.   *)
(*                                                                         *)
(* S3 is a set of keys (put and delete are atomic per key). SQLite is a    *)
(* set of rows plus side tables; every transaction is one atomic step.     *)
(* The in-flight map is `claims`. Time is two sets of flags that an Age    *)
(* action clears (push grace and orphan grace).                            *)
(*                                                                         *)
(* The Boolean constants DeleteClaims, EvictRecheck and SweepRecheck are   *)
(* TRUE in the real code. Setting one to FALSE builds a mutant that the    *)
(* invariants must reject (see the mutant_*.cfg files).                    *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Hashes,          \* store path hashes (model values)
    Uploaders,       \* concurrent PUT /nar handlers (model values)
    Sizes,           \* possible file_size values of an upload
    High, Low,       \* GC watermarks in bytes (high starts a pass, low ends it)
    Budget,          \* number of activities that may start (bounds the model)
    MaxFaults,       \* injected S3/DB/client failures allowed in one run
    DeleteClaims,    \* deleters take a Delete claim before the row delete
    EvictRecheck,    \* evict_object re-checks the whole predicate in its tx
    SweepRecheck,    \* the orphan sweep re-checks that no row landed
    ExternalFaults,  \* S3 objects may vanish outside the protocol (fsck cases)
    FsckRecheck,     \* fsck repair deletes only the row it listed (created_at)
    PeriodicSweep    \* the sweep starts without spending Budget (liveness)

None == "none"

VARIABLES
    rows,      \* SQLite objects: set of hashes with a row
    rsize,     \* objects.file_size per hash (0 without a row)
    refs,      \* object_refs: references per referrer (basename hashes)
    pins,      \* live pins (an expired pin is a removed pin)
    total,     \* stats.total_bytes
    blobs,     \* S3 keys that exist
    claims,    \* in-flight map: "none" | "up" (Upload) | "del" (Delete)
    recent,    \* hashes with pushed_at inside the push grace
    young,     \* hashes whose blob or row is younger than orphan_grace
    forced,    \* history: hashes removed by an unconditional operator delete
    hist,      \* history flags set by the actions that decide eviction/prune
    faults,    \* failures injected so far
    budget,    \* activities still allowed to start
    up, gc, adm, rd, sw, fs   \* the processes

dbv   == <<rows, rsize, refs, pins, total>>
aux   == <<recent, young, forced, hist, faults, budget>>
procs == <<up, gc, adm, rd, sw, fs>>
vars  == <<dbv, blobs, claims, aux, procs>>

----------------------------------------------------------------------------
\* Helpers

Max0(x) == IF x < 0 THEN 0 ELSE x

RECURSIVE SumOver(_, _)
SumOver(f, S) ==
    IF S = {} THEN 0
    ELSE LET x == CHOOSE y \in S : TRUE IN f[x] + SumOver(f, S \ {x})

RowBytes == SumOver(rsize, rows)       \* SUM(file_size) over objects

\* Rows other than h that list h as a reference. Self-edges are ignored,
\* as in evictable_where! (db.rs).
Referrers(h) == {r \in rows : r # h /\ h \in refs[r]}

\* evictable_where! (db.rs): no surviving referrer, no live pin, not pushed
\* inside the push grace.
Evictable(h) == /\ h \in rows
                /\ Referrers(h) = {}
                /\ h \notin pins
                /\ h \notin recent

\* An uploader that negotiated h as present and still relies on it.
Relying(h) == \E u \in Uploaders :
                 up[u].pc # "idle" /\ h \in (up[u].rs \ {up[u].h})

\* prune's mark: roots are recent rows and live pins; keep their closure.
RECURSIVE Closure(_, _)
Closure(S, n) ==
    IF n = 0 THEN S
    ELSE Closure(S \cup {d \in rows : \E r \in S : d \in refs[r]}, n - 1)
Keep      == Closure({h \in rows : h \in recent \/ h \in pins}, Cardinality(Hashes))
Doomed    == rows \ Keep

Unclaimed(S) == \A h \in S : claims[h] = "none"
\* Claim S as Delete, or do nothing when the mutant has no claims.
TakeDel(S)    == IF DeleteClaims THEN [h \in Hashes |-> IF h \in S THEN "del" ELSE claims[h]] ELSE claims
ReleaseDel(S) == IF DeleteClaims THEN [h \in Hashes |-> IF h \in S THEN "none" ELSE claims[h]] ELSE claims
Free(h)       == [claims EXCEPT ![h] = "none"]

IdleUp == [pc |-> "idle", h |-> None, sz |-> 0, rs |-> {}]

\* One atomic transaction that deletes rows (and, by cascade, their refs and
\* pins) and debits stats.total_bytes. A Puller read that already saw one of
\* them is marked `gone`: its blob may legitimately be missing afterwards.
DelRows(S) ==
    /\ rows'  = rows \ S
    /\ rsize' = [h \in Hashes |-> IF h \in S THEN 0 ELSE rsize[h]]
    /\ refs'  = [h \in Hashes |-> IF h \in S THEN {} ELSE refs[h]]
    /\ pins'  = pins \ S
    /\ total' = Max0(total - SumOver(rsize, S))
    /\ rd'    = IF rd.pc = "fetch" /\ rd.h \in S THEN [rd EXCEPT !.gone = TRUE] ELSE rd

----------------------------------------------------------------------------
Init ==
    /\ rows = {} /\ rsize = [h \in Hashes |-> 0] /\ refs = [h \in Hashes |-> {}]
    /\ pins = {} /\ total = 0
    /\ blobs = {} /\ claims = [h \in Hashes |-> "none"]
    /\ recent = {} /\ young = {} /\ forced = {}
    /\ hist = [evictBad |-> FALSE, pruneBad |-> FALSE, fsckFalse |-> FALSE]
    /\ faults = 0 /\ budget = Budget
    /\ up = [u \in Uploaders |-> IdleUp]
    /\ gc = [pc |-> "idle", cands |-> {}, cur |-> None, evicted |-> {},
             usage |-> 0, settled |-> FALSE]
    /\ adm = [pc |-> "idle", held |-> {}, keys |-> {}]
    /\ rd = [pc |-> "idle", h |-> None, gone |-> FALSE]
    /\ sw = [pc |-> "idle", cand |-> {}, orph |-> {}]
    /\ fs = [pc |-> "idle", lst |-> {}, dang |-> {}]

----------------------------------------------------------------------------
\* Upload: PUT /api/v1/nar/{hash} (crates/garret-pusher/src/main.rs).

\* A client negotiates the dependencies `rs` (each present, so Negotiation
\* refreshes pushed_at) and starts uploading h whose references are `rs`.
\* Modeling assumption, not a system property: a referrer is pushed only
\* after its references are rows. The client does not guarantee it (with
\* jobs > 1 a referrer can land first, and a failed reference does not hold
\* back its referrers; push.rs) and insert_object does not check it, so
\* ClosureIntact holds only for clients that push in dependency order.
UpStart(u, h, sz, rs) ==
    /\ up[u].pc = "idle" /\ budget > 0
    /\ (rs \ {h}) \subseteq rows
    /\ up' = [up EXCEPT ![u] = [pc |-> "check1", h |-> h, sz |-> sz, rs |-> rs]]
    /\ recent' = recent \cup (rs \ {h})
    /\ budget' = budget - 1
    /\ UNCHANGED <<dbv, blobs, claims, young, forced, hist, faults, gc, adm, rd, sw, fs>>

\* already_stored before the claim.
UpCheck1(u) ==
    /\ up[u].pc = "check1"
    /\ up' = [up EXCEPT ![u] = IF up[u].h \in rows THEN IdleUp
                               ELSE [up[u] EXCEPT !.pc = "claim"]]
    /\ UNCHANGED <<dbv, blobs, claims, aux, gc, adm, rd, sw, fs>>

\* in_flight.claim(hash, Upload): first claimer wins; "in-progress" (Upload)
\* and 503 retry (Delete) both end this attempt.
UpClaim(u) ==
    /\ up[u].pc = "claim"
    /\ LET h == up[u].h IN
       IF claims[h] = "none"
       THEN /\ claims' = [claims EXCEPT ![h] = "up"]
            /\ up' = [up EXCEPT ![u].pc = "recheck"]
       ELSE /\ claims' = claims
            /\ up' = [up EXCEPT ![u] = IdleUp]
    /\ UNCHANGED <<dbv, blobs, aux, gc, adm, rd, sw, fs>>

\* already_stored again, now holding the claim.
UpRecheck(u) ==
    /\ up[u].pc = "recheck"
    /\ LET h == up[u].h IN
       IF h \in rows
       THEN /\ claims' = Free(h)
            /\ up' = [up EXCEPT ![u] = IdleUp]
       ELSE /\ claims' = claims
            /\ up' = [up EXCEPT ![u].pc = "stream"]
    /\ UNCHANGED <<dbv, blobs, aux, gc, adm, rd, sw, fs>>

\* Client disconnect, stalled body, NarHash mismatch or an S3 error before
\* the object is complete: the multipart is aborted, no blob, claim dropped.
UpStreamFail(u) ==
    /\ up[u].pc = "stream" /\ faults < MaxFaults
    /\ claims' = Free(up[u].h)
    /\ up' = [up EXCEPT ![u] = IdleUp]
    /\ faults' = faults + 1
    /\ UNCHANGED <<dbv, blobs, recent, young, forced, hist, budget, gc, adm, rd, sw, fs>>

\* PutObject / CompleteMultipartUpload: atomic per key, overwrites.
UpPut(u) ==
    /\ up[u].pc = "stream"
    /\ blobs' = blobs \cup {up[u].h}
    /\ young' = young \cup {up[u].h}
    /\ up' = [up EXCEPT ![u].pc = "insert"]
    /\ UNCHANGED <<dbv, claims, recent, forced, hist, faults, budget, gc, adm, rd, sw, fs>>

\* insert_object: one IMMEDIATE transaction (upsert, refs replaced, stats
\* debited by the previous size). The claim is still held.
UpInsert(u) ==
    /\ up[u].pc = "insert"
    /\ LET h == up[u].h IN
       /\ rows'  = rows \cup {h}
       /\ rsize' = [rsize EXCEPT ![h] = up[u].sz]
       /\ refs'  = [refs EXCEPT ![h] = up[u].rs]
       /\ total' = total + up[u].sz - rsize[h]
       /\ recent' = recent \cup {h}
    /\ up' = [up EXCEPT ![u].pc = "release"]
    /\ UNCHANGED <<pins, blobs, claims, young, forced, hist, faults, budget, gc, adm, rd, sw, fs>>

\* The blob completed but the handler dies before the claim reaches the
\* insert closure, or the insert fails: an orphan blob, claim dropped.
UpInsertFail(u) ==
    /\ up[u].pc = "insert" /\ faults < MaxFaults
    /\ claims' = Free(up[u].h)
    /\ up' = [up EXCEPT ![u] = IdleUp]
    /\ faults' = faults + 1
    /\ UNCHANGED <<dbv, blobs, recent, young, forced, hist, budget, gc, adm, rd, sw, fs>>

\* The Claim guard drops after the commit.
UpRelease(u) ==
    /\ up[u].pc = "release"
    /\ claims' = Free(up[u].h)
    /\ up' = [up EXCEPT ![u] = IdleUp]
    /\ UNCHANGED <<dbv, blobs, aux, gc, adm, rd, sw, fs>>

UpStep(u) == UpCheck1(u) \/ UpClaim(u) \/ UpRecheck(u) \/ UpStreamFail(u)
             \/ UpPut(u) \/ UpInsert(u) \/ UpInsertFail(u) \/ UpRelease(u)

----------------------------------------------------------------------------
\* Negotiation, pins, time.

\* POST /missing-paths for a present path: pushed_at refreshed.
Negotiate(h) ==
    /\ budget > 0 /\ h \in rows
    /\ recent' = recent \cup {h}
    /\ budget' = budget - 1
    /\ UNCHANGED <<dbv, blobs, claims, young, forced, hist, faults, procs>>

Pin(h) ==
    /\ budget > 0 /\ h \in rows
    /\ pins' = pins \cup {h}
    /\ budget' = budget - 1
    /\ UNCHANGED <<rows, rsize, refs, total, blobs, claims, recent, young, forced, hist, faults, procs>>

\* garret-admin unpin, or a pin whose expires_at has passed.
Unpin(h) ==
    /\ budget > 0 /\ h \in pins
    /\ pins' = pins \ {h}
    /\ budget' = budget - 1
    /\ UNCHANGED <<rows, rsize, refs, total, blobs, claims, recent, young, forced, hist, faults, procs>>

\* The push grace (24 h) runs out. A client finishes its push well inside
\* it: the model does not age a path an in-progress upload negotiated.
AgeRecent(h) ==
    /\ h \in recent /\ ~Relying(h)
    /\ recent' = recent \ {h}
    /\ UNCHANGED <<dbv, blobs, claims, young, forced, hist, faults, budget, procs>>

\* The orphan grace (24 h) runs out. An fsck run is shorter than the grace.
AgeYoung(h) ==
    /\ h \in young /\ fs.pc = "idle"
    /\ young' = young \ {h}
    /\ UNCHANGED <<dbv, blobs, claims, recent, forced, hist, faults, budget, procs>>

\* Drift outside the protocol: a manually deleted S3 object (fsck cases).
S3Lose(h) ==
    /\ ExternalFaults /\ budget > 0 /\ h \in blobs
    /\ blobs' = blobs \ {h}
    /\ budget' = budget - 1
    /\ UNCHANGED <<dbv, claims, recent, young, forced, hist, faults, procs>>

----------------------------------------------------------------------------
\* GC pass: eviction under quota (crates/garret-pusher/src/gc.rs `evict`).

\* Timer tick above the high watermark: reconcile stats.total_bytes against
\* SUM(file_size), then count down from that.
GcStart ==
    /\ gc.pc = "idle" /\ budget > 0 /\ total >= High
    /\ total' = RowBytes
    /\ gc' = [gc EXCEPT !.pc = "snap", !.usage = RowBytes, !.evicted = {},
                        !.cands = {}, !.settled = FALSE]
    /\ budget' = budget - 1
    /\ UNCHANGED <<rows, rsize, refs, pins, blobs, claims, recent, young, forced, hist, faults, up, adm, rd, sw, fs>>

\* `while usage > low`, then db::evictable (a snapshot).
GcSnap ==
    /\ gc.pc = "snap"
    /\ LET c == {h \in Hashes : Evictable(h)} IN
       IF gc.usage <= Low \/ c = {}      \* done, or candidates exhausted (alarm)
       THEN gc' = [gc EXCEPT !.pc = "idle"]
       ELSE gc' = [gc EXCEPT !.pc = "pick", !.cands = c, !.settled = FALSE]
    /\ UNCHANGED <<dbv, blobs, claims, aux, up, adm, rd, sw, fs>>

\* Next candidate: skip it when an upload or deletion holds the path,
\* else claim it as Delete.
GcPick ==
    /\ gc.pc = "pick"
    /\ \/ /\ gc.cands = {}
          /\ gc' = [gc EXCEPT !.pc = "blobdel"]
          /\ UNCHANGED claims
       \/ \E c \in gc.cands :
            IF DeleteClaims /\ claims[c] # "none"
            THEN /\ gc' = [gc EXCEPT !.cands = @ \ {c}]
                 /\ UNCHANGED claims
            ELSE /\ gc' = [gc EXCEPT !.cands = @ \ {c}, !.cur = c,
                                      !.pc = "evict", !.settled = TRUE]
                 /\ claims' = TakeDel({c})
    /\ UNCHANGED <<dbv, blobs, aux, up, adm, rd, sw, fs>>

\* db::evict_object: re-check the whole predicate and delete the row, refs,
\* pins and debit the counter in one IMMEDIATE transaction.
GcEvict ==
    /\ gc.pc = "evict"
    /\ LET c  == gc.cur
           ok == IF EvictRecheck THEN Evictable(c) ELSE c \in rows
       IN IF ok
          THEN /\ hist' = [hist EXCEPT !.evictBad = @ \/ ~Evictable(c)]
               /\ DelRows({c})
               /\ gc' = [gc EXCEPT !.pc = "pick", !.cur = None,
                                   !.evicted = @ \cup {c},
                                   !.usage = @ - rsize[c],
                                   !.cands = IF gc.usage - rsize[c] <= Low
                                             THEN {} ELSE @]
               /\ UNCHANGED claims
          ELSE /\ claims' = ReleaseDel({c})    \* stays; the claim is dropped
               /\ gc' = [gc EXCEPT !.pc = "pick", !.cur = None]
               /\ UNCHANGED <<dbv, hist, rd>>
    /\ UNCHANGED <<blobs, recent, young, forced, faults, budget, up, adm, sw, fs>>

\* storage.delete_objects, then the claims drop. A failure drops them too
\* (the `?` unwinds) and leaves the blobs as orphans for the sweep.
GcBlobDelOk ==
    /\ gc.pc = "blobdel"
    /\ blobs' = blobs \ gc.evicted
    /\ claims' = ReleaseDel(gc.evicted)
    /\ gc' = [gc EXCEPT !.pc = IF gc.settled THEN "snap" ELSE "idle", !.evicted = {}]
    /\ UNCHANGED <<dbv, aux, up, adm, rd, sw, fs>>

GcBlobDelFail ==
    /\ gc.pc = "blobdel" /\ faults < MaxFaults
    /\ claims' = ReleaseDel(gc.evicted)
    /\ gc' = [gc EXCEPT !.pc = "idle", !.evicted = {}]
    /\ faults' = faults + 1
    /\ UNCHANGED <<dbv, blobs, recent, young, forced, hist, budget, up, adm, rd, sw, fs>>

GcStep == GcSnap \/ GcPick \/ GcEvict \/ GcBlobDelOk \/ GcBlobDelFail

----------------------------------------------------------------------------
\* garret-admin delete and prune (crates/garret-pusher/src/admin.rs).

\* delete: claim every path up front (any taken: nothing deleted), then
\* delete_object unconditionally for the paths that have a row.
AdmDelete(S) ==
    /\ adm.pc = "idle" /\ budget > 0 /\ S # {}
    /\ ~DeleteClaims \/ Unclaimed(S)
    /\ claims' = TakeDel(S)
    /\ DelRows(S \cap rows)
    /\ forced' = forced \cup (S \cap rows)
    /\ adm' = [pc |-> "blobdel", held |-> S, keys |-> S \cap rows]
    /\ budget' = budget - 1
    /\ UNCHANGED <<blobs, recent, young, hist, faults, up, gc, sw, fs>>

\* prune: mark (recent rows and live pins, closed under references), claim
\* every doomed path or fail, delete the rows under the writer lock.
AdmPrune ==
    /\ adm.pc = "idle" /\ budget > 0 /\ Doomed # {}
    /\ ~DeleteClaims \/ Unclaimed(Doomed)
    /\ claims' = TakeDel(Doomed)
    /\ DelRows(Doomed)
    /\ hist' = [hist EXCEPT !.pruneBad = @ \/
                 \E d \in Doomed : d \in recent \/ d \in pins
                                   \/ \E r \in rows \ Doomed : d \in (refs[r] \ {r})]
    /\ adm' = [pc |-> "blobdel", held |-> Doomed, keys |-> Doomed]
    /\ budget' = budget - 1
    /\ UNCHANGED <<blobs, recent, young, forced, faults, up, gc, sw, fs>>

AdmBlobDelOk ==
    /\ adm.pc = "blobdel"
    /\ blobs' = blobs \ adm.keys
    /\ claims' = ReleaseDel(adm.held)
    /\ adm' = [pc |-> "idle", held |-> {}, keys |-> {}]
    /\ UNCHANGED <<dbv, aux, up, gc, rd, sw, fs>>

AdmBlobDelFail ==
    /\ adm.pc = "blobdel" /\ faults < MaxFaults
    /\ claims' = ReleaseDel(adm.held)
    /\ adm' = [pc |-> "idle", held |-> {}, keys |-> {}]
    /\ faults' = faults + 1
    /\ UNCHANGED <<dbv, blobs, recent, young, forced, hist, budget, up, gc, rd, sw, fs>>

AdmStep == AdmBlobDelOk \/ AdmBlobDelFail

----------------------------------------------------------------------------
\* Puller read (crates/garret-puller/src/main.rs `nar_redirect`): redirect
\* only when the row exists, then the client fetches the blob later.

RdStart(h) ==
    /\ rd.pc = "idle" /\ budget > 0 /\ h \in rows
    /\ rd' = [pc |-> "fetch", h |-> h, gone |-> FALSE]
    /\ budget' = budget - 1
    /\ UNCHANGED <<dbv, blobs, claims, recent, young, forced, hist, faults, up, gc, adm, sw, fs>>

RdFetch ==
    /\ rd.pc = "fetch"
    /\ rd' = [pc |-> "idle", h |-> None, gone |-> FALSE]
    /\ UNCHANGED <<dbv, blobs, claims, aux, up, gc, adm, sw, fs>>

----------------------------------------------------------------------------
\* Orphan sweep (gc.rs `sweep`): list DB rows and blobs, drop what is in
\* flight or younger than the grace, claim each orphan, re-check the row,
\* delete the blobs, release.

SwStart ==
    /\ sw.pc = "idle" /\ (PeriodicSweep \/ budget > 0)
    /\ sw' = [pc |-> "claim", orph |-> {},
              cand |-> {h \in blobs : h \notin rows /\ claims[h] = "none" /\ h \notin young}]
    /\ budget' = IF PeriodicSweep THEN budget ELSE budget - 1
    /\ UNCHANGED <<dbv, blobs, claims, recent, young, forced, hist, faults, up, gc, adm, rd, fs>>

SwClaim ==
    /\ sw.pc = "claim"
    /\ LET ok == {c \in sw.cand : (~DeleteClaims \/ claims[c] = "none")
                                  /\ (~SweepRecheck \/ c \notin rows)}
       IN /\ claims' = TakeDel(ok)
          /\ sw' = [sw EXCEPT !.pc = "del", !.orph = ok]
    /\ UNCHANGED <<dbv, blobs, aux, up, gc, adm, rd, fs>>

SwDelOk ==
    /\ sw.pc = "del"
    /\ blobs' = blobs \ sw.orph
    /\ claims' = ReleaseDel(sw.orph)
    /\ sw' = [pc |-> "idle", cand |-> {}, orph |-> {}]
    /\ UNCHANGED <<dbv, aux, up, gc, adm, rd, fs>>

SwDelFail ==
    /\ sw.pc = "del" /\ faults < MaxFaults
    /\ claims' = ReleaseDel(sw.orph)
    /\ sw' = [pc |-> "idle", cand |-> {}, orph |-> {}]
    /\ faults' = faults + 1
    /\ UNCHANGED <<dbv, blobs, recent, young, forced, hist, budget, up, gc, adm, rd, fs>>

SwStep == SwClaim \/ SwDelOk \/ SwDelFail

----------------------------------------------------------------------------
\* garret-admin fsck --repair (crates/garret-pusher/src/fsck.rs): list S3
\* first, then DB rows; a dangling row is one with no listed blob, no claim
\* and older than the grace; repair is db::delete_object on the row only if
\* it is the one listed (same created_at). A row written since the listing
\* is young, and stays young for the whole run (fsck is shorter than the
\* grace), so `h \notin young` is that check. FsckRecheck = FALSE is the
\* code before the fix: it checked only that a row still exists.

FsStart ==
    /\ fs.pc = "idle" /\ budget > 0
    /\ fs' = [pc |-> "rows", lst |-> blobs, dang |-> {}]
    /\ budget' = budget - 1
    /\ UNCHANGED <<dbv, blobs, claims, recent, young, forced, hist, faults, up, gc, adm, rd, sw>>

FsRows ==
    /\ fs.pc = "rows"
    /\ fs' = [fs EXCEPT !.pc = "repair",
                 !.dang = {h \in rows : h \notin fs.lst /\ claims[h] = "none" /\ h \notin young}]
    /\ UNCHANGED <<dbv, blobs, claims, aux, up, gc, adm, rd, sw>>

FsDone ==
    /\ fs.pc = "repair" /\ fs.dang = {}
    /\ fs' = [pc |-> "idle", lst |-> {}, dang |-> {}]
    /\ UNCHANGED <<dbv, blobs, claims, aux, up, gc, adm, rd, sw>>

FsRepair(h) ==
    /\ fs.pc = "repair" /\ h \in fs.dang
    /\ fs' = [fs EXCEPT !.dang = @ \ {h}]
    /\ IF h \in rows /\ (FsckRecheck => h \notin young)
       THEN /\ hist' = [hist EXCEPT !.fsckFalse = @ \/ h \in blobs]
            /\ DelRows({h})
       ELSE /\ UNCHANGED <<dbv, rd, hist>>
    /\ UNCHANGED <<blobs, claims, recent, young, forced, faults, budget, up, gc, adm, sw>>

FsStep == FsRows \/ FsDone \/ \E h \in Hashes : FsRepair(h)

----------------------------------------------------------------------------
\* Every started activity finished and the budget is spent: the run ends.
\* Without this, TLC's deadlock check could never fire.
Done ==
    /\ budget = 0
    /\ \A u \in Uploaders : up[u].pc = "idle"
    /\ gc.pc = "idle" /\ adm.pc = "idle" /\ rd.pc = "idle"
    /\ sw.pc = "idle" /\ fs.pc = "idle"
    /\ UNCHANGED vars

Next ==
    \/ \E u \in Uploaders : UpStep(u)
    \/ \E u \in Uploaders, h \in Hashes, sz \in Sizes, rs \in SUBSET Hashes : UpStart(u, h, sz, rs)
    \/ \E h \in Hashes : Negotiate(h) \/ Pin(h) \/ Unpin(h) \/ AgeRecent(h)
                         \/ AgeYoung(h) \/ S3Lose(h) \/ RdStart(h)
    \/ GcStart \/ GcStep
    \/ \E S \in SUBSET Hashes : AdmDelete(S)
    \/ AdmPrune \/ AdmStep
    \/ RdFetch
    \/ SwStart \/ SwStep
    \/ FsStart \/ FsStep
    \/ Done

\* Weak fairness for work already started, for time, and for the sweep.
\* Fresh activities (UpStart, GcStart, ...) are not forced.
Fairness ==
    /\ \A u \in Uploaders : WF_vars(UpStep(u))
    /\ WF_vars(GcStep) /\ WF_vars(AdmStep) /\ WF_vars(RdFetch) /\ WF_vars(FsStep)
    /\ WF_vars(SwStart) /\ WF_vars(SwStep)
    /\ \A h \in Hashes : WF_vars(AgeYoung(h)) /\ WF_vars(AgeRecent(h))

Spec     == Init /\ [][Next]_vars
FairSpec == Spec /\ Fairness

----------------------------------------------------------------------------
\* Invariants

MaxSize == CHOOSE m \in Sizes : \A s \in Sizes : s <= m

TypeOK ==
    /\ rows \subseteq Hashes /\ pins \subseteq rows /\ blobs \subseteq Hashes
    /\ \A h \in Hashes : /\ rsize[h] \in 0..MaxSize
                         /\ refs[h] \subseteq Hashes
                         /\ claims[h] \in {"none", "up", "del"}
                         /\ (h \notin rows => rsize[h] = 0 /\ refs[h] = {})
    /\ total \in Nat /\ faults \in 0..MaxFaults /\ budget \in 0..Budget

\* spec 02: "row exists => blob exists", with no window. It holds in every
\* state, so in particular at every point a Puller read can observe a row.
\* (Not meaningful with ExternalFaults: that is the drift fsck repairs.)
RowImpliesBlob == rows \subseteq blobs

\* spec 03/05: the Puller redirects only when the row exists, but a row
\* deleted after that check takes its blob with it, so the redirect (or
\* presigned URL) can outlive the blob. Expected to FAIL
\* (redirect_window.cfg): it demonstrates that window, which the spec does
\* not mention and which ends as a 404 from S3, a cache miss. (A read whose
\* row survives always finds the blob: that is RowImpliesBlob.)
RedirectWindow == (rd.pc = "fetch" /\ rd.gone) => rd.h \in blobs

\* spec 05 "Deletion claims": nothing deletes the blob of an upload that
\* holds the claim and can still commit; the claim is exclusive.
InsertStages == {"recheck", "stream", "insert", "release"}
BlobSurvivesClaim ==
    \A u \in Uploaders :
        up[u].pc \in {"insert", "release"} =>
            /\ up[u].h \in blobs
            /\ claims[up[u].h] = "up"
            /\ (up[u].pc = "release" => up[u].h \in rows)
ClaimExclusive ==
    \A u \in Uploaders :
        up[u].pc \in InsertStages =>
            /\ claims[up[u].h] = "up"
            /\ \A v \in Uploaders \ {u} : ~(up[v].pc \in InsertStages /\ up[v].h = up[u].h)
\* Sanity of the model itself: every Delete claim has a holder.
DelClaimsHeld ==
    DeleteClaims =>
        \A h \in Hashes : claims[h] = "del" =>
            h \in (gc.evicted \cup {gc.cur} \cup adm.held \cup sw.orph)

\* spec 02/05: stats.total_bytes is updated in the transaction that changes
\* the rows, so it equals SUM(file_size) after every commit.
QuotaCounter == total = RowBytes

\* spec 05: eviction only removes an object with no surviving referrer, no
\* live pin and no push inside the push grace (the flag is set at the
\* instant GC deletes a row for which that predicate is false).
EvictionSafe == ~hist.evictBad
\* spec 05: prune keeps recent pushes, live pins and their closure.
PruneSafe == ~hist.pruneBad
\* The pass counts down from its own reconcile and stops at the low
\* watermark: it never evicts at or below it.
GcCountdown == gc.pc = "evict" => gc.usage > Low
\* "Every closure the cache still serves remains complete": every
\* reference of a surviving row that is not itself a row was removed by an
\* explicit operator `delete`, which is unconditional by design.
ClosureIntact ==
    \A r \in rows : \A d \in refs[r] \ {r} : d \in rows \/ d \in forced
\* fsck --repair deletes a row only while the blob is really missing.
FsckSafe == ~hist.fsckFalse

\* spec 05: a blob with no row and no claim is transient. The orphan sweep
\* deletes it once the orphan grace has passed; an upload may also adopt it.
Orphan(h) == h \in blobs /\ h \notin rows /\ claims[h] = "none"
\* Leaving Orphan(h) by being claimed is not enough: the sweep must delete
\* the blob, or an upload adopt it.
OrphanTransient == \A h \in Hashes : Orphan(h) ~> (h \notin blobs \/ h \in rows)

\* Symmetry reduction for safety runs (not for the liveness run).
Sym == Permutations(Hashes) \cup Permutations(Uploaders)

=============================================================================
