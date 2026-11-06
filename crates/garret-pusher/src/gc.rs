//! Quota + LRU garbage collection, root-first and closure-safe (spec 05-gc).
//! Runs inside the Pusher: single-writer discipline holds, and the orphan
//! sweep can consult in-memory in-flight upload state.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use garret_server::{
    config::GcConfig,
    db,
    inflight::InFlight,
    now,
    storage::{self, Storage},
};
use parking_lot::Mutex;
use rusqlite::Connection;

use crate::deletion::Deletion;

/// Candidates are re-queried after each batch: deleting a root frees its
/// dependencies, which the next query then surfaces.
const BATCH: usize = 500;

/// How often the orphan sweep runs after the startup one (spec 05: weekly).
const SWEEP_INTERVAL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

pub struct Gc {
    conn: Arc<Mutex<Connection>>,
    storage: Storage,
    in_flight: InFlight,
    cfg: GcConfig,
    /// Held for a whole eviction pass. The timer and `garret-admin gc run`
    /// share this `Gc`, and a pass counts down from its own reconciled usage,
    /// so two passes at once would each evict to the low watermark as if the
    /// other's deletions had not happened.
    pass: tokio::sync::Mutex<()>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct PassResult {
    pub evicted: usize,
    pub bytes_freed: i64,
    /// True when usage is still above the low watermark but nothing is
    /// evictable — everything left is referenced, pinned, or pushed within
    /// the push grace. Alarm, do not break closures.
    pub candidates_exhausted: bool,
}

impl Gc {
    pub fn new(
        conn: Arc<Mutex<Connection>>,
        storage: Storage,
        in_flight: InFlight,
        cfg: GcConfig,
    ) -> Self {
        Self {
            conn,
            storage,
            in_flight,
            cfg,
            pass: tokio::sync::Mutex::new(()),
        }
    }

    /// The configured quota, for `garret-admin status`.
    pub fn quota_bytes(&self) -> u64 {
        self.cfg.quota_bytes
    }

    /// How old a rowless blob must be before it counts as an orphan.
    pub fn orphan_grace(&self) -> Duration {
        Duration::from_secs(self.cfg.orphan_grace_secs)
    }

    /// The GC schedule (spec 05): an orphan sweep at startup and then every
    /// [`SWEEP_INTERVAL`], and a tick every `interval_secs`.
    pub async fn run_forever(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(Duration::from_secs(self.cfg.interval_secs));
        let mut last_sweep: Option<Instant> = None;
        loop {
            if last_sweep.is_none_or(|at| at.elapsed() >= SWEEP_INTERVAL) {
                last_sweep = Some(Instant::now());
                if let Err(e) = self.sweep_orphans().await {
                    tracing::error!("orphan sweep failed: {e:#}");
                }
            }
            ticker.tick().await;
            if let Err(e) = self.tick().await {
                tracing::error!("GC pass failed: {e:#}");
            }
        }
    }

    /// Every tick is a cheap counter check; eviction only happens past the
    /// high watermark (spec 05). A check that finds nothing to evict is a
    /// successful run too: the timestamp says GC is alive, not that it evicted.
    async fn tick(&self) -> Result<()> {
        let usage = counted(
            "pass",
            crate::db_run(&self.conn, |conn| db::total_bytes(conn)).await,
        )?;
        metrics::gauge!("garret_gc_usage_bytes").set(usage as f64);
        metrics::gauge!("garret_gc_quota_bytes").set(self.cfg.quota_bytes as f64);
        metrics::gauge!("garret_gc_high_watermark_bytes").set(self.cfg.high() as f64);
        metrics::gauge!("garret_gc_low_watermark_bytes").set(self.cfg.low() as f64);

        if usage < self.cfg.high() {
            metrics::gauge!("garret_gc_last_success_timestamp").set(now() as f64);
            return Ok(());
        }
        self.run().await.map(drop)
    }

    /// One eviction pass: loops until usage reaches the low watermark or no
    /// candidate remains. Every surviving root keeps a complete closure. A
    /// pass requested while another runs waits for it, then starts from the
    /// usage it left behind.
    pub async fn run(&self) -> Result<PassResult> {
        let _pass = self.pass.lock().await;
        counted("pass", self.evict().await)
    }

    async fn evict(&self) -> Result<PassResult> {
        let started = Instant::now();
        let mut result = PassResult::default();
        let low = self.cfg.low();

        // Correct any drift before deciding how much to delete.
        let mut usage = crate::db_run(&self.conn, |conn| db::reconcile_total_bytes(conn)).await?;

        while usage > low {
            let at = now();
            let candidates =
                crate::db_run(&self.conn, move |conn| db::evictable(conn, BATCH, at)).await?;
            if candidates.is_empty() {
                result.candidates_exhausted = true;
                metrics::counter!("garret_gc_candidates_exhausted_total").increment(1);
                tracing::error!(
                    usage,
                    low,
                    "GC cannot reach the low watermark: everything left is referenced, \
                     pinned, or pushed within the last day"
                );
                break;
            }

            let mut deletion = Deletion::default();
            let mut settled = false;
            for (hash, _) in &candidates {
                // Being uploaded or deleted elsewhere: not ours to touch.
                let Some(claim) = Deletion::claim_available(&self.in_flight, hash) else {
                    continue;
                };
                settled = true;
                // A candidate negotiated or referenced since the snapshot
                // stays; the next query no longer returns it.
                let owned = hash.clone();
                let Some(freed) =
                    crate::db_run(&self.conn, move |conn| db::evict_object(conn, &owned, at))
                        .await?
                else {
                    continue;
                };
                usage -= freed;
                result.evicted += 1;
                result.bytes_freed += freed;
                deletion.hold(claim);
                if usage <= low {
                    break;
                }
            }
            deletion.finish(&self.storage).await?;
            if !settled {
                // Every candidate is claimed by an upload or deletion; the
                // query would return them all again. The next tick retries.
                tracing::warn!(usage, low, "GC pass stopped: every candidate is in flight");
                break;
            }
        }

        metrics::counter!("garret_gc_evicted_objects_total").increment(result.evicted as u64);
        metrics::counter!("garret_gc_evicted_bytes_total").increment(result.bytes_freed as u64);
        metrics::histogram!("garret_gc_pass_duration_seconds").record(started.elapsed());
        metrics::gauge!("garret_gc_last_success_timestamp").set(now() as f64);
        metrics::gauge!("garret_gc_usage_bytes").set(usage as f64);
        tracing::info!(
            evicted = result.evicted,
            bytes_freed = result.bytes_freed,
            "GC pass complete"
        );
        Ok(result)
    }

    /// Deletes blobs with no row and aborts stale multiparts (spec 05). Both
    /// are only touched past `orphan_grace`, so an upload in progress — whose
    /// row is written only after the blob completes — is never swept away.
    async fn sweep_orphans(&self) -> Result<usize> {
        counted("sweep", self.sweep().await)
    }

    async fn sweep(&self) -> Result<usize> {
        let grace = self.orphan_grace();
        let known = crate::db_run(&self.conn, |conn| db::all_hashes(conn)).await?;
        let blobs = self.storage.list_blobs().await?;
        // Before the orphans are claimed: the abort skips in-flight paths.
        let aborted = self
            .storage
            .abort_stale_multiparts(grace, &self.in_flight)
            .await?;

        // Each orphan is claimed from a final row check until its blob delete
        // returns (spec 05). A path claimed elsewhere is skipped, and one
        // whose row landed since `known` was read is no orphan.
        let deletion = {
            let in_flight = self.in_flight.clone();
            crate::db_run(&self.conn, move |conn| {
                let mut deletion = Deletion::default();
                for hash in orphan_hashes(&blobs, &known, &in_flight, grace) {
                    let Some(claim) = Deletion::claim_available(&in_flight, hash) else {
                        continue;
                    };
                    if db::exists(conn, hash)? {
                        continue;
                    }
                    deletion.hold(claim);
                }
                Ok(deletion)
            })
            .await?
        };
        let count = deletion.len();
        deletion.finish(&self.storage).await?;

        metrics::counter!("garret_gc_orphans_deleted_total").increment(count as u64);
        metrics::counter!("garret_gc_stale_multiparts_aborted_total").increment(aborted as u64);
        metrics::gauge!("garret_gc_last_sweep_timestamp_seconds").set(now() as f64);
        if count > 0 || aborted > 0 {
            tracing::info!(
                orphans = count,
                multiparts = aborted,
                "orphan sweep complete"
            );
        }
        Ok(count)
    }
}

/// Counts a failed pass or sweep (spec 08): the timer loop only logs them,
/// and a log line is nothing an alert can watch.
fn counted<T>(phase: &'static str, result: Result<T>) -> Result<T> {
    if result.is_err() {
        metrics::counter!("garret_gc_failures_total", "phase" => phase).increment(1);
    }
    result
}

/// Store path hashes of blobs with no DB row, past both guards: an upload in
/// the in-flight set, or younger than `grace` — a row lands only after its
/// blob completes, so a rowless blob might just be mid-push. Shared with
/// `garret-admin fsck`'s read-only mirror-image check.
pub(crate) fn orphan_hashes<'a>(
    blobs: &'a [(String, Duration, i64)],
    known: &std::collections::HashSet<String>,
    in_flight: &InFlight,
    grace: Duration,
) -> Vec<&'a str> {
    blobs
        .iter()
        .filter_map(|(key, age, _size)| {
            let hash = storage::hash_for(key)?;
            if known.contains(hash) || in_flight.contains(hash) || *age < grace {
                return None;
            }
            Some(hash)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use garret_server::inflight;
    use garret_server::testing::{S3_DELETED, fake_s3, gc_config, object, unreachable_storage};

    #[test]
    fn orphan_hashes_excludes_known_in_flight_and_young_blobs() {
        let grace = Duration::from_secs(86400);
        let blobs = vec![
            (
                "nar/known.nar.zst".to_string(),
                Duration::from_secs(90000),
                1,
            ),
            (
                "nar/uploading.nar.zst".to_string(),
                Duration::from_secs(90000),
                1,
            ),
            ("nar/young.nar.zst".to_string(), Duration::from_secs(10), 1),
            (
                "nar/stale.nar.zst".to_string(),
                Duration::from_secs(90000),
                1,
            ),
        ];
        let known: std::collections::HashSet<String> = ["known".to_string()].into();
        let in_flight = InFlight::new();
        let _claim = in_flight.claim("uploading").unwrap();

        assert_eq!(
            orphan_hashes(&blobs, &known, &in_flight, grace),
            vec!["stale"]
        );
    }

    /// An S3 stand-in that acknowledges every `DeleteObjects`, holding the
    /// first until the returned sender fires: it parks a GC pass between its
    /// row deletes and its blob delete, where the pass yields.
    async fn gated_s3() -> (
        Storage,
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (held_tx, held_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let gate = Arc::new(Mutex::new(Some((held_tx, release_rx))));
        let storage = fake_s3(axum::Router::new().fallback(move || {
            let gate = gate.lock().take();
            async move {
                if let Some((held, release)) = gate {
                    let _ = held.send(());
                    let _ = release.await;
                }
                S3_DELETED
            }
        }))
        .await;
        (storage, held_rx, release_tx)
    }

    /// The process-wide recorder, installed on first use and shared by every
    /// test in this binary.
    static METRICS: std::sync::LazyLock<metrics_exporter_prometheus::PrometheusHandle> =
        std::sync::LazyLock::new(|| garret_server::metrics::install("garret-pusher").unwrap());

    fn gc_failures(phase: &str) -> u64 {
        let series = format!("garret_gc_failures_total{{phase=\"{phase}\"}} ");
        METRICS
            .render()
            .lines()
            .find_map(|line| line.strip_prefix(&series)?.parse().ok())
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn failed_passes_and_sweeps_are_counted() {
        // Failures used to be logged only, which no alert can watch.
        let (passes, sweeps) = (gc_failures("pass"), gc_failures("sweep"));
        let mut conn = db::open(":memory:", true).unwrap();
        db::migrate(&conn).unwrap();
        db::insert_object(&mut conn, &object(&"a".repeat(32)).sized(100), 0).unwrap();
        let gc = Gc::new(
            Arc::new(Mutex::new(conn)),
            unreachable_storage().await,
            InFlight::new(),
            gc_config(100),
        );

        // Over quota, so the pass evicts and then fails on the blob delete.
        assert!(gc.run().await.is_err());
        assert!(gc.sweep_orphans().await.is_err());
        assert!(gc_failures("pass") > passes, "failed pass not counted");
        assert!(gc_failures("sweep") > sweeps, "failed sweep not counted");
    }

    #[tokio::test]
    async fn a_pass_keeps_what_negotiation_just_reported_present_and_alarms() {
        // Over quota with one candidate, d. A push negotiates d, is told it
        // is present, and will upload d's referrers next: evicting d now
        // would leave them with a hole. The pass must stop and alarm instead.
        let mut conn = db::open(":memory:", true).unwrap();
        db::migrate(&conn).unwrap();
        let d = "d".repeat(32);
        db::insert_object(&mut conn, &object(&d).sized(100), 0).unwrap();
        let reported = db::missing(&mut conn, std::slice::from_ref(&d), now()).unwrap();
        assert!(reported.is_empty(), "d should be reported present");
        let gc = Gc::new(
            Arc::new(Mutex::new(conn)),
            unreachable_storage().await,
            InFlight::new(),
            gc_config(100),
        );

        let pass = gc.run().await.unwrap();
        assert!(db::exists(&gc.conn.lock(), &d).unwrap());
        assert_eq!(pass.evicted, 0);
        assert!(pass.candidates_exhausted, "over quota with no alarm");
    }

    #[tokio::test]
    async fn an_evicted_path_stays_claimed_until_its_blob_is_deleted() {
        // Between the row delete and the blob delete, an upload of the same
        // path would write a blob the pending delete then removes, under the
        // row that upload inserts.
        let mut conn = db::open(":memory:", true).unwrap();
        db::migrate(&conn).unwrap();
        let r = "r".repeat(32);
        db::insert_object(&mut conn, &object(&r).sized(100), 0).unwrap();
        let (storage, held, release) = gated_s3().await;
        let in_flight = InFlight::new();
        let gc = Arc::new(Gc::new(
            Arc::new(Mutex::new(conn)),
            storage,
            in_flight.clone(),
            gc_config(100),
        ));

        let pass = tokio::spawn({
            let gc = gc.clone();
            async move { gc.run().await.unwrap() }
        });
        held.await.unwrap();
        assert!(!db::exists(&gc.conn.lock(), &r).unwrap());
        assert_eq!(
            in_flight.claim(&r).err(),
            Some(inflight::Refused::Deleting),
            "an upload got in before the blob delete"
        );

        release.send(()).unwrap();
        assert_eq!(pass.await.unwrap().evicted, 1);
        assert!(in_flight.claim(&r).is_ok());
    }

    /// An S3 stand-in whose bucket holds one blob, `nar/<hash>.nar.zst`, long
    /// past the orphan grace. `on_list` runs as the blobs are listed, and the
    /// first `DeleteObjects` is held as in `gated_s3`.
    async fn orphan_s3(
        hash: &str,
        on_list: impl Fn() + Send + Sync + 'static,
    ) -> (
        Storage,
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (held_tx, held_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let gate = Arc::new(Mutex::new(Some((held_tx, release_rx))));
        let listing = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>test</Name><Prefix>nar/</Prefix><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>{}</Key><LastModified>2000-01-01T00:00:00.000Z</LastModified><Size>1</Size></Contents></ListBucketResult>"#,
            storage::key_for(hash)
        );
        let on_list = Arc::new(on_list);
        let storage = fake_s3(axum::Router::new().fallback(move |uri: axum::http::Uri| {
            let query = uri.query().unwrap_or_default().to_owned();
            let (gate, listing, on_list) = (gate.clone(), listing.clone(), on_list.clone());
            async move {
                if query.contains("list-type=2") {
                    on_list();
                    return listing;
                }
                if query.contains("uploads") {
                    return r#"<?xml version="1.0" encoding="UTF-8"?><ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>test</Bucket><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>"#.to_owned();
                }
                let gate = gate.lock().take();
                if let Some((held, release)) = gate {
                    let _ = held.send(());
                    let _ = release.await;
                }
                S3_DELETED.to_owned()
            }
        }))
        .await;
        (storage, held_rx, release_tx)
    }

    #[tokio::test]
    async fn an_orphan_stays_claimed_until_its_blob_is_deleted() {
        // Between the orphan check and the blob delete, an upload re-pushing
        // the path would land a row whose blob the pending delete removes.
        let o = "o".repeat(32);
        let conn = db::open(":memory:", true).unwrap();
        db::migrate(&conn).unwrap();
        let (storage, held, release) = orphan_s3(&o, || {}).await;
        let in_flight = InFlight::new();
        let gc = Arc::new(Gc::new(
            Arc::new(Mutex::new(conn)),
            storage,
            in_flight.clone(),
            gc_config(100),
        ));

        let sweep = tokio::spawn({
            let gc = gc.clone();
            async move { gc.sweep_orphans().await.unwrap() }
        });
        held.await.unwrap();
        assert_eq!(
            in_flight.claim(&o).err(),
            Some(inflight::Refused::Deleting),
            "an upload got in before the blob delete"
        );

        release.send(()).unwrap();
        assert_eq!(sweep.await.unwrap(), 1);
        assert!(in_flight.claim(&o).is_ok());
    }

    #[tokio::test]
    async fn an_orphan_whose_row_lands_mid_sweep_is_kept() {
        // An upload of the path completes, row and all, after the sweep read
        // the rows but before it claims the path: its blob is no orphan.
        let o = "o".repeat(32);
        let conn = db::open(":memory:", true).unwrap();
        db::migrate(&conn).unwrap();
        let conn = Arc::new(Mutex::new(conn));
        let (storage, mut held, _release) = orphan_s3(&o, {
            let (conn, o) = (conn.clone(), o.clone());
            move || {
                db::insert_object(&mut conn.lock(), &object(&o).sized(1), 0).unwrap();
            }
        })
        .await;
        let gc = Gc::new(conn, storage, InFlight::new(), gc_config(100));

        assert_eq!(gc.sweep_orphans().await.unwrap(), 0);
        assert!(held.try_recv().is_err(), "the blob was deleted");
    }

    #[tokio::test]
    async fn a_pass_whose_every_candidate_is_being_deleted_stops() {
        // `garret-admin delete` holds the only candidate. The candidate query
        // keeps returning it until that delete commits, so a pass that only
        // skipped it would spin.
        let mut conn = db::open(":memory:", true).unwrap();
        db::migrate(&conn).unwrap();
        let r = "r".repeat(32);
        db::insert_object(&mut conn, &object(&r).sized(100), 0).unwrap();
        let in_flight = InFlight::new();
        let _deleting = in_flight.claim_delete(&r).unwrap();
        let gc = Gc::new(
            Arc::new(Mutex::new(conn)),
            unreachable_storage().await,
            in_flight,
            gc_config(100),
        );

        let pass = tokio::time::timeout(Duration::from_secs(5), gc.run())
            .await
            .expect("the pass spun on a claimed candidate")
            .unwrap();
        assert_eq!(pass.evicted, 0);
        assert!(
            !pass.candidates_exhausted,
            "a transient claim raised the alarm"
        );
        assert!(db::exists(&gc.conn.lock(), &r).unwrap());
    }

    #[tokio::test]
    async fn a_pass_requested_mid_pass_does_not_evict_past_the_low_watermark() {
        // Quota 100, low watermark 85. A pinned 70-byte filler plus the chain
        // r → d → y of 10-byte objects: one pass evicts r, then (next batch)
        // d, and stops at 80 with y kept.
        let mut conn = db::open(":memory:", true).unwrap();
        db::migrate(&conn).unwrap();
        let [filler, r, d, y] = ["f", "r", "d", "y"].map(|c| c.repeat(32));
        db::insert_object(&mut conn, &object(&filler).sized(70), 0).unwrap();
        db::pin(&conn, "filler", &filler, None, 0).unwrap();
        let r_object = object(&r).sized(10).referencing(&[&format!("{d}-thing")]);
        db::insert_object(&mut conn, &r_object, 0).unwrap();
        let d_object = object(&d).sized(10).referencing(&[&format!("{y}-thing")]);
        db::insert_object(&mut conn, &d_object, 0).unwrap();
        db::insert_object(&mut conn, &object(&y).sized(10), 0).unwrap();

        let (storage, held, release) = gated_s3().await;
        let gc = Arc::new(Gc::new(
            Arc::new(Mutex::new(conn)),
            storage,
            InFlight::new(),
            gc_config(100),
        ));
        let run = |gc: Arc<Gc>| tokio::spawn(async move { gc.run().await.unwrap() });

        // The timer's pass has deleted r's row and waits on its blob delete
        // when `garret-admin gc run` asks for another.
        let first = run(gc.clone());
        held.await.unwrap();
        let second = run(gc.clone());
        tokio::time::sleep(Duration::from_millis(200)).await;
        release.send(()).unwrap();
        let evicted = first.await.unwrap().evicted + second.await.unwrap().evicted;

        let conn = gc.conn.lock();
        assert!(
            db::exists(&conn, &y).unwrap(),
            "two passes evicted past the low watermark"
        );
        assert_eq!(db::total_bytes(&conn).unwrap(), 80);
        assert_eq!(evicted, 2);
    }
}
