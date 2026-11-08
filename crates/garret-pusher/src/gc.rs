//! Quota + LRU garbage collection, root-first and closure-safe (spec 05-gc).
//! Runs inside the Pusher: single-writer discipline holds, and the orphan
//! sweep can consult in-memory in-flight upload state.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use garret_common::unix_now;
use garret_server::{
    config::GcConfig,
    db,
    inflight::InFlight,
    storage::{self, Storage},
};
use parking_lot::Mutex;
use rusqlite::Connection;

use crate::{deletion::Deletion, state::db_run};

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
    /// [`SWEEP_INTERVAL`], and a tick every `interval_secs`. A failed sweep
    /// is retried on the next tick.
    pub async fn run_forever(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(Duration::from_secs(self.cfg.interval_secs));
        // A slow pass or a retried sweep must not be followed by a burst of missed ticks.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_sweep: Option<Instant> = None;
        loop {
            if last_sweep.is_none_or(|at| at.elapsed() >= SWEEP_INTERVAL) {
                match self.sweep_orphans().await {
                    Ok(_) => last_sweep = Some(Instant::now()),
                    Err(e) => tracing::error!("orphan sweep failed: {e:#}"),
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
            db_run(&self.conn, |conn| db::total_bytes(conn)).await,
        )?;
        metrics::gauge!("garret_gc_usage_bytes").set(usage as f64);
        metrics::gauge!("garret_gc_quota_bytes").set(self.cfg.quota_bytes as f64);
        metrics::gauge!("garret_gc_high_watermark_bytes").set(self.cfg.high() as f64);
        metrics::gauge!("garret_gc_low_watermark_bytes").set(self.cfg.low() as f64);

        if usage < self.cfg.high() {
            metrics::gauge!("garret_gc_last_success_timestamp").set(unix_now() as f64);
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

    #[tracing::instrument(name = "gc.evict", skip_all)]
    async fn evict(&self) -> Result<PassResult> {
        let started = Instant::now();
        let mut result = PassResult::default();
        let low = self.cfg.low();

        let mut usage = db_run(&self.conn, |conn| db::reconcile_total_bytes(conn)).await?;

        while usage > low {
            let at = unix_now();
            let candidates = db_run(&self.conn, move |conn| db::evictable(conn, BATCH, at)).await?;
            if candidates.is_empty() {
                result.candidates_exhausted = true;
                metrics::counter!("garret_gc_candidates_exhausted_total").increment(1);
                tracing::error!(
                    usage,
                    low,
                    "GC cannot reach the low watermark: everything left is referenced, \
                     pinned, or pushed within the last {} hours",
                    garret_common::PUSH_GRACE_HOURS
                );
                break;
            }

            let mut deletion = Deletion::default();
            let mut settled = false;
            for (hash, _) in &candidates {
                let Ok(claim) = self.in_flight.claim_delete(hash) else {
                    continue;
                };
                settled = true;
                // A candidate negotiated or referenced since the snapshot
                // stays; the next query no longer returns it.
                let owned = hash.clone();
                let Some(freed) =
                    db_run(&self.conn, move |conn| db::evict_object(conn, &owned, at)).await?
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
        metrics::gauge!("garret_gc_last_success_timestamp").set(unix_now() as f64);
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

    #[tracing::instrument(name = "gc.sweep", skip_all)]
    async fn sweep(&self) -> Result<usize> {
        let grace = self.orphan_grace();
        // Before the orphans are claimed: the abort skips in-flight paths.
        let aborted = self
            .storage
            .abort_stale_multiparts(grace, &self.in_flight)
            .await?;

        // One sorted pass over the bucket listing and the object rows, a page
        // of each at a time: a blob with no row is an orphan. Neither side is
        // ever fully materialized, and the connection is never held across an
        // S3 await.
        let mut lister = self.storage.list_blobs();
        let mut rows = storage::RowWindow::new();
        let mut deletion = Deletion::default();
        while let Some(page) = lister.next_page().await? {
            let Some(last) = page.last() else { continue };
            while rows.needs_through(&last.hash) {
                let cursor = rows.cursor().map(str::to_owned);
                rows.extend(
                    db_run(&self.conn, move |conn| {
                        db::objects_after(conn, cursor.as_deref())
                    })
                    .await?,
                );
            }
            let orphans: Vec<String> = storage::merge_page(&page, &mut rows)
                .orphans
                .into_iter()
                .filter(|blob| storage::is_orphan(blob, &self.in_flight, grace))
                .map(|blob| blob.hash.clone())
                .collect();
            if orphans.is_empty() {
                continue;
            }

            // Each orphan is claimed from a final row check until its blob
            // delete returns (spec 05). A path claimed elsewhere is skipped,
            // and one whose row landed since the merge read it is no orphan.
            let in_flight = self.in_flight.clone();
            let claims = db_run(&self.conn, move |conn| {
                let mut claims = Vec::new();
                for hash in orphans {
                    let Ok(claim) = in_flight.claim_delete(&hash) else {
                        continue;
                    };
                    if db::exists(conn, &hash)? {
                        continue;
                    }
                    claims.push(claim);
                }
                Ok(claims)
            })
            .await?;
            for claim in claims {
                deletion.hold(claim);
            }
        }
        let count = deletion.len();
        deletion.finish(&self.storage).await?;

        metrics::counter!("garret_gc_orphans_deleted_total").increment(count as u64);
        metrics::counter!("garret_gc_stale_multiparts_aborted_total").increment(aborted as u64);
        metrics::gauge!("garret_gc_last_sweep_timestamp_seconds").set(unix_now() as f64);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::testing::open_db;
    use garret_server::inflight;
    use garret_server::testing::{S3_DELETED, fake_s3, gc_config, object, unreachable_storage};

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
        let (passes, sweeps) = (gc_failures("pass"), gc_failures("sweep"));
        let mut conn = open_db();
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
        let mut conn = open_db();
        let d = "d".repeat(32);
        db::insert_object(&mut conn, &object(&d).sized(100), 0).unwrap();
        let reported = db::missing(&mut conn, std::slice::from_ref(&d), unix_now()).unwrap();
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
        let mut conn = open_db();
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

    /// What S3 answers a `ListMultipartUploads` with no uploads.
    const NO_UPLOADS: &str = r#"<?xml version="1.0" encoding="UTF-8"?><ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>test</Bucket><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>"#;

    /// An S3 stand-in whose bucket lists `keys` in the order given, `per_page`
    /// at a time with continuation tokens, and records the body of every
    /// `DeleteObjects`.
    async fn paged_s3(keys: Vec<String>, per_page: usize) -> (Storage, Arc<Mutex<Vec<String>>>) {
        let keys = Arc::new(keys);
        let deletes = Arc::new(Mutex::new(Vec::new()));
        let storage = fake_s3(axum::Router::new().fallback({
            let deletes = deletes.clone();
            move |uri: axum::http::Uri, body: axum::body::Bytes| {
                let query = uri.query().unwrap_or_default().to_owned();
                let (keys, deletes) = (keys.clone(), deletes.clone());
                async move {
                    if query.contains("uploads") {
                        return NO_UPLOADS.to_owned();
                    }
                    if query.contains("list-type=2") {
                        let start = query
                            .split("continuation-token=p")
                            .nth(1)
                            .and_then(|t| t.split('&').next())
                            .and_then(|t| t.parse().ok())
                            .unwrap_or(0);
                        let end = (start + per_page).min(keys.len());
                        let contents: String = keys[start..end]
                            .iter()
                            .map(|key| {
                                format!(
                                    "<Contents><Key>{key}</Key>\
                                     <LastModified>2000-01-01T00:00:00.000Z</LastModified>\
                                     <Size>1</Size></Contents>"
                                )
                            })
                            .collect();
                        let next = (end < keys.len()).then(|| format!("p{end}"));
                        let truncated = next.is_some();
                        let token = next
                            .map(|t| format!("<NextContinuationToken>{t}</NextContinuationToken>"))
                            .unwrap_or_default();
                        return format!(
                            r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>test</Name><Prefix>nar/</Prefix><KeyCount>{}</KeyCount><MaxKeys>{per_page}</MaxKeys><IsTruncated>{truncated}</IsTruncated>{token}{contents}</ListBucketResult>"#,
                            end - start,
                        );
                    }
                    deletes.lock().push(String::from_utf8_lossy(&body).into_owned());
                    S3_DELETED.to_owned()
                }
            }
        }))
        .await;
        (storage, deletes)
    }

    /// Orphans, known blobs and a dangling row interleave across listing
    /// pages: only the orphans are deleted, and no key is lost or repeated at
    /// a page boundary.
    #[tokio::test]
    async fn a_sweep_merges_across_listing_pages() {
        let [a, b, c, d, e] = ["a", "b", "c", "d", "e"].map(|c| c.repeat(32));
        // Bucket: a, b | c, d, e. Rows: a and c (known), e (dangling).
        let keys = [&a, &b, &c, &d, &e]
            .map(|hash| storage::key_for(hash))
            .to_vec();
        let (storage, deletes) = paged_s3(keys, 2).await;
        let mut conn = open_db();
        for hash in [&a, &c, &e] {
            db::insert_object(&mut conn, &object(hash).sized(1), 0).unwrap();
        }
        let gc = Gc::new(
            Arc::new(Mutex::new(conn)),
            storage,
            InFlight::new(),
            gc_config(100),
        );

        assert_eq!(gc.sweep_orphans().await.unwrap(), 2, "orphans are b and d");
        let deletes = deletes.lock();
        assert_eq!(deletes.len(), 1, "one batched delete");
        for orphan in [&b, &d] {
            assert!(deletes[0].contains(&storage::key_for(orphan)));
        }
        for known in [&a, &c] {
            assert!(
                !deletes[0].contains(&storage::key_for(known)),
                "a known blob was deleted"
            );
        }
        let conn = gc.conn.lock();
        assert!(db::exists(&conn, &e).unwrap(), "a dangling row was deleted");
    }

    /// A listing that is not in key order would make the merge misclassify
    /// every key after the break, so the sweep fails instead of deleting.
    #[tokio::test]
    async fn a_listing_out_of_key_order_fails_the_sweep() {
        let [a, b] = ["a", "b"].map(|c| c.repeat(32));
        let keys = vec![storage::key_for(&b), storage::key_for(&a)];
        let (storage, deletes) = paged_s3(keys, 2).await;
        let gc = Gc::new(
            Arc::new(Mutex::new(open_db())),
            storage,
            InFlight::new(),
            gc_config(100),
        );

        let err = format!("{:#}", gc.sweep_orphans().await.unwrap_err());
        assert!(err.contains("not sorted"), "unexpected error: {err}");
        assert!(
            deletes.lock().is_empty(),
            "a misordered listing still deleted blobs"
        );
    }

    #[tokio::test]
    async fn an_orphan_stays_claimed_until_its_blob_is_deleted() {
        // Between the orphan check and the blob delete, an upload re-pushing
        // the path would land a row whose blob the pending delete removes.
        let o = "o".repeat(32);
        let conn = open_db();
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
        // An upload of the path completes, row and all, after the bucket
        // listed the blob but before the sweep reads the rows: its blob is no
        // orphan.
        let o = "o".repeat(32);
        let conn = open_db();
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
    async fn a_failed_sweep_is_retried_on_the_next_tick() {
        use axum::response::IntoResponse;
        let lists = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (retried_tx, retried_rx) = tokio::sync::oneshot::channel();
        let retried = Arc::new(Mutex::new(Some(retried_tx)));
        let storage = fake_s3(axum::Router::new().fallback({
            let lists = lists.clone();
            move |uri: axum::http::Uri| {
                let query = uri.query().unwrap_or_default().to_owned();
                let (lists, retried) = (lists.clone(), retried.clone());
                async move {
                    if query.contains("uploads") {
                        return r#"<?xml version="1.0" encoding="UTF-8"?><ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>test</Bucket><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>"#.into_response();
                    }
                    if lists.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                        return axum::http::StatusCode::FORBIDDEN.into_response();
                    }
                    if let Some(tx) = retried.lock().take() {
                        let _ = tx.send(());
                    }
                    r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>test</Name><Prefix>nar/</Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>"#.into_response()
                }
            }
        }))
        .await;
        let gc = Arc::new(Gc::new(
            Arc::new(Mutex::new(open_db())),
            storage,
            InFlight::new(),
            gc_config(100),
        ));

        let timer = tokio::spawn(gc.run_forever());
        // The first tick fires at once; the next sweep comes right after it,
        // well before the configured interval.
        tokio::time::timeout(Duration::from_secs(10), retried_rx)
            .await
            .expect("the failed startup sweep was not retried")
            .unwrap();
        timer.abort();
    }

    #[tokio::test]
    async fn a_pass_whose_every_candidate_is_being_deleted_stops() {
        // `garret-admin delete` holds the only candidate. The candidate query
        // keeps returning it until that delete commits, so a pass that only
        // skipped it would spin.
        let mut conn = open_db();
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
        let mut conn = open_db();
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

        // The timer's pass has deleted r's row and waits on its blob delete
        // when `garret-admin gc run` asks for another. Polling the second
        // request once queues it on the pass lock before the first resumes.
        let first = tokio::spawn({
            let gc = gc.clone();
            async move { gc.run().await.unwrap() }
        });
        held.await.unwrap();
        let mut second = std::pin::pin!(gc.run());
        assert!(futures::poll!(second.as_mut()).is_pending());
        release.send(()).unwrap();
        let (first, second) = tokio::join!(first, second);
        let evicted = first.unwrap().evicted + second.unwrap().evicted;

        let conn = gc.conn.lock();
        assert!(
            db::exists(&conn, &y).unwrap(),
            "two passes evicted past the low watermark"
        );
        assert_eq!(db::total_bytes(&conn).unwrap(), 80);
        assert_eq!(evicted, 2);
    }
}
