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
    inflight::{self, InFlight},
    now,
    storage::{self, Storage},
};
use parking_lot::Mutex;
use rusqlite::Connection;

/// Candidates are re-queried after each batch: deleting a root frees its
/// dependencies, which the next query then surfaces.
const BATCH: usize = 500;

pub struct Gc {
    pub conn: Arc<Mutex<Connection>>,
    pub storage: Storage,
    pub in_flight: InFlight,
    pub cfg: GcConfig,
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

    /// Every tick is a cheap counter check; eviction only happens past the
    /// high watermark (spec 05). A check that finds nothing to evict is a
    /// successful run too: the timestamp says GC is alive, not that it evicted.
    pub async fn tick(&self) -> Result<Option<PassResult>> {
        let usage = {
            let conn = self.conn.lock();
            counted("pass", db::total_bytes(&conn))?
        };
        metrics::gauge!("garret_gc_usage_bytes").set(usage as f64);
        metrics::gauge!("garret_gc_quota_bytes").set(self.cfg.quota_bytes as f64);

        if usage < self.cfg.high() {
            metrics::gauge!("garret_gc_last_success_timestamp").set(now() as f64);
            return Ok(None);
        }
        Ok(Some(self.run().await?))
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
        let mut usage = {
            let conn = self.conn.lock();
            db::reconcile_total_bytes(&conn)?
        };

        while usage > low {
            let at = now();
            let candidates = {
                let conn = self.conn.lock();
                db::evictable(&conn, BATCH, at)?
            };
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

            let mut keys = Vec::with_capacity(candidates.len());
            // Held from each row delete until the blob delete returns: an
            // upload of the path in between would write a blob that delete
            // then removes, under the row the upload inserts (spec 05).
            let mut claims = Vec::with_capacity(candidates.len());
            let mut settled = false;
            for (hash, _) in &candidates {
                // Being uploaded or deleted elsewhere: not ours to touch.
                let Ok(claim) = self.in_flight.claim(hash, inflight::Kind::Delete) else {
                    continue;
                };
                settled = true;
                // Row first, blob second: a failed blob delete leaves an orphan
                // for the sweep, never a row without a blob (spec 05). A
                // candidate negotiated or referenced since the snapshot stays;
                // the next query no longer returns it.
                let Some(freed) = ({
                    let mut conn = self.conn.lock();
                    db::evict_object(&mut conn, hash, at)?
                }) else {
                    continue;
                };
                usage -= freed;
                result.evicted += 1;
                result.bytes_freed += freed;
                keys.push(storage::key_for(hash));
                claims.push(claim);
                if usage <= low {
                    break;
                }
            }
            self.storage.delete_objects(&keys).await?;
            drop(claims);
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
    pub async fn sweep_orphans(&self) -> Result<usize> {
        counted("sweep", self.sweep().await)
    }

    async fn sweep(&self) -> Result<usize> {
        let grace = Duration::from_secs(self.cfg.orphan_grace_secs);
        let known = {
            let conn = self.conn.lock();
            db::all_hashes(&conn)?
        };
        let blobs = self.storage.list_blobs().await?;
        // Before the orphans are claimed: the abort skips in-flight paths.
        let aborted = self
            .storage
            .abort_stale_multiparts(grace, &self.in_flight)
            .await?;

        // Each orphan is claimed from a final row check until its blob delete
        // returns: an upload re-pushing the path in between would otherwise
        // land a row whose blob the pending delete then removes (spec 05).
        // A path claimed elsewhere is skipped, and one whose row landed since
        // `known` was read is no orphan.
        let (orphans, claims) = {
            let conn = self.conn.lock();
            let mut orphans = Vec::new();
            let mut claims = Vec::new();
            for key in orphan_keys(&blobs, &known, &self.in_flight, grace) {
                let hash = storage::hash_for(&key).unwrap_or_default();
                let Ok(claim) = self.in_flight.claim(hash, inflight::Kind::Delete) else {
                    continue;
                };
                if db::exists(&conn, hash)? {
                    continue;
                }
                orphans.push(key);
                claims.push(claim);
            }
            (orphans, claims)
        };
        let count = orphans.len();
        self.storage.delete_objects(&orphans).await?;
        drop(claims);

        metrics::counter!("garret_gc_orphans_deleted_total").increment(count as u64);
        metrics::counter!("garret_gc_stale_multiparts_aborted_total").increment(aborted as u64);
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

/// Blob keys with no DB row, past both guards: an upload in the in-flight
/// set, or younger than `grace` — a row lands only after its blob
/// completes, so a rowless blob might just be mid-push. Shared with
/// `garret-admin fsck`'s read-only mirror-image check.
pub(crate) fn orphan_keys(
    blobs: &[(String, Duration, i64)],
    known: &std::collections::HashSet<String>,
    in_flight: &InFlight,
    grace: Duration,
) -> Vec<String> {
    blobs
        .iter()
        .filter_map(|(key, age, _size)| {
            let hash = storage::hash_for(key)?;
            if known.contains(hash) || in_flight.contains(hash) || *age < grace {
                return None;
            }
            Some(key.clone())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orphan_keys_excludes_known_in_flight_and_young_blobs() {
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
        let _claim = in_flight
            .claim("uploading", garret_server::inflight::Kind::Upload)
            .unwrap();

        assert_eq!(
            orphan_keys(&blobs, &known, &in_flight, grace),
            vec!["nar/stale.nar.zst".to_string()],
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
        let app = axum::Router::new().fallback(move || {
            let gate = gate.lock().take();
            async move {
                if let Some((held, release)) = gate {
                    let _ = held.send(());
                    let _ = release.await;
                }
                r#"<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>"#
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (storage_at(&endpoint, 30).await, held_rx, release_tx)
    }

    async fn storage_at(endpoint: &str, timeout_secs: u64) -> Storage {
        Storage::new(&garret_server::config::S3Config {
            bucket: "test".into(),
            endpoint_url: Some(endpoint.into()),
            region: Some("us-east-1".into()),
            path_style: true,
            access_key_id: Some("x".into()),
            secret_access_key: Some("x".into()),
            operation_timeout_secs: timeout_secs,
        })
        .await
        .unwrap()
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
        db::insert_object(&mut conn, &object(&"a".repeat(32), 100, &[]), 0).unwrap();
        let gc = Gc::new(
            Arc::new(Mutex::new(conn)),
            storage_at("http://127.0.0.1:1", 1).await,
            InFlight::new(),
            config(100),
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
        db::insert_object(&mut conn, &object(&d, 100, &[]), 0).unwrap();
        let reported = db::missing(&mut conn, std::slice::from_ref(&d), now()).unwrap();
        assert!(reported.is_empty(), "d should be reported present");
        let gc = Gc::new(
            Arc::new(Mutex::new(conn)),
            storage_at("http://127.0.0.1:1", 1).await,
            InFlight::new(),
            config(100),
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
        db::insert_object(&mut conn, &object(&r, 100, &[]), 0).unwrap();
        let (storage, held, release) = gated_s3().await;
        let in_flight = InFlight::new();
        let gc = Arc::new(Gc::new(
            Arc::new(Mutex::new(conn)),
            storage,
            in_flight.clone(),
            config(100),
        ));

        let pass = tokio::spawn({
            let gc = gc.clone();
            async move { gc.run().await.unwrap() }
        });
        held.await.unwrap();
        assert!(!db::exists(&gc.conn.lock(), &r).unwrap());
        assert_eq!(
            in_flight.claim(&r, inflight::Kind::Upload).err(),
            Some(inflight::Kind::Delete),
            "an upload got in before the blob delete"
        );

        release.send(()).unwrap();
        assert_eq!(pass.await.unwrap().evicted, 1);
        assert!(in_flight.claim(&r, inflight::Kind::Upload).is_ok());
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
        let app = axum::Router::new().fallback(move |uri: axum::http::Uri| {
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
                r#"<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>"#.to_owned()
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (storage_at(&endpoint, 30).await, held_rx, release_tx)
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
            config(100),
        ));

        let sweep = tokio::spawn({
            let gc = gc.clone();
            async move { gc.sweep_orphans().await.unwrap() }
        });
        held.await.unwrap();
        assert_eq!(
            in_flight.claim(&o, inflight::Kind::Upload).err(),
            Some(inflight::Kind::Delete),
            "an upload got in before the blob delete"
        );

        release.send(()).unwrap();
        assert_eq!(sweep.await.unwrap(), 1);
        assert!(in_flight.claim(&o, inflight::Kind::Upload).is_ok());
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
                db::insert_object(&mut conn.lock(), &object(&o, 1, &[]), 0).unwrap();
            }
        })
        .await;
        let gc = Gc::new(conn, storage, InFlight::new(), config(100));

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
        db::insert_object(&mut conn, &object(&r, 100, &[]), 0).unwrap();
        let in_flight = InFlight::new();
        let _deleting = in_flight.claim(&r, inflight::Kind::Delete).unwrap();
        let gc = Gc::new(
            Arc::new(Mutex::new(conn)),
            storage_at("http://127.0.0.1:1", 1).await,
            in_flight,
            config(100),
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

    fn config(quota_bytes: u64) -> GcConfig {
        GcConfig {
            quota_bytes,
            high_watermark: 0.95,
            low_watermark: 0.85,
            interval_secs: 300,
            orphan_grace_secs: 86400,
        }
    }

    fn object(hash: &str, size: i64, refs: &[&str]) -> db::Object {
        db::Object {
            store_path_hash: hash.into(),
            store_path: format!("/nix/store/{hash}-x"),
            name: "x".into(),
            nar_hash: "sha256:x".into(),
            nar_size: size,
            file_hash: "sha256:y".into(),
            file_size: size,
            deriver: None,
            ca: None,
            references: refs.iter().map(|r| format!("{r}-x")).collect(),
            sigs: vec![],
            pushed_by: None,
        }
    }

    #[tokio::test]
    async fn a_pass_requested_mid_pass_does_not_evict_past_the_low_watermark() {
        // Quota 100, low watermark 85. A pinned 70-byte filler plus the chain
        // r → d → y of 10-byte objects: one pass evicts r, then (next batch)
        // d, and stops at 80 with y kept.
        let mut conn = db::open(":memory:", true).unwrap();
        db::migrate(&conn).unwrap();
        let [filler, r, d, y] = ["f", "r", "d", "y"].map(|c| c.repeat(32));
        db::insert_object(&mut conn, &object(&filler, 70, &[]), 0).unwrap();
        db::pin(&conn, "filler", &filler, None, 0).unwrap();
        db::insert_object(&mut conn, &object(&r, 10, &[&d]), 0).unwrap();
        db::insert_object(&mut conn, &object(&d, 10, &[&y]), 0).unwrap();
        db::insert_object(&mut conn, &object(&y, 10, &[]), 0).unwrap();

        let (storage, held, release) = gated_s3().await;
        let gc = Arc::new(Gc::new(
            Arc::new(Mutex::new(conn)),
            storage,
            InFlight::new(),
            config(100),
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
