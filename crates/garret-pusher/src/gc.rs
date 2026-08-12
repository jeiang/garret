//! Quota + LRU garbage collection, root-first and closure-safe (spec 05-gc).
//! Runs inside the Pusher: single-writer discipline holds, and the orphan
//! sweep can consult in-memory in-flight upload state.

use std::{
    sync::{Arc, Mutex},
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
use rusqlite::Connection;

/// Candidates are re-queried after each batch: deleting a root frees its
/// dependencies, which the next query then surfaces.
const BATCH: usize = 500;

pub struct Gc {
    pub conn: Arc<Mutex<Connection>>,
    pub storage: Storage,
    pub in_flight: InFlight,
    pub cfg: GcConfig,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct PassResult {
    pub evicted: usize,
    pub bytes_freed: i64,
    /// True when usage is still above the low watermark but nothing is
    /// evictable — everything left is referenced. Alarm, do not break closures.
    pub candidates_exhausted: bool,
}

impl Gc {
    /// Every tick is a cheap counter check; eviction only happens past the
    /// high watermark (spec 05).
    pub async fn tick(&self) -> Result<Option<PassResult>> {
        let usage = {
            let conn = self.conn.lock().unwrap();
            db::total_bytes(&conn)?
        };
        metrics::gauge!("garret_gc_usage_bytes").set(usage as f64);
        metrics::gauge!("garret_gc_quota_bytes").set(self.cfg.quota_bytes as f64);

        if usage < self.cfg.high() {
            return Ok(None);
        }
        Ok(Some(self.run().await?))
    }

    /// One eviction pass: loops until usage reaches the low watermark or no
    /// candidate remains. Every surviving root keeps a complete closure.
    pub async fn run(&self) -> Result<PassResult> {
        let started = Instant::now();
        let mut result = PassResult::default();
        let low = self.cfg.low();

        // Correct any drift before deciding how much to delete.
        let mut usage = {
            let conn = self.conn.lock().unwrap();
            db::reconcile_total_bytes(&conn)?
        };

        while usage > low {
            let candidates = {
                let conn = self.conn.lock().unwrap();
                db::evictable(&conn, BATCH, now())?
            };
            if candidates.is_empty() {
                result.candidates_exhausted = true;
                metrics::counter!("garret_gc_candidates_exhausted_total").increment(1);
                tracing::error!(
                    usage,
                    low,
                    "GC cannot reach the low watermark: everything left is referenced"
                );
                break;
            }

            let mut keys = Vec::with_capacity(candidates.len());
            for (hash, _) in &candidates {
                // Row first, blob second: a failed blob delete leaves an orphan
                // for the sweep, never a row without a blob (spec 05).
                let freed = {
                    let mut conn = self.conn.lock().unwrap();
                    db::delete_object(&mut conn, hash)?
                };
                usage -= freed;
                result.evicted += 1;
                result.bytes_freed += freed;
                keys.push(storage::key_for(hash));
                if usage <= low {
                    break;
                }
            }
            self.storage.delete_objects(&keys).await?;
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
        let grace = Duration::from_secs(self.cfg.orphan_grace_secs);
        let known = {
            let conn = self.conn.lock().unwrap();
            db::all_hashes(&conn)?
        };
        let blobs = self.storage.list_blobs().await?;
        let orphans = orphan_keys(&blobs, &known, &self.in_flight, grace);

        let aborted = self
            .storage
            .abort_stale_multiparts(grace, &self.in_flight)
            .await?;
        let count = orphans.len();
        self.storage.delete_objects(&orphans).await?;

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
        let _claim = in_flight.claim("uploading").unwrap();

        assert_eq!(
            orphan_keys(&blobs, &known, &in_flight, grace),
            vec!["nar/stale.nar.zst".to_string()],
        );
    }
}
