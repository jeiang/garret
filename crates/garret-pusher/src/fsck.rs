//! `garret-admin fsck` (spec 02/03): verifies row ⇒ blob and reports/repairs
//! drift. Runs inside the Pusher so repair can reuse [`db::delete_object`]
//! under the single-writer connection, exactly like `resign`/`delete`
//! (spec 10-packaging).

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};

use anyhow::Result;
use garret_common::admin::{FsckRow, FsckSizeMismatch, Response};
use garret_server::{db, inflight::InFlight, now, storage};
use rusqlite::Connection;

use crate::{AppState, gc};

/// Ceiling on how long `--quiesce` waits for in-flight uploads to drain.
/// ponytail: fixed timeout, add a CLI/config knob if an operator ever needs
/// to wait longer than 5 minutes for uploads to drain.
const QUIESCE_TIMEOUT: Duration = Duration::from_secs(300);
const QUIESCE_POLL: Duration = Duration::from_millis(250);

/// Runs one fsck pass: lists S3 keys, then DB rows (in that order — a push
/// landing in the gap between the two listings looks exactly like the
/// dangling-row race the in-flight/age guards exist to close), classifies
/// the difference, and repairs if asked.
pub async fn run(
    state: &AppState,
    gc: &gc::Gc,
    repair: bool,
    verify_sizes: bool,
    quiesce: bool,
) -> Result<Response> {
    metrics::counter!("garret_fsck_runs_total").increment(1);

    let quiesce_drained = if quiesce {
        state.quiescing.store(true, Ordering::SeqCst);
        wait_for_drain(&state.in_flight, QUIESCE_TIMEOUT, QUIESCE_POLL).await
    } else {
        true
    };
    // Always cleared, even if classification below returns early via `?` —
    // a wedged flag would reject pushes forever.
    let _clear_quiescing = quiesce.then(|| ClearOnDrop(state));

    let grace = Duration::from_secs(gc.cfg.orphan_grace_secs);
    let blobs = state.storage.list_blobs().await?;
    let db_rows = {
        let conn = state.conn.lock().unwrap();
        db::all_objects_brief(&conn)?
    };

    let (dangling, size_mismatches) = classify_rows(
        &db_rows,
        &blobs,
        &state.in_flight,
        grace,
        now(),
        verify_sizes,
    );
    let known: HashSet<String> = db_rows.iter().map(|r| r.0.clone()).collect();
    let orphans = gc::orphan_keys(&blobs, &known, &state.in_flight, grace);

    metrics::gauge!("garret_fsck_findings", "kind" => "dangling").set(dangling.len() as f64);
    metrics::gauge!("garret_fsck_findings", "kind" => "orphan").set(orphans.len() as f64);
    metrics::gauge!("garret_fsck_findings", "kind" => "size_mismatch")
        .set(size_mismatches.len() as f64);

    // A timed-out drain means the live signal was never trustworthy enough
    // to delete rows on — report the findings, but repair nothing.
    let repaired = if repair && quiesce_drained {
        apply_repair(&state.conn, &dangling, &size_mismatches)?
    } else {
        0
    };

    Ok(Response::Fsck {
        dangling,
        size_mismatches,
        orphans,
        repaired,
        quiesced: quiesce,
        quiesce_drained,
    })
}

/// Splits DB rows into dangling (no blob, past both guards) and
/// size-mismatched (blob exists, sizes disagree — `verify_sizes` only). A
/// hash with a size mismatch is never also reported dangling.
fn classify_rows(
    db_rows: &[(String, String, i64, i64)],
    blobs: &[(String, Duration, i64)],
    in_flight: &InFlight,
    grace: Duration,
    now: i64,
    verify_sizes: bool,
) -> (Vec<FsckRow>, Vec<FsckSizeMismatch>) {
    let s3_sizes: HashMap<&str, i64> = blobs
        .iter()
        .filter_map(|(key, _age, size)| storage::hash_for(key).map(|h| (h, *size)))
        .collect();

    let mut dangling = Vec::new();
    let mut size_mismatches = Vec::new();
    for (hash, name, created_at, file_size) in db_rows {
        match s3_sizes.get(hash.as_str()) {
            None => {
                if in_flight.contains(hash) || now - created_at < grace.as_secs() as i64 {
                    continue;
                }
                dangling.push(FsckRow {
                    store_path_hash: hash.clone(),
                    name: name.clone(),
                });
            }
            Some(&s3_size) if verify_sizes && s3_size != *file_size => {
                size_mismatches.push(FsckSizeMismatch {
                    store_path_hash: hash.clone(),
                    name: name.clone(),
                    db_size: *file_size,
                    s3_size,
                });
            }
            Some(_) => {}
        }
    }
    (dangling, size_mismatches)
}

/// Deletes every dangling and size-mismatched row (`db::delete_object`,
/// row+refs+stats in one transaction each — never the S3 object, which is
/// left for the orphan sweep by design). Returns the count actually
/// deleted.
fn apply_repair(
    conn: &Arc<Mutex<Connection>>,
    dangling: &[FsckRow],
    size_mismatches: &[FsckSizeMismatch],
) -> Result<usize> {
    for row in dangling {
        let mut conn = conn.lock().unwrap();
        db::delete_object(&mut conn, &row.store_path_hash)?;
    }
    metrics::counter!("garret_fsck_rows_repaired_total", "reason" => "dangling")
        .increment(dangling.len() as u64);

    for row in size_mismatches {
        let mut conn = conn.lock().unwrap();
        db::delete_object(&mut conn, &row.store_path_hash)?;
    }
    metrics::counter!("garret_fsck_rows_repaired_total", "reason" => "size_mismatch")
        .increment(size_mismatches.len() as u64);

    Ok(dangling.len() + size_mismatches.len())
}

/// Polls until no upload is in progress or `timeout` elapses.
async fn wait_for_drain(in_flight: &InFlight, timeout: Duration, poll: Duration) -> bool {
    tokio::time::timeout(timeout, async {
        while !in_flight.is_empty() {
            tokio::time::sleep(poll).await;
        }
    })
    .await
    .is_ok()
}

/// RAII: clears the quiescing flag no matter how `run` returns, so an
/// error partway through classification cannot wedge pushes rejected
/// forever. Mirrors [`garret_server::inflight::Claim`]'s drop-releases
/// pattern.
struct ClearOnDrop<'a>(&'a AppState);

impl Drop for ClearOnDrop<'_> {
    fn drop(&mut self) {
        self.0.quiescing.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(hash: &str, name: &str, created_at: i64, file_size: i64) -> (String, String, i64, i64) {
        (hash.to_string(), name.to_string(), created_at, file_size)
    }

    fn blob(hash: &str, age_secs: u64, size: i64) -> (String, Duration, i64) {
        (storage::key_for(hash), Duration::from_secs(age_secs), size)
    }

    const GRACE: Duration = Duration::from_secs(86400);
    const NOW: i64 = 1_000_000;

    #[test]
    fn a_genuinely_dangling_row_is_reported() {
        let rows = vec![row("a", "thing", NOW - 2 * 86400, 5)];
        let blobs = vec![];
        let in_flight = InFlight::new();
        let (dangling, mismatches) = classify_rows(&rows, &blobs, &in_flight, GRACE, NOW, false);
        assert_eq!(
            dangling,
            vec![FsckRow {
                store_path_hash: "a".into(),
                name: "thing".into()
            }]
        );
        assert!(mismatches.is_empty());
    }

    #[test]
    fn an_in_flight_upload_is_not_reported_dangling() {
        let rows = vec![row("a", "thing", NOW - 2 * 86400, 5)];
        let blobs = vec![];
        let in_flight = InFlight::new();
        let _claim = in_flight.claim("a").unwrap();
        let (dangling, _) = classify_rows(&rows, &blobs, &in_flight, GRACE, NOW, false);
        assert!(dangling.is_empty(), "in-flight upload reported as dangling");
    }

    #[test]
    fn a_row_younger_than_the_grace_period_is_not_reported_dangling() {
        let rows = vec![row("a", "thing", NOW - 10, 5)];
        let blobs = vec![];
        let in_flight = InFlight::new();
        let (dangling, _) = classify_rows(&rows, &blobs, &in_flight, GRACE, NOW, false);
        assert!(dangling.is_empty(), "young row reported as dangling");
    }

    #[test]
    fn a_size_mismatch_is_its_own_category_only_when_verify_sizes_is_set() {
        let rows = vec![row("a", "thing", NOW - 2 * 86400, 5)];
        let blobs = vec![blob("a", 90000, 9)];
        let in_flight = InFlight::new();

        let (dangling, mismatches) = classify_rows(&rows, &blobs, &in_flight, GRACE, NOW, false);
        assert!(dangling.is_empty());
        assert!(mismatches.is_empty(), "no size check without verify_sizes");

        let (dangling, mismatches) = classify_rows(&rows, &blobs, &in_flight, GRACE, NOW, true);
        assert!(dangling.is_empty(), "mismatch double-reported as dangling");
        assert_eq!(
            mismatches,
            vec![FsckSizeMismatch {
                store_path_hash: "a".into(),
                name: "thing".into(),
                db_size: 5,
                s3_size: 9,
            }]
        );
    }

    #[test]
    fn matching_rows_are_reported_in_neither_category() {
        let rows = vec![row("a", "thing", NOW - 2 * 86400, 5)];
        let blobs = vec![blob("a", 90000, 5)];
        let in_flight = InFlight::new();
        let (dangling, mismatches) = classify_rows(&rows, &blobs, &in_flight, GRACE, NOW, true);
        assert!(dangling.is_empty());
        assert!(mismatches.is_empty());
    }

    /// The full classify → repair round trip against a real in-memory
    /// connection, not fabricated tuples: a dangling row is found, deleted,
    /// and gone from the database.
    #[test]
    fn end_to_end_against_a_real_in_memory_connection() {
        let mut conn = db::open(":memory:", true).unwrap();
        db::migrate(&conn).unwrap();
        let hash = "a".repeat(32);
        db::insert_object(
            &mut conn,
            &db::Object {
                store_path_hash: hash.clone(),
                store_path: format!("/nix/store/{hash}-thing"),
                name: "thing".into(),
                nar_hash: "sha256:x".into(),
                nar_size: 10,
                file_hash: "sha256:y".into(),
                file_size: 5,
                deriver: None,
                ca: None,
                references: vec![],
                sigs: vec![],
                pushed_by: None,
            },
            NOW - 2 * 86400,
        )
        .unwrap();
        let conn = Arc::new(Mutex::new(conn));

        let db_rows = {
            let c = conn.lock().unwrap();
            db::all_objects_brief(&c).unwrap()
        };
        let in_flight = InFlight::new();
        let (dangling, mismatches) = classify_rows(&db_rows, &[], &in_flight, GRACE, NOW, false);
        assert_eq!(dangling.len(), 1);

        let repaired = apply_repair(&conn, &dangling, &mismatches).unwrap();
        assert_eq!(repaired, 1);
        let c = conn.lock().unwrap();
        assert!(!db::exists(&c, &hash).unwrap());
    }

    #[tokio::test]
    async fn a_drain_that_finishes_in_time_reports_success() {
        let in_flight = InFlight::new();
        assert!(
            wait_for_drain(
                &in_flight,
                Duration::from_millis(200),
                Duration::from_millis(10)
            )
            .await
        );
    }

    #[tokio::test]
    async fn a_drain_that_never_finishes_times_out() {
        let in_flight = InFlight::new();
        let _claim = in_flight.claim("a").unwrap();
        assert!(
            !wait_for_drain(
                &in_flight,
                Duration::from_millis(100),
                Duration::from_millis(10)
            )
            .await
        );
    }
}
