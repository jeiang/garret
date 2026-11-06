//! `garret-admin fsck` (spec 02/03): verifies row ⇒ blob and reports/repairs
//! drift. Runs inside the Pusher so repair can reuse [`db::delete_object`]
//! under the single-writer connection, exactly like `resign`/`delete`
//! (spec 10-packaging).

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use garret_common::admin::{FsckRow, FsckSizeMismatch, Response};
use garret_server::{db, inflight::InFlight, now, storage};
use parking_lot::Mutex;
use rusqlite::Connection;

use crate::{AppState, gc};

/// Ceiling on how long `--quiesce` waits for in-flight uploads to drain.
/// ponytail: fixed timeout, add a CLI/config knob if an operator ever needs
/// to wait longer than 5 minutes for uploads to drain.
const QUIESCE_TIMEOUT: Duration = Duration::from_secs(300);

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

    // Held to the end of `run`, however it returns: dropping the guard
    // reopens the gate unless another quiesce still holds it.
    let guard = quiesce.then(|| state.in_flight.quiesce());
    let quiesce_drained = match &guard {
        Some(guard) => guard.drain(QUIESCE_TIMEOUT).await,
        None => true,
    };

    let grace = Duration::from_secs(gc.cfg.orphan_grace_secs);
    let blobs = state.storage.list_blobs().await?;
    let db_rows = {
        let conn = state.conn.lock();
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
    let known: HashSet<String> = db_rows.iter().map(|r| r.store_path_hash.clone()).collect();
    let orphans = gc::orphan_keys(&blobs, &known, &state.in_flight, grace);

    metrics::gauge!("garret_fsck_findings", "kind" => "dangling").set(dangling.len() as f64);
    metrics::gauge!("garret_fsck_findings", "kind" => "orphan").set(orphans.len() as f64);
    metrics::gauge!("garret_fsck_findings", "kind" => "size_mismatch")
        .set(size_mismatches.len() as f64);

    // A timed-out drain means the live signal was never trustworthy enough
    // to delete rows on — report the findings, but repair nothing.
    let repaired = if repair && quiesce_drained {
        apply_repair(&state.conn, &db_rows, &dangling, &size_mismatches)?
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

/// Splits DB rows into dangling (no blob) and size-mismatched (blob exists,
/// sizes disagree — `verify_sizes` only), each only past both guards (not
/// in flight, older than the grace). A hash with a size mismatch is never
/// also reported dangling.
fn classify_rows(
    db_rows: &[db::ObjectBrief],
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
    for row in db_rows {
        let hash = &row.store_path_hash;
        // Both findings get both guards: a row in flight or younger than the
        // grace may be an upload overwriting an orphan blob of another size
        // since the S3 listing, not drift.
        if in_flight.contains(hash) || now - row.created_at < grace.as_secs() as i64 {
            continue;
        }
        match s3_sizes.get(hash.as_str()) {
            None => {
                dangling.push(FsckRow {
                    store_path_hash: hash.clone(),
                    name: row.name.clone(),
                });
            }
            Some(&s3_size) if verify_sizes && s3_size != row.file_size => {
                size_mismatches.push(FsckSizeMismatch {
                    store_path_hash: hash.clone(),
                    name: row.name.clone(),
                    db_size: row.file_size,
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
/// left for the orphan sweep by design). Only the row the listing saw is
/// deleted: one gone since (GC evicted it, say) or replaced since (deleted
/// and pushed again, so its blob is back) is skipped, not counted. `listed`
/// is the row listing the findings came from. Returns the count actually
/// deleted.
fn apply_repair(
    conn: &Arc<Mutex<Connection>>,
    listed: &[db::ObjectBrief],
    dangling: &[FsckRow],
    size_mismatches: &[FsckSizeMismatch],
) -> Result<usize> {
    let listed: HashMap<&str, i64> = listed
        .iter()
        .map(|row| (row.store_path_hash.as_str(), row.created_at))
        .collect();
    let listed = &listed;
    let dangling = repair_rows(
        conn,
        listed,
        dangling.iter().map(|r| r.store_path_hash.as_str()),
        "dangling",
    )?;
    let mismatched = repair_rows(
        conn,
        listed,
        size_mismatches.iter().map(|r| r.store_path_hash.as_str()),
        "size_mismatch",
    )?;
    Ok(dangling + mismatched)
}

/// Deletes each row still exactly as listed, counting the deletions under
/// `reason`. A re-push rewrites `created_at`, so an unchanged `created_at`
/// is the listed row (found by the TLA+ model, `verification/tla`).
fn repair_rows<'a>(
    conn: &Mutex<Connection>,
    listed: &HashMap<&str, i64>,
    hashes: impl Iterator<Item = &'a str>,
    reason: &'static str,
) -> Result<usize> {
    let mut repaired = 0;
    for hash in hashes {
        let mut conn = conn.lock();
        if db::created_at(&conn, hash)?.is_some_and(|at| listed.get(hash) == Some(&at))
            && db::delete_object(&mut conn, hash)?.is_some()
        {
            repaired += 1;
        }
    }
    metrics::counter!("garret_fsck_rows_repaired_total", "reason" => reason)
        .increment(repaired as u64);
    Ok(repaired)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(hash: &str, name: &str, created_at: i64, file_size: i64) -> db::ObjectBrief {
        db::ObjectBrief {
            store_path_hash: hash.to_string(),
            name: name.to_string(),
            created_at,
            file_size,
        }
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
        let _claim = in_flight
            .claim("a", garret_server::inflight::Kind::Upload)
            .unwrap();
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

    /// An upload that overwrote an orphan blob of another size since the S3
    /// listing (review of #72): its fresh row would read as a mismatch and
    /// `--repair` would delete it, so the guards apply here too.
    #[test]
    fn a_young_or_in_flight_row_is_not_reported_as_a_size_mismatch() {
        let blobs = vec![blob("a", 90000, 9)];
        let young = vec![row("a", "thing", NOW - 10, 5)];
        let (_, mismatches) = classify_rows(&young, &blobs, &InFlight::new(), GRACE, NOW, true);
        assert!(mismatches.is_empty(), "young row reported as a mismatch");

        let old = vec![row("a", "thing", NOW - 2 * 86400, 5)];
        let in_flight = InFlight::new();
        let _claim = in_flight
            .claim("a", garret_server::inflight::Kind::Upload)
            .unwrap();
        let (_, mismatches) = classify_rows(&old, &blobs, &in_flight, GRACE, NOW, true);
        assert!(
            mismatches.is_empty(),
            "in-flight row reported as a mismatch"
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
        db::insert_object(&mut conn, &object(&hash), NOW - 2 * 86400).unwrap();
        let conn = Arc::new(Mutex::new(conn));

        let db_rows = {
            let c = conn.lock();
            db::all_objects_brief(&c).unwrap()
        };
        let in_flight = InFlight::new();
        let (dangling, mismatches) = classify_rows(&db_rows, &[], &in_flight, GRACE, NOW, false);
        assert_eq!(dangling.len(), 1);

        let repaired = apply_repair(&conn, &db_rows, &dangling, &mismatches).unwrap();
        assert_eq!(repaired, 1);
        let c = conn.lock();
        assert!(!db::exists(&c, &hash).unwrap());
    }

    #[test]
    fn a_row_gone_before_the_repair_is_skipped_and_the_rest_repaired() {
        // GC evicts `a` between fsck's listing and its repair.
        let mut conn = db::open(":memory:", true).unwrap();
        db::migrate(&conn).unwrap();
        let [a, b, c] = ["a", "b", "c"].map(|h| h.repeat(32));
        for hash in [&a, &b, &c] {
            db::insert_object(&mut conn, &object(hash), NOW - 2 * 86400).unwrap();
        }
        let rows = db::all_objects_brief(&conn).unwrap();
        // `c`'s blob is the wrong size; `a` and `b` have none.
        let blobs = [blob(&c, 90000, 9)];
        let (dangling, mismatches) =
            classify_rows(&rows, &blobs, &InFlight::new(), GRACE, NOW, true);
        assert_eq!((dangling.len(), mismatches.len()), (2, 1));
        db::delete_object(&mut conn, &a).unwrap();
        let conn = Arc::new(Mutex::new(conn));

        let repaired = apply_repair(&conn, &rows, &dangling, &mismatches).unwrap();
        assert_eq!(repaired, 2, "the vanished row counted as repaired");
        let conn = conn.lock();
        assert!(!db::exists(&conn, &b).unwrap());
        assert!(!db::exists(&conn, &c).unwrap());
    }

    /// The TLA+ model's counterexample (`verification/tla/fsck_race.cfg`):
    /// a dangling row is deleted and pushed again, blob and all, between
    /// fsck's listing and its repair. The healthy new row must survive.
    #[test]
    fn a_row_pushed_again_since_the_listing_is_not_repaired() {
        let mut conn = db::open(":memory:", true).unwrap();
        db::migrate(&conn).unwrap();
        let hash = "a".repeat(32);
        db::insert_object(&mut conn, &object(&hash), NOW - 2 * 86400).unwrap();
        let rows = db::all_objects_brief(&conn).unwrap();
        let (dangling, mismatches) = classify_rows(&rows, &[], &InFlight::new(), GRACE, NOW, false);
        assert_eq!(dangling.len(), 1);
        db::delete_object(&mut conn, &hash).unwrap();
        db::insert_object(&mut conn, &object(&hash), NOW).unwrap();
        let conn = Arc::new(Mutex::new(conn));

        let repaired = apply_repair(&conn, &rows, &dangling, &mismatches).unwrap();
        assert_eq!(repaired, 0);
        assert!(db::exists(&conn.lock(), &hash).unwrap());
    }

    fn object(hash: &str) -> db::Object {
        db::Object {
            store_path_hash: hash.into(),
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
        }
    }
}
