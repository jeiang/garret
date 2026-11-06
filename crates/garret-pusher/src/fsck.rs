//! `garret-admin fsck` (spec 02/03): verifies row ⇒ blob and reports/repairs
//! drift. Runs inside the Pusher so repair can reuse [`db::delete_object`]
//! under the single-writer connection, exactly like `resign`/`delete`
//! (spec 10-packaging).

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use anyhow::Result;
use garret_common::admin::{FsckRow, FsckSizeMismatch, Repair, Response};
use garret_server::{db, inflight::InFlight, now, storage};
use parking_lot::Mutex;
use rusqlite::Connection;

use crate::{AppState, gc};

/// Ceiling on how long `--quiesce` waits for in-flight uploads to drain.
/// ponytail: fixed timeout, add a CLI/config knob if an operator ever needs
/// to wait longer than 5 minutes for uploads to drain.
pub const QUIESCE_TIMEOUT: Duration = Duration::from_secs(300);

/// Runs one fsck pass: lists S3 keys, then DB rows (in that order — a push
/// landing in the gap between the two listings looks exactly like the
/// dangling-row race the in-flight/age guards exist to close), classifies
/// the difference, and repairs if asked. Rows and blobs younger than `grace`
/// (the GC orphan grace) may be mid-push and are never findings; a quiesced
/// repair waits at most `quiesce_timeout` for uploads to drain.
pub async fn run(
    state: &AppState,
    grace: Duration,
    quiesce_timeout: Duration,
    verify_sizes: bool,
    repair: Repair,
) -> Result<Response> {
    metrics::counter!("garret_fsck_runs_total").increment(1);

    // Held to the end of `run`, however it returns: dropping the guard
    // reopens the gate unless another quiesce still holds it.
    let guard = (repair == Repair::Quiesced).then(|| state.in_flight.quiesce());
    let quiesce_drained = match &guard {
        Some(guard) => Some(guard.drain(quiesce_timeout).await),
        None => None,
    };

    let blobs = state.storage.list_blobs().await?;
    let db_rows = {
        let conn = state.conn.lock();
        db::all_objects_brief(&conn)?
    };

    let findings = classify_rows(
        &db_rows,
        &blobs,
        &state.in_flight,
        grace,
        now(),
        verify_sizes,
    );
    let (dangling, size_mismatches) = to_wire(&findings);
    let known: HashSet<String> = db_rows.iter().map(|r| r.store_path_hash.clone()).collect();
    let orphans: Vec<String> = gc::orphan_hashes(&blobs, &known, &state.in_flight, grace)
        .into_iter()
        .map(storage::key_for)
        .collect();

    metrics::gauge!("garret_fsck_findings", "kind" => "dangling").set(dangling.len() as f64);
    metrics::gauge!("garret_fsck_findings", "kind" => "orphan").set(orphans.len() as f64);
    metrics::gauge!("garret_fsck_findings", "kind" => "size_mismatch")
        .set(size_mismatches.len() as f64);

    // A timed-out drain means the live signal was never trustworthy enough
    // to delete rows on — report the findings, but repair nothing.
    let repaired = if repair != Repair::No && quiesce_drained != Some(false) {
        apply_repair(&state.conn, &findings)?
    } else {
        0
    };

    Ok(Response::Fsck {
        dangling,
        size_mismatches,
        orphans,
        repaired,
        quiesce_drained,
    })
}

/// A row fsck would repair, carrying the `created_at` it was listed with so
/// repair can tell that row from one written since.
struct Finding<'a> {
    hash: &'a str,
    name: &'a str,
    listed_at: i64,
    drift: Drift,
}

/// How a row disagrees with S3. One per row, so a size-mismatched row is
/// never also dangling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Drift {
    /// No blob.
    Dangling,
    /// A blob of another size (`verify_sizes` only).
    SizeMismatch { db: i64, s3: i64 },
}

/// Finds dangling rows (no blob) and size-mismatched ones (blob exists,
/// sizes disagree — `verify_sizes` only), each only past both guards (not
/// in flight, older than the grace).
fn classify_rows<'a>(
    db_rows: &'a [db::ObjectBrief],
    blobs: &[(String, Duration, i64)],
    in_flight: &InFlight,
    grace: Duration,
    now: i64,
    verify_sizes: bool,
) -> Vec<Finding<'a>> {
    let s3_sizes: HashMap<&str, i64> = blobs
        .iter()
        .filter_map(|(key, _age, size)| storage::hash_for(key).map(|h| (h, *size)))
        .collect();

    db_rows
        .iter()
        .filter_map(|row| {
            let hash = row.store_path_hash.as_str();
            // Both findings get both guards: a row in flight or younger than
            // the grace may be an upload overwriting an orphan blob of another
            // size since the S3 listing, not drift.
            if in_flight.contains(hash) || now - row.created_at < grace.as_secs() as i64 {
                return None;
            }
            let drift = match s3_sizes.get(hash) {
                None => Drift::Dangling,
                Some(&s3) if verify_sizes && s3 != row.file_size => Drift::SizeMismatch {
                    db: row.file_size,
                    s3,
                },
                Some(_) => return None,
            };
            Some(Finding {
                hash,
                name: &row.name,
                listed_at: row.created_at,
                drift,
            })
        })
        .collect()
}

/// The findings as the admin reply reports them, in listing order.
fn to_wire(findings: &[Finding<'_>]) -> (Vec<FsckRow>, Vec<FsckSizeMismatch>) {
    let mut dangling = Vec::new();
    let mut size_mismatches = Vec::new();
    for f in findings {
        match f.drift {
            Drift::Dangling => dangling.push(FsckRow {
                store_path_hash: f.hash.to_owned(),
                name: f.name.to_owned(),
            }),
            Drift::SizeMismatch { db, s3 } => size_mismatches.push(FsckSizeMismatch {
                store_path_hash: f.hash.to_owned(),
                name: f.name.to_owned(),
                db_size: db,
                s3_size: s3,
            }),
        }
    }
    (dangling, size_mismatches)
}

/// Deletes every finding's row (`db::delete_object`, row+refs+stats in one
/// transaction each — never the S3 object, which is left for the orphan
/// sweep by design). Only the row the listing saw is deleted: one gone since
/// (GC evicted it, say) or replaced since (deleted and pushed again, so its
/// blob is back) is skipped, not counted. A re-push rewrites `created_at`,
/// so an unchanged `created_at` is the listed row (found by the TLA+ model,
/// `verification/tla`). Returns the count actually deleted.
fn apply_repair(conn: &Mutex<Connection>, findings: &[Finding<'_>]) -> Result<usize> {
    let (mut dangling, mut mismatched) = (0, 0);
    for f in findings {
        let mut conn = conn.lock();
        if db::created_at(&conn, f.hash)? == Some(f.listed_at)
            && db::delete_object(&mut conn, f.hash)?.is_some()
        {
            match f.drift {
                Drift::Dangling => dangling += 1,
                Drift::SizeMismatch { .. } => mismatched += 1,
            }
        }
    }
    metrics::counter!("garret_fsck_rows_repaired_total", "reason" => "dangling")
        .increment(dangling as u64);
    metrics::counter!("garret_fsck_rows_repaired_total", "reason" => "size_mismatch")
        .increment(mismatched as u64);
    Ok(dangling + mismatched)
}

#[cfg(test)]
mod tests {
    use super::*;
    use garret_server::testing::object;

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

    /// [`classify_rows`] at `GRACE` and `NOW`, as the reply reports it.
    fn classified(
        rows: &[db::ObjectBrief],
        blobs: &[(String, Duration, i64)],
        in_flight: &InFlight,
        verify_sizes: bool,
    ) -> (Vec<FsckRow>, Vec<FsckSizeMismatch>) {
        to_wire(&classify_rows(
            rows,
            blobs,
            in_flight,
            GRACE,
            NOW,
            verify_sizes,
        ))
    }

    const GRACE: Duration = Duration::from_secs(86400);
    const NOW: i64 = 1_000_000;

    #[test]
    fn a_genuinely_dangling_row_is_reported() {
        let rows = vec![row("a", "thing", NOW - 2 * 86400, 5)];
        let blobs = vec![];
        let in_flight = InFlight::new();
        let (dangling, mismatches) = classified(&rows, &blobs, &in_flight, false);
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
        let (dangling, _) = classified(&rows, &blobs, &in_flight, false);
        assert!(dangling.is_empty(), "in-flight upload reported as dangling");
    }

    #[test]
    fn a_row_younger_than_the_grace_period_is_not_reported_dangling() {
        let rows = vec![row("a", "thing", NOW - 10, 5)];
        let blobs = vec![];
        let in_flight = InFlight::new();
        let (dangling, _) = classified(&rows, &blobs, &in_flight, false);
        assert!(dangling.is_empty(), "young row reported as dangling");
    }

    #[test]
    fn a_size_mismatch_is_its_own_category_only_when_verify_sizes_is_set() {
        let rows = vec![row("a", "thing", NOW - 2 * 86400, 5)];
        let blobs = vec![blob("a", 90000, 9)];
        let in_flight = InFlight::new();

        let (dangling, mismatches) = classified(&rows, &blobs, &in_flight, false);
        assert!(dangling.is_empty());
        assert!(mismatches.is_empty(), "no size check without verify_sizes");

        let (dangling, mismatches) = classified(&rows, &blobs, &in_flight, true);
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
        let (_, mismatches) = classified(&young, &blobs, &InFlight::new(), true);
        assert!(mismatches.is_empty(), "young row reported as a mismatch");

        let old = vec![row("a", "thing", NOW - 2 * 86400, 5)];
        let in_flight = InFlight::new();
        let _claim = in_flight.claim("a").unwrap();
        let (_, mismatches) = classified(&old, &blobs, &in_flight, true);
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
        let (dangling, mismatches) = classified(&rows, &blobs, &in_flight, true);
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
        let conn = Mutex::new(conn);

        let db_rows = {
            let c = conn.lock();
            db::all_objects_brief(&c).unwrap()
        };
        let in_flight = InFlight::new();
        let findings = classify_rows(&db_rows, &[], &in_flight, GRACE, NOW, false);
        assert_eq!(findings.len(), 1);

        let repaired = apply_repair(&conn, &findings).unwrap();
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
        let findings = classify_rows(&rows, &blobs, &InFlight::new(), GRACE, NOW, true);
        let (dangling, mismatches) = to_wire(&findings);
        assert_eq!((dangling.len(), mismatches.len()), (2, 1));
        db::delete_object(&mut conn, &a).unwrap();
        let conn = Mutex::new(conn);

        let repaired = apply_repair(&conn, &findings).unwrap();
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
        let findings = classify_rows(&rows, &[], &InFlight::new(), GRACE, NOW, false);
        assert_eq!(findings.len(), 1);
        db::delete_object(&mut conn, &hash).unwrap();
        db::insert_object(&mut conn, &object(&hash), NOW).unwrap();
        let conn = Mutex::new(conn);

        let repaired = apply_repair(&conn, &findings).unwrap();
        assert_eq!(repaired, 0);
        assert!(db::exists(&conn.lock(), &hash).unwrap());
    }

    /// An S3 stand-in whose bucket lists no blobs.
    async fn empty_bucket() -> storage::Storage {
        garret_server::testing::fake_s3(axum::Router::new().fallback(|| async {
            r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>test</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>"#
        }))
        .await
    }

    /// A quiesced repair whose drain times out reports it and deletes
    /// nothing, though the row is dangling; a live repair deletes it and
    /// reports no drain at all.
    #[tokio::test]
    async fn a_quiesce_that_cannot_drain_repairs_nothing() {
        let mut conn = crate::tests::open_db();
        let hash = "a".repeat(32);
        db::insert_object(&mut conn, &object(&hash), now() - 2 * 86400).unwrap();
        let state = crate::tests::test_state_with_storage(conn, empty_bucket().await).await;
        let fsck = |repair| run(&state, GRACE, Duration::from_millis(50), false, repair);

        let upload = state.in_flight.claim(&"b".repeat(32)).unwrap();
        let Response::Fsck {
            dangling,
            repaired,
            quiesce_drained,
            ..
        } = fsck(Repair::Quiesced).await.unwrap()
        else {
            panic!("not an fsck response");
        };
        assert_eq!((dangling.len(), repaired), (1, 0));
        assert_eq!(quiesce_drained, Some(false));
        assert!(db::exists(&state.conn.lock(), &hash).unwrap());
        drop(upload);

        let Response::Fsck {
            repaired,
            quiesce_drained,
            ..
        } = fsck(Repair::Live).await.unwrap()
        else {
            panic!("not an fsck response");
        };
        assert_eq!((repaired, quiesce_drained), (1, None));
        assert!(!db::exists(&state.conn.lock(), &hash).unwrap());
    }
}
