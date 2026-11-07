//! `garret-admin fsck` (spec 02/03): verifies row ⇒ blob and reports/repairs
//! drift. Runs inside the Pusher so repair can reuse [`db::delete_object`]
//! under the single-writer connection, exactly like `resign`/`delete`
//! (spec 10-packaging).

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use garret_common::{
    admin::{FsckReport, FsckRow, FsckSizeMismatch, Repair, Response},
    unix_now,
};
use garret_server::{db, inflight::InFlight, storage};
use parking_lot::Mutex;
use rusqlite::Connection;

use crate::state::{AppState, db_run};

/// Ceiling on how long `--quiesce` waits for in-flight uploads to drain.
/// ponytail: fixed timeout, add a CLI/config knob if an operator ever needs
/// to wait longer than 5 minutes for uploads to drain.
pub const QUIESCE_TIMEOUT: Duration = Duration::from_secs(300);

/// Runs one fsck pass: lists the bucket, then reads the rows it needs, in
/// that order per range — a push landing in the gap between the two listings
/// looks exactly like the dangling-row race the in-flight/age guards exist to
/// close. Both sides are paged and merged in key order, so neither is ever
/// fully materialized. Classifies the difference and repairs if asked. Rows
/// and blobs younger than `grace` (the GC orphan grace) may be mid-push and
/// are never findings; a quiesced repair waits at most `quiesce_timeout` for
/// uploads to drain.
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

    let now = unix_now();
    let mut lister = state.storage.list_blobs();
    let mut rows = storage::RowWindow::new();
    let mut findings = Findings::default();
    while let Some(page) = lister.next_page().await? {
        let Some(last) = page.last() else { continue };
        while rows.needs_through(&last.hash) {
            fetch_rows(&state.conn, &mut rows).await?;
        }
        findings.absorb(
            storage::merge_page(&page, &mut rows),
            &state.in_flight,
            grace,
            now,
            verify_sizes,
        );
    }
    // Rows past the last blob — every row when the bucket is empty — have no
    // blob at all.
    while rows.more() {
        fetch_rows(&state.conn, &mut rows).await?;
    }
    findings.rest(rows.drain(), &state.in_flight, grace, now, verify_sizes);

    let (dangling, size_mismatches) = to_wire(&findings.rows);
    let orphans = findings.orphans;

    metrics::gauge!("garret_fsck_findings", "kind" => "dangling").set(dangling.len() as f64);
    metrics::gauge!("garret_fsck_findings", "kind" => "orphan").set(orphans.len() as f64);
    metrics::gauge!("garret_fsck_findings", "kind" => "size_mismatch")
        .set(size_mismatches.len() as f64);

    // A timed-out drain means the live signal was never trustworthy enough
    // to delete rows on — report the findings, but repair nothing.
    let repaired = if repair != Repair::No && quiesce_drained != Some(false) {
        apply_repair(&state.conn, &findings.rows).await?
    } else {
        0
    };

    Ok(Response::Fsck(FsckReport {
        dangling,
        size_mismatches,
        orphans,
        repaired,
        quiesce_drained,
    }))
}

/// Fetches the next page of object rows into the window. The connection lock
/// is taken for the fetch alone, never across an S3 await.
async fn fetch_rows(conn: &Arc<Mutex<Connection>>, rows: &mut storage::RowWindow) -> Result<()> {
    let cursor = rows.cursor().map(str::to_owned);
    rows.extend(db_run(conn, move |conn| db::objects_after(conn, cursor.as_deref())).await?);
    Ok(())
}

/// A row fsck would repair, carrying the `created_at` it was listed with so
/// repair can tell that row from one written since.
struct Finding {
    hash: String,
    name: String,
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

/// What one pass found, accumulated as the merge walks the bucket and the
/// rows.
#[derive(Default)]
struct Findings {
    rows: Vec<Finding>,
    /// Blob keys with no row, past the guards.
    orphans: Vec<String>,
}

impl Findings {
    /// Classifies one merged page: a row with no blob is dangling, a blob of
    /// another size is a size mismatch (`verify_sizes` only), a blob with no
    /// row is an orphan.
    fn absorb(
        &mut self,
        merged: storage::Merged<'_>,
        in_flight: &InFlight,
        grace: Duration,
        now: i64,
        verify_sizes: bool,
    ) {
        for (blob, row) in merged.pairs {
            self.row(row, Some(blob), in_flight, grace, now, verify_sizes);
        }
        for row in merged.dangling {
            self.row(row, None, in_flight, grace, now, verify_sizes);
        }
        for blob in merged.orphans {
            if storage::is_orphan(blob, in_flight, grace) {
                self.orphans.push(storage::key_for(&blob.hash));
            }
        }
    }

    /// Classifies the rows the listing never reached: they have no blob at
    /// all.
    fn rest(
        &mut self,
        rows: impl Iterator<Item = db::ObjectBrief>,
        in_flight: &InFlight,
        grace: Duration,
        now: i64,
        verify_sizes: bool,
    ) {
        for row in rows {
            self.row(row, None, in_flight, grace, now, verify_sizes);
        }
    }

    /// One row against its blob (`None`: no blob). Both findings get both
    /// guards: a row in flight or younger than the grace may be an upload
    /// overwriting an orphan blob of another size since the S3 listing, not
    /// drift.
    fn row(
        &mut self,
        row: db::ObjectBrief,
        blob: Option<&storage::Blob>,
        in_flight: &InFlight,
        grace: Duration,
        now: i64,
        verify_sizes: bool,
    ) {
        if in_flight.contains(&row.store_path_hash) || now - row.created_at < grace.as_secs() as i64
        {
            return;
        }
        let drift = match blob {
            None => Drift::Dangling,
            Some(blob) if verify_sizes && blob.size != row.file_size => Drift::SizeMismatch {
                db: row.file_size,
                s3: blob.size,
            },
            Some(_) => return,
        };
        self.rows.push(Finding {
            hash: row.store_path_hash,
            name: row.name,
            listed_at: row.created_at,
            drift,
        });
    }
}

fn to_wire(findings: &[Finding]) -> (Vec<FsckRow>, Vec<FsckSizeMismatch>) {
    let mut dangling = Vec::new();
    let mut size_mismatches = Vec::new();
    for f in findings {
        match f.drift {
            Drift::Dangling => dangling.push(FsckRow {
                store_path_hash: f.hash.clone(),
                name: f.name.clone(),
            }),
            Drift::SizeMismatch { db, s3 } => size_mismatches.push(FsckSizeMismatch {
                store_path_hash: f.hash.clone(),
                name: f.name.clone(),
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
async fn apply_repair(conn: &Arc<Mutex<Connection>>, findings: &[Finding]) -> Result<usize> {
    let (mut dangling, mut mismatched) = (0, 0);
    for f in findings {
        let (hash, listed_at) = (f.hash.to_owned(), f.listed_at);
        let deleted = db_run(conn, move |conn| {
            Ok(db::created_at(conn, &hash)? == Some(listed_at)
                && db::delete_object(conn, &hash)?.is_some())
        })
        .await?;
        if deleted {
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
    use crate::state::testing::{open_db, test_state_with_storage};
    use garret_server::testing::object;

    fn row(hash: &str, name: &str, created_at: i64, file_size: i64) -> db::ObjectBrief {
        db::ObjectBrief {
            store_path_hash: hash.to_string(),
            name: name.to_string(),
            created_at,
            file_size,
        }
    }

    fn blob(hash: &str, age_secs: u64, size: i64) -> storage::Blob {
        storage::Blob {
            hash: hash.into(),
            age: Duration::from_secs(age_secs),
            size,
        }
    }

    /// Classifies `rows` against `blobs` through the same merge the pass
    /// uses, returning the wire report's three lists.
    fn classified(
        rows: Vec<db::ObjectBrief>,
        blobs: &[storage::Blob],
        in_flight: &InFlight,
        verify_sizes: bool,
    ) -> (Vec<FsckRow>, Vec<FsckSizeMismatch>, Vec<String>) {
        let mut window = storage::RowWindow::new();
        window.extend(rows);
        let mut findings = Findings::default();
        findings.absorb(
            storage::merge_page(blobs, &mut window),
            in_flight,
            GRACE,
            NOW,
            verify_sizes,
        );
        findings.rest(window.drain(), in_flight, GRACE, NOW, verify_sizes);
        let (dangling, mismatches) = to_wire(&findings.rows);
        (dangling, mismatches, findings.orphans)
    }

    const GRACE: Duration = Duration::from_secs(86400);
    const NOW: i64 = 1_000_000;

    #[test]
    fn a_genuinely_dangling_row_is_reported() {
        let rows = vec![row("a", "thing", NOW - 2 * 86400, 5)];
        let blobs = vec![];
        let in_flight = InFlight::new();
        let (dangling, mismatches, orphans) = classified(rows, &blobs, &in_flight, false);
        assert_eq!(
            dangling,
            vec![FsckRow {
                store_path_hash: "a".into(),
                name: "thing".into()
            }]
        );
        assert!(mismatches.is_empty());
        assert!(orphans.is_empty());
    }

    #[test]
    fn an_in_flight_upload_is_not_reported_dangling() {
        let rows = vec![row("a", "thing", NOW - 2 * 86400, 5)];
        let blobs = vec![];
        let in_flight = InFlight::new();
        let _claim = in_flight.claim("a").unwrap();
        let (dangling, _, _) = classified(rows, &blobs, &in_flight, false);
        assert!(dangling.is_empty(), "in-flight upload reported as dangling");
    }

    #[test]
    fn a_row_younger_than_the_grace_period_is_not_reported_dangling() {
        let rows = vec![row("a", "thing", NOW - 10, 5)];
        let blobs = vec![];
        let in_flight = InFlight::new();
        let (dangling, _, _) = classified(rows, &blobs, &in_flight, false);
        assert!(dangling.is_empty(), "young row reported as dangling");
    }

    #[test]
    fn a_size_mismatch_is_its_own_category_only_when_verify_sizes_is_set() {
        let rows = vec![row("a", "thing", NOW - 2 * 86400, 5)];
        let blobs = vec![blob("a", 90000, 9)];
        let in_flight = InFlight::new();

        let (dangling, mismatches, _) = classified(rows.clone(), &blobs, &in_flight, false);
        assert!(dangling.is_empty());
        assert!(mismatches.is_empty(), "no size check without verify_sizes");

        let (dangling, mismatches, _) = classified(rows, &blobs, &in_flight, true);
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
    /// listing: its fresh row would read as a mismatch and `--repair` would
    /// delete it, so the guards apply here too.
    #[test]
    fn a_young_or_in_flight_row_is_not_reported_as_a_size_mismatch() {
        let blobs = vec![blob("a", 90000, 9)];
        let young = vec![row("a", "thing", NOW - 10, 5)];
        let (_, mismatches, _) = classified(young, &blobs, &InFlight::new(), true);
        assert!(mismatches.is_empty(), "young row reported as a mismatch");

        let old = vec![row("a", "thing", NOW - 2 * 86400, 5)];
        let in_flight = InFlight::new();
        let _claim = in_flight.claim("a").unwrap();
        let (_, mismatches, _) = classified(old, &blobs, &in_flight, true);
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
        let (dangling, mismatches, orphans) = classified(rows, &blobs, &in_flight, true);
        assert!(dangling.is_empty());
        assert!(mismatches.is_empty());
        assert!(orphans.is_empty());
    }

    #[test]
    fn a_rowless_blob_is_an_orphan_unless_young_or_in_flight() {
        let blobs = vec![
            blob("known", 90000, 1),
            blob("uploading", 90000, 1),
            blob("young", 10, 1),
            blob("stale", 90000, 1),
        ];
        let rows = vec![row("known", "thing", NOW - 2 * 86400, 1)];
        let in_flight = InFlight::new();
        let _claim = in_flight.claim("uploading").unwrap();

        let (_, _, orphans) = classified(rows, &blobs, &in_flight, false);
        assert_eq!(orphans, vec![storage::key_for("stale")]);
    }

    #[tokio::test]
    async fn a_row_gone_before_the_repair_is_skipped_and_the_rest_repaired() {
        // GC evicts `a` between fsck's listing and its repair.
        let mut conn = open_db();
        let [a, b, c] = ["a", "b", "c"].map(|h| h.repeat(32));
        for hash in [&a, &b, &c] {
            db::insert_object(&mut conn, &object(hash), NOW - 2 * 86400).unwrap();
        }
        let rows = db::objects_after(&conn, None).unwrap();
        // `c`'s blob is the wrong size; `a` and `b` have none.
        let blobs = [blob(&c, 90000, 9)];
        let mut window = storage::RowWindow::new();
        window.extend(rows);
        let mut found = Findings::default();
        found.absorb(
            storage::merge_page(&blobs, &mut window),
            &InFlight::new(),
            GRACE,
            NOW,
            true,
        );
        let findings = found.rows;
        let (dangling, mismatches) = to_wire(&findings);
        assert_eq!((dangling.len(), mismatches.len()), (2, 1));
        db::delete_object(&mut conn, &a).unwrap();
        let conn = Arc::new(Mutex::new(conn));

        let repaired = apply_repair(&conn, &findings).await.unwrap();
        assert_eq!(repaired, 2, "the vanished row counted as repaired");
        let conn = conn.lock();
        assert!(!db::exists(&conn, &b).unwrap());
        assert!(!db::exists(&conn, &c).unwrap());
    }

    /// The TLA+ model's counterexample (`verification/tla/fsck_race.cfg`):
    /// a dangling row is deleted and pushed again, blob and all, between
    /// fsck's listing and its repair. The healthy new row must survive.
    #[tokio::test]
    async fn a_row_pushed_again_since_the_listing_is_not_repaired() {
        let mut conn = open_db();
        let hash = "a".repeat(32);
        db::insert_object(&mut conn, &object(&hash), NOW - 2 * 86400).unwrap();
        let rows = db::objects_after(&conn, None).unwrap();
        let mut window = storage::RowWindow::new();
        window.extend(rows);
        let mut found = Findings::default();
        found.absorb(
            storage::merge_page(&[], &mut window),
            &InFlight::new(),
            GRACE,
            NOW,
            false,
        );
        found.rest(window.drain(), &InFlight::new(), GRACE, NOW, false);
        let findings = found.rows;
        assert_eq!(findings.len(), 1);
        db::delete_object(&mut conn, &hash).unwrap();
        db::insert_object(&mut conn, &object(&hash), NOW).unwrap();
        let conn = Arc::new(Mutex::new(conn));

        let repaired = apply_repair(&conn, &findings).await.unwrap();
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
        let mut conn = open_db();
        let hash = "a".repeat(32);
        db::insert_object(&mut conn, &object(&hash), unix_now() - 2 * 86400).unwrap();
        let state = test_state_with_storage(conn, empty_bucket().await).await;
        let fsck = |repair| run(&state, GRACE, Duration::from_millis(50), false, repair);

        let upload = state.in_flight.claim(&"b".repeat(32)).unwrap();
        let Response::Fsck(FsckReport {
            dangling,
            repaired,
            quiesce_drained,
            ..
        }) = fsck(Repair::Quiesced).await.unwrap()
        else {
            panic!("not an fsck response");
        };
        assert_eq!((dangling.len(), repaired), (1, 0));
        assert_eq!(quiesce_drained, Some(false));
        assert!(db::exists(&state.conn.lock(), &hash).unwrap());
        drop(upload);

        let Response::Fsck(FsckReport {
            repaired,
            quiesce_drained,
            ..
        }) = fsck(Repair::Live).await.unwrap()
        else {
            panic!("not an fsck response");
        };
        assert_eq!((repaired, quiesce_drained), (1, None));
        assert!(!db::exists(&state.conn.lock(), &hash).unwrap());
    }
}
