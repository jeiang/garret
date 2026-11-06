//! The Pusher's admin socket (spec 10-packaging). Owner-only by file mode
//! (0600, the Pusher's own user): only root and the Pusher's uid can connect,
//! and both already hold everything the socket grants, so there is no
//! separate auth layer to keep in sync. That is why no other process may run
//! as that uid — the Puller has a user of its own.

use std::{collections::HashSet, path::Path, sync::Arc};

use anyhow::{Context, Result};
use garret_common::admin::{Request, Response};
use garret_server::{db, narinfo};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
};

use crate::{AppState, deletion::Deletion, fsck, gc::Gc};

/// Binds the admin socket (0600), replacing a stale one, so a bind failure
/// fails startup instead of a background task.
pub fn bind(path: &str) -> Result<UnixListener> {
    if let Some(parent) = Path::new(path).parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    // A socket left behind by a crashed process would block the bind.
    match std::fs::symlink_metadata(path) {
        Ok(meta) => {
            use std::os::unix::fs::FileTypeExt;
            anyhow::ensure!(
                meta.file_type().is_socket(),
                "{path} exists and is not a socket; refusing to replace it"
            );
            std::fs::remove_file(path).with_context(|| format!("removing stale socket {path}"))?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("checking {path}")),
    }
    let listener = UnixListener::bind(path).with_context(|| format!("binding {path}"))?;
    restrict(path)?;
    tracing::info!("admin socket listening on {path}");
    Ok(listener)
}

pub async fn serve(
    listener: UnixListener,
    state: Arc<AppState>,
    gc: Option<Arc<Gc>>,
) -> Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let (state, gc) = (state.clone(), gc.clone());
        tokio::spawn(async move {
            if let Err(e) = handle(stream, state, gc).await {
                tracing::warn!("admin connection failed: {e:#}");
            }
        });
    }
}

fn restrict(path: &str) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

async fn handle(stream: UnixStream, state: Arc<AppState>, gc: Option<Arc<Gc>>) -> Result<()> {
    let (read, mut write) = stream.into_split();
    let mut lines = BufReader::new(read).lines();
    while let Some(line) = lines.next_line().await? {
        let response = match serde_json::from_str::<Request>(&line) {
            Ok(request) => dispatch(request, &state, gc.as_deref()).await,
            Err(e) => Response::Error {
                message: format!("bad request: {e}"),
            },
        };
        write
            .write_all(format!("{}\n", serde_json::to_string(&response)?).as_bytes())
            .await?;
    }
    Ok(())
}

async fn dispatch(request: Request, state: &AppState, gc: Option<&Gc>) -> Response {
    let result = match request {
        Request::Status => status(state, gc).await,
        Request::GcRun => match gc {
            Some(gc) => gc.run().await.map(|pass| Response::Gc {
                evicted: pass.evicted,
                bytes_freed: pass.bytes_freed,
                candidates_exhausted: pass.candidates_exhausted,
            }),
            None => Ok(no_gc()),
        },
        Request::Resign => resign(state).await,
        Request::Delete { hashes, dry_run } => {
            delete(state, &hashes, dry_run)
                .await
                .map(|done| Response::Delete {
                    deleted: done.deleted,
                    bytes_freed: done.bytes_freed,
                    missing: done.missing,
                })
        }
        Request::Pin {
            name,
            hash,
            expires_at,
        } => {
            crate::db_run(&state.conn, move |conn| {
                db::pin(conn, &name, &hash, expires_at, garret_server::now())
                    .map(|()| Response::Pin)
            })
            .await
        }
        Request::Unpin { name } => {
            crate::db_run(&state.conn, move |conn| {
                db::unpin(conn, &name).map(|removed| Response::Unpin { removed })
            })
            .await
        }
        Request::Fsck {
            verify_sizes,
            repair,
        } => match gc {
            Some(gc) => {
                fsck::run(
                    state,
                    gc.orphan_grace(),
                    fsck::QUIESCE_TIMEOUT,
                    verify_sizes,
                    repair,
                )
                .await
            }
            None => Ok(no_gc()),
        },
        Request::Prune { before, dry_run } => prune(state, before, dry_run).await,
        Request::DeletePushedBy {
            subject,
            since,
            dry_run,
        } => delete_pushed_by(state, &subject, since, dry_run).await,
        Request::Backup { path } => backup(state, path).await,
    };
    result.unwrap_or_else(|e| Response::Error {
        message: format!("{e:#}"),
    })
}

/// The reply to a command that needs the `[gc]` section: `gc run`, and
/// `fsck`, whose guards use the orphan grace.
fn no_gc() -> Response {
    Response::Error {
        message: "no [gc] section is configured".into(),
    }
}

async fn status(state: &AppState, gc: Option<&Gc>) -> Result<Response> {
    let (quota_bytes, uploads_in_flight) = (gc.map(Gc::quota_bytes), state.in_flight.uploads());
    crate::db_run(&state.conn, move |conn| {
        Ok(Response::Status {
            objects: conn.query_row("SELECT COUNT(*) FROM objects", [], |r| r.get(0))?,
            total_bytes: db::total_bytes(conn)?,
            quota_bytes,
            uploads_in_flight,
        })
    })
    .await
}

#[derive(Debug)]
struct Deleted {
    deleted: usize,
    bytes_freed: i64,
    /// Hashes with no row, in request order.
    missing: Vec<String>,
}

/// Removes objects outright: row first, blob second, exactly as GC does
/// (spec 05) so a failed blob delete leaves an orphan for the sweep rather
/// than a row pointing at nothing.
///
/// Deliberately unconditional -- it does not check whether anything still
/// references the object. GC decides by reachability; this is the operator
/// saying "remove it regardless", which is the whole reason it exists.
async fn delete(state: &AppState, hashes: &[String], dry_run: bool) -> Result<Deleted> {
    let mut seen = HashSet::new();
    let hashes: Vec<&str> = hashes
        .iter()
        .map(String::as_str)
        .filter(|hash| seen.insert(*hash))
        .collect();
    // A dry run deletes nothing, so it claims nothing and cannot collide with
    // an upload or a GC pass.
    let mut deletion = if dry_run {
        None
    } else {
        Some(Deletion::claim_all(&state.in_flight, &hashes)?)
    };
    let mut done = Deleted {
        deleted: 0,
        bytes_freed: 0,
        missing: Vec::new(),
    };

    for hash in hashes {
        // One lock hold per path: whether the row exists is decided by the
        // delete itself, so a row fsck repair removed meanwhile is missing,
        // not a zero-byte delete.
        let owned = hash.to_owned();
        let freed = crate::db_run(&state.conn, move |conn| {
            if dry_run {
                db::file_size(conn, &owned)
            } else {
                db::delete_object(conn, &owned)
            }
        })
        .await?;
        let Some(freed) = freed else {
            done.missing.push(hash.to_owned());
            continue;
        };
        done.deleted += 1;
        done.bytes_freed += freed;
        if let Some(deletion) = &mut deletion {
            deletion.queue_blob(hash);
        }
    }

    if let Some(deletion) = deletion {
        deletion.finish(&state.storage).await?;
    }
    Ok(done)
}

/// Incident response after a push credential leaks (spec 10-packaging): every
/// object `subject` pushed, optionally only since a cutoff, removed through
/// [`delete`] so it gets exactly the treatment a hand-typed hash list would.
async fn delete_pushed_by(
    state: &AppState,
    subject: &str,
    since: Option<i64>,
    dry_run: bool,
) -> Result<Response> {
    let matched = {
        let subject = subject.to_owned();
        crate::db_run(&state.conn, move |conn| pushed_by(conn, &subject, since)).await?
    };
    let (matched, bytes_freed) = delete_listed(state, matched, dry_run).await?;
    if !dry_run {
        tracing::info!(
            subject,
            since,
            objects = matched.len(),
            bytes_freed,
            "deleted objects by pusher"
        );
    }
    Ok(Response::DeletePushedBy {
        objects: basenames(matched),
        bytes_freed,
    })
}

fn basenames(rows: Vec<db::ObjectBrief>) -> Vec<String> {
    rows.into_iter()
        .map(|row| format!("{}-{}", row.store_path_hash, row.name))
        .collect()
}

/// Deletes the listed objects through [`delete`]; returns those it actually
/// removed and the bytes freed. On a dry run, all of them and their sizes.
async fn delete_listed(
    state: &AppState,
    mut matched: Vec<db::ObjectBrief>,
    dry_run: bool,
) -> Result<(Vec<db::ObjectBrief>, i64)> {
    if dry_run {
        let bytes_freed = matched.iter().map(|row| row.file_size).sum();
        return Ok((matched, bytes_freed));
    }
    let hashes: Vec<String> = matched
        .iter()
        .map(|row| row.store_path_hash.clone())
        .collect();
    let done = delete(state, &hashes, false).await?;
    // Gone between the listing and the delete (GC or fsck repair got there
    // first): not deleted by this command, so not reported as such.
    let missing: HashSet<&str> = done.missing.iter().map(String::as_str).collect();
    matched.retain(|row| !missing.contains(row.store_path_hash.as_str()));
    Ok((matched, done.bytes_freed))
}

/// Every object first pushed by `subject`, at or after `since` when given.
/// Exact match: a subject is `<issuer>#<sub>`, and a prefix of one GitHub
/// ref's subject can be another's.
fn pushed_by(
    conn: &rusqlite::Connection,
    subject: &str,
    since: Option<i64>,
) -> Result<Vec<db::ObjectBrief>> {
    let mut stmt = conn.prepare(
        "SELECT store_path_hash, name, created_at, file_size FROM objects
         WHERE pushed_by = ?1 AND created_at >= ?2
         ORDER BY created_at, store_path_hash",
    )?;
    let rows = stmt.query_map(
        rusqlite::params![subject, since.unwrap_or(i64::MIN)],
        db::ObjectBrief::from_row,
    )?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// Deletes old closures (spec 05). Unlike GC this ignores quota: the operator
/// chose the cutoff.
async fn prune(state: &AppState, before: i64, dry_run: bool) -> Result<Response> {
    let in_flight = state.in_flight.clone();
    // One closure, so listing and deleting stay atomic under the lock.
    let (pruned, deletion) = crate::db_run(&state.conn, move |conn| {
        let doomed = db::prunable(conn, before, garret_server::now())?;
        if dry_run {
            return Ok((doomed, None));
        }
        let hashes: Vec<&str> = doomed
            .iter()
            .map(|row| row.store_path_hash.as_str())
            .collect();
        let mut deletion = Deletion::claim_all(&in_flight, &hashes)?;
        db::delete_pruned(conn, &doomed)?;
        for hash in hashes {
            deletion.queue_blob(hash);
        }
        Ok((doomed, Some(deletion)))
    })
    .await?;
    let bytes_freed = pruned.iter().map(|row| row.file_size).sum();
    if let Some(deletion) = deletion {
        deletion.finish(&state.storage).await?;
        tracing::info!(
            before,
            objects = pruned.len(),
            bytes_freed,
            "prune complete"
        );
    }
    Ok(Response::Prune {
        pruned: basenames(pruned),
        bytes_freed,
    })
}

/// Backfills signatures after a key is added, so both the old and new key
/// appear on every object during an overlap rotation (spec 10-packaging).
///
/// A row is re-signed only if one of its existing signatures verifies under a
/// configured key: re-signing blindly would turn tampered metadata (a database
/// write by anything that can reach the file) into a valid signature. Rows that
/// fail are counted as refused and left untouched.
async fn resign(state: &AppState) -> Result<Response> {
    /// Rows per lock hold: pushes wait at most one chunk's reads or writes.
    const CHUNK: usize = 500;
    let hashes: Vec<String> = crate::db_run(&state.conn, |conn| db::all_hashes(conn))
        .await?
        .into_iter()
        .collect();
    let mut resigned = 0;
    let mut refused = 0;
    for chunk in hashes.chunks(CHUNK) {
        let chunk = chunk.to_vec();
        let objects = crate::db_run(&state.conn, move |conn| {
            chunk
                .iter()
                .filter_map(|hash| db::get_object(conn, hash).transpose())
                .collect::<Result<Vec<_>>>()
        })
        .await?;
        let mut updates = Vec::new();
        for object in objects {
            if !narinfo::verify(&object, &state.store_dir, &state.keys) {
                tracing::warn!(
                    hash = object.store_path_hash,
                    "refusing to re-sign: no existing signature verifies"
                );
                refused += 1;
                continue;
            }
            let sigs = narinfo::sign(&object, &state.store_dir, &state.keys)?;
            if sigs != object.sigs {
                updates.push((object.store_path_hash, sigs));
            }
        }
        resigned += updates.len();
        crate::db_run(&state.conn, move |conn| {
            let tx = conn.transaction()?;
            for (hash, sigs) in &updates {
                db::update_sigs(&tx, hash, sigs)?;
            }
            Ok(tx.commit()?)
        })
        .await?;
    }
    Ok(Response::Resign { resigned, refused })
}

/// The online backup: a consistent copy of the whole database, taken while
/// both services keep running (spec 10-packaging). Takes the source path from
/// the shared connection and copies on a connection of its own, so pushes
/// wait on nothing longer than that lookup.
async fn backup(state: &AppState, dest: String) -> Result<Response> {
    let source = state
        .conn
        .lock()
        .path()
        // rusqlite reports an in-memory database as an empty path.
        .filter(|path| !path.is_empty())
        .map(str::to_owned)
        .context("the database is in memory; there is no file to back up")?;
    let bytes = tokio::task::spawn_blocking(move || backup_to(&source, Path::new(&dest))).await??;
    Ok(Response::Backup { bytes })
}

/// `VACUUM INTO` reads inside one read transaction, which WAL lets run
/// alongside the Pusher's writes, so the copy is a snapshot of a single
/// moment -- WAL frames not yet checkpointed included.
fn backup_to(source: &str, dest: &Path) -> Result<u64> {
    use std::os::unix::fs::OpenOptionsExt;
    // Created here rather than by SQLite: `create_new` refuses to overwrite
    // anything (a planted symlink included), and the copy is 0600 from its
    // first byte -- it holds every row, pushers' subjects among them.
    // `VACUUM INTO` accepts an empty existing file.
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(dest)
        .with_context(|| format!("creating {}", dest.display()))?;
    let copy = || -> Result<u64> {
        let conn = rusqlite::Connection::open_with_flags(
            source,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let dest = dest.to_str().context("backup path is not UTF-8")?;
        conn.execute("VACUUM INTO ?1", [dest])?;
        // A backup that a crash can lose is not one: flush the copy and its
        // directory entry before reporting.
        file.sync_all()?;
        let dir = Path::new(dest)
            .parent()
            .filter(|dir| !dir.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        std::fs::File::open(dir)?.sync_all()?;
        Ok(file.metadata()?.len())
    };
    copy().inspect_err(|_| {
        // Ours, and incomplete: removing it lets a retry use the same path.
        let _ = std::fs::remove_file(dest);
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{open_db, test_state, test_state_with_storage};
    use garret_server::testing::{S3_DELETED, fake_s3, object, scratch_dir};

    fn push(conn: &mut rusqlite::Connection, hash: &str, subject: &str, at: i64) {
        let object = db::Object {
            pushed_by: Some(subject.into()),
            ..object(hash)
        };
        db::insert_object(conn, &object, at).unwrap();
    }

    /// Matches only the exact subject -- never one it is a prefix of -- and
    /// `since` is inclusive, so the first push after a leak is not missed.
    #[test]
    fn pushed_by_matches_the_exact_subject_since_the_cutoff() {
        let mut conn = db::open(":memory:", true).unwrap();
        db::migrate(&conn).unwrap();
        let leaked = "https://issuer#repo:o/r:ref:refs/heads/main";
        let (a, b, c, d) = (
            "a".repeat(32),
            "b".repeat(32),
            "c".repeat(32),
            "d".repeat(32),
        );
        push(&mut conn, &a, leaked, 100);
        push(&mut conn, &b, leaked, 200);
        push(&mut conn, &c, &format!("{leaked}-other"), 200);
        push(&mut conn, &d, "https://issuer#someone-else", 300);
        // Negotiation refreshes pushed_at, never who pushed first or when.
        conn.execute("UPDATE objects SET pushed_at = 500", [])
            .unwrap();

        let hashes = |since| -> Vec<String> {
            pushed_by(&conn, leaked, since)
                .unwrap()
                .into_iter()
                .map(|row| {
                    assert_eq!((row.name.as_str(), row.file_size), ("thing", 5));
                    row.store_path_hash
                })
                .collect()
        };
        assert_eq!(hashes(None), [a, b.clone()]);
        assert_eq!(hashes(Some(200)), [b]);
        assert!(hashes(Some(201)).is_empty());
    }

    /// The copy must hold rows that so far live only in the WAL: copying the
    /// main file's bytes would silently drop everything since the last
    /// checkpoint.
    #[test]
    fn backup_copies_uncheckpointed_rows_with_mode_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = scratch_dir();
        let source = dir.join("garret.db");
        let mut conn = db::open(source.to_str().unwrap(), true).unwrap();
        db::migrate(&conn).unwrap();
        conn.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
        db::insert_object(&mut conn, &object(&"a".repeat(32)), 1).unwrap();

        let dest = dir.join("backup.db");
        let bytes = backup_to(source.to_str().unwrap(), &dest).unwrap();

        assert_eq!(bytes, std::fs::metadata(&dest).unwrap().len());
        let mode = std::fs::metadata(&dest).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        let copy = db::open(dest.to_str().unwrap(), false).unwrap();
        assert!(db::exists(&copy, &"a".repeat(32)).unwrap());
    }

    /// An existing file is never overwritten, and never removed either -- it
    /// may be yesterday's good backup.
    #[test]
    fn backup_refuses_to_overwrite() {
        let dir = scratch_dir();
        let source = dir.join("garret.db");
        db::migrate(&db::open(source.to_str().unwrap(), true).unwrap()).unwrap();
        let dest = dir.join("backup.db");
        std::fs::write(&dest, "yesterday").unwrap();

        assert!(backup_to(source.to_str().unwrap(), &dest).is_err());
        assert_eq!(std::fs::read_to_string(&dest).unwrap(), "yesterday");
    }

    /// GC is between the row and blob delete of `b` (or an upload of it is
    /// landing): an operator delete naming it stops before touching a row,
    /// and keeps no claim on the rest.
    #[tokio::test]
    async fn a_delete_meeting_a_claimed_path_deletes_nothing() {
        let (a, b) = ("a".repeat(32), "b".repeat(32));
        let mut conn = open_db();
        for h in [&a, &b] {
            db::insert_object(&mut conn, &object(h), 100).unwrap();
        }
        let state = test_state(conn).await;
        let _gc = state.in_flight.claim_delete(&b).unwrap();

        let err = delete(&state, &[a.clone(), a.clone(), b.clone()], false)
            .await
            .unwrap_err();
        assert!(err.to_string().contains(&b), "{err:#}");
        assert!(db::exists(&state.conn.lock(), &a).unwrap());
        assert!(
            state
                .in_flight
                .claim(&a, garret_server::inflight::Kind::Upload)
                .is_ok()
        );
    }

    #[tokio::test]
    async fn a_prune_meeting_a_claimed_path_deletes_nothing_but_can_still_list() {
        let a = "a".repeat(32);
        let mut conn = open_db();
        db::insert_object(&mut conn, &object(&a), 100).unwrap();
        let state = test_state(conn).await;
        let _gc = state.in_flight.claim_delete(&a).unwrap();
        let before = garret_server::now() - 2 * 86400;

        let Response::Prune { pruned, .. } = prune(&state, before, true).await.unwrap() else {
            panic!("not a prune response");
        };
        assert_eq!(pruned, vec![format!("{a}-thing")]);
        assert!(prune(&state, before, false).await.is_err());
        assert!(db::exists(&state.conn.lock(), &a).unwrap());
    }

    /// A signing key from base64 of a 64-byte keypair; `parse` uses only the
    /// first 32 bytes (the seed), so any 64 bytes do.
    fn signing_key(name: &str, b64: &str) -> narinfo::SigningKeyFile {
        narinfo::SigningKeyFile::parse(&format!("{name}:{b64}")).unwrap()
    }

    // 64 bytes of 0x00 and of 0x01.
    fn key_one() -> narinfo::SigningKeyFile {
        signing_key("test-1", &format!("{}==", "A".repeat(86)))
    }

    fn key_two() -> narinfo::SigningKeyFile {
        signing_key("test-2", &format!("{}AQ==", "AQEB".repeat(21)))
    }

    /// A row whose metadata was altered after signing must not be re-signed:
    /// that would mint a valid signature over the alteration.
    #[tokio::test]
    async fn resign_refuses_rows_that_do_not_verify() {
        let (good, tampered, unsigned) = ("a".repeat(32), "b".repeat(32), "c".repeat(32));
        let key = key_one();
        let mut conn = open_db();
        for h in [&good, &tampered, &unsigned] {
            let mut obj = object(h);
            if h != &unsigned {
                obj.sigs = narinfo::sign(&obj, "/nix/store", std::slice::from_ref(&key)).unwrap();
            }
            db::insert_object(&mut conn, &obj, 100).unwrap();
        }
        conn.execute(
            "UPDATE objects SET nar_hash = 'sha256:evil' WHERE store_path_hash = ?1",
            [&tampered],
        )
        .unwrap();
        // A second key joins: the good row must pick up its signature too.
        let mut state = test_state(conn).await;
        Arc::get_mut(&mut state).unwrap().keys = vec![key, key_two()];

        let Response::Resign { resigned, refused } = resign(&state).await.unwrap() else {
            panic!("not a resign response");
        };
        assert_eq!((resigned, refused), (1, 2));
        let conn = state.conn.lock();
        assert_eq!(db::get_object(&conn, &good).unwrap().unwrap().sigs.len(), 2);
        assert_eq!(
            db::get_object(&conn, &tampered)
                .unwrap()
                .unwrap()
                .sigs
                .len(),
            1
        );
        assert!(
            db::get_object(&conn, &unsigned)
                .unwrap()
                .unwrap()
                .sigs
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_dry_run_delete_reports_without_deleting() {
        let (a, missing) = ("a".repeat(32), "z".repeat(32));
        let mut conn = open_db();
        db::insert_object(&mut conn, &object(&a), 100).unwrap();
        let state = test_state(conn).await;

        let done = delete(&state, &[a.clone(), missing.clone()], true)
            .await
            .unwrap();
        assert_eq!(
            (done.deleted, done.bytes_freed, done.missing),
            (1, 5, vec![missing])
        );
        assert!(db::exists(&state.conn.lock(), &a).unwrap());
    }

    /// An absent hash is reported missing, not counted as a zero-byte delete.
    #[tokio::test]
    async fn deleting_an_absent_hash_reports_it_missing() {
        let missing = "z".repeat(32);
        let state = test_state(open_db()).await;

        let done = delete(&state, std::slice::from_ref(&missing), false)
            .await
            .unwrap();
        assert_eq!(
            (done.deleted, done.bytes_freed, done.missing),
            (0, 0, vec![missing])
        );
    }

    /// A listed object whose row goes before the delete runs (GC or fsck
    /// repair got there first) is neither reported nor counted as freed.
    #[tokio::test]
    async fn delete_pushed_by_reports_only_what_it_deleted() {
        let leaked = "https://issuer#leaked";
        let (a, b) = ("a".repeat(32), "b".repeat(32));
        let mut conn = open_db();
        push(&mut conn, &a, leaked, 100);
        push(&mut conn, &b, leaked, 200);
        let listed = pushed_by(&conn, leaked, None).unwrap();
        db::delete_object(&mut conn, &a).unwrap();
        let acking = fake_s3(axum::Router::new().fallback(|| async { S3_DELETED })).await;
        let state = test_state_with_storage(conn, acking).await;

        let (deleted, bytes_freed) = delete_listed(&state, listed, false).await.unwrap();
        let deleted: Vec<&str> = deleted
            .iter()
            .map(|row| row.store_path_hash.as_str())
            .collect();
        assert_eq!((deleted, bytes_freed), (vec![b.as_str()], 5));
        assert!(!db::exists(&state.conn.lock(), &b).unwrap());
    }
}
