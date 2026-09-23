//! The Pusher's admin socket (spec 10-packaging). Owner-only by file mode
//! (0600, the Pusher's own user): only root and the Pusher's uid can connect,
//! and both already hold everything the socket grants, so there is no
//! separate auth layer to keep in sync. That is why no other process may run
//! as that uid — the Puller has a user of its own.

use std::{path::Path, sync::Arc};

use anyhow::{Context, Result};
use garret_common::admin::{Request, Response};
use garret_server::{db, narinfo};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
};

use crate::{AppState, fsck, gc::Gc};

pub async fn serve(path: String, state: Arc<AppState>, gc: Option<Arc<Gc>>) -> Result<()> {
    // A socket left behind by a crashed process would block the bind.
    let _ = std::fs::remove_file(&path);
    if let Some(parent) = std::path::Path::new(&path).parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let listener = UnixListener::bind(&path).with_context(|| format!("binding {path}"))?;
    restrict(&path)?;
    tracing::info!("admin socket listening on {path}");

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

/// 0600: the file mode *is* the authorization for this socket.
#[cfg(unix)]
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
        Request::Status => status(state, gc),
        Request::GcRun => match gc {
            Some(gc) => gc.run().await.map(|pass| Response::Gc {
                evicted: pass.evicted,
                bytes_freed: pass.bytes_freed,
                candidates_exhausted: pass.candidates_exhausted,
            }),
            None => Ok(Response::Error {
                message: "no [gc] section is configured".into(),
            }),
        },
        Request::Resign => resign(state).map(|resigned| Response::Resign { resigned }),
        Request::Delete { hashes } => delete(state, &hashes).await,
        Request::Pin {
            name,
            hash,
            expires_at,
        } => {
            let conn = state.conn.lock().unwrap();
            db::pin(&conn, &name, &hash, expires_at, garret_server::now()).map(|()| Response::Pin)
        }
        Request::Unpin { name } => {
            let conn = state.conn.lock().unwrap();
            db::unpin(&conn, &name).map(|removed| Response::Unpin { removed })
        }
        Request::Fsck {
            repair,
            verify_sizes,
            quiesce,
        } => match (quiesce && !repair, gc) {
            (true, _) => Ok(Response::Error {
                message: "--quiesce requires --repair".into(),
            }),
            (false, Some(gc)) => fsck::run(state, gc, repair, verify_sizes, quiesce).await,
            (false, None) => Ok(Response::Error {
                message: "no [gc] section is configured".into(),
            }),
        },
        Request::Prune { before, dry_run } => prune(state, before, dry_run).await,
        Request::Backup { path } => backup(state, path).await,
    };
    result.unwrap_or_else(|e| Response::Error {
        message: format!("{e:#}"),
    })
}

fn status(state: &AppState, gc: Option<&Gc>) -> Result<Response> {
    let conn = state.conn.lock().unwrap();
    Ok(Response::Status {
        objects: conn.query_row("SELECT COUNT(*) FROM objects", [], |r| r.get(0))?,
        total_bytes: db::total_bytes(&conn)?,
        quota_bytes: gc.map(|gc| gc.cfg.quota_bytes),
        uploads_in_flight: state.in_flight.len(),
    })
}

/// Removes objects outright: row first, blob second, exactly as GC does
/// (spec 05) so a failed blob delete leaves an orphan for the sweep rather
/// than a row pointing at nothing.
///
/// Deliberately unconditional -- it does not check whether anything still
/// references the object. GC decides by reachability; this is the operator
/// saying "remove it regardless", which is the whole reason it exists.
async fn delete(state: &AppState, hashes: &[String]) -> Result<Response> {
    let mut deleted = 0;
    let mut bytes_freed = 0;
    let mut missing = Vec::new();
    let mut keys = Vec::new();

    for hash in hashes {
        let present = {
            let conn = state.conn.lock().unwrap();
            db::exists(&conn, hash)?
        };
        if !present {
            missing.push(hash.clone());
            continue;
        }
        let freed = {
            let mut conn = state.conn.lock().unwrap();
            db::delete_object(&mut conn, hash)?
        };
        deleted += 1;
        bytes_freed += freed;
        keys.push(garret_server::storage::key_for(hash));
    }

    if !keys.is_empty() {
        state.storage.delete_objects(&keys).await?;
    }
    Ok(Response::Delete {
        deleted,
        bytes_freed,
        missing,
    })
}

/// Deletes old closures (spec 05). Unlike GC this ignores quota: the operator
/// chose the cutoff. Row first, blob second, as everywhere else.
async fn prune(state: &AppState, before: i64, dry_run: bool) -> Result<Response> {
    let pruned = {
        let mut conn = state.conn.lock().unwrap();
        db::prune(&mut conn, before, garret_server::now(), dry_run)?
    };
    let bytes_freed = pruned.iter().map(|(_, _, size)| size).sum();
    if !dry_run {
        let keys: Vec<String> = pruned
            .iter()
            .map(|(hash, _, _)| garret_server::storage::key_for(hash))
            .collect();
        state.storage.delete_objects(&keys).await?;
        tracing::info!(
            before,
            objects = pruned.len(),
            bytes_freed,
            "prune complete"
        );
    }
    Ok(Response::Prune {
        pruned: pruned
            .into_iter()
            .map(|(hash, name, _)| format!("{hash}-{name}"))
            .collect(),
        bytes_freed,
    })
}

/// Backfills signatures after a key is added, so both the old and new key
/// appear on every object during an overlap rotation (spec 10-packaging).
fn resign(state: &AppState) -> Result<usize> {
    let mut conn = state.conn.lock().unwrap();
    let hashes = db::all_hashes(&conn)?;
    let mut resigned = 0;
    for hash in hashes {
        let Some(object) = db::get_object(&conn, &hash)? else {
            continue;
        };
        let sigs = narinfo::sign(&object, &state.store_dir, &state.keys)?;
        if sigs != object.sigs {
            db::update_sigs(&mut conn, &hash, &sigs)?;
            resigned += 1;
        }
    }
    Ok(resigned)
}

/// The online backup: a consistent copy of the whole database, taken while
/// both services keep running (spec 10-packaging). Takes the source path from
/// the shared connection and copies on a connection of its own, so pushes
/// wait on nothing longer than that lookup.
async fn backup(state: &AppState, dest: String) -> Result<Response> {
    let source = state
        .conn
        .lock()
        .unwrap()
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

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("garret-backup-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// The copy must hold rows that so far live only in the WAL: copying the
    /// main file's bytes would silently drop everything since the last
    /// checkpoint.
    #[test]
    fn backup_copies_uncheckpointed_rows_with_mode_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = scratch("wal");
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
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// An existing file is never overwritten, and never removed either -- it
    /// may be yesterday's good backup.
    #[test]
    fn backup_refuses_to_overwrite() {
        let dir = scratch("exists");
        let source = dir.join("garret.db");
        db::migrate(&db::open(source.to_str().unwrap(), true).unwrap()).unwrap();
        let dest = dir.join("backup.db");
        std::fs::write(&dest, "yesterday").unwrap();

        assert!(backup_to(source.to_str().unwrap(), &dest).is_err());
        assert_eq!(std::fs::read_to_string(&dest).unwrap(), "yesterday");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
