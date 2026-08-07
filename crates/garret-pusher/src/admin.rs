//! The Pusher's admin socket (spec 10-packaging). Root-only by file mode:
//! anything reaching this socket is already privileged, so there is no
//! separate auth layer to keep in sync.

use std::sync::Arc;

use anyhow::{Context, Result};
use garret_common::admin::{Request, Response};
use garret_server::{db, narinfo};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
};

use crate::{AppState, gc::Gc};

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
