//! State shared by the HTTP handlers, the admin socket, GC and fsck.

use std::sync::Arc;

use anyhow::{Context, Result};
use garret_server::{
    auth::Authenticator,
    inflight::InFlight,
    narinfo::SigningKeyFile,
    storage::{Storage, UploadLimits},
};
use parking_lot::Mutex;
use tokio::sync::Semaphore;

use crate::gc;

pub(crate) struct AppState {
    pub conn: Arc<Mutex<rusqlite::Connection>>,
    /// The database file, for `garret-admin backup` to copy on a connection
    /// of its own.
    pub db_path: String,
    pub storage: Storage,
    pub keys: Vec<SigningKeyFile>,
    pub store_dir: String,
    pub auth: Authenticator,
    pub limits: UploadLimits,
    pub uploads: Arc<Semaphore>,
    pub in_flight: InFlight,
    /// `None` without a `[gc]` section.
    pub gc: Option<Arc<gc::Gc>>,
}

/// Runs a blocking database call on the blocking pool, holding the
/// connection lock only inside it. SQLite can wait seconds on another
/// writer's lock; that must park a blocking thread, not a Tokio worker.
pub(crate) async fn db_run<T, F>(conn: &Arc<Mutex<rusqlite::Connection>>, f: F) -> Result<T>
where
    F: FnOnce(&mut rusqlite::Connection) -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    // A blocking thread does not inherit the async caller's span, so the
    // `db.call` span is created here (a child of the request) and entered
    // there. The lock wait is recorded on it: it is the part of a database
    // call the query itself does not explain.
    let span = tracing::debug_span!(
        "db.call",
        db.conn = "pusher",
        db.lock_wait_ms = tracing::field::Empty,
    );
    run_blocking(conn, f, Some(span)).await
}

/// [`db_run`] without a span of its own, for a loop that runs one query per
/// candidate (GC eviction): a span each would bury the trace in them.
pub(crate) async fn db_run_quiet<T, F>(conn: &Arc<Mutex<rusqlite::Connection>>, f: F) -> Result<T>
where
    F: FnOnce(&mut rusqlite::Connection) -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    run_blocking(conn, f, None).await
}

async fn run_blocking<T, F>(
    conn: &Arc<Mutex<rusqlite::Connection>>,
    f: F,
    span: Option<tracing::Span>,
) -> Result<T>
where
    F: FnOnce(&mut rusqlite::Connection) -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    let conn = conn.clone();
    tokio::task::spawn_blocking(move || {
        let _entered = span.as_ref().map(tracing::Span::enter);
        let waited = std::time::Instant::now();
        let mut conn = conn.lock();
        let waited = waited.elapsed();
        metrics::histogram!(garret_server::metrics::DB_LOCK_WAIT_SECONDS, "conn" => "pusher")
            .record(waited);
        if let Some(span) = &span {
            span.record("db.lock_wait_ms", waited.as_millis() as u64);
        }
        f(&mut conn)
    })
    .await
    .context("database task failed")?
}

#[cfg(test)]
pub(crate) mod testing {
    use std::time::Duration;

    use garret_server::{db, testing::unreachable_storage};

    use super::*;

    pub(crate) async fn test_state(conn: rusqlite::Connection) -> Arc<AppState> {
        test_state_with_storage(conn, unreachable_storage().await).await
    }

    pub(crate) async fn test_state_with_storage(
        conn: rusqlite::Connection,
        storage: Storage,
    ) -> Arc<AppState> {
        Arc::new(AppState {
            conn: Arc::new(Mutex::new(conn)),
            db_path: ":memory:".into(),
            storage,
            keys: vec![],
            store_dir: "/nix/store".into(),
            auth: Authenticator::new(vec![garret_server::testing::dev_issuer("unused")]).unwrap(),
            limits: UploadLimits::new(1024 * 1024, 4, 4 * 1024 * 1024, Duration::from_secs(60)),
            uploads: Arc::new(Semaphore::new(1)),
            in_flight: InFlight::new(),
            gc: None,
        })
    }

    pub(crate) fn open_db() -> rusqlite::Connection {
        let conn = db::open(":memory:", true).unwrap();
        db::migrate(&conn).unwrap();
        conn
    }
}
