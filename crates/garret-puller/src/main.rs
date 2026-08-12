//! Puller: the public Nix substituter. Serves narinfo (it holds the
//! signatures) and redirects NAR requests to presigned S3 URLs (ADR-0005).
//! M1 slice — no browse API (M4), no last-accessed bumps (M4).

use std::{
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use anyhow::{Context, Result};
use axum::{
    Router,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Redirect, Response},
    routing::get,
};
use garret_server::{
    auth::Authenticator,
    browse,
    config::PullerConfig,
    db, metrics as garret_metrics, narinfo, now,
    storage::{self, Storage},
};
use serde::Deserialize;

struct AppState {
    /// Empty until the Pusher has created the database (spec 02: it owns the
    /// schema). Every reader goes through `conn`, so a Puller that boots first
    /// answers 503 instead of dying. Arc'd so pull-path reads can move onto
    /// the blocking pool under a budget (ticket 25).
    conn: OnceLock<Arc<Mutex<rusqlite::Connection>>>,
    storage: Storage,
    presign_ttl: Duration,
    bump_debounce: i64,
    db_read_budget: Duration,
    presign_budget: Duration,
}

impl AppState {
    fn conn(&self) -> Option<std::sync::MutexGuard<'_, rusqlite::Connection>> {
        Some(self.conn.get()?.lock().unwrap())
    }

    fn conn_handle(&self) -> Option<Arc<Mutex<rusqlite::Connection>>> {
        self.conn.get().cloned()
    }
}

fn unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, "database not ready").into_response()
}

/// Degrade to a miss (ticket 25). A substituter's contract is bounded latency
/// and harmless failure: nix handles a 404 natively (try the next
/// substituter, build locally), while a hang stalls builds fleet-wide and a
/// 500 is noise it isn't built for. Counted by reason so degradation is
/// observable, never silent.
fn degraded_miss(reason: &'static str) -> Response {
    metrics::counter!("garret_degraded_total", "reason" => reason).increment(1);
    StatusCode::NOT_FOUND.into_response()
}

/// A pull-path database read under the configured budget. The read is sync
/// rusqlite under a Mutex, so it runs on the blocking pool — a wedged read
/// (or one queued behind a wedged lock holder) then trips the timeout instead
/// of stalling the request; the orphaned read keeps its blocking thread until
/// it returns, but the client has its answer. `None` means the budget tripped.
async fn db_read<T, F>(
    conn: Arc<Mutex<rusqlite::Connection>>,
    budget: Duration,
    read: F,
) -> Option<anyhow::Result<T>>
where
    F: FnOnce(&rusqlite::Connection) -> anyhow::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let task = tokio::task::spawn_blocking(move || read(&conn.lock().unwrap()));
    match tokio::time::timeout(budget, task).await {
        Ok(joined) => {
            Some(joined.unwrap_or_else(|e| Err(anyhow::anyhow!("db read task failed: {e}"))))
        }
        Err(_elapsed) => None,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let path = std::env::args()
        .nth(1)
        .context("usage: garret-puller <config.toml>")?;
    let cfg: PullerConfig = garret_server::config::load(&path)?;

    let state = Arc::new(AppState {
        conn: OnceLock::new(),
        storage: Storage::new(&cfg.s3).await?,
        presign_ttl: Duration::from_secs(cfg.presign_ttl_secs),
        bump_debounce: cfg.bump_debounce_secs,
        db_read_budget: Duration::from_millis(cfg.db_read_budget_ms),
        presign_budget: Duration::from_millis(cfg.presign_budget_ms),
    });

    // Opened off the request path so the listener (and /ready) comes up now.
    tokio::spawn({
        let state = state.clone();
        let db_path = cfg.db_path.clone();
        let timeout = Duration::from_secs(cfg.db_wait_timeout_secs);
        async move {
            match db::open_when_ready(&db_path, timeout).await {
                Ok(conn) => {
                    let _ = state.conn.set(Arc::new(Mutex::new(conn)));
                    tracing::info!("database ready: serving");
                }
                // Nothing this process can do but let the supervisor restart it.
                Err(e) => {
                    tracing::error!("{e:#}");
                    std::process::exit(1);
                }
            }
        }
    });

    let metrics_handle = garret_metrics::install("puller")?;
    let metrics_listen = cfg.metrics_listen.clone();
    tokio::spawn(async move {
        if let Err(e) = garret_metrics::serve(metrics_handle, &metrics_listen).await {
            tracing::error!("metrics listener failed: {e:#}");
        }
    });

    let store_dir = cfg.store_dir.clone();
    // Browse routes are the only authenticated surface here, and they are
    // simply absent when no issuer is configured (spec 07).
    let browse_routes = match &cfg.browse_oidc {
        Some(issuer) => {
            let auth = Arc::new(Authenticator::new(vec![issuer.clone()])?);
            Router::new()
                .route("/api/v1/objects", get(list_objects))
                .route("/api/v1/objects/{hash}", get(object_detail))
                .route("/api/v1/objects/{hash}/tree", get(object_tree))
                .route("/api/v1/objects/{hash}/referrers", get(object_referrers))
                .route("/api/v1/pins", get(list_pins))
                .layer(axum::middleware::from_fn_with_state(
                    auth,
                    require_browse_oidc,
                ))
        }
        None => {
            tracing::info!("no browse_oidc configured: the browse API is not served");
            Router::new()
        }
    };

    let app = Router::new()
        .route(
            "/nix-cache-info",
            get(move || {
                let body = format!("StoreDir: {store_dir}\nWantMassQuery: 1\nPriority: 40\n");
                async move { body }
            }),
        )
        .route(
            "/ready",
            get(|State(state): State<Arc<AppState>>| async move {
                match state.conn().is_some() {
                    true => (StatusCode::OK, "ready").into_response(),
                    false => unavailable(),
                }
            }),
        )
        // axum 0.8 wants whole-segment params, so the suffix is split here.
        .route("/{file}", get(narinfo_route))
        .route("/nar/{file}", get(nar_route))
        .merge(browse_routes)
        .layer(axum::middleware::from_fn(garret_metrics::track_http))
        .with_state(state);

    let addr: std::net::SocketAddr = cfg.listen.parse().context("invalid listen address")?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("puller listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn narinfo_route(State(state): State<Arc<AppState>>, Path(file): Path<String>) -> Response {
    let Some(hash) = file.strip_suffix(".narinfo").map(str::to_owned) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(conn) = state.conn_handle() else {
        return unavailable();
    };
    let object = {
        let hash = hash.clone();
        db_read(conn, state.db_read_budget, move |conn| {
            db::get_object(conn, &hash)
        })
        .await
    };
    // A degraded request is served as a miss, so it counts as one here too.
    metrics::counter!(
        "garret_narinfo_requests_total",
        "outcome" => if matches!(object, Some(Ok(Some(_)))) { "hit" } else { "miss" },
    )
    .increment(1);
    let Some(object) = object else {
        return degraded_miss("db_timeout");
    };

    // Fire-and-forget: LRU only needs day granularity, so a bump must never
    // sit on the request path or hold up a substituter (spec 02-database).
    if matches!(object, Ok(Some(_))) {
        let state = state.clone();
        let hash = hash.clone();
        tokio::spawn(async move {
            let Some(conn) = state.conn() else { return };
            if let Err(e) = db::bump_last_accessed(&conn, &hash, now(), state.bump_debounce) {
                metrics::counter!("garret_bump_failures_total").increment(1);
                tracing::warn!("last-accessed bump for {hash} failed: {e:#}");
            }
        });
    }
    match object {
        Ok(Some(obj)) => (
            [(header::CONTENT_TYPE, "text/x-nix-narinfo")],
            narinfo::render(&obj),
        )
            .into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!("narinfo {hash}: {e:#}");
            degraded_miss("db_error")
        }
    }
}

async fn nar_route(State(state): State<Arc<AppState>>, Path(file): Path<String>) -> Response {
    let Some(hash) = file.strip_suffix(".nar.zst") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    // Only redirect to blobs we have a row for — row exists ⇒ blob exists.
    let Some(conn) = state.conn_handle() else {
        return unavailable();
    };
    let exists = {
        let hash = hash.to_owned();
        db_read(conn, state.db_read_budget, move |conn| {
            db::exists(conn, &hash)
        })
        .await
    };
    match exists {
        None => return degraded_miss("db_timeout"),
        Some(Ok(false)) => return StatusCode::NOT_FOUND.into_response(),
        Some(Err(e)) => {
            tracing::error!("nar {hash}: {e:#}");
            return degraded_miss("db_error");
        }
        Some(Ok(true)) => {}
    }
    let started = std::time::Instant::now();
    match tokio::time::timeout(
        state.presign_budget,
        state
            .storage
            .presigned_get(&storage::key_for(hash), state.presign_ttl),
    )
    .await
    {
        Ok(Ok(url)) => {
            metrics::counter!("garret_nar_redirects_total").increment(1);
            metrics::histogram!("garret_presign_duration_seconds").record(started.elapsed());
            Redirect::temporary(&url).into_response()
        }
        Ok(Err(e)) => {
            tracing::error!("presigning {hash}: {e:#}");
            degraded_miss("presign_error")
        }
        Err(_elapsed) => degraded_miss("presign_timeout"),
    }
}

/// Pocket ID only, and only here — narinfo and NAR stay anonymous so any
/// machine's nix.conf works untouched (spec 04-auth).
async fn require_browse_oidc(
    State(auth): State<Arc<Authenticator>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let token = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match token {
        Some(token) if auth.authenticate(token).await.is_ok() => next.run(request).await,
        _ => {
            metrics::counter!("garret_browse_auth_failures_total").increment(1);
            (
                StatusCode::UNAUTHORIZED,
                [(axum::http::header::WWW_AUTHENTICATE, "Bearer")],
                "unauthorized",
            )
                .into_response()
        }
    }
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    q: Option<String>,
    #[serde(default = "default_limit")]
    limit: usize,
    cursor: Option<String>,
}

fn default_limit() -> usize {
    50
}

fn browse_response<T: serde::Serialize>(
    endpoint: &'static str,
    result: anyhow::Result<Option<T>>,
) -> Response {
    metrics::counter!("garret_browse_requests_total", "endpoint" => endpoint).increment(1);
    match result {
        Ok(Some(value)) => axum::Json(value).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!("browse {endpoint}: {e:#}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn list_objects(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(query): axum::extract::Query<ListQuery>,
) -> Response {
    let Some(conn) = state.conn() else {
        return unavailable();
    };
    browse_response(
        "objects",
        browse::list(
            &conn,
            query.q.as_deref(),
            query.limit,
            query.cursor.as_deref(),
        )
        .map(Some),
    )
}

async fn object_detail(State(state): State<Arc<AppState>>, Path(hash): Path<String>) -> Response {
    let Some(conn) = state.conn() else {
        return unavailable();
    };
    browse_response("object", db::get_object(&conn, &hash))
}

async fn object_tree(State(state): State<Arc<AppState>>, Path(hash): Path<String>) -> Response {
    let Some(conn) = state.conn() else {
        return unavailable();
    };
    browse_response("tree", browse::tree(&conn, &hash, 64))
}

async fn object_referrers(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
) -> Response {
    let Some(conn) = state.conn() else {
        return unavailable();
    };
    browse_response("referrers", browse::referrers(&conn, &hash).map(Some))
}

async fn list_pins(State(state): State<Arc<AppState>>) -> Response {
    let Some(conn) = state.conn() else {
        return unavailable();
    };
    browse_response("pins", browse::pins(&conn).map(Some))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn() -> Arc<Mutex<rusqlite::Connection>> {
        Arc::new(Mutex::new(rusqlite::Connection::open_in_memory().unwrap()))
    }

    #[tokio::test]
    async fn a_read_within_budget_returns_its_result() {
        let got = db_read(conn(), Duration::from_secs(5), |_| Ok(42)).await;
        assert_eq!(got.unwrap().unwrap(), 42);
    }

    /// The read is sync under the connection Mutex; the budget must still
    /// trip while it is wedged (ticket 25) — hence the blocking pool.
    #[tokio::test]
    async fn a_wedged_read_trips_the_budget_instead_of_hanging() {
        let got = db_read(conn(), Duration::from_millis(25), |_| {
            std::thread::sleep(Duration::from_millis(500));
            Ok(())
        })
        .await;
        assert!(got.is_none(), "the budget should have tripped");
    }

    /// Same failure mode, different cause: the wedged reader holds the lock,
    /// and the next read queues behind it. Its budget must trip too.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_read_queued_behind_a_wedged_lock_holder_trips_its_budget() {
        let conn = conn();
        let (holding_tx, holding_rx) = std::sync::mpsc::channel();
        let wedged = db_read(conn.clone(), Duration::from_millis(25), move |_| {
            // Signal that the lock is held so the queued read provably starts
            // second; otherwise it could win the lock and succeed.
            holding_tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(500));
            Ok(())
        });
        let wedged = tokio::spawn(wedged);
        holding_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("wedged read should acquire the lock");
        let queued = db_read(conn, Duration::from_millis(25), |_| Ok(())).await;
        let wedged = wedged.await.unwrap();
        assert!(wedged.is_none());
        assert!(queued.is_none(), "queued read should degrade, not wait");
    }

    #[tokio::test]
    async fn a_panicking_read_surfaces_as_an_error_not_a_crash() {
        let got = db_read(conn(), Duration::from_secs(5), |_| -> anyhow::Result<()> {
            panic!("boom")
        })
        .await;
        assert!(got.unwrap().is_err());
    }
}
