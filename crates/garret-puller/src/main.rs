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
    /// answers 503 instead of dying.
    conn: OnceLock<Mutex<rusqlite::Connection>>,
    storage: Storage,
    presign_ttl: Duration,
    bump_debounce: i64,
}

impl AppState {
    fn conn(&self) -> Option<std::sync::MutexGuard<'_, rusqlite::Connection>> {
        Some(self.conn.get()?.lock().unwrap())
    }
}

fn unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, "database not ready").into_response()
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
    });

    // Opened off the request path so the listener (and /ready) comes up now.
    tokio::spawn({
        let state = state.clone();
        let db_path = cfg.db_path.clone();
        let timeout = Duration::from_secs(cfg.db_wait_timeout_secs);
        async move {
            match db::open_when_ready(&db_path, timeout).await {
                Ok(conn) => {
                    let _ = state.conn.set(Mutex::new(conn));
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
    let Some(conn) = state.conn() else {
        return unavailable();
    };
    let object = db::get_object(&conn, &hash);
    drop(conn);
    metrics::counter!(
        "garret_narinfo_requests_total",
        "outcome" => if matches!(object, Ok(Some(_))) { "hit" } else { "miss" },
    )
    .increment(1);

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
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn nar_route(State(state): State<Arc<AppState>>, Path(file): Path<String>) -> Response {
    let Some(hash) = file.strip_suffix(".nar.zst") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    // Only redirect to blobs we have a row for — row exists ⇒ blob exists.
    // Scoped: a std guard cannot be held across the presign await.
    let exists = match state.conn() {
        Some(conn) => db::exists(&conn, hash),
        None => return unavailable(),
    };
    match exists {
        Ok(false) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!("nar {hash}: {e:#}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        Ok(true) => {}
    }
    let started = std::time::Instant::now();
    match state
        .storage
        .presigned_get(&storage::key_for(hash), state.presign_ttl)
        .await
    {
        Ok(url) => {
            metrics::counter!("garret_nar_redirects_total").increment(1);
            metrics::histogram!("garret_presign_duration_seconds").record(started.elapsed());
            Redirect::temporary(&url).into_response()
        }
        Err(e) => {
            tracing::error!("presigning {hash}: {e:#}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
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
