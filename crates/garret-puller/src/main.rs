//! Puller: the public Nix substituter. Serves narinfo (it holds the
//! signatures) and redirects NAR requests to presigned S3 URLs (ADR-0005).
//! M1 slice — no browse API (M4), no last-accessed bumps (M4).

use std::{
    sync::{Arc, Mutex},
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
    config::PullerConfig,
    db, metrics as garret_metrics, narinfo,
    storage::{self, Storage},
};

struct AppState {
    conn: Mutex<rusqlite::Connection>,
    storage: Storage,
    presign_ttl: Duration,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let path = std::env::args()
        .nth(1)
        .context("usage: garret-puller <config.toml>")?;
    let cfg: PullerConfig = garret_server::config::load(&path)?;

    let state = Arc::new(AppState {
        conn: Mutex::new(db::open(&cfg.db_path, false)?),
        storage: Storage::new(&cfg.s3).await?,
        presign_ttl: Duration::from_secs(cfg.presign_ttl_secs),
    });

    let metrics_handle = garret_metrics::install("puller")?;
    let metrics_listen = cfg.metrics_listen.clone();
    tokio::spawn(async move {
        if let Err(e) = garret_metrics::serve(metrics_handle, &metrics_listen).await {
            tracing::error!("metrics listener failed: {e:#}");
        }
    });

    let store_dir = cfg.store_dir.clone();
    let app = Router::new()
        .route(
            "/nix-cache-info",
            get(move || {
                let body = format!("StoreDir: {store_dir}\nWantMassQuery: 1\nPriority: 40\n");
                async move { body }
            }),
        )
        // axum 0.8 wants whole-segment params, so the suffix is split here.
        .route("/{file}", get(narinfo_route))
        .route("/nar/{file}", get(nar_route))
        .layer(axum::middleware::from_fn(garret_metrics::track_http))
        .with_state(state);

    let addr: std::net::SocketAddr = cfg.listen.parse().context("invalid listen address")?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("puller listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn narinfo_route(State(state): State<Arc<AppState>>, Path(file): Path<String>) -> Response {
    let Some(hash) = file.strip_suffix(".narinfo") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let object = { db::get_object(&state.conn.lock().unwrap(), hash) };
    metrics::counter!(
        "garret_narinfo_requests_total",
        "outcome" => if matches!(object, Ok(Some(_))) { "hit" } else { "miss" },
    )
    .increment(1);
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
    match db::exists(&state.conn.lock().unwrap(), hash) {
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
