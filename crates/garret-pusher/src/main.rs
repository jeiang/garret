//! Pusher: accepts NARs over the garret push protocol (spec 01-push-protocol).
//! M3 slice — negotiation, streamed multipart upload, signing, OIDC,
//! backpressure and metrics. No GC yet (M4).

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;

mod admin;
mod gc;

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{post, put},
};
use futures::StreamExt;
use garret_common::Preamble;
use garret_server::{
    auth::{Authenticator, Subject},
    config::PusherConfig,
    db::{self, Object},
    inflight::InFlight,
    metrics as garret_metrics,
    narinfo::{self, SigningKeyFile},
    nix_base32, now,
    storage::{self, Storage, UploadLimits},
};
use serde_json::json;
use tokio::sync::Semaphore;

pub(crate) struct AppState {
    pub conn: Arc<Mutex<rusqlite::Connection>>,
    pub storage: Storage,
    pub keys: Vec<SigningKeyFile>,
    pub store_dir: String,
    pub auth: Authenticator,
    pub limits: UploadLimits,
    pub uploads: Arc<Semaphore>,
    pub in_flight: InFlight,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let path = std::env::args()
        .nth(1)
        .context("usage: garret-pusher <config.toml>")?;
    let cfg: PusherConfig = garret_server::config::load(&path)?;

    // Before any metric call: the recorder must exist first or the value is lost.
    let metrics_handle = garret_metrics::install("pusher")?;
    let metrics_listen = cfg.metrics_listen.clone();
    tokio::spawn(async move {
        if let Err(e) = garret_metrics::serve(metrics_handle, &metrics_listen).await {
            tracing::error!("metrics listener failed: {e:#}");
        }
    });

    let addr: std::net::SocketAddr = cfg.listen.parse().context("invalid listen address")?;
    // Authenticator::new refuses an empty issuer list, so this cannot start
    // unauthenticated (spec 04: no auth-disable flag).
    let auth = Authenticator::new(cfg.oidc.clone())?;

    let conn = db::open(&cfg.db_path, true)?;
    db::migrate(&conn)?;
    conn.pragma_update(None, "wal_checkpoint", "TRUNCATE").ok();

    let state = Arc::new(AppState {
        conn: Arc::new(Mutex::new(conn)),
        storage: Storage::new(&cfg.s3).await?,
        keys: garret_server::load_signing_keys(&cfg.signing_key_files)?,
        store_dir: cfg.store_dir,
        auth,
        limits: UploadLimits::new(
            cfg.limits.part_size,
            cfg.limits.max_parts_in_flight,
            cfg.limits.max_in_flight_bytes,
        ),
        uploads: Arc::new(Semaphore::new(cfg.limits.max_concurrent_uploads)),
        in_flight: InFlight::new(),
    });

    // Saturation must be visible before it hurts, so the caps are exported
    // alongside the gauges that approach them (spec 08). The recorder is
    // already installed above — a gauge set before it would go nowhere.
    metrics::gauge!("garret_uploads_limit").set(cfg.limits.max_concurrent_uploads as f64);
    metrics::gauge!("garret_in_flight_bytes_limit").set(cfg.limits.max_in_flight_bytes as f64);
    metrics::gauge!("garret_part_slots_limit").set(state.limits.total_slots() as f64);

    let collector = cfg.gc.clone().map(|gc_cfg| {
        Arc::new(gc::Gc {
            conn: state.conn.clone(),
            storage: state.storage.clone(),
            in_flight: state.in_flight.clone(),
            cfg: gc_cfg,
        })
    });

    if let Some(admin_socket) = cfg.admin_socket.clone() {
        let (state, collector) = (state.clone(), collector.clone());
        tokio::spawn(async move {
            if let Err(e) = admin::serve(admin_socket, state, collector).await {
                tracing::error!("admin socket failed: {e:#}");
            }
        });
    }

    if let Some(collector) = collector.clone() {
        let gc_cfg = collector.cfg.clone();
        tokio::spawn(async move {
            // Startup sweep first, then a tick loop. Each tick is a counter
            // check; eviction only happens past the high watermark (spec 05).
            if let Err(e) = collector.sweep_orphans().await {
                tracing::error!("startup orphan sweep failed: {e:#}");
            }
            let mut ticker = tokio::time::interval(Duration::from_secs(gc_cfg.interval_secs));
            let mut since_sweep = Duration::ZERO;
            loop {
                ticker.tick().await;
                if let Err(e) = collector.tick().await {
                    tracing::error!("GC pass failed: {e:#}");
                }
                since_sweep += Duration::from_secs(gc_cfg.interval_secs);
                if since_sweep >= Duration::from_secs(7 * 24 * 60 * 60) {
                    since_sweep = Duration::ZERO;
                    if let Err(e) = collector.sweep_orphans().await {
                        tracing::error!("weekly orphan sweep failed: {e:#}");
                    }
                }
            }
        });
    } else {
        tracing::warn!("no [gc] section: the cache is unbounded and will never evict");
    }

    let app = Router::new()
        .route("/api/v1/missing-paths", post(missing_paths))
        .route("/api/v1/nar/{hash}", put(upload))
        .layer(middleware::from_fn_with_state(state.clone(), require_oidc))
        .layer(middleware::from_fn(garret_metrics::track_http))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("pusher listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

struct Error(StatusCode, String);

impl Error {
    /// 429s carry Retry-After so clients back off on the server's terms.
    fn retry_after(status: StatusCode, message: &str) -> Self {
        Error(status, message.to_owned())
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        if self.0 == StatusCode::TOO_MANY_REQUESTS {
            return (
                self.0,
                [(header::RETRY_AFTER, "1")],
                Json(json!({ "error": self.1 })),
            )
                .into_response();
        }
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        tracing::error!("{e:#}");
        Error(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"))
    }
}

/// Every Pusher endpoint requires a valid token from a configured issuer.
async fn require_oidc(
    State(state): State<Arc<AppState>>,
    mut request: Request,
    next: Next,
) -> Response {
    let token = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let Some(token) = token else {
        return unauthorized("missing bearer token");
    };
    match state.auth.authenticate(token).await {
        Ok(subject) => {
            request.extensions_mut().insert(subject);
            next.run(request).await
        }
        // The reason stays in the log; the caller learns only that it failed.
        Err(e) => {
            tracing::warn!("rejected token: {e:#}");
            unauthorized("invalid token")
        }
    }
}

fn unauthorized(message: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        Json(json!({ "error": message })),
    )
        .into_response()
}

async fn missing_paths(
    State(state): State<Arc<AppState>>,
    Json(hashes): Json<Vec<String>>,
) -> Result<Json<Vec<String>>, Error> {
    let missing = {
        let conn = state.conn.lock().unwrap();
        db::missing(&conn, &hashes)?
    };
    metrics::histogram!("garret_negotiation_batch_size").record(hashes.len() as f64);
    if !hashes.is_empty() {
        metrics::histogram!("garret_negotiation_missing_ratio")
            .record(missing.len() as f64 / hashes.len() as f64);
    }
    Ok(Json(missing))
}

async fn upload(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    axum::Extension(subject): axum::Extension<Subject>,
    body: Body,
) -> Result<Response, Error> {
    // Idempotency: answered before the body is read, so `Expect: 100-continue`
    // clients skip the transfer entirely (spec 01).
    if db::exists(&state.conn.lock().unwrap(), &hash).map_err(Error::from)? {
        metrics::counter!("garret_upload_skipped_total", "reason" => "exists").increment(1);
        return Ok((StatusCode::OK, Json(json!({"status": "exists"}))).into_response());
    }

    // Shed before reading a byte: the queue belongs in the clients, and past
    // the cap the server stays fast rather than slowly running out of memory.
    let Ok(_slot) = state.uploads.clone().try_acquire_owned() else {
        metrics::counter!("garret_uploads_shed_total").increment(1);
        return Err(Error::retry_after(
            StatusCode::TOO_MANY_REQUESTS,
            "too many concurrent uploads",
        ));
    };
    // Second pusher for the same path: first writer wins, and the loser treats
    // it as success rather than racing to overwrite an identical blob.
    let Some(_claim) = state.in_flight.claim(&hash) else {
        metrics::counter!("garret_upload_skipped_total", "reason" => "in-progress").increment(1);
        return Ok((StatusCode::OK, Json(json!({"status": "in-progress"}))).into_response());
    };

    metrics::gauge!("garret_uploads_in_flight").increment(1.0);
    let started = std::time::Instant::now();
    let result = store_upload(&state, &hash, &subject, body).await;
    metrics::gauge!("garret_uploads_in_flight").decrement(1.0);

    match &result {
        Ok(size) => {
            metrics::counter!("garret_uploads_accepted_total").increment(1);
            metrics::histogram!("garret_upload_bytes").record(*size as f64);
            metrics::histogram!("garret_upload_duration_seconds").record(started.elapsed());
        }
        Err(_) => metrics::counter!("garret_uploads_failed_total").increment(1),
    }
    result?;
    Ok((StatusCode::CREATED, Json(json!({"status": "created"}))).into_response())
}

/// Reads the preamble, then streams the rest straight to S3. Returns the
/// stored size. The body never lands in memory whole.
async fn store_upload(
    state: &AppState,
    hash: &str,
    subject: &Subject,
    body: Body,
) -> Result<i64, Error> {
    let mut stream = body.into_data_stream();
    let mut head: Vec<u8> = Vec::new();
    let preamble = loop {
        if let Some(preamble) = take_preamble(&mut head)? {
            break preamble;
        }
        let Some(chunk) = stream.next().await else {
            return Err(Error(
                StatusCode::BAD_REQUEST,
                "body ended before the preamble was complete".into(),
            ));
        };
        head.extend_from_slice(&chunk.map_err(|e| Error(StatusCode::BAD_REQUEST, e.to_string()))?);
    };

    // Whatever followed the preamble in that chunk is the start of the NAR.
    let leftover = Bytes::from(head);
    let nar = futures::stream::once(async move { Ok::<_, axum::Error>(leftover) }).chain(stream);

    let (digest, file_size) = state
        .storage
        .put_streaming(&storage::key_for(hash), Box::pin(nar), &state.limits)
        .await?;

    let object = build_object(hash, &preamble, &digest, file_size, state, subject)?;
    db::insert_object(&mut state.conn.lock().unwrap(), &object, now())?;
    Ok(file_size)
}

/// Splits the 4-byte-LE-length-prefixed JSON preamble off the front of `buf`,
/// leaving the compressed NAR bytes behind. `None` means "need more bytes".
fn take_preamble(buf: &mut Vec<u8>) -> Result<Option<Preamble>, Error> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
    if len > 8 * 1024 * 1024 {
        return Err(Error(
            StatusCode::BAD_REQUEST,
            "preamble length is implausible".into(),
        ));
    }
    if buf.len() < 4 + len {
        return Ok(None);
    }
    let preamble: Preamble = serde_json::from_slice(&buf[4..4 + len])
        .map_err(|e| Error(StatusCode::BAD_REQUEST, format!("bad preamble: {e}")))?;
    buf.drain(..4 + len);
    Ok(Some(preamble))
}

fn build_object(
    hash: &str,
    preamble: &Preamble,
    digest: &[u8],
    file_size: i64,
    state: &AppState,
    subject: &Subject,
) -> Result<Object, Error> {
    let name = preamble
        .store_path
        .rsplit('/')
        .next()
        .and_then(|base| base.split_once('-'))
        .map(|(_, name)| name.to_owned())
        .ok_or_else(|| Error(StatusCode::BAD_REQUEST, "malformed store path".into()))?;

    if !preamble.store_path.contains(hash) {
        return Err(Error(
            StatusCode::BAD_REQUEST,
            "store path does not match the URL hash".into(),
        ));
    }

    let mut object = Object {
        store_path_hash: hash.to_owned(),
        store_path: preamble.store_path.clone(),
        name,
        // Normalised on the way in, so the DB, narinfo and fingerprint all
        // agree on the spelling nix signs over.
        nar_hash: narinfo::normalize_hash(&preamble.nar_hash)
            .map_err(|e| Error(StatusCode::BAD_REQUEST, format!("{e:#}")))?,
        nar_size: preamble.nar_size,
        // Server-computed over exactly the bytes stored — the only integrity
        // check in the system now that the Puller redirects (ADR-0005).
        file_hash: format!("sha256:{}", nix_base32::encode(digest)),
        file_size,
        deriver: preamble.deriver.clone().map(basename),
        ca: preamble.ca.clone(),
        references: preamble.references.iter().cloned().map(basename).collect(),
        sigs: vec![],
        pushed_by: Some(subject.0.clone()),
    };
    object.references.sort();
    object.sigs = narinfo::sign(&object, &state.store_dir, &state.keys)?;
    Ok(object)
}

fn basename(path: String) -> String {
    path.rsplit('/').next().unwrap_or(&path).to_owned()
}
