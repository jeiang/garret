//! Pusher: accepts NARs over the garret push protocol (spec 01-push-protocol).
//! M2 slice — negotiation, upload, signing, OIDC. No multipart (M3), no GC (M4).

use std::sync::{Arc, Mutex};

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
    narinfo::{self, SigningKeyFile},
    nix_base32, now,
    storage::{self, Storage},
};
use serde_json::json;
use sha2::{Digest, Sha256};

struct AppState {
    conn: Mutex<rusqlite::Connection>,
    storage: Storage,
    keys: Vec<SigningKeyFile>,
    store_dir: String,
    max_body_bytes: u64,
    auth: Authenticator,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let path = std::env::args()
        .nth(1)
        .context("usage: garret-pusher <config.toml>")?;
    let cfg: PusherConfig = garret_server::config::load(&path)?;

    let addr: std::net::SocketAddr = cfg.listen.parse().context("invalid listen address")?;
    // Authenticator::new refuses an empty issuer list, so this cannot start
    // unauthenticated (spec 04: no auth-disable flag).
    let auth = Authenticator::new(cfg.oidc.clone())?;

    let conn = db::open(&cfg.db_path, true)?;
    db::migrate(&conn)?;
    conn.pragma_update(None, "wal_checkpoint", "TRUNCATE").ok();

    let state = Arc::new(AppState {
        conn: Mutex::new(conn),
        storage: Storage::new(&cfg.s3).await?,
        keys: garret_server::load_signing_keys(&cfg.signing_key_files)?,
        store_dir: cfg.store_dir,
        max_body_bytes: cfg.max_body_bytes,
        auth,
    });

    let app = Router::new()
        .route("/api/v1/missing-paths", post(missing_paths))
        .route("/api/v1/nar/{hash}", put(upload))
        .layer(middleware::from_fn_with_state(state.clone(), require_oidc))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("pusher listening on {addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

struct Error(StatusCode, String);

impl IntoResponse for Error {
    fn into_response(self) -> Response {
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
    let conn = state.conn.lock().unwrap();
    Ok(Json(db::missing(&conn, &hashes)?))
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
        return Ok((StatusCode::OK, Json(json!({"status": "exists"}))).into_response());
    }

    // ponytail: M1 buffers the whole body — PutObject needs a length up front,
    // and the cap keeps that honest. M3 replaces this with the spec's multipart
    // path (64 MiB parts, permit acquired before each read) and the cap goes.
    let mut stream = body.into_data_stream();
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| Error(StatusCode::BAD_REQUEST, e.to_string()))?;
        if (buf.len() + chunk.len()) as u64 > state.max_body_bytes {
            return Err(Error(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("body exceeds max_body_bytes ({})", state.max_body_bytes),
            ));
        }
        buf.extend_from_slice(&chunk);
    }

    let preamble = take_preamble(&mut buf)?.ok_or_else(|| {
        Error(
            StatusCode::BAD_REQUEST,
            "body ended before the preamble was complete".into(),
        )
    })?;
    let compressed = buf; // take_preamble drained it off the front

    let object = build_object(&hash, &preamble, &compressed, &state, &subject)?;
    state
        .storage
        .put(&storage::key_for(&hash), compressed)
        .await?;
    db::insert_object(&mut state.conn.lock().unwrap(), &object, now())?;

    Ok((StatusCode::CREATED, Json(json!({"status": "created"}))).into_response())
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
    compressed: &[u8],
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
        file_hash: format!("sha256:{}", nix_base32::encode(&Sha256::digest(compressed))),
        file_size: compressed.len() as i64,
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
