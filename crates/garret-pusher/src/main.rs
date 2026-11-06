//! Pusher: accepts NARs over the garret push protocol (spec 01-push-protocol).

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use parking_lot::Mutex;

mod admin;
mod deletion;
mod fsck;
mod gc;
mod verify;

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use futures::StreamExt;
use garret_common::{
    Preamble, is_store_hash, sha256_nix32, store_basename,
    wire::{self, Discovery, Oidc, UploadAck},
};
use garret_server::{
    auth::{self, Authenticator, Rejection, Subject},
    config::PusherConfig,
    db::{self, Object},
    inflight::{self, InFlight},
    metrics as garret_metrics,
    narinfo::{self, SigningKeyFile},
    now,
    storage::{self, Storage, UploadLimits},
};
use serde_json::json;
use tokio::sync::Semaphore;
use verify::VerifiedNar;

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

#[tokio::main]
async fn main() -> Result<()> {
    let garret_server::runtime::Started {
        config: cfg,
        listener,
    } = garret_server::runtime::start::<PusherConfig>("pusher").await?;
    // Bound before anything serves, like the listeners, so an unusable socket
    // path fails startup instead of leaving a half-up Pusher.
    let admin_listener = cfg.admin_socket.as_deref().map(admin::bind).transpose()?;
    // Authenticator::new refuses an empty issuer list, so this cannot start
    // unauthenticated (spec 04: no auth-disable flag).
    let auth = Authenticator::new(cfg.oidc.clone())?;

    let conn = db::open(&cfg.db_path, true)?;
    db::migrate(&conn)?;
    if let Err(e) = conn.pragma_update(None, "wal_checkpoint", "TRUNCATE") {
        tracing::warn!("startup WAL checkpoint failed: {e}");
    }

    let conn = Arc::new(Mutex::new(conn));
    let storage = Storage::new(&cfg.s3).await?;
    let in_flight = InFlight::new();
    let gc = cfg.gc.clone().map(|gc_cfg| {
        Arc::new(gc::Gc::new(
            conn.clone(),
            storage.clone(),
            in_flight.clone(),
            gc_cfg,
        ))
    });

    let state = Arc::new(AppState {
        conn,
        db_path: cfg.db_path,
        storage,
        keys: garret_server::load_signing_keys(&cfg.signing_key_files)?,
        store_dir: cfg.store_dir,
        auth,
        limits: UploadLimits::new(
            cfg.limits.part_size,
            cfg.limits.max_parts_in_flight,
            cfg.limits.max_in_flight_bytes,
            Duration::from_secs(cfg.limits.body_idle_timeout_secs),
        ),
        uploads: Arc::new(Semaphore::new(cfg.limits.max_concurrent_uploads)),
        in_flight,
        gc,
    });

    // Saturation must be visible before it hurts, so the caps are exported
    // alongside the gauges that approach them (spec 08).
    metrics::gauge!("garret_uploads_limit").set(cfg.limits.max_concurrent_uploads as f64);
    metrics::gauge!("garret_in_flight_bytes_limit").set(cfg.limits.max_in_flight_bytes as f64);
    metrics::gauge!("garret_part_slots_limit").set(state.limits.total_slots() as f64);
    // Moved by the upload budget from here on; exported before the first
    // upload so the series exists.
    metrics::gauge!("garret_in_flight_bytes").set(0.0);

    if let Some(admin_listener) = admin_listener {
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = admin::serve(admin_listener, state).await {
                tracing::error!("admin socket failed: {e:#}");
            }
        });
    }

    match &state.gc {
        Some(gc) => {
            tokio::spawn(gc.clone().run_forever());
        }
        None => tracing::warn!("no [gc] section: the cache is unbounded and will never evict"),
    }

    // Everything `garret login` needs to write a whole client config from one
    // URL. Static, so it is rendered once here rather than per request.
    //
    // There is no `pusher_endpoint`: the client already dialled this server to
    // ask, and behind a reverse proxy `listen` is a loopback address that would
    // be wrong to advertise. The Puller URL is the one thing the Pusher cannot
    // infer, which is why it is configured.
    //
    // Nothing here is secret — the public halves of the signing keys, and OIDC
    // client metadata that every device-flow request already sends in the clear.
    let discovery = {
        let device = cfg.oidc.iter().find(|i| i.client_id.is_some());
        if device.is_none() {
            tracing::warn!(
                "no issuer sets `client_id`: /api/v1/discovery omits its oidc \
                 section and `garret login` cannot bootstrap a config"
            );
        }
        if cfg.puller_endpoint.is_none() {
            tracing::warn!(
                "no `puller_endpoint`: /api/v1/discovery omits it and \
                 `garret use`, `list` and `tree` stay unconfigured"
            );
        }
        serde_json::to_string(&Discovery {
            oidc: device.map(|i| Oidc {
                audience: i.audience.clone(),
                client_id: i.client_id.clone(),
                issuer: i.issuer.clone(),
            }),
            public_keys: state.keys.iter().map(|k| k.public_key()).collect(),
            puller_endpoint: cfg.puller_endpoint.clone(),
        })?
    };

    let storage = state.storage.clone();
    let app = router(state, discovery);

    // Every listener is bound and every background task spawned.
    garret_server::runtime::notify_ready();
    serve_until_stopped(listener, app, storage).await
}

fn router(state: Arc<AppState>, discovery: String) -> Router {
    Router::new()
        .route(wire::MISSING_PATHS, post(missing_paths))
        .route(wire::NAR_UPLOAD, put(upload))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_oidc))
        // `route_layer` wraps only the routes above it, so unmatched paths get
        // a plain 404, not a 401. Discovery stays anonymous because it is
        // registered after the layer; do not reorder it upwards.
        .route(
            wire::DISCOVERY,
            get(move || {
                let body = discovery.clone();
                async move { ([(header::CONTENT_TYPE, "application/json")], body) }
            }),
        )
        .layer(middleware::from_fn(garret_metrics::track_http))
        .with_state(state)
}

/// How long a SIGTERM/SIGINT waits for in-flight uploads to finish. With
/// [`ABORT_BUDGET`] it stays under systemd's default 90 s stop timeout.
const DRAIN: Duration = Duration::from_secs(60);
/// How long aborting the uploads that outlived [`DRAIN`] may take.
const ABORT_BUDGET: Duration = Duration::from_secs(20);

/// Serves until SIGTERM or SIGINT, then stops accepting and lets in-flight
/// uploads finish for up to [`DRAIN`]. Whatever is still uploading after
/// that has its multipart aborted, so a restart leaks no parts (spec 03).
async fn serve_until_stopped(
    listener: tokio::net::TcpListener,
    app: Router,
    storage: Storage,
) -> Result<()> {
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = garret_server::runtime::serve(listener, app, async move {
        stop_signal().await;
        tracing::info!("stopping: draining in-flight uploads for up to {DRAIN:?}");
        let _ = stop.send(());
    });
    let deadline = async {
        match stopped.await {
            Ok(()) => tokio::time::sleep(DRAIN).await,
            // The server ended on its own; its branch has already won.
            Err(_) => std::future::pending().await,
        }
    };
    tokio::select! {
        served = server => return Ok(served?),
        () = deadline => {}
    }
    // The Pusher is the bucket's only writer and is exiting, so every open
    // multipart is one of the uploads being cut off here: abort them all.
    // The cut-off handlers are still running until `main` returns, so one
    // may open a multipart after a listing, or finish one before its abort
    // (which fails the pass): list again until a pass finds nothing.
    tracing::warn!("uploads still in flight after {DRAIN:?}: aborting their multiparts");
    let abort_all = async {
        loop {
            match storage
                .abort_stale_multiparts(Duration::ZERO, &InFlight::new())
                .await
            {
                Ok(0) => return,
                Ok(n) => tracing::info!("aborted {n} multipart upload(s)"),
                Err(e) => {
                    tracing::warn!("aborting multiparts on shutdown: {e:#}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    };
    if tokio::time::timeout(ABORT_BUDGET, abort_all).await.is_err() {
        tracing::error!("multiparts still open after {ABORT_BUDGET:?}: left to the orphan sweep");
    }
    Ok(())
}

async fn stop_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("installing the SIGTERM handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}

#[derive(Debug)]
struct Error {
    status: StatusCode,
    /// What the caller sees as `{"error": ...}`.
    message: String,
    /// The `garret_uploads_failed_total` reason. `None` for policy answers
    /// (shed, quiescing, deleting): they come before an upload starts.
    reason: Option<&'static str>,
    /// Sends `Retry-After: 1`, so the client backs off on the server's
    /// terms (spec 01).
    retry_after: bool,
}

impl Error {
    /// A refused upload: the request itself is wrong and retrying cannot
    /// fix it.
    fn refused(message: String) -> Self {
        Error {
            status: StatusCode::BAD_REQUEST,
            message,
            reason: Some("refused"),
            retry_after: false,
        }
    }

    /// A policy answer that tells the client when to come back.
    fn retry_after(status: StatusCode, message: &str) -> Self {
        Error {
            status,
            message: message.to_owned(),
            reason: None,
            retry_after: true,
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let body = Json(json!({ "error": self.message }));
        if self.retry_after {
            (self.status, [(header::RETRY_AFTER, "1")], body).into_response()
        } else {
            (self.status, body).into_response()
        }
    }
}

/// An unexpected failure's chain names S3 keys, SQLite errors and file
/// paths: operator detail, not caller detail. The chain is logged under a
/// fresh id and the caller gets only the id to quote back.
impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        if let Some(over) = e.downcast_ref::<storage::BodyOverLimit>() {
            return Error::refused(over.to_string());
        }
        let id = format!("{:016x}", fastrand::u64(..));
        let reason = if e.downcast_ref::<storage::ClientBodyError>().is_some() {
            // A sender that stalled or dropped mid-body failed; the server
            // did not. Same id and 500, but a warning rather than an error.
            tracing::warn!(error_id = %id, kind = "client_body", "client body failed: {e:#}");
            "client_body"
        } else {
            tracing::error!(error_id = %id, "{e:#}");
            if e.downcast_ref::<storage::S3Failure>().is_some() {
                "storage"
            } else {
                "internal"
            }
        };
        Error {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: format!("internal error (id {id})"),
            reason: Some(reason),
            retry_after: false,
        }
    }
}

/// Largest preamble accepted (spec 01). Real ones are a few KiB of JSON; the
/// body is buffered whole until the preamble is complete.
const MAX_PREAMBLE: usize = 1024 * 1024;

/// Runs a blocking database call on the blocking pool, holding the
/// connection lock only inside it. SQLite can wait seconds on another
/// writer's lock; that must park a blocking thread, not a Tokio worker.
pub(crate) async fn db_run<T, F>(conn: &Arc<Mutex<rusqlite::Connection>>, f: F) -> Result<T>
where
    F: FnOnce(&mut rusqlite::Connection) -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    let conn = conn.clone();
    tokio::task::spawn_blocking(move || {
        let waited = std::time::Instant::now();
        let mut conn = conn.lock();
        metrics::histogram!("garret_db_lock_wait_seconds", "conn" => "pusher")
            .record(waited.elapsed());
        f(&mut conn)
    })
    .await
    .context("database task failed")?
}

async fn already_stored(state: &AppState, hash: &str) -> Result<bool, Error> {
    let hash = hash.to_owned();
    let stored = db_run(&state.conn, move |conn| db::exists(conn, &hash)).await?;
    if stored {
        metrics::counter!("garret_upload_skipped_total", "reason" => "exists").increment(1);
    }
    Ok(stored)
}

/// Holds an upload slot's accounting for as long as it lives: counts in
/// `garret_uploads_in_flight` and, on drop, adds the slot's hold time to
/// `garret_upload_slot_milliseconds_total`. A handler future dropped mid-upload
/// (client disconnect, shutdown) still gives both back.
struct UploadGauge(std::time::Instant);

impl UploadGauge {
    fn enter() -> Self {
        metrics::gauge!("garret_uploads_in_flight").increment(1.0);
        Self(std::time::Instant::now())
    }
}

impl Drop for UploadGauge {
    fn drop(&mut self) {
        metrics::gauge!("garret_uploads_in_flight").decrement(1.0);
        metrics::counter!("garret_upload_slot_milliseconds_total")
            .increment(self.0.elapsed().as_millis() as u64);
    }
}

async fn require_oidc(
    State(state): State<Arc<AppState>>,
    mut request: Request,
    next: Next,
) -> Response {
    match auth::bearer(&state.auth, request.headers()).await {
        Ok(subject) => {
            request.extensions_mut().insert(subject);
            next.run(request).await
        }
        // The reason stays in the log; the caller learns only that it failed.
        Err(rejection) => {
            if let Some(reason) = rejection.reason() {
                tracing::warn!("rejected token: {reason}");
            }
            let error = match &rejection {
                Rejection::Missing => "missing bearer token",
                Rejection::Invalid(_) => "invalid token",
                Rejection::Refused(_) => "not authorized",
            };
            rejection.respond(Json(json!({ "error": error })))
        }
    }
}

async fn missing_paths(
    State(state): State<Arc<AppState>>,
    axum::Extension(subject): axum::Extension<Subject>,
    Json(hashes): Json<Vec<String>>,
) -> Result<Json<Vec<String>>, Error> {
    let batch = hashes.len();
    let missing = db_run(&state.conn, move |conn| {
        db::missing(conn, &hashes, garret_server::now())
    })
    .await?;
    tracing::debug!(subject = %subject.0, batch, missing = missing.len(), "negotiation");
    metrics::histogram!("garret_negotiation_batch_size").record(batch as f64);
    metrics::counter!("garret_negotiation_missing_paths_total").increment(missing.len() as u64);
    if batch > 0 {
        metrics::histogram!("garret_negotiation_missing_ratio")
            .record(missing.len() as f64 / batch as f64);
    }
    Ok(Json(missing))
}

/// The answer to a push while `fsck --repair --quiesce` holds the gate.
fn quiescing() -> Error {
    metrics::counter!("garret_upload_skipped_total", "reason" => "quiescing").increment(1);
    Error {
        status: StatusCode::SERVICE_UNAVAILABLE,
        message: "cache is quiescing for fsck --repair --quiesce".into(),
        reason: None,
        retry_after: false,
    }
}

async fn upload(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    axum::Extension(subject): axum::Extension<Subject>,
    body: Body,
) -> Result<Response, Error> {
    // Checked before anything else: `fsck --repair --quiesce` needs new
    // pushes rejected outright while it drains and repairs (spec 05-gc).
    // Only a fast path; the claim below is the gate a drain relies on.
    if state.in_flight.is_quiescing() {
        return Err(quiescing());
    }

    // The hash becomes the DB key and the S3 key, so it is checked before
    // either sees it — and before a byte of the body is read.
    if !is_store_hash(&hash) {
        return Err(Error::refused(format!("{hash:?} is not a store path hash")));
    }

    // Idempotency: answered before the body is read, so `Expect: 100-continue`
    // clients skip the transfer entirely (spec 01).
    if already_stored(&state, &hash).await? {
        return Ok((StatusCode::OK, Json(UploadAck::Exists)).into_response());
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
    let _gauge = UploadGauge::enter();
    let claim = match state.in_flight.claim(&hash) {
        Ok(claim) => claim,
        // Second pusher for the same path: first writer wins. The loser is
        // told `in-progress` and asks again later (spec 01) — this upload may
        // yet fail — rather than racing to overwrite an identical blob.
        Err(inflight::Refused::Uploading) => {
            metrics::counter!("garret_upload_skipped_total", "reason" => "in-progress")
                .increment(1);
            return Ok((StatusCode::OK, Json(UploadAck::InProgress)).into_response());
        }
        // GC, `delete` or prune has removed the row and not yet the blob: a
        // blob uploaded now would go with it (spec 05). By the retry the path
        // is simply missing.
        Err(inflight::Refused::Deleting) => {
            metrics::counter!("garret_upload_skipped_total", "reason" => "deleting").increment(1);
            return Err(Error::retry_after(
                StatusCode::SERVICE_UNAVAILABLE,
                "this path is being deleted; retry",
            ));
        }
        // Quiesce began after the early check: refused here, under the same
        // lock the drain counts uploads by.
        Err(inflight::Refused::Quiescing) => return Err(quiescing()),
    };

    // The row may have landed between the first check and the claim: an
    // upload that finished just before this one released its claim. The
    // blob is already stored; do not overwrite it (spec 01).
    if already_stored(&state, &hash).await? {
        return Ok((StatusCode::OK, Json(UploadAck::Exists)).into_response());
    }

    let started = std::time::Instant::now();
    let result = store_upload(&state, &hash, &subject, body, claim).await;

    match &result {
        Ok(size) => {
            metrics::counter!("garret_uploads_accepted_total").increment(1);
            metrics::histogram!("garret_upload_bytes").record(*size as f64);
        }
        Err(Error {
            reason: Some(reason),
            ..
        }) => metrics::counter!("garret_uploads_failed_total", "reason" => *reason).increment(1),
        Err(_) => {}
    }
    let outcome = if result.is_ok() { "accepted" } else { "failed" };
    metrics::histogram!("garret_upload_duration_seconds", "outcome" => outcome)
        .record(started.elapsed());
    result?;
    Ok((StatusCode::CREATED, Json(UploadAck::Created)).into_response())
}

/// Returns the stored size. The body never lands in memory whole.
async fn store_upload(
    state: &AppState,
    hash: &str,
    subject: &Subject,
    body: Body,
    claim: inflight::Claim,
) -> Result<i64, Error> {
    let mut stream = body.into_data_stream();
    let mut head: Vec<u8> = Vec::new();
    let started = std::time::Instant::now();
    let preamble = loop {
        if let Some(preamble) = take_preamble(&mut head)? {
            break preamble;
        }
        let at = storage::Stall {
            received: head.len() as u64,
            started,
            phase: "preamble",
        };
        let Some(chunk) = storage::next_chunk(&mut stream, state.limits.body_idle, at).await?
        else {
            return Err(Error::refused(
                "body ended before the preamble was complete".into(),
            ));
        };
        head.extend_from_slice(&chunk.map_err(|e| Error::refused(e.to_string()))?);
    };

    // Validated before a byte is stored: a refused preamble leaves no blob.
    let mut object = build_object(hash, &preamble, &state.store_dir, subject)?;

    // Whatever followed the preamble in that chunk is the start of the NAR.
    let leftover = Bytes::from(head);
    let nar = futures::stream::once(async move { Ok::<_, axum::Error>(leftover) }).chain(stream);
    // Checked on the way through against the claimed NarHash/NarSize, so
    // the key signs only what nix will verify on download (ADR-0010).
    let mut nar = VerifiedNar::new(Box::pin(nar), &object.nar_hash, object.nar_size)
        .context("starting the zstd decoder")?;

    // The first buffer reserves only what an honest compressed body can
    // reach, so a small NAR does not hold a whole part slot (spec 03).
    let stored = state
        .storage
        .put_streaming(
            &storage::key_for(hash),
            &mut nar,
            &state.limits,
            verify::max_compressed_len(object.nar_size),
        )
        .await;
    // Only an `Accepted` verdict may be signed (ADR-0010). No verdict means
    // the stream never reached its end: the store failed, and its error is
    // the cause. A store that succeeded without one is a server bug.
    match nar.into_outcome() {
        Some(verify::Outcome::Accepted) => {}
        Some(verify::Outcome::Failed(reason)) => return Err(anyhow::anyhow!(reason).into()),
        Some(verify::Outcome::Refused(reason)) => {
            return Err(Error::refused(reason));
        }
        None => {
            stored?;
            return Err(anyhow::anyhow!("stored a NAR the check never accepted").into());
        }
    }
    let (digest, file_size) = stored?;

    // Server-computed over exactly the bytes stored — what nix checks the
    // redirected download against (ADR-0005).
    object.file_hash = sha256_nix32(&digest);
    object.file_size = file_size;
    object.sigs = narinfo::sign(&object, &state.store_dir, &state.keys)?;
    let at = now();
    // The claim moves into the blocking closure: a dropped handler future
    // (client gone) must not release the path while the insert can still
    // commit, or a delete could race the row it is about to remove.
    db_run(&state.conn, move |conn| {
        let _claim = claim;
        db::insert_object(conn, &object, at)
    })
    .await?;
    Ok(file_size)
}

/// Splits the 4-byte-LE-length-prefixed JSON preamble off the front of `buf`,
/// leaving the compressed NAR bytes behind. `None` means "need more bytes".
fn take_preamble(buf: &mut Vec<u8>) -> Result<Option<Preamble>, Error> {
    let end = match garret_common::preamble_end(buf, MAX_PREAMBLE) {
        Ok(Some(end)) => end,
        Ok(None) => return Ok(None),
        Err(_) => {
            return Err(Error::refused("preamble length is implausible".into()));
        }
    };
    let preamble: Preamble = serde_json::from_slice(&buf[4..end])
        .map_err(|e| Error::refused(format!("bad preamble: {e}")))?;
    buf.drain(..end);
    Ok(Some(preamble))
}

/// Checks the preamble and turns it into an unsigned [`Object`]; the blob's
/// `FileHash`/`FileSize` and the signatures are filled in once it is stored.
///
/// Every field lands in the DB, the narinfo or the signed fingerprint, so
/// each must be what nix itself would produce: the store path is exactly
/// `{store_dir}/{hash}-{name}` for the URL hash, references and the deriver
/// are store paths, and `ca` — unsigned, printed raw — is one line, or a
/// newline would smuggle unsigned lines into the narinfo (spec 01).
fn build_object(
    hash: &str,
    preamble: &Preamble,
    store_dir: &str,
    subject: &Subject,
) -> Result<Object, Error> {
    let bad = Error::refused;
    let name = store_basename(&preamble.store_path, store_dir)
        .and_then(|base| base.strip_prefix(hash)?.strip_prefix('-'))
        .ok_or_else(|| {
            bad(format!(
                "store path {:?} is not {store_dir}/{hash}-<name>",
                preamble.store_path
            ))
        })?;
    let basename = |path: &str, what: &str| {
        store_basename(path, store_dir)
            .map(str::to_owned)
            .ok_or_else(|| {
                bad(format!(
                    "{what} {path:?} is not a store path in {store_dir}"
                ))
            })
    };
    let mut references = preamble
        .references
        .iter()
        .map(|r| basename(r, "reference"))
        .collect::<Result<Vec<_>, _>>()?;
    references.sort();
    let deriver = preamble
        .deriver
        .as_deref()
        .map(|d| basename(d, "deriver"))
        .transpose()?;
    if let Some(ca) = preamble
        .ca
        .as_deref()
        .filter(|ca| ca.contains(['\n', '\r']))
    {
        return Err(bad(format!("ca {ca:?} is not a single line")));
    }

    Ok(Object {
        store_path_hash: hash.to_owned(),
        store_path: preamble.store_path.clone(),
        name: name.to_owned(),
        // Normalised on the way in, so the DB, narinfo and fingerprint all
        // agree on the spelling nix signs over.
        nar_hash: narinfo::normalize_hash(&preamble.nar_hash).map_err(|e| bad(format!("{e:#}")))?,
        nar_size: preamble.nar_size,
        file_hash: String::new(),
        file_size: 0,
        deriver,
        ca: preamble.ca.clone(),
        references,
        sigs: vec![],
        pushed_by: Some(subject.0.clone()),
    })
}

#[cfg(test)]
mod tests {
    use garret_server::config::IssuerConfig;
    use garret_server::testing::unreachable_storage;

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

    /// The quiescing check sits before the idempotency check, the upload
    /// semaphore and the in-flight claim — this exercises it in isolation,
    /// with no upload machinery reached.
    #[tokio::test]
    async fn quiescing_rejects_a_push_before_anything_else() {
        let state = test_state(open_db()).await;
        let _quiesce = state.in_flight.quiesce();

        let err = upload(
            State(state),
            Path("a".repeat(32)),
            axum::Extension(Subject("test#user".into())),
            Body::empty(),
        )
        .await
        .expect_err("quiescing must reject the push");
        assert_eq!(err.status, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Past the upload cap the push is shed before its body is read, with a
    /// Retry-After the client waits out (spec 01 backpressure).
    #[tokio::test]
    async fn a_push_past_the_upload_cap_is_shed_with_retry_after() {
        let state = test_state(open_db()).await;
        let _held = state.uploads.clone().try_acquire_owned().unwrap();

        let response = upload(
            State(state),
            Path("a".repeat(32)),
            axum::Extension(Subject("test#user".into())),
            Body::empty(),
        )
        .await
        .expect_err("a push past the cap must be shed")
        .into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");
    }

    /// GC, `delete` and prune claim a path from its row delete until its
    /// blob delete returns. An upload in that window would have its new blob
    /// deleted under a row it then inserts, so it is turned away, retryably.
    #[tokio::test]
    async fn an_upload_meeting_a_deletion_is_told_to_retry() {
        let state = test_state(open_db()).await;
        let hash = "a".repeat(32);
        let _deleting = state.in_flight.claim_delete(&hash).unwrap();

        let response = upload(
            State(state),
            Path(hash),
            axum::Extension(Subject("test#user".into())),
            Body::empty(),
        )
        .await
        .expect_err("a deleting path must turn the push away")
        .into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response.headers().contains_key(header::RETRY_AFTER));
    }

    #[tokio::test]
    async fn a_non_quiescing_push_reaches_the_idempotency_check() {
        let mut conn = open_db();
        let hash = "a".repeat(32);
        db::insert_object(&mut conn, &garret_server::testing::object(&hash), 0).unwrap();
        let state = test_state(conn).await;

        let response = upload(
            State(state),
            Path(hash),
            axum::Extension(Subject("test#user".into())),
            Body::empty(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    const H: &str = "0123456789abcdfghijklmnpqrsvwxyz";
    const DEP: &str = "zyxwvsrqpnmlkjihgfdcba9876543210";

    /// What the client sends for a real input-addressed path: SRI hash, full
    /// store paths (unsorted), a self-reference and a `.drv` deriver.
    fn preamble() -> Preamble {
        Preamble {
            store_path: format!("/nix/store/{H}-hello-2.12.1"),
            nar_hash: "sha256-47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=".into(),
            nar_size: 4096,
            references: vec![
                format!("/nix/store/{DEP}-glibc-2.40-66"),
                format!("/nix/store/{H}-hello-2.12.1"),
            ],
            deriver: Some(format!("/nix/store/{DEP}-hello-2.12.1.drv")),
            ca: None,
        }
    }

    #[test]
    fn a_well_formed_preamble_becomes_an_object_of_basenames() {
        let mut p = preamble();
        p.ca = Some(format!("fixed:r:sha256:{}", "0".repeat(52)));
        let object = build_object(H, &p, "/nix/store", &Subject("test#user".into())).unwrap();
        assert_eq!(object.name, "hello-2.12.1");
        // Sorted, as the signed fingerprint requires.
        assert_eq!(
            object.references,
            [format!("{H}-hello-2.12.1"), format!("{DEP}-glibc-2.40-66")]
        );
        assert_eq!(
            object.deriver.as_deref(),
            Some(format!("{DEP}-hello-2.12.1.drv").as_str())
        );
    }

    /// The URL hash keys the DB row and the S3 blob; a bad one is refused
    /// before the body is read — this body panics if it ever is.
    #[tokio::test]
    async fn a_malformed_url_hash_is_refused_before_the_body_is_read() {
        for hash in ["a".repeat(31), "e".repeat(32)] {
            let unreadable = futures::stream::poll_fn(
                |_| -> std::task::Poll<Option<Result<Bytes, std::io::Error>>> {
                    panic!("the body must not be read")
                },
            );
            let err = upload(
                State(test_state(open_db()).await),
                Path(hash.clone()),
                axum::Extension(Subject("test#user".into())),
                Body::from_stream(unreadable),
            )
            .await
            .expect_err(&hash);
            assert_eq!(err.status, StatusCode::BAD_REQUEST, "{hash}");
        }
    }

    /// Each row breaks one rule. The storage is unreachable, so a 400 (not a
    /// 500) also shows the preamble was refused before anything was stored.
    #[tokio::test]
    async fn a_malformed_preamble_is_refused_before_anything_is_stored() {
        let with = |break_rule: fn(&mut Preamble)| {
            let mut p = preamble();
            break_rule(&mut p);
            p
        };
        let cases = [
            (
                "URL hash only as a substring",
                with(|p| {
                    p.store_path = format!("/nix/store/{DEP}-hello-{H}");
                }),
            ),
            (
                "another store dir",
                with(|p| {
                    p.store_path = format!("/other/store/{H}-hello-2.12.1");
                }),
            ),
            (
                "name outside nix's charset",
                with(|p| {
                    p.store_path = format!("/nix/store/{H}-héllo");
                }),
            ),
            // Byte 32 falls inside `é`.
            (
                "reference with a non-ASCII hash",
                with(|p| {
                    p.references = vec![format!("/nix/store/{}é-x", "a".repeat(31))];
                }),
            ),
            // `deriver` and `ca` are printed raw into the narinfo and are not
            // signed: a newline would inject unsigned lines.
            (
                "deriver with a newline",
                with(|p| {
                    p.deriver = Some(format!("/nix/store/{DEP}-x.drv\nSig: forged:AAAA"));
                }),
            ),
            (
                "ca with a newline",
                with(|p| {
                    p.ca = Some("fixed:r:sha256:x\nSig: forged:AAAA".into());
                }),
            ),
        ];
        for (case, p) in cases {
            let mut body = p.to_framed().unwrap();
            body.extend_from_slice(b"compressed NAR bytes");
            let err = upload(
                State(test_state(open_db()).await),
                Path(H.into()),
                axum::Extension(Subject("test#user".into())),
                Body::from(body),
            )
            .await
            .expect_err(case);
            assert_eq!(
                err.status,
                StatusCode::BAD_REQUEST,
                "{case}: {}",
                err.message
            );
        }
    }

    /// The NAR is checked against its claim before the store commits: the
    /// storage is unreachable, so a 400 (not a 500) shows the mismatch
    /// stopped the push before anything was stored or signed.
    #[tokio::test]
    async fn a_nar_that_does_not_match_its_claim_is_refused_before_it_is_stored() {
        use sha2::{Digest, Sha256};
        let nar = b"nix-archive-1 and then some".repeat(100);
        let hash_of = |bytes: &[u8]| sha256_nix32(&Sha256::digest(bytes));
        let size = nar.len() as i64;
        let claims = [
            ("wrong NarHash", hash_of(b"another NAR"), size, "NarHash is"),
            ("wrong NarSize", hash_of(&nar), size + 1, "NarSize is"),
        ];
        for (case, nar_hash, nar_size, why) in claims {
            let mut p = preamble();
            (p.nar_hash, p.nar_size) = (nar_hash, nar_size);
            let mut body = p.to_framed().unwrap();
            body.extend(zstd::encode_all(nar.as_slice(), 3).unwrap());
            let err = upload(
                State(test_state(open_db()).await),
                Path(H.into()),
                axum::Extension(Subject("test#user".into())),
                Body::from(body),
            )
            .await
            .expect_err(case);
            assert_eq!(
                err.status,
                StatusCode::BAD_REQUEST,
                "{case}: {}",
                err.message
            );
            assert!(err.message.contains(why), "{case}: {}", err.message);
        }
    }

    /// Runs `f` under a subscriber that records every log line; returns
    /// `f`'s result and the log text.
    fn capture_logs<T>(f: impl FnOnce() -> T) -> (T, String) {
        #[derive(Clone, Default)]
        struct Log(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Log {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let log = Log::default();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer({
                let log = log.clone();
                move || log.clone()
            })
            .finish();
        let result = tracing::subscriber::with_default(subscriber, f);
        let text = String::from_utf8(log.0.lock().clone()).unwrap();
        (result, text)
    }

    /// A 500's body must not echo the error chain, yet must carry an id the
    /// operator can find in the log next to that chain.
    #[tokio::test]
    async fn an_internal_error_hides_its_chain_behind_a_logged_id() {
        let (err, log) = capture_logs(|| {
            Error::from(
                anyhow::anyhow!("sqlite: disk I/O error at /var/lib/garret/db")
                    .context("inserting object"),
            )
        });

        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let message = body["error"].as_str().unwrap();
        let id = message
            .strip_prefix("internal error (id ")
            .and_then(|rest| rest.strip_suffix(')'))
            .unwrap_or_else(|| panic!("unexpected body: {message}"));
        assert!(!id.is_empty());
        for secret in ["sqlite", "/var/lib/garret", "inserting object"] {
            assert!(
                !message.contains(secret),
                "body leaks {secret:?}: {message}"
            );
        }

        let line = log
            .lines()
            .find(|l| l.contains(&format!("error_id={id}")))
            .unwrap_or_else(|| panic!("id {id} not logged: {log}"));
        assert!(
            line.contains("inserting object: sqlite: disk I/O error at /var/lib/garret/db"),
            "{line}"
        );
    }

    /// A stalled or interrupted body is the sender's failure: still a 500
    /// with an id, but logged as a warning, not an error.
    #[test]
    fn a_client_body_failure_is_a_warning_with_the_same_response() {
        let (err, log) = capture_logs(|| {
            Error::from(anyhow::Error::new(storage::ClientBodyError(
                "upload body stalled".into(),
            )))
        });
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            err.message.starts_with("internal error (id "),
            "{}",
            err.message
        );
        assert!(log.contains("WARN") && log.contains("error_id="), "{log}");
        assert!(!log.contains("ERROR"), "{log}");
    }

    /// The failure reason comes from the kind storage attaches, whatever
    /// context the caller wraps around it.
    #[tokio::test]
    async fn storage_error_kinds_set_the_failure_reason() {
        let storage = unreachable_storage().await;
        let limits = UploadLimits::new(1024, 1, 1024, Duration::from_secs(60));

        let s3 = storage
            .put("nar/k.nar.zst", Bytes::from_static(b"x"))
            .await
            .unwrap_err();
        assert_eq!(Error::from(s3.context("storing")).reason, Some("storage"));

        let at = storage::Stall {
            received: 0,
            started: std::time::Instant::now(),
            phase: "preamble",
        };
        let mut silent = futures::stream::pending::<Result<Bytes, axum::Error>>();
        let stalled = storage::next_chunk(&mut silent, Duration::from_millis(1), at)
            .await
            .unwrap_err();
        assert_eq!(
            Error::from(stalled.context("storing")).reason,
            Some("client_body")
        );

        let body = futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(
            b"more than four bytes",
        ))]);
        let over = storage
            .put_streaming("nar/k.nar.zst", body, &limits, 4)
            .await
            .unwrap_err();
        let over = Error::from(over);
        assert_eq!(
            (over.status, over.reason),
            (StatusCode::BAD_REQUEST, Some("refused"))
        );

        assert_eq!(
            Error::from(anyhow::anyhow!("disk")).reason,
            Some("internal")
        );
    }

    /// Auth wraps only the push routes: an unmatched path is a plain 404, and
    /// a push route without a token is still a 401.
    #[tokio::test]
    async fn auth_leaves_unmatched_paths_a_plain_404() {
        use tower::ServiceExt;
        let app = router(test_state(open_db()).await, "{}".into());
        let send = |req: axum::http::request::Builder| {
            let app = app.clone();
            async move { app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap() }
        };
        let res = send(Request::get("/a/b").header(header::AUTHORIZATION, "Bearer x")).await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let res = send(Request::post(wire::MISSING_PATHS)).await;
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    /// The 401/403 split is `auth::bearer`'s; the Pusher's part is the JSON
    /// body spec 04 names for the 403.
    #[tokio::test]
    async fn a_policy_refusal_answers_the_spec_04_body() {
        use tower::ServiceExt;
        let jwks = garret_server::testing::jwks_file();
        let mut state = Arc::into_inner(test_state(open_db()).await).unwrap();
        state.auth = Authenticator::new(vec![IssuerConfig {
            repository_ids: vec!["42".into()],
            ..garret_server::testing::dev_issuer(jwks.path())
        }])
        .unwrap();
        let token = garret_server::testing::sign(&format!(
            r#"{{"iss":"{}","sub":"ci","aud":"garret","exp":4102444800,"repository_id":"99"}}"#,
            garret_server::testing::DEV_ISSUER
        ));
        let req = Request::post(wire::MISSING_PATHS)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let res = router(Arc::new(state), "{}".into())
            .oneshot(req)
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(res.into_body(), 1024).await.unwrap();
        assert_eq!(&body[..], br#"{"error":"not authorized"}"#);
    }
}
