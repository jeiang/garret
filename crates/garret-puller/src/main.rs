//! Puller: the public Nix substituter. Serves narinfo (it holds the
//! signatures) and redirects NAR requests to presigned S3 URLs (ADR-0005).

use std::{
    collections::HashSet,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use axum::{
    Router,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Redirect, Response},
    routing::get,
};
use garret_common::{is_store_hash, wire};
use garret_server::{
    auth::{self, Authenticator, Rejection},
    browse,
    config::{IssuerConfig, PullerConfig},
    db, metrics as garret_metrics, narinfo, now,
    storage::{self, Storage},
};
use parking_lot::Mutex;
use serde::Deserialize;

/// A database connection. Sync rusqlite behind an async lock, so requests
/// queued on it hold no threads; see [`on_conn`].
type Conn = Arc<tokio::sync::Mutex<rusqlite::Connection>>;

/// The Puller's database connections, opened together once the Pusher has
/// created the database. Separate connections, so neither a bump flush
/// waiting on a Pusher write nor a slow browse query (spec 07) ever holds up
/// a pull-path read.
struct Db {
    pull: Conn,
    bump: Conn,
    browse: Conn,
}

struct AppState {
    /// Empty until the Pusher has created the database (spec 02: it owns the
    /// schema). Every reader goes through [`AppState::db`], so a Puller that
    /// boots first answers 503 instead of dying.
    db: OnceLock<Db>,
    storage: Storage,
    presign_ttl: Duration,
    bump_debounce: i64,
    bumps: Bumps,
    db_read_budget: Duration,
    presign_budget: Duration,
    read_probe: ProbeCache,
    /// Fetches the probe's presigned URL, as a substituter would.
    http: reqwest::Client,
}

impl AppState {
    fn db(&self) -> Option<&Db> {
        self.db.get()
    }
}

/// How often queued last-accessed bumps are written. LRU needs only day
/// granularity; this just batches a burst of hits into one transaction.
const BUMP_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// How long a bump flush waits on a Pusher write before giving up. Short: a
/// dropped batch costs nothing, since its rows stay stale and re-queue on
/// their next hit.
const BUMP_BUSY_TIMEOUT: Duration = Duration::from_secs(1);

/// Last-accessed bumps waiting for the next flush (spec 02). Only hits on
/// stale rows are queued, so it is bounded by the object count.
#[derive(Default)]
struct Bumps(Mutex<HashSet<String>>);

impl Bumps {
    /// A fresh row, which is nearly every hit, costs no write at all: not
    /// even the WAL write lock a no-op UPDATE would take.
    fn hit(&self, hash: &str, last_accessed_at: i64, now: i64, debounce: i64) {
        if last_accessed_at >= now - debounce {
            metrics::counter!("garret_bump_debounced_total").increment(1);
            return;
        }
        let mut pending = self.0.lock();
        // Not a bare insert: that would allocate the owned key on every repeat hit.
        if !pending.contains(hash) {
            pending.insert(hash.to_owned());
        }
        metrics::gauge!("garret_bump_queue_depth").set(pending.len() as f64);
    }

    fn flush(&self, conn: &mut rusqlite::Connection, now: i64, debounce: i64) -> Result<usize> {
        let batch = {
            let mut pending = self.0.lock();
            metrics::gauge!("garret_bump_queue_depth").set(0.0);
            std::mem::take(&mut *pending)
        };
        if batch.is_empty() {
            return Ok(0);
        }
        db::bump_last_accessed(conn, &batch, now, debounce)?;
        Ok(batch.len())
    }
}

async fn flush_bumps(state: Arc<AppState>, conn: Conn) {
    let mut tick = tokio::time::interval(BUMP_FLUSH_INTERVAL);
    loop {
        tick.tick().await;
        let state = state.clone();
        let flushed = on_conn(conn.clone(), "bump", move |conn| {
            state.bumps.flush(conn, now(), state.bump_debounce)
        })
        .await;
        let Err(e) = flushed else { continue };
        metrics::counter!("garret_bump_failures_total").increment(1);
        tracing::warn!("last-accessed bump flush failed: {e:#}");
    }
}

/// How long a browse request may take, queueing included, before it answers
/// 503. Only there so requests cannot pile up behind a slow one without end:
/// browse is interactive and rare, and a slow query delays only other
/// browse requests.
const BROWSE_BUDGET: Duration = Duration::from_secs(10);

/// How long a deep-readiness answer is reused. However hard `/ready/deep`
/// is hit, it costs at most one S3 read per window.
const READ_PROBE_TTL: Duration = Duration::from_secs(30);

/// Deadline for one whole probe: pick a blob, presign, fetch its first byte.
const READ_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

fn unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, "database not ready").into_response()
}

/// Why a pull-path request degraded to a miss: the `reason` label of
/// `garret_degraded_total` (spec 08).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Degraded {
    DbTimeout,
    DbError,
    PresignTimeout,
    PresignError,
}

impl Degraded {
    fn reason(self) -> &'static str {
        match self {
            Degraded::DbTimeout => "db_timeout",
            Degraded::DbError => "db_error",
            Degraded::PresignTimeout => "presign_timeout",
            Degraded::PresignError => "presign_error",
        }
    }
}

/// Degrade to a miss (ticket 25). A substituter's contract is bounded latency
/// and harmless failure: nix handles a 404 natively (try the next
/// substituter, build locally), while a hang stalls builds fleet-wide and a
/// 500 is noise it isn't built for.
fn degraded_miss(degraded: Degraded) -> Response {
    metrics::counter!("garret_degraded_total", "reason" => degraded.reason()).increment(1);
    StatusCode::NOT_FOUND.into_response()
}

/// Records the wait for `conn`'s lock as `garret_db_lock_wait_seconds`, then
/// runs `run` on the blocking pool. Off the async workers a wedged statement
/// (or one queued behind it) delays only this future, which a budget can
/// abandon: queued, it never runs; running, it keeps its blocking thread and
/// the connection until it returns.
async fn on_conn<T, F>(conn: Conn, which: &'static str, run: F) -> anyhow::Result<T>
where
    F: FnOnce(&mut rusqlite::Connection) -> anyhow::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let waited = Instant::now();
    let mut conn = conn.lock_owned().await;
    metrics::histogram!("garret_db_lock_wait_seconds", "conn" => which).record(waited.elapsed());
    tokio::task::spawn_blocking(move || run(&mut conn))
        .await
        .unwrap_or_else(|e| Err(anyhow::anyhow!("{which} database task failed: {e}")))
}

/// A pull-path database read under the configured budget (ticket 25),
/// queueing for the connection included.
async fn db_read<T, F>(conn: Conn, budget: Duration, read: F) -> Result<T, Degraded>
where
    F: FnOnce(&rusqlite::Connection) -> anyhow::Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::time::timeout(budget, on_conn(conn, "pull", move |conn| read(conn))).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => {
            tracing::error!("{e:#}");
            Err(Degraded::DbError)
        }
        Err(_elapsed) => Err(Degraded::DbTimeout),
    }
}

/// How a narinfo or NAR request ended; counted once, by [`counted`].
enum Outcome {
    /// A narinfo served or a NAR redirect issued.
    Hit(Response),
    /// No such object.
    Miss,
    /// The database does not exist yet.
    Unavailable,
    Degraded(Degraded),
    /// Not the file name of a store hash: a plain 404 before any database
    /// read, and not counted, so junk requests cannot inflate the miss rate.
    Junk,
}

fn counted(counter: &'static str, outcome: Outcome) -> Response {
    let (label, response) = match outcome {
        Outcome::Hit(response) => ("hit", response),
        Outcome::Miss => ("miss", StatusCode::NOT_FOUND.into_response()),
        Outcome::Unavailable => ("miss", unavailable()),
        Outcome::Degraded(degraded) => ("miss", degraded_miss(degraded)),
        Outcome::Junk => return StatusCode::NOT_FOUND.into_response(),
    };
    metrics::counter!(counter, "outcome" => label).increment(1);
    response
}

fn pull_routes() -> Router<Arc<AppState>> {
    Router::new()
        // axum 0.8 wants whole-segment params, so the suffix is split here.
        .route(
            "/{file}",
            get(
                |State(state): State<Arc<AppState>>, Path(file): Path<String>| async move {
                    counted(
                        "garret_narinfo_requests_total",
                        narinfo(&state, &file).await,
                    )
                },
            ),
        )
        .route(
            wire::NAR_FILE,
            get(
                |State(state): State<Arc<AppState>>, Path(file): Path<String>| async move {
                    counted(
                        "garret_nar_requests_total",
                        nar_redirect(&state, &file).await,
                    )
                },
            ),
        )
}

/// Browse routes are the only authenticated surface here, and they are
/// simply absent when no issuer is configured (spec 07). `route_layer` keeps
/// the auth check off unmatched paths, which stay a plain 404.
fn browse_routes(issuer: Option<&IssuerConfig>) -> Result<Router<Arc<AppState>>> {
    let Some(issuer) = issuer else {
        tracing::info!("no browse_oidc configured: the browse API is not served");
        return Ok(Router::new());
    };
    let auth = Arc::new(Authenticator::new(vec![issuer.clone()])?);
    Ok(Router::new()
        .route(wire::OBJECTS, get(list_objects))
        .route(wire::OBJECT, get(object_detail))
        .route(wire::OBJECT_TREE, get(object_tree))
        .route(wire::OBJECT_REFERRERS, get(object_referrers))
        .route(wire::PINS, get(list_pins))
        .route_layer(axum::middleware::from_fn_with_state(
            auth,
            require_browse_oidc,
        )))
}

#[tokio::main]
async fn main() -> Result<()> {
    let garret_server::runtime::Started {
        config: cfg,
        listener,
    } = garret_server::runtime::start::<PullerConfig>("puller").await?;

    let state = Arc::new(AppState {
        db: OnceLock::new(),
        storage: Storage::new(&cfg.s3).await?,
        presign_ttl: Duration::from_secs(cfg.presign_ttl_secs),
        bump_debounce: cfg.bump_debounce_secs,
        bumps: Bumps::default(),
        db_read_budget: Duration::from_millis(cfg.db_read_budget_ms),
        presign_budget: Duration::from_millis(cfg.presign_budget_ms),
        read_probe: ProbeCache::default(),
        http: reqwest::Client::new(),
    });

    // Opened off the request path so the listener (and /ready) comes up now.
    tokio::spawn({
        let state = state.clone();
        let db_path = cfg.db_path.clone();
        let timeout = Duration::from_secs(cfg.db_wait_timeout_secs);
        async move {
            let opened = db::open_when_ready(&db_path, timeout)
                .await
                .and_then(|pull| {
                    // A pull read must not outwait its own budget on SQLite.
                    pull.busy_timeout(state.db_read_budget)?;
                    let bump = db::open(&db_path, false)?;
                    bump.busy_timeout(BUMP_BUSY_TIMEOUT)?;
                    let conn = |conn| Arc::new(tokio::sync::Mutex::new(conn));
                    let db = Db {
                        pull: conn(pull),
                        bump: conn(bump),
                        browse: conn(db::open(&db_path, false)?),
                    };
                    let bump = db.bump.clone();
                    state
                        .db
                        .set(db)
                        .map_err(|_| anyhow::anyhow!("database connections opened twice"))?;
                    Ok(bump)
                });
            match opened {
                Ok(bump) => {
                    tokio::spawn(flush_bumps(state.clone(), bump));
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

    let store_dir = cfg.store_dir.clone();
    let browse_routes = browse_routes(cfg.browse_oidc.as_ref())?;

    let app = Router::new()
        .route(
            wire::NIX_CACHE_INFO,
            get(move || {
                let body = format!("StoreDir: {store_dir}\nWantMassQuery: 1\nPriority: 40\n");
                async move { body }
            }),
        )
        .route(
            "/ready",
            get(|State(state): State<Arc<AppState>>| async move {
                match state.db().is_some() {
                    true => (StatusCode::OK, "ready").into_response(),
                    false => unavailable(),
                }
            }),
        )
        .route("/ready/deep", get(ready_deep))
        .merge(pull_routes())
        .merge(browse_routes)
        .layer(axum::middleware::from_fn(garret_metrics::track_http))
        .with_state(state);

    garret_server::runtime::notify_ready();
    garret_server::runtime::serve(listener, app, std::future::pending()).await?;
    Ok(())
}

async fn narinfo(state: &AppState, file: &str) -> Outcome {
    let Some(hash) = wire::narinfo_hash(file).filter(|hash| is_store_hash(hash)) else {
        return Outcome::Junk;
    };
    let Some(db) = state.db() else {
        return Outcome::Unavailable;
    };
    let read = {
        let hash = hash.to_owned();
        move |conn: &rusqlite::Connection| {
            db::get_object_and_last_accessed(conn, &hash).with_context(|| format!("narinfo {hash}"))
        }
    };
    match db_read(db.pull.clone(), state.db_read_budget, read).await {
        Ok(Some((obj, last_accessed_at))) => {
            state
                .bumps
                .hit(hash, last_accessed_at, now(), state.bump_debounce);
            Outcome::Hit(
                (
                    [(header::CONTENT_TYPE, "text/x-nix-narinfo")],
                    narinfo::render(&obj),
                )
                    .into_response(),
            )
        }
        Ok(None) => Outcome::Miss,
        Err(degraded) => Outcome::Degraded(degraded),
    }
}

/// The last probe answer and when it was taken.
type ProbeCache = Arc<tokio::sync::Mutex<Option<(Instant, Result<(), String>)>>>;

/// The cached answer while it is younger than [`READ_PROBE_TTL`], else
/// `probe`'s. The probe runs in its own task holding the cache lock:
/// concurrent callers wait for it rather than start their own, and a caller
/// that goes away mid-probe (a client disconnect cancels its handler) still
/// leaves the answer cached, rather than an empty cache for the next caller
/// to probe again.
async fn cached_probe<F>(cache: &ProbeCache, probe: F) -> Result<(), String>
where
    F: Future<Output = Result<(), String>> + Send + 'static,
{
    let mut cached = Arc::clone(cache).lock_owned().await;
    if let Some((at, outcome)) = &*cached
        && at.elapsed() < READ_PROBE_TTL
    {
        return outcome.clone();
    }
    tokio::spawn(async move {
        let outcome = probe.await;
        *cached = Some((Instant::now(), outcome.clone()));
        outcome
    })
    .await
    .unwrap_or_else(|e| Err(format!("probe task failed: {e}")))
}

/// Deep readiness (spec 08): `/ready`, plus one real read through the
/// presigned-URL path a substituter follows. Presigning is signature-only
/// (spec 03), so revoked or rotated S3 credentials or an S4 outage fail
/// neither `/ready` nor the NAR redirect itself; only a fetch shows them.
/// Cached for [`READ_PROBE_TTL`] by [`cached_probe`], so the public route
/// cannot be used to hammer S4.
async fn ready_deep(State(state): State<Arc<AppState>>) -> Response {
    let Some(conn) = state.db().map(|db| db.pull.clone()) else {
        return unavailable();
    };
    let probe = {
        let state = state.clone();
        async move {
            let started = Instant::now();
            let outcome = tokio::time::timeout(READ_PROBE_TIMEOUT, probe_read(&state, conn))
                .await
                .unwrap_or_else(|_| Err(format!("no answer within {READ_PROBE_TIMEOUT:?}")));
            let label = if outcome.is_ok() { "ok" } else { "failed" };
            metrics::counter!("garret_s3_read_probes_total", "outcome" => label).increment(1);
            metrics::histogram!("garret_s3_read_probe_duration_seconds").record(started.elapsed());
            if let Err(e) = &outcome {
                tracing::warn!("deep readiness: {e}");
            }
            outcome
        }
    };
    match cached_probe(&state.read_probe, probe).await {
        Ok(()) => (StatusCode::OK, "ready").into_response(),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "S3 read path failing").into_response(),
    }
}

/// Fetches the first byte of the most recently accessed blob through a
/// presigned URL. An empty cache has nothing to read, so it passes.
async fn probe_read(state: &AppState, conn: Conn) -> Result<(), String> {
    let read = |conn: &rusqlite::Connection| {
        db::most_recently_accessed(conn).context("deep readiness probe")
    };
    let hash = db_read(conn, state.db_read_budget, read)
        .await
        .map_err(|degraded| format!("database read degraded: {}", degraded.reason()))?;
    let Some(hash) = hash else {
        return Ok(());
    };
    let url = state
        .storage
        .presigned_get(&storage::key_for(&hash), state.presign_ttl)
        .await
        .map_err(|e| format!("presigning: {e:#}"))?;
    // The URL carries a live signature: keep it out of errors and logs.
    let response = state
        .http
        .get(url)
        .header(header::RANGE, "bytes=0-0")
        .send()
        .await
        .map_err(|e| format!("fetching: {}", e.without_url()))?;
    match response.status() {
        status if status.is_success() => Ok(()),
        status => {
            let request_id = response
                .headers()
                .get("x-amz-request-id")
                .and_then(|v| v.to_str().ok())
                .map(|v| v.chars().take(64).collect::<String>());
            let body = response.text().await.unwrap_or_default();
            let code = body
                .split_once("<Code>")
                .and_then(|(_, rest)| rest.split_once("</Code>"))
                .map(|(code, _)| code.chars().take(64).collect::<String>());
            let mut detail = format!("presigned GET answered {}", status.as_u16());
            if let Some(code) = code {
                detail.push_str(&format!(", S3 error code {code}"));
            }
            if let Some(id) = request_id {
                detail.push_str(&format!(", x-amz-request-id {id}"));
            }
            Err(detail)
        }
    }
}

async fn nar_redirect(state: &AppState, file: &str) -> Outcome {
    let Some(hash) = wire::nar_file_hash(file).filter(|hash| is_store_hash(hash)) else {
        return Outcome::Junk;
    };
    let Some(db) = state.db() else {
        return Outcome::Unavailable;
    };
    let read = {
        let hash = hash.to_owned();
        move |conn: &rusqlite::Connection| {
            db::exists(conn, &hash).with_context(|| format!("nar {hash}"))
        }
    };
    match db_read(db.pull.clone(), state.db_read_budget, read).await {
        Ok(true) => {}
        Ok(false) => return Outcome::Miss,
        Err(degraded) => return Outcome::Degraded(degraded),
    }
    let started = Instant::now();
    match tokio::time::timeout(
        state.presign_budget,
        state
            .storage
            .presigned_get(&storage::key_for(hash), state.presign_ttl),
    )
    .await
    {
        Ok(Ok(url)) => {
            metrics::histogram!("garret_presign_duration_seconds").record(started.elapsed());
            Outcome::Hit(Redirect::temporary(&url).into_response())
        }
        Ok(Err(e)) => {
            tracing::error!("presigning {hash}: {e:#}");
            Outcome::Degraded(Degraded::PresignError)
        }
        Err(_elapsed) => Outcome::Degraded(Degraded::PresignTimeout),
    }
}

async fn require_browse_oidc(
    State(authenticator): State<Arc<Authenticator>>,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    match auth::bearer(&authenticator, request.headers()).await {
        Ok(subject) => {
            request.extensions_mut().insert(subject);
            next.run(request).await
        }
        Err(rejection) => {
            let reason = match rejection.reason() {
                None => "missing_bearer",
                Some(logged) => {
                    // Debug, not warn: browse is a public surface and any
                    // client can flood this line.
                    tracing::debug!("rejected browse token: {logged}");
                    "token_rejected"
                }
            };
            metrics::counter!("garret_browse_auth_failures_total", "reason" => reason).increment(1);
            let body = match &rejection {
                Rejection::Refused(_) => "not authorized",
                Rejection::Missing | Rejection::Invalid(_) => "unauthorized",
            };
            rejection.respond(body)
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

async fn browse<T, F>(state: &AppState, endpoint: &'static str, read: F) -> Response
where
    F: FnOnce(&rusqlite::Connection) -> anyhow::Result<Option<T>> + Send + 'static,
    T: serde::Serialize + Send + 'static,
{
    let Some(db) = state.db() else {
        return unavailable();
    };
    metrics::counter!("garret_browse_requests_total", "endpoint" => endpoint).increment(1);
    let query = on_conn(db.browse.clone(), "browse", move |conn| read(conn));
    match tokio::time::timeout(BROWSE_BUDGET, query).await {
        Ok(Ok(Some(value))) => axum::Json(value).into_response(),
        Ok(Ok(None)) => StatusCode::NOT_FOUND.into_response(),
        Ok(Err(e)) => {
            tracing::error!("browse {endpoint}: {e:#}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
        Err(_elapsed) => {
            tracing::warn!("browse {endpoint}: over its {BROWSE_BUDGET:?} budget");
            (StatusCode::SERVICE_UNAVAILABLE, "browse query timed out").into_response()
        }
    }
}

async fn list_objects(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(query): axum::extract::Query<ListQuery>,
) -> Response {
    browse(&state, "objects", move |conn| {
        browse::list(
            conn,
            query.q.as_deref(),
            query.limit,
            query.cursor.as_deref(),
        )
        .map(Some)
    })
    .await
}

async fn object_detail(State(state): State<Arc<AppState>>, Path(hash): Path<String>) -> Response {
    browse(&state, "object", move |conn| db::get_object(conn, &hash)).await
}

async fn object_tree(State(state): State<Arc<AppState>>, Path(hash): Path<String>) -> Response {
    browse(&state, "tree", move |conn| browse::tree(conn, &hash, 64)).await
}

async fn object_referrers(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
) -> Response {
    browse(&state, "referrers", move |conn| {
        browse::referrers(conn, &hash).map(Some)
    })
    .await
}

async fn list_pins(State(state): State<Arc<AppState>>) -> Response {
    browse(&state, "pins", |conn| browse::pins(conn).map(Some)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use garret_server::testing::{object, scratch_dir, unreachable_storage};

    fn wrap(conn: rusqlite::Connection) -> Conn {
        Arc::new(tokio::sync::Mutex::new(conn))
    }

    fn conn() -> Conn {
        wrap(rusqlite::Connection::open_in_memory().unwrap())
    }

    /// Stands in for a wedged statement: blocks its thread until the test
    /// sends on `gate`'s sender, and fails if that never comes.
    fn wedge(gate: &std::sync::mpsc::Receiver<()>) -> anyhow::Result<()> {
        gate.recv_timeout(Duration::from_secs(5))
            .context("never released")
    }

    /// The read is sync under the connection lock; the budget must still
    /// trip while it is wedged (ticket 25) — hence the blocking pool.
    #[tokio::test]
    async fn a_wedged_read_trips_the_budget_instead_of_hanging() {
        let (release, gate) = std::sync::mpsc::channel();
        let got = db_read(conn(), Duration::from_millis(25), move |_| wedge(&gate)).await;
        let _ = release.send(());
        assert_eq!(got, Err(Degraded::DbTimeout));
    }

    /// Same failure mode, different cause: the wedged reader holds the lock,
    /// and the next read queues behind it. Its budget must trip too.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_read_queued_behind_a_wedged_lock_holder_trips_its_budget() {
        let conn = conn();
        let (holding_tx, holding_rx) = std::sync::mpsc::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let wedged = db_read(conn.clone(), Duration::from_millis(25), move |_| {
            // Signal that the lock is held so the queued read provably starts
            // second; otherwise it could win the lock and succeed.
            holding_tx.send(()).unwrap();
            wedge(&gate)
        });
        let wedged = tokio::spawn(wedged);
        holding_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("wedged read should acquire the lock");
        let queued = db_read(conn, Duration::from_millis(25), |_| Ok(())).await;
        let _ = release.send(());
        let wedged = wedged.await.unwrap();
        assert_eq!(wedged, Err(Degraded::DbTimeout));
        assert_eq!(
            queued,
            Err(Degraded::DbTimeout),
            "queued read should degrade, not wait"
        );
    }

    const DAY: i64 = 86400;

    /// Flushes `state`'s queued bumps on its pull connection; how many it wrote.
    async fn flush(state: &AppState) -> usize {
        let mut conn = state.db().unwrap().pull.lock().await;
        state
            .bumps
            .flush(&mut conn, now(), state.bump_debounce)
            .unwrap()
    }

    /// Nearly every hit is on a fresh row, and it must not write: not even
    /// take the WAL write lock the Pusher needs. Proved against a Pusher
    /// holding that lock, where any write attempt fails busy.
    #[tokio::test]
    async fn a_debounced_hit_performs_no_write() {
        let dir = scratch_dir();
        let path = dir.join("garret.sqlite");
        let path = path.to_str().unwrap();
        let mut pusher = db::open(path, true).unwrap();
        db::migrate(&pusher).unwrap();
        let hash = "a".repeat(32);
        db::insert_object(&mut pusher, &object(&hash), now()).unwrap();
        let puller = db::open(path, false).unwrap();
        puller.busy_timeout(Duration::from_millis(10)).unwrap();
        let state = state_with_db(puller).await;
        let app = pull_routes().with_state(state.clone());

        pusher.execute_batch("BEGIN IMMEDIATE").unwrap();
        let served = request(&app, &format!("/{hash}.narinfo")).await;
        assert_eq!(served.status(), StatusCode::OK);
        // The last second of the window, where the UPDATE's WHERE would
        // match nothing: still no write lock.
        state.bumps.hit(&hash, 1000, 1000 + DAY, DAY);
        let flushed = flush(&state).await;
        pusher.execute_batch("ROLLBACK").unwrap();
        assert_eq!(flushed, 0, "a fresh row must not be written");
    }

    #[tokio::test]
    async fn a_burst_of_hits_on_one_hash_is_one_write() {
        let mut conn = db::open(":memory:", true).unwrap();
        db::migrate(&conn).unwrap();
        let hash = "a".repeat(32);
        db::insert_object(&mut conn, &object(&hash), 1000).unwrap();
        let state = state_with_db(conn).await;
        let app = pull_routes().with_state(state.clone());
        let narinfo = format!("/{hash}.narinfo");

        for _ in 0..100 {
            assert_eq!(request(&app, &narinfo).await.status(), StatusCode::OK);
        }
        assert_eq!(flush(&state).await, 1);

        // Written: later hits read the fresh value and queue nothing.
        assert_eq!(request(&app, &narinfo).await.status(), StatusCode::OK);
        assert_eq!(flush(&state).await, 0);
    }

    /// A slow browse query (a big tree walk, a full-scan search) must not
    /// delay a narinfo read: it runs on its own connection, and off the
    /// async worker, which with one runtime thread would stall every request.
    #[tokio::test(flavor = "current_thread")]
    async fn a_slow_browse_query_does_not_delay_pull_reads() {
        let mut conn = db::open(":memory:", true).unwrap();
        db::migrate(&conn).unwrap();
        let hash = "a".repeat(32);
        db::insert_object(&mut conn, &object(&hash), 1000).unwrap();
        let state = state_with_db(conn).await;

        let (running_tx, running_rx) = tokio::sync::oneshot::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let slow = tokio::spawn({
            let state = state.clone();
            async move {
                browse(&state, "tree", move |_| {
                    running_tx.send(()).unwrap();
                    wedge(&gate).map(Some)
                })
                .await
            }
        });
        running_rx.await.unwrap();

        let app = pull_routes().with_state(state);
        let served = request(&app, &format!("/{hash}.narinfo")).await;
        let _ = release.send(());
        assert_eq!(
            served.status(),
            StatusCode::OK,
            "the narinfo read waited on the browse query"
        );
        assert_eq!(slow.await.unwrap().status(), StatusCode::OK);
    }

    /// A client that disconnects mid-probe cancels its handler. That must not
    /// leave the cache empty, or a loop of abandoned requests would cost one
    /// S3 read each instead of one per TTL.
    #[tokio::test(start_paused = true)]
    async fn an_abandoned_probe_still_fills_the_cache() {
        let cache = ProbeCache::default();
        let probes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let probe = |probes: Arc<std::sync::atomic::AtomicUsize>| async move {
            probes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(100)).await;
            Err("presigned GET answered 403".to_owned())
        };
        let abandoned = tokio::spawn({
            let (cache, probe) = (cache.clone(), probe(probes.clone()));
            async move { cached_probe(&cache, probe).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        abandoned.abort();

        let next = cached_probe(&cache, probe(probes.clone())).await;
        assert_eq!(next, Err("presigned GET answered 403".to_owned()));
        assert_eq!(probes.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// One bad row that panics a read must cost that request, not the
    /// Puller: the process stays up, so nothing would restart it, and the
    /// connection must keep serving.
    #[tokio::test]
    async fn a_panicked_read_does_not_break_later_reads() {
        let conn = conn();
        let panicked = db_read(
            conn.clone(),
            Duration::from_secs(5),
            |_| -> anyhow::Result<()> { panic!("boom") },
        )
        .await;
        assert_eq!(panicked, Err(Degraded::DbError));
        let next = db_read(conn, Duration::from_secs(5), |_| Ok(42)).await;
        assert_eq!(next, Ok(42));
    }

    async fn state() -> Arc<AppState> {
        let storage = unreachable_storage().await;
        Arc::new(AppState {
            db: OnceLock::new(),
            storage,
            presign_ttl: Duration::from_secs(1),
            bump_debounce: DAY,
            bumps: Bumps::default(),
            db_read_budget: Duration::from_secs(1),
            presign_budget: Duration::from_secs(1),
            read_probe: ProbeCache::default(),
            http: reqwest::Client::new(),
        })
    }

    /// [`state`] with its database open, `pull` as the pull-path connection.
    async fn state_with_db(pull: rusqlite::Connection) -> Arc<AppState> {
        let state = state().await;
        let db = Db {
            pull: wrap(pull),
            bump: conn(),
            browse: conn(),
        };
        assert!(state.db.set(db).is_ok());
        state
    }

    async fn request(app: &Router, uri: &str) -> Response {
        use tower::ServiceExt;
        let req = axum::http::Request::get(uri)
            .body(axum::body::Body::empty())
            .unwrap();
        app.clone().oneshot(req).await.unwrap()
    }

    /// Each route counts its own hits and misses. A name that is not a
    /// store hash's file is a 404 before any database read (the state here
    /// has no database, which would answer 503) and is not counted.
    #[tokio::test]
    async fn pull_routes_count_hits_and_misses_but_not_junk_names() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);

        let no_db = pull_routes().with_state(state().await);
        let invalid = "e".repeat(32);
        for uri in [
            "/zzz.narinfo".to_owned(),
            format!("/{invalid}.narinfo"),
            format!("/{}.txt", "a".repeat(32)),
            "/nar/zzz.nar.zst".to_owned(),
            format!("/nar/{invalid}.nar.zst"),
        ] {
            let status = request(&no_db, &uri).await.status();
            assert_eq!(status, StatusCode::NOT_FOUND, "{uri}");
        }
        assert!(
            !handle.render().contains("requests_total"),
            "junk was counted"
        );

        let mut conn = db::open(":memory:", true).unwrap();
        db::migrate(&conn).unwrap();
        let (cached, absent) = ("a".repeat(32), "b".repeat(32));
        db::insert_object(&mut conn, &object(&cached), 1000).unwrap();
        let app = pull_routes().with_state(state_with_db(conn).await);
        for (uri, status) in [
            (format!("/{cached}.narinfo"), StatusCode::OK),
            (format!("/{absent}.narinfo"), StatusCode::NOT_FOUND),
            (
                format!("/nar/{cached}.nar.zst"),
                StatusCode::TEMPORARY_REDIRECT,
            ),
            (format!("/nar/{absent}.nar.zst"), StatusCode::NOT_FOUND),
        ] {
            assert_eq!(request(&app, &uri).await.status(), status, "{uri}");
        }
        let rendered = handle.render();
        for line in [
            r#"garret_narinfo_requests_total{outcome="hit"} 1"#,
            r#"garret_narinfo_requests_total{outcome="miss"} 1"#,
            r#"garret_nar_requests_total{outcome="hit"} 1"#,
            r#"garret_nar_requests_total{outcome="miss"} 1"#,
        ] {
            assert!(rendered.contains(line), "{line} not in:\n{rendered}");
        }
    }

    /// Browse auth wraps only the browse routes: an unmatched path is a plain
    /// 404, not a 401 that counts as a browse auth failure.
    #[tokio::test]
    async fn browse_auth_leaves_unmatched_paths_a_plain_404() {
        let issuer = garret_server::testing::dev_issuer("unused");
        let app = pull_routes()
            .merge(browse_routes(Some(&issuer)).unwrap())
            .with_state(state().await);
        for uri in ["/a/b", "/"] {
            let res = request(&app, uri).await;
            assert_eq!(res.status(), StatusCode::NOT_FOUND, "{uri}");
            assert!(res.headers().get(header::WWW_AUTHENTICATE).is_none());
        }
        let res = request(&app, wire::OBJECTS).await;
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }
}
