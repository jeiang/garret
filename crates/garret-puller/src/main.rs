//! Puller: the public Nix substituter. Serves narinfo (it holds the
//! signatures) and redirects NAR requests to presigned S3 URLs (ADR-0005).

use std::{
    collections::HashSet,
    sync::{Arc, Mutex, OnceLock, PoisonError},
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
    /// Browse's own connection, set alongside `conn`. An async lock, so
    /// browse requests queue without holding blocking threads (spec 07).
    browse_conn: OnceLock<Arc<tokio::sync::Mutex<rusqlite::Connection>>>,
    storage: Storage,
    presign_ttl: Duration,
    bump_debounce: i64,
    bumps: Bumps,
    db_read_budget: Duration,
    presign_budget: Duration,
    /// The last deep-readiness answer and when it was taken; see
    /// [`cached_probe`].
    read_probe: ProbeCache,
    /// Fetches the probe's presigned URL, as a substituter would.
    http: reqwest::Client,
}

impl AppState {
    fn conn_handle(&self) -> Option<Arc<Mutex<rusqlite::Connection>>> {
        self.conn.get().cloned()
    }
}

/// How often queued last-accessed bumps are written. LRU needs only day
/// granularity; this just batches a burst of hits into one transaction.
const BUMP_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// How long a bump flush waits on a Pusher write before giving up. Short: a
/// dropped batch costs nothing, since its rows stay stale and re-queue on
/// their next hit.
const BUMP_BUSY_TIMEOUT: Duration = Duration::from_secs(1);

/// Last-accessed bumps waiting for the next flush (spec 02). A set, so a
/// burst of hits on one hash is one write. Only hits on stale rows are
/// queued, so it is bounded by the object count.
#[derive(Default)]
struct Bumps(Mutex<HashSet<String>>);

impl Bumps {
    /// Called on every narinfo hit with the `last_accessed_at` the read
    /// already fetched. A fresh row, which is nearly every hit, costs no
    /// write at all: not even the WAL write lock a no-op UPDATE would take.
    fn hit(&self, hash: &str, last_accessed_at: i64, now: i64, debounce: i64) {
        if last_accessed_at >= now - debounce {
            metrics::counter!("garret_bump_debounced_total").increment(1);
            return;
        }
        let mut pending = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if !pending.contains(hash) {
            pending.insert(hash.to_owned());
        }
        metrics::gauge!("garret_bump_queue_depth").set(pending.len() as f64);
    }

    /// Writes everything queued so far on `conn`, the Puller's dedicated bump
    /// connection, and returns how many hashes it wrote. Blocking: callers
    /// run it on the blocking pool, never under the pull-path lock.
    fn flush(&self, conn: &mut rusqlite::Connection, now: i64, debounce: i64) -> Result<usize> {
        let batch = {
            let mut pending = self.0.lock().unwrap_or_else(PoisonError::into_inner);
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

/// Flushes queued bumps every [`BUMP_FLUSH_INTERVAL`] for the life of the
/// process. A failed flush drops its batch; see [`BUMP_BUSY_TIMEOUT`].
async fn flush_bumps(state: Arc<AppState>, conn: rusqlite::Connection) {
    let conn = Arc::new(Mutex::new(conn));
    let mut tick = tokio::time::interval(BUMP_FLUSH_INTERVAL);
    loop {
        tick.tick().await;
        let (state, conn) = (state.clone(), conn.clone());
        let flushed = tokio::task::spawn_blocking(move || {
            let mut conn = conn.lock().unwrap_or_else(PoisonError::into_inner);
            state.bumps.flush(&mut conn, now(), state.bump_debounce)
        })
        .await;
        let error = match flushed {
            Ok(Ok(_)) => continue,
            Ok(Err(e)) => format!("{e:#}"),
            Err(e) => e.to_string(),
        };
        metrics::counter!("garret_bump_failures_total").increment(1);
        tracing::warn!("last-accessed bump flush failed: {error}");
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
    // A read that panicked poisons the lock, but it held no transaction and
    // its statements are finalized on unwind, so the connection is fine.
    // Refusing it would turn one bad row into a permanent 404 server.
    let task = tokio::task::spawn_blocking(move || {
        read(&conn.lock().unwrap_or_else(PoisonError::into_inner))
    });
    match tokio::time::timeout(budget, task).await {
        Ok(joined) => {
            Some(joined.unwrap_or_else(|e| Err(anyhow::anyhow!("db read task failed: {e}"))))
        }
        Err(_elapsed) => None,
    }
}

/// A browse query (spec 07) on the browse connection: one at a time, on the
/// blocking pool, never on the pull-path connection, so a big tree walk or a
/// full-scan search cannot delay a narinfo read. Queued requests wait on the
/// async lock, not on blocking threads the pull path also needs. `None`
/// means the budget tripped; the orphaned query keeps the connection until
/// it returns.
async fn browse_read<T, F>(
    conn: Arc<tokio::sync::Mutex<rusqlite::Connection>>,
    budget: Duration,
    read: F,
) -> Option<anyhow::Result<T>>
where
    F: FnOnce(&rusqlite::Connection) -> anyhow::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let query = async move {
        let conn = conn.lock_owned().await;
        tokio::task::spawn_blocking(move || read(&conn))
            .await
            .unwrap_or_else(|e| Err(anyhow::anyhow!("browse task failed: {e}")))
    };
    tokio::time::timeout(budget, query).await.ok()
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
        browse_conn: OnceLock::new(),
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
                .and_then(|conn| {
                    // Bumps and browse get their own connections, so neither
                    // a flush waiting on a Pusher write nor a slow browse
                    // query ever holds up a pull-path read.
                    let bump_conn = db::open(&db_path, false)?;
                    bump_conn.busy_timeout(BUMP_BUSY_TIMEOUT)?;
                    Ok((conn, bump_conn, db::open(&db_path, false)?))
                });
            match opened {
                Ok((conn, bump_conn, browse_conn)) => {
                    let _ = state
                        .browse_conn
                        .set(Arc::new(tokio::sync::Mutex::new(browse_conn)));
                    let _ = state.conn.set(Arc::new(Mutex::new(conn)));
                    tokio::spawn(flush_bumps(state.clone(), bump_conn));
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
                match state.conn.get().is_some() {
                    true => (StatusCode::OK, "ready").into_response(),
                    false => unavailable(),
                }
            }),
        )
        .route("/ready/deep", get(ready_deep))
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
            db::get_object_and_last_accessed(conn, &hash)
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

    match object {
        Ok(Some((obj, last_accessed_at))) => {
            // Queued, never written here: LRU only needs day granularity, so
            // a bump must never sit on the request path (spec 02-database).
            state
                .bumps
                .hit(&hash, last_accessed_at, now(), state.bump_debounce);
            (
                [(header::CONTENT_TYPE, "text/x-nix-narinfo")],
                narinfo::render(&obj),
            )
                .into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => {
            tracing::error!("narinfo {hash}: {e:#}");
            degraded_miss("db_error")
        }
    }
}

/// The last probe answer and when it was taken.
type ProbeCache = Arc<tokio::sync::Mutex<Option<(Instant, Result<(), String>)>>>;

/// The cached answer while it is younger than `ttl`, else `probe`'s. The
/// probe runs in its own task holding the cache lock: concurrent callers
/// wait for it rather than start their own, and a caller that goes away
/// mid-probe (a client disconnect cancels its handler) still leaves the
/// answer cached, rather than an empty cache for the next caller to probe
/// again.
async fn cached_probe<F>(cache: &ProbeCache, ttl: Duration, probe: F) -> Result<(), String>
where
    F: Future<Output = Result<(), String>> + Send + 'static,
{
    let mut cached = Arc::clone(cache).lock_owned().await;
    if let Some((at, outcome)) = &*cached
        && at.elapsed() < ttl
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
    let Some(conn) = state.conn_handle() else {
        return unavailable();
    };
    let probe = {
        let state = state.clone();
        async move {
            let outcome = tokio::time::timeout(READ_PROBE_TIMEOUT, probe_read(&state, conn))
                .await
                .unwrap_or_else(|_| Err(format!("no answer within {READ_PROBE_TIMEOUT:?}")));
            let label = if outcome.is_ok() { "ok" } else { "failed" };
            metrics::counter!("garret_s3_read_probes_total", "outcome" => label).increment(1);
            if let Err(e) = &outcome {
                tracing::warn!("deep readiness: {e}");
            }
            outcome
        }
    };
    match cached_probe(&state.read_probe, READ_PROBE_TTL, probe).await {
        Ok(()) => (StatusCode::OK, "ready").into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("S3 read path failing: {e}"),
        )
            .into_response(),
    }
}

/// Fetches the first byte of the most recently accessed blob through a
/// presigned URL. An empty cache has nothing to read, so it passes.
async fn probe_read(
    state: &AppState,
    conn: Arc<Mutex<rusqlite::Connection>>,
) -> Result<(), String> {
    let hash = db_read(conn, state.db_read_budget, db::most_recently_accessed)
        .await
        .ok_or("database read over budget")?
        .map_err(|e| format!("{e:#}"))?;
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
        status => Err(format!("presigned GET answered {}", status.as_u16())),
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

/// Answers a browse route: `read` through [`browse_read`], rendered as JSON;
/// `None` is a 404.
async fn browse<T, F>(
    conn: Arc<tokio::sync::Mutex<rusqlite::Connection>>,
    endpoint: &'static str,
    read: F,
) -> Response
where
    F: FnOnce(&rusqlite::Connection) -> anyhow::Result<Option<T>> + Send + 'static,
    T: serde::Serialize + Send + 'static,
{
    metrics::counter!("garret_browse_requests_total", "endpoint" => endpoint).increment(1);
    match browse_read(conn, BROWSE_BUDGET, read).await {
        Some(Ok(Some(value))) => axum::Json(value).into_response(),
        Some(Ok(None)) => StatusCode::NOT_FOUND.into_response(),
        Some(Err(e)) => {
            tracing::error!("browse {endpoint}: {e:#}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
        None => {
            tracing::warn!("browse {endpoint}: over its {BROWSE_BUDGET:?} budget");
            (StatusCode::SERVICE_UNAVAILABLE, "browse query timed out").into_response()
        }
    }
}

async fn list_objects(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(query): axum::extract::Query<ListQuery>,
) -> Response {
    let Some(conn) = state.browse_conn.get().cloned() else {
        return unavailable();
    };
    browse(conn, "objects", move |conn| {
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
    let Some(conn) = state.browse_conn.get().cloned() else {
        return unavailable();
    };
    browse(conn, "object", move |conn| db::get_object(conn, &hash)).await
}

async fn object_tree(State(state): State<Arc<AppState>>, Path(hash): Path<String>) -> Response {
    let Some(conn) = state.browse_conn.get().cloned() else {
        return unavailable();
    };
    browse(conn, "tree", move |conn| browse::tree(conn, &hash, 64)).await
}

async fn object_referrers(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
) -> Response {
    let Some(conn) = state.browse_conn.get().cloned() else {
        return unavailable();
    };
    browse(conn, "referrers", move |conn| {
        browse::referrers(conn, &hash).map(Some)
    })
    .await
}

async fn list_pins(State(state): State<Arc<AppState>>) -> Response {
    let Some(conn) = state.browse_conn.get().cloned() else {
        return unavailable();
    };
    browse(conn, "pins", |conn| browse::pins(conn).map(Some)).await
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

    const DAY: i64 = 86400;

    /// The pull-path read of `hash`'s `last_accessed_at`, fed to a hit the
    /// way `narinfo_route` does.
    fn hit(bumps: &Bumps, conn: &rusqlite::Connection, hash: &str, now: i64) {
        let (_, last_accessed_at) = db::get_object_and_last_accessed(conn, hash)
            .unwrap()
            .unwrap();
        bumps.hit(hash, last_accessed_at, now, DAY);
    }

    /// Nearly every hit is on a fresh row, and it must not write: not even
    /// take the WAL write lock, which is what starved the Pusher. Proved
    /// against a Pusher holding that lock, where any write attempt fails busy.
    #[test]
    fn a_debounced_hit_performs_no_write() {
        let path = std::env::temp_dir().join(format!("garret-bump-{}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let path = path.to_str().unwrap();
        let mut pusher = db::open(path, true).unwrap();
        db::migrate(&pusher).unwrap();
        let hash = "a".repeat(32);
        db::insert_object(&mut pusher, &object(&hash), 1000).unwrap();
        let mut puller = db::open(path, false).unwrap();
        puller.busy_timeout(Duration::from_millis(10)).unwrap();

        pusher.execute_batch("BEGIN IMMEDIATE").unwrap();
        let bumps = Bumps::default();
        // The last second of the window, where the UPDATE's WHERE would
        // match nothing: still no write lock.
        hit(&bumps, &puller, &hash, 1000 + DAY);
        let flushed = bumps.flush(&mut puller, 1000 + DAY, DAY);
        pusher.execute_batch("ROLLBACK").unwrap();
        let _ = std::fs::remove_file(path);
        assert_eq!(flushed.unwrap(), 0, "a fresh row must not be written");
    }

    #[test]
    fn a_burst_of_hits_on_one_hash_is_one_write() {
        let mut conn = db::open(":memory:", true).unwrap();
        db::migrate(&conn).unwrap();
        let hash = "a".repeat(32);
        db::insert_object(&mut conn, &object(&hash), 1000).unwrap();
        let now = 1000 + DAY + 1;

        let bumps = Bumps::default();
        for _ in 0..100 {
            hit(&bumps, &conn, &hash, now);
        }
        assert_eq!(bumps.flush(&mut conn, now, DAY).unwrap(), 1);

        // Written: later hits read the fresh value and queue nothing.
        hit(&bumps, &conn, &hash, now + 1);
        assert_eq!(bumps.flush(&mut conn, now + 1, DAY).unwrap(), 0);
    }

    /// A slow browse query (a big tree walk, a full-scan search) must not
    /// delay a pull-path read. With one runtime thread, a query run on the
    /// async worker would stall the narinfo read's own task. (Sharing the
    /// pull-path connection is ruled out by type: browse's is an async lock.)
    #[tokio::test(flavor = "current_thread")]
    async fn a_slow_browse_query_does_not_delay_pull_reads() {
        let browse_conn = Arc::new(tokio::sync::Mutex::new(
            rusqlite::Connection::open_in_memory().unwrap(),
        ));
        let (running_tx, running_rx) = tokio::sync::oneshot::channel();
        let started = std::time::Instant::now();
        let slow = tokio::spawn(browse_read(browse_conn, Duration::from_secs(5), |_| {
            running_tx.send(()).unwrap();
            std::thread::sleep(Duration::from_secs(1));
            Ok(())
        }));
        running_rx.await.unwrap();

        let read = db_read(conn(), Duration::from_millis(900), |_| Ok(42)).await;
        assert_eq!(read.expect("the pull read tripped its budget").unwrap(), 42);
        assert!(
            started.elapsed() < Duration::from_millis(900),
            "the pull read waited on the browse query: {:?}",
            started.elapsed()
        );
        slow.await.unwrap().unwrap().unwrap();
    }

    /// The budget covers the wait for the browse lock too: requests queued
    /// behind a slow query answer 503 instead of piling up.
    #[tokio::test]
    async fn a_browse_request_queued_behind_a_slow_one_trips_its_budget() {
        let browse_conn = Arc::new(tokio::sync::Mutex::new(
            rusqlite::Connection::open_in_memory().unwrap(),
        ));
        let _slow = browse_conn.clone().lock_owned().await;
        let queued = tokio::time::timeout(
            Duration::from_secs(5),
            browse_read(browse_conn, Duration::from_millis(25), |_| Ok(())),
        )
        .await
        .expect("the queued request waited past its budget");
        assert!(queued.is_none());
    }

    /// A client that disconnects mid-probe cancels its handler. That must not
    /// leave the cache empty, or a loop of abandoned requests would cost one
    /// S3 read each instead of one per TTL.
    #[tokio::test]
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
            async move { cached_probe(&cache, READ_PROBE_TTL, probe).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        abandoned.abort();

        let next = cached_probe(&cache, READ_PROBE_TTL, probe(probes.clone())).await;
        assert_eq!(next, Err("presigned GET answered 403".to_owned()));
        assert_eq!(probes.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// One bad row that panics a read must cost that request, not the
    /// Puller: the process stays up, so nothing would restart it.
    #[tokio::test]
    async fn a_panicked_read_does_not_break_later_reads() {
        let conn = conn();
        let panicked = db_read(
            conn.clone(),
            Duration::from_secs(5),
            |_| -> anyhow::Result<()> { panic!("boom") },
        )
        .await;
        assert!(panicked.unwrap().is_err());
        let next = db_read(conn, Duration::from_secs(5), |_| Ok(42)).await;
        assert_eq!(next.unwrap().unwrap(), 42);
    }

    /// Browse's async lock cannot poison at all; this guards that design
    /// choice, and the 500 a panicking browse request must still answer.
    #[tokio::test]
    async fn a_panicking_browse_request_is_a_500_and_the_next_one_is_served() {
        let conn = Arc::new(tokio::sync::Mutex::new(
            rusqlite::Connection::open_in_memory().unwrap(),
        ));
        let panicked = browse(conn.clone(), "tree", |_| -> anyhow::Result<Option<()>> {
            panic!("boom")
        })
        .await;
        assert_eq!(panicked.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let next = browse(conn, "tree", |_| Ok(Some(42))).await;
        assert_eq!(next.status(), StatusCode::OK);
    }
}
