//! Pushing closures: one negotiation round-trip, then parallel streamed PUTs
//! (spec 01-push-protocol, 06-client).

use std::{
    collections::{HashMap, HashSet},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_compression::{Level, tokio::bufread::ZstdEncoder, zstd::CParameter};
use futures::{StreamExt, stream};
use garret_common::{Preamble, hash_of_store_path};
use indicatif::{MultiProgress, ProgressBar, ProgressBarIter, ProgressStyle};
use reqwest::{Body, StatusCode};
use serde::Deserialize;
use serde_json::json;
use tokio::{
    io::{AsyncRead, AsyncReadExt, BufReader},
    process::Command,
    time::Instant,
};
use tokio_util::io::ReaderStream;

/// Where push output goes.
///
/// The bar draws to stderr (via the shared `MultiProgress`, which also carries
/// tracing's writer so a log line suspends it instead of shredding it), while
/// per-path lines and NDJSON events go to stdout. That split is what makes
/// `garret push --json > events.ndjson` show a live bar and still write clean
/// JSON.
#[derive(Clone)]
pub struct Report {
    json: bool,
    mp: MultiProgress,
    bar: Option<ProgressBar>,
    /// When the run began, for the human `done:` line.
    started: Instant,
}

impl Report {
    /// Announces the negotiation result and opens the bar. Called once, after
    /// `missing`, because that is the first moment the byte total is known.
    ///
    /// `upstream` is the paths the upstream filter dropped *before* the
    /// Negotiation. Under `--json` each is reported here with status
    /// `upstream`; the human output only counts them, since a closure routinely
    /// holds thousands. Either way `closure` minus `upstream` is what was
    /// negotiated and the counts stay honest.
    pub fn start(
        json: bool,
        mp: &MultiProgress,
        closure: usize,
        upstream: &[PathInfo],
        missing: &[PathInfo],
    ) -> Self {
        let nar_bytes: u64 = missing.iter().map(|p| p.nar_size.max(0) as u64).sum();
        let mut report = Self {
            json,
            mp: mp.clone(),
            bar: None,
            started: Instant::now(),
        };
        if json {
            report.event(json!({
                "event": "negotiated",
                "closure": closure,
                "upstream": upstream.len(),
                "missing": missing.len(),
                "nar_bytes": nar_bytes,
            }));
            for info in upstream {
                report.path(info, &Outcome::Upstream);
            }
            return report;
        }
        report.out(&Self::negotiated_line(
            closure,
            upstream.len(),
            missing.len(),
        ));
        if missing.is_empty() {
            return report;
        }
        // One overall bar over *uncompressed* NAR bytes. That total is exactly
        // known from the closure before a byte moves, whereas compressed
        // bytes-on-wire have no total until the upload is over — and a bar over
        // path count lurches, because a closure is 1 KB man-pages sitting next
        // to 400 MB toolchains. The rate is therefore NAR bytes/s, not wire
        // bytes/s, and the template says so.
        let bar = mp.add(ProgressBar::new(nar_bytes));
        bar.set_style(
            ProgressStyle::with_template(
                "{spinner} [{elapsed_precise}] [{wide_bar}] {bytes}/{total_bytes} ({bytes_per_sec} NAR, eta {eta})",
            )
            .expect("static template")
            .progress_chars("=> "),
        );
        report.bar = Some(bar);
        report
    }

    /// The human Negotiation summary, shared with `--dry-run`. The upstream
    /// count appears only when the filter dropped something — the common case
    /// has nothing to say.
    pub fn negotiated_line(closure: usize, upstream: usize, missing: usize) -> String {
        match upstream {
            0 => format!("{closure} path(s) in closure, {missing} missing"),
            u => format!("{closure} path(s) in closure, {u} served upstream, {missing} missing"),
        }
    }

    /// The daemon's reporter: per-path lines to the journal, no bar (the
    /// journal is not a TTY anyway), no events.
    pub fn plain() -> Self {
        Self {
            json: false,
            mp: MultiProgress::with_draw_target(indicatif::ProgressDrawTarget::hidden()),
            bar: None,
            started: Instant::now(),
        }
    }

    /// Writes to stdout without the bar clobbering the line.
    fn out(&self, line: &str) {
        self.mp.suspend(|| println!("{line}"));
    }

    fn event(&self, value: serde_json::Value) {
        self.out(&value.to_string());
    }

    /// One path finished, one way or another. A failure is reported here and
    /// the run continues: every path gets an event, and the non-zero exit comes
    /// after `finish`, so a consumer always sees the totals.
    fn path(&self, info: &PathInfo, outcome: &Outcome) {
        if self.json {
            self.event(path_event(info, outcome));
        } else if let Outcome::Failed(error) = outcome {
            self.out(&format!("  failed   {}: {error}", info.path));
        } else {
            self.out(&format!("  {:<8} {}", outcome.as_str(), info.path));
        }
    }

    /// The closing Negotiation found `gone` missing again after a clean run;
    /// they are pushed next and their events follow this one.
    pub fn rechecked(&self, gone: &[PathInfo]) {
        let nar_bytes: u64 = gone.iter().map(|p| p.nar_size.max(0) as u64).sum();
        if let Some(bar) = &self.bar {
            bar.inc_length(nar_bytes);
        }
        if self.json {
            self.event(json!({
                "event": "rechecked",
                "missing": gone.len(),
                "nar_bytes": nar_bytes,
            }));
        } else {
            self.out(&format!(
                "re-check: {} path(s) went missing during the run, pushing them",
                gone.len()
            ));
        }
    }

    /// Clears the bar and emits the totals — the `done` event under `--json`,
    /// which is what makes a truncated NDJSON stream detectable.
    pub fn finish(&self, summary: &Summary) {
        if let Some(bar) = &self.bar {
            bar.finish_and_clear();
        }
        if self.json {
            self.event(json!({
                "event": "done",
                "pushed": summary.pushed,
                "deduped": summary.deduped,
                "failed": summary.failed,
                "nar_bytes": summary.nar_bytes,
            }));
        } else {
            self.out(&format!(
                "done: {} pushed, {} deduped, {} failed ({} in {:.1}s)",
                summary.pushed,
                summary.deduped,
                summary.failed,
                garret_common::human_bytes(summary.nar_bytes.min(i64::MAX as u64) as i64),
                self.started.elapsed().as_secs_f64()
            ));
        }
    }

    /// Counts NAR bytes as they are read, *before* compression — the units the
    /// bar's total is denominated in.
    ///
    /// A retried path is re-read and so counted twice, nudging the bar past its
    /// total — routinely while an `in-progress` path is polled, since each poll
    /// streams part of the body before the early reply. That is cosmetic, and
    /// correcting it would mean threading a per-attempt counter through the
    /// body stream to decrement on error; the bar clamps its display, so it is
    /// left alone.
    fn wrap<R: AsyncRead + Unpin>(&self, reader: R) -> ProgressBarIter<R> {
        match &self.bar {
            Some(bar) => bar.clone().wrap_async_read(reader),
            None => ProgressBar::hidden().wrap_async_read(reader),
        }
    }
}

/// The `--json` event for one finished path; `error` appears only on failure.
fn path_event(info: &PathInfo, outcome: &Outcome) -> serde_json::Value {
    let mut event = json!({
        "event": "path",
        "path": info.path,
        "status": outcome.as_str(),
        "nar_size": info.nar_size,
    });
    if let Outcome::Failed(error) = outcome {
        event["error"] = error.as_str().into();
    }
    event
}

/// How one path ended up. The status strings are the NDJSON `path` event
/// contract (spec 06).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Outcome {
    Pushed,
    Deduped,
    /// Failed even after retries; carries the `{e:#}` error text.
    Failed(String),
    /// Signed by an upstream key, so never negotiated or uploaded.
    Upstream,
}

impl Outcome {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Pushed => "pushed",
            Self::Deduped => "deduped",
            Self::Failed(_) => "failed",
            Self::Upstream => "upstream",
        }
    }
}

/// What a successful upload attempt achieved: the server stored the path, or
/// already held it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stored {
    Pushed,
    Deduped,
}

impl From<Stored> for Outcome {
    fn from(stored: Stored) -> Self {
        match stored {
            Stored::Pushed => Self::Pushed,
            Stored::Deduped => Self::Deduped,
        }
    }
}

/// Folds per-path outcomes into the run's [`Summary`] and the failed paths.
fn tally(outcomes: Vec<(String, Outcome)>, nar_bytes: u64) -> (Summary, Vec<String>) {
    let mut summary = Summary {
        nar_bytes,
        ..Summary::default()
    };
    let mut failed = Vec::new();
    for (path, outcome) in outcomes {
        match outcome {
            Outcome::Pushed => summary.pushed += 1,
            Outcome::Deduped => summary.deduped += 1,
            Outcome::Failed(_) => {
                summary.failed += 1;
                failed.push(path);
            }
            Outcome::Upstream => {}
        }
    }
    (summary, failed)
}

/// What a push run did. `deduped` means the bytes were uploaded and *then*
/// found redundant server-side — not that they were skipped.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Summary {
    /// Paths whose upload created a new Object.
    pub pushed: usize,
    /// Paths another writer beat us to: an `exists` ack, including one
    /// reached by waiting out another pusher's `in-progress` upload.
    pub deduped: usize,
    /// Paths that failed even after retries; a non-zero count is what turns
    /// into a non-zero exit code.
    pub failed: usize,
    /// Total uncompressed NAR bytes across the attempted paths.
    pub nar_bytes: u64,
}

impl std::ops::AddAssign for Summary {
    fn add_assign(&mut self, other: Self) {
        self.pushed += other.pushed;
        self.deduped += other.deduped;
        self.failed += other.failed;
        self.nar_bytes += other.nar_bytes;
    }
}

/// A configured connection to the Pusher: everything [`missing`] and
/// [`push_all`] need, assembled once per run.
///
/// [`missing`]: Pusher::missing
/// [`push_all`]: Pusher::push_all
pub struct Pusher {
    /// Shared HTTP client, reused across the negotiation and every PUT.
    pub http: reqwest::Client,
    /// Pusher base URL.
    pub endpoint: String,
    /// Bearer tokens, asked for per request: a run can outlive any one token.
    pub tokens: crate::auth::TokenSource,
    /// Maximum concurrent uploads.
    pub jobs: usize,
    /// zstd level for compressing NARs on the way out.
    pub zstd_level: i32,
    /// Retries per path on 5xx and dropped connections, with exponential
    /// jittered backoff; 429s wait out `Retry-After` instead.
    pub max_retries: u32,
    /// How long an upload may go without progress before it is abandoned as
    /// [`Stalled`] and retried.
    pub upload_idle: Duration,
}

/// The subset of `nix path-info --json` garret needs. Shelling out to nix
/// beats re-implementing its database: the client already requires nix.
#[derive(Debug, Deserialize, Clone)]
pub struct PathInfo {
    /// Full store path.
    pub path: String,
    /// Hash of the uncompressed NAR, `sha256:` prefixed.
    #[serde(rename = "narHash")]
    pub nar_hash: String,
    /// Uncompressed NAR size in bytes.
    #[serde(rename = "narSize")]
    pub nar_size: i64,
    /// Direct references as full store paths — the preamble needs names, not
    /// hashes, or the server cannot produce a valid narinfo (spec 01).
    #[serde(default)]
    pub references: Vec<String>,
    /// Deriver store path, when nix knows it.
    pub deriver: Option<String>,
    /// Content-address string for fixed-output and CA paths.
    pub ca: Option<String>,
    /// Signatures, `key-name:base64` each — what the upstream filter reads.
    #[serde(default)]
    pub signatures: Vec<String>,
}

/// True when any signature's key name (the part before `:`) is one of the
/// configured upstream keys. Exact name equality, not substring: a key merely
/// *containing* `cache.nixos.org-1` must not match.
pub fn signed_upstream(signatures: &[String], upstream_keys: &[String]) -> bool {
    signatures.iter().any(|sig| {
        let key = sig.split(':').next().unwrap_or_default();
        upstream_keys.iter().any(|u| u == key)
    })
}

/// Splits a closure into (ours, upstream). A path signed by an upstream key is
/// already served elsewhere: pushing it would spend bandwidth and Quota on
/// bytes Eviction would happily reclaim but the cache never needed to hold. It
/// never enters the Negotiation, so the batch shrinks for free (spec 06).
pub fn partition_upstream(
    closure: Vec<PathInfo>,
    upstream_keys: &[String],
) -> (Vec<PathInfo>, Vec<PathInfo>) {
    closure
        .into_iter()
        .partition(|p| !signed_upstream(&p.signatures, upstream_keys))
}

/// Whole closure of the given installables, roots included.
pub async fn closure(paths: &[String]) -> Result<Vec<PathInfo>> {
    path_info(paths, true).await
}

/// `nix path-info --json` over `paths`: with `recursive`, their whole closure;
/// without, just the paths themselves (their direct references are named in
/// each entry's `references`).
pub async fn path_info(paths: &[String], recursive: bool) -> Result<Vec<PathInfo>> {
    let mut command = Command::new("nix");
    command.args(["path-info", "--json", "--json-format", "1"]);
    if recursive {
        command.arg("--recursive");
    }
    let output = command
        .args(paths)
        .output()
        .await
        .context("running `nix path-info` — is nix on PATH?")?;
    if !output.status.success() {
        bail!(
            "nix path-info failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    parse_path_info(&output.stdout)
}

/// Nix has emitted both a list and a path-keyed object over the years; accept
/// either rather than pinning a nix version. A path missing from the local
/// store comes back as a `null` entry (object form) or `"valid": false` (list
/// form), with nix still exiting 0, so it is named here rather than left to
/// fail as a missing `narHash`.
fn parse_path_info(stdout: &[u8]) -> Result<Vec<PathInfo>> {
    let value: serde_json::Value =
        serde_json::from_slice(stdout).context("parsing nix path-info output")?;
    let entries: Vec<(Option<String>, serde_json::Value)> = match value {
        serde_json::Value::Array(items) => items.into_iter().map(|i| (None, i)).collect(),
        serde_json::Value::Object(map) => map.into_iter().map(|(p, i)| (Some(p), i)).collect(),
        other => bail!("unexpected nix path-info output: {other}"),
    };
    let mut infos = Vec::new();
    let mut invalid = Vec::new();
    for (path, mut info) in entries {
        if info.is_null() || info.get("valid") == Some(&serde_json::Value::Bool(false)) {
            let named = info.get("path").and_then(|p| p.as_str()).map(str::to_owned);
            invalid.push(path.or(named).unwrap_or_default());
            continue;
        }
        if let Some(path) = path.filter(|_| info.get("path").is_none()) {
            info["path"] = path.into();
        }
        infos.push(serde_json::from_value(info)?);
    }
    if !invalid.is_empty() {
        bail!(
            "not valid in the local store (build or fetch them first): {}",
            invalid.join(" ")
        );
    }
    Ok(infos)
}

/// The entries of `infos` whose hash is in `missing`, in order. A set lookup
/// per entry: Negotiations over thousands of paths must not go quadratic.
pub fn retain_missing(infos: &[PathInfo], missing: &HashSet<String>) -> Vec<PathInfo> {
    infos
        .iter()
        .filter(|p| missing.contains(hash_of_store_path(&p.path)))
        .cloned()
        .collect()
}

impl Pusher {
    /// The one pre-upload round-trip (spec 01): the subset of `closure` the
    /// cache does not hold.
    pub async fn missing(&self, closure: &[PathInfo]) -> Result<Vec<PathInfo>> {
        let hashes: Vec<&str> = closure
            .iter()
            .map(|p| hash_of_store_path(&p.path))
            .collect();
        Ok(retain_missing(closure, &self.negotiate(&hashes).await?))
    }

    /// One Negotiation over `hashes`: the subset the cache does not hold.
    pub async fn negotiate(&self, hashes: &[&str]) -> Result<HashSet<String>> {
        let send = |token: &str| {
            self.http
                .post(format!("{}/api/v1/missing-paths", self.endpoint))
                .bearer_auth(token)
                .json(&hashes)
                // Small and quick when healthy; without a cap a stalled
                // connection would hang the push or the watcher for good.
                .timeout(NEGOTIATION_TIMEOUT)
                .send()
        };
        let token = self.tokens.get().await?;
        let mut response = send(&token).await.context("negotiating missing paths")?;
        // Expired or revoked since it was minted: replace it once and ask
        // again. Only a 401 renews: a 403 is a permanent policy refusal.
        if response.status() == StatusCode::UNAUTHORIZED
            && let Some(token) = self
                .tokens
                .renew(&token)
                .await
                .context("the Pusher refused the token, and renewing it failed")?
        {
            response = send(&token).await.context("negotiating missing paths")?;
        }

        // `error_for_status` drops the body, which is where the server explains
        // itself; a bare "403 Forbidden" says nothing a log can act on.
        let status = response.status();
        if status == StatusCode::FORBIDDEN {
            let body = response.text().await.unwrap_or_default();
            return Err(Refused(body.trim().to_owned()).into());
        }
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("negotiation rejected with {status}: {}", body.trim());
        }
        let missing: HashSet<String> = response
            .json()
            .await
            .context("parsing the negotiation response")?;

        Ok(missing)
    }

    /// Uploads every path with at most `jobs` in flight, references first.
    ///
    /// A failure does not abort the run: every path is attempted and reported,
    /// and the caller decides the exit code from `Summary::failed`. That way a
    /// `--json` consumer always receives the terminating `done` event.
    pub async fn push_all(&self, paths: Vec<PathInfo>, report: &Report) -> Summary {
        self.push_tracked(paths, report).await.0
    }

    /// [`push_all`](Self::push_all), also naming the paths that failed — what
    /// the Watcher puts on its failed list.
    pub async fn push_tracked(
        &self,
        paths: Vec<PathInfo>,
        report: &Report,
    ) -> (Summary, Vec<String>) {
        let nar_bytes: u64 = paths.iter().map(|p| p.nar_size.max(0) as u64).sum();
        let outcomes = stream::iter(references_first(paths))
            .map(|info| async move {
                let outcome = match self.push_one(&info, report).await {
                    Ok(stored) => Outcome::from(stored),
                    Err(e) => Outcome::Failed(format!("{e:#}")),
                };
                report.path(&info, &outcome);
                (info.path, outcome)
            })
            .buffer_unordered(self.jobs)
            .collect::<Vec<_>>()
            .await;
        tally(outcomes, nar_bytes)
    }

    async fn push_one(&self, info: &PathInfo, report: &Report) -> Result<Stored> {
        let mut backoff = Backoff::new(self.max_retries);
        let mut renewed = false;
        loop {
            let token = self.tokens.get().await?;
            let error = match self.attempt(info, report, &token).await {
                Ok(Attempt::Stored(stored)) => return Ok(stored),
                Ok(Attempt::InProgress) => match backoff.in_progress() {
                    Some(wait) => {
                        tokio::time::sleep(wait).await;
                        continue;
                    }
                    None => bail!("another pusher is still uploading this path"),
                },
                Err(e) => e,
            };
            // The token expired or was revoked mid-run: replace it once and go
            // again straight away. A second 401 is a real refusal.
            if !renewed
                && error.is::<Unauthorized>()
                && self
                    .tokens
                    .renew(&token)
                    .await
                    .context("the Pusher refused the token, and renewing it failed")?
                    .is_some()
            {
                renewed = true;
                continue;
            }
            match backoff.next(&error) {
                Some(wait) => tokio::time::sleep(wait).await,
                None => return Err(error),
            }
        }
    }

    async fn attempt(&self, info: &PathInfo, report: &Report, token: &str) -> Result<Attempt> {
        let preamble = Preamble {
            store_path: info.path.clone(),
            nar_hash: info.nar_hash.clone(),
            nar_size: info.nar_size,
            references: info.references.clone(),
            deriver: info.deriver.clone(),
            ca: info.ca.clone(),
        };

        // `nix nar dump-path` → zstd → the wire, never landing whole in memory.
        let mut dump = Command::new("nix");
        dump.args(["nar", "dump-path", &info.path]);
        let nar = compressed_nar(dump, report, self.zstd_level, zstd_workers(info.nar_size))?;

        let framed = preamble.to_framed()?;
        let body = stream::once(async move { Ok::<_, std::io::Error>(bytes::Bytes::from(framed)) })
            .chain(nar);

        let request = self
            .http
            .put(format!(
                "{}/api/v1/nar/{}",
                self.endpoint,
                hash_of_store_path(&info.path)
            ))
            .bearer_auth(token);
        let response = send_watched(request, body, self.upload_idle)
            .await
            .context("uploading NAR")?;

        let status = response.status();
        if let Some(after) = retry_after(status, &response) {
            return Err(Shed(after).into());
        }
        if status == StatusCode::UNAUTHORIZED {
            let body = response.text().await.unwrap_or_default();
            return Err(Unauthorized(body.trim().to_owned()).into());
        }
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(Rejected {
                status,
                body: body.trim().to_owned(),
            }
            .into());
        }

        #[derive(Deserialize)]
        struct Ack {
            status: String,
        }
        let ack: Ack = response.json().await.context("parsing upload response")?;
        match ack.status.as_str() {
            // "deduped", not "skipped": the bytes were uploaded and only then
            // found redundant, so the bandwidth was spent either way.
            "exists" => Ok(Attempt::Stored(Stored::Deduped)),
            // Not success yet: the other upload may still fail. Wait until
            // the server answers `exists`, or lets this one through.
            "in-progress" => Ok(Attempt::InProgress),
            _ => Ok(Attempt::Stored(Stored::Pushed)),
        }
    }
}

/// Orders `paths` so each comes after the paths in the batch it references.
/// The worker pool only *starts* uploads in this order: with `jobs > 1` a
/// referrer can still finish before its references, and a reference that
/// fails does not hold its referrers back, so an interrupted or failed run can
/// leave a referrer without its references until the next push's Negotiation
/// finds them. At `jobs = 1` an interrupted run leaves only whole closures.
/// Self-references and references outside the batch impose nothing; a cycle
/// (nix forbids them) keeps its original order at the end rather than dropping
/// a path.
fn references_first(paths: Vec<PathInfo>) -> Vec<PathInfo> {
    let index: HashMap<&str, usize> = paths
        .iter()
        .enumerate()
        .map(|(i, p)| (p.path.as_str(), i))
        .collect();
    // Kahn's algorithm: `waiting[i]` counts i's references not yet placed.
    let mut waiting = vec![0usize; paths.len()];
    let mut referrers = vec![Vec::new(); paths.len()];
    for (i, info) in paths.iter().enumerate() {
        for reference in &info.references {
            if let Some(&j) = index.get(reference.as_str())
                && j != i
            {
                waiting[i] += 1;
                referrers[j].push(i);
            }
        }
    }
    let mut order: Vec<usize> = (0..paths.len()).filter(|&i| waiting[i] == 0).collect();
    let mut next = 0;
    while let Some(&i) = order.get(next) {
        next += 1;
        for &k in &referrers[i] {
            waiting[k] -= 1;
            if waiting[k] == 0 {
                order.push(k);
            }
        }
    }
    order.extend((0..paths.len()).filter(|&i| waiting[i] > 0));

    let mut slots: Vec<Option<PathInfo>> = paths.into_iter().map(Some).collect();
    order
        .into_iter()
        .map(|i| slots[i].take().expect("each index is placed once"))
        .collect()
}

/// Generous for one missing-paths round-trip, even over a whole closure.
const NEGOTIATION_TIMEOUT: Duration = Duration::from_secs(2 * 60);

/// The upload went [`Pusher::upload_idle`] without progress (default 5 min,
/// generous because the server stops reading on purpose while it waits for a
/// part slot behind other uploads). Retryable: negotiation
/// makes the retry idempotent, and a fresh connection may well not stall.
#[derive(Debug)]
struct Stalled(Duration);

impl std::fmt::Display for Stalled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "upload stalled: no progress for {:?}", self.0)
    }
}

impl std::error::Error for Stalled {}

/// When an upload last made progress: the moment the connection last took a
/// body chunk. Milliseconds since `start`, so ticking needs no lock.
#[derive(Clone)]
struct Progress {
    start: Instant,
    since_start: Arc<AtomicU64>,
}

impl Progress {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            since_start: Arc::new(AtomicU64::new(0)),
        }
    }

    fn tick(&self) {
        let now = self.start.elapsed().as_millis() as u64;
        self.since_start.store(now, Ordering::Relaxed);
    }

    fn last(&self) -> Instant {
        self.start + Duration::from_millis(self.since_start.load(Ordering::Relaxed))
    }
}

/// Sends `request` with `body`, failing it as [`Stalled`] once the connection
/// goes `idle` without taking a chunk, or without answering after the last.
///
/// Not reqwest's `read_timeout`: that timer starts when the request is sent
/// and is not reset until the response head arrives, so on an upload it caps
/// the whole body transfer rather than detecting a stall. Progress is instead
/// read off the body stream, which hyper only polls for more once the socket
/// has taken what it had.
async fn send_watched(
    request: reqwest::RequestBuilder,
    body: impl futures::Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static,
    idle: Duration,
) -> Result<reqwest::Response> {
    let progress = Progress::new();
    let ticks = progress.clone();
    let body = body.inspect(move |_| ticks.tick());
    Ok(unless_stalled(
        request.body(Body::wrap_stream(body)).send(),
        &progress,
        idle,
    )
    .await??)
}

/// Runs `request` unless `progress` goes `idle` without a tick first.
async fn unless_stalled<T>(
    request: impl Future<Output = T>,
    progress: &Progress,
    idle: Duration,
) -> Result<T, Stalled> {
    tokio::pin!(request);
    loop {
        tokio::select! {
            done = &mut request => return Ok(done),
            _ = tokio::time::sleep_until(progress.last() + idle) => {
                if progress.last() + idle <= Instant::now() {
                    return Err(Stalled(idle));
                }
            }
        }
    }
}

/// The Pusher refused the token. Typed so [`Pusher::push_one`] can tell it
/// apart from other rejections and renew the token instead of failing.
#[derive(Debug)]
struct Unauthorized(String);

impl std::fmt::Display for Unauthorized {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "upload rejected with {}: {}",
            StatusCode::UNAUTHORIZED,
            self.0
        )
    }
}

impl std::error::Error for Unauthorized {}

/// The Pusher refused an upload with a status other than 401 or 429. Typed so
/// [`is_retryable`] reads the status instead of searching the message.
#[derive(Debug)]
struct Rejected {
    status: StatusCode,
    body: String,
}

impl std::fmt::Display for Rejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "upload rejected with {}: {}", self.status, self.body)
    }
}

impl std::error::Error for Rejected {}

/// The Negotiation got a 403: the Pusher's policy refuses this identity, now
/// and on every retry. Typed so the watcher logs it once and drops the batch
/// instead of retrying. A 401 that survives renewal is not this: it can be
/// transient (JWKS outage), so it is an ordinary failure.
#[derive(Debug)]
pub struct Refused(
    /// The Pusher's explanation, as sent.
    pub String,
);

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "negotiation rejected with {}: {}",
            StatusCode::FORBIDDEN,
            self.0
        )
    }
}

impl std::error::Error for Refused {}

/// The server shed the upload (429) and said when to come back. Typed so
/// [`Backoff`] waits out `Retry-After` instead of parsing the message.
#[derive(Debug)]
struct Shed(Duration);

impl std::fmt::Display for Shed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "server is shedding load (429), retry after {:?}", self.0)
    }
}

impl std::error::Error for Shed {}

/// What one upload attempt got back: the path stored, or another pusher
/// uploading it right now. The latter is waited out rather than counted as
/// pushed: that upload may yet fail, and then nobody would have pushed it.
enum Attempt {
    Stored(Stored),
    InProgress,
}

/// How long one path keeps waiting out 429s and other pushers' uploads before
/// it fails: long enough to queue behind several multi-minute uploads, short
/// enough that a server that never frees a slot fails the run instead of
/// hanging it.
const WAIT_DEADLINE: Duration = Duration::from_secs(15 * 60);

/// The longest pause between asking whether another pusher's upload is done.
const IN_PROGRESS_MAX_POLL: Duration = Duration::from_secs(30);

/// One path's retry schedule (spec 01). A 429 is the server's queue and an
/// `in-progress` ack another pusher's upload, not faults: both are waited out
/// for as long as [`WAIT_DEADLINE`] allows and never spend the error budget —
/// 429s for `Retry-After`, `in-progress` in waits doubling from 1 s to
/// [`IN_PROGRESS_MAX_POLL`]. Other retryable failures get `max_retries`
/// doubling waits from 250 ms; either wait resets that budget, so it counts
/// consecutive faults — early replies lost to the connection race (spec 01)
/// must not add up over a quarter hour of waiting. Every wait is jittered so a
/// fleet of pushers doesn't retry in lockstep.
struct Backoff {
    started: Instant,
    retries: u32,
    max_retries: u32,
    delay: Duration,
    poll: Duration,
}

impl Backoff {
    fn new(max_retries: u32) -> Self {
        Self {
            started: Instant::now(),
            retries: 0,
            max_retries,
            delay: Duration::from_millis(250),
            poll: Duration::from_secs(1),
        }
    }

    /// How long to wait before trying again after `error`; `None` to give up.
    fn next(&mut self, error: &anyhow::Error) -> Option<Duration> {
        if let Some(Shed(after)) = error.downcast_ref::<Shed>() {
            return self.queued(*after);
        }
        if self.retries >= self.max_retries || !is_retryable(error) {
            return None;
        }
        let wait = self.delay + Duration::from_millis(fastrand_millis(self.delay));
        self.retries += 1;
        self.delay *= 2;
        Some(wait)
    }

    /// How long to wait before asking again after an `in-progress` ack;
    /// `None` to give up.
    fn in_progress(&mut self) -> Option<Duration> {
        let poll = self.poll;
        self.poll = (poll * 2).min(IN_PROGRESS_MAX_POLL);
        self.queued(poll)
    }

    /// A wait that is not a fault: it resets the error budget and is bounded
    /// only by [`WAIT_DEADLINE`].
    fn queued(&mut self, base: Duration) -> Option<Duration> {
        self.retries = 0;
        self.delay = Duration::from_millis(250);
        let wait = base + Duration::from_millis(fastrand_millis(base));
        (self.started.elapsed() + wait <= WAIT_DEADLINE).then_some(wait)
    }
}

/// NarSize from which a NAR is compressed on several threads. Smaller ones
/// fit in about one zstd job anyway, and many run side by side (`jobs`).
const ZSTD_MT_MIN: i64 = 32 * 1024 * 1024;

/// zstd worker threads for a NAR of `nar_size` bytes: 0 (compress on the
/// calling thread) below [`ZSTD_MT_MIN`], else one per CPU, so one large
/// NAR at the tail of a push does not run on a single core.
fn zstd_workers(nar_size: i64) -> u32 {
    if nar_size < ZSTD_MT_MIN {
        return 0;
    }
    std::thread::available_parallelism().map_or(0, |n| n.get() as u32)
}

/// Streams `dump`'s stdout through zstd, then fails the stream if `dump` exited
/// non-zero.
///
/// A dump that dies part-way (the path GC'd locally since `nix path-info`, a
/// daemon error) still closes its stdout, so the encoder would seal a valid
/// zstd frame around a truncated NAR. The server rejects that as a NarHash
/// mismatch (spec 01), but with a `400` that hides the cause. An error as the
/// stream's last item instead aborts the request before the body ends, so the
/// push fails with the dump's own error.
fn compressed_nar(
    mut dump: Command,
    report: &Report,
    zstd_level: i32,
    zstd_workers: u32,
) -> Result<impl futures::Stream<Item = std::io::Result<bytes::Bytes>> + use<>> {
    let mut child = dump
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("running `nix nar dump-path`")?;
    let stdout = child.stdout.take().expect("stdout was piped");
    let mut stderr = child.stderr.take().expect("stderr was piped");
    // Drained alongside stdout, not after it: a child blocked writing to a
    // full stderr pipe would never close stdout. The head is what explains the
    // failure; the rest is discarded.
    let stderr = tokio::spawn(async move {
        let mut head = Vec::new();
        let _ = (&mut stderr).take(4096).read_to_end(&mut head).await;
        let _ = tokio::io::copy(&mut stderr, &mut tokio::io::sink()).await;
        head
    });
    // Counted here, before the encoder, so the ticks are in the same units as
    // the bar's total.
    let encoder = ZstdEncoder::with_quality_and_params(
        BufReader::with_capacity(256 * 1024, report.wrap(stdout)),
        Level::Precise(zstd_level),
        &[CParameter::nb_workers(zstd_workers)],
    );
    let exit = stream::once(async move {
        let status = child.wait().await?;
        if status.success() {
            return Ok(None);
        }
        let stderr = stderr.await.unwrap_or_default();
        Err(std::io::Error::other(format!(
            "`nix nar dump-path` failed ({status}): {}",
            String::from_utf8_lossy(&stderr).trim()
        )))
    })
    .filter_map(|end: std::io::Result<Option<bytes::Bytes>>| async move { end.transpose() });
    // 256 KiB reads and chunks: the 8 KiB defaults cost a syscall and a body
    // frame per 8 KiB of a multi-hundred-MB NAR.
    Ok(ReaderStream::with_capacity(encoder, 256 * 1024).chain(exit))
}

fn retry_after(status: StatusCode, response: &reqwest::Response) -> Option<Duration> {
    (status == StatusCode::TOO_MANY_REQUESTS).then(|| {
        shed_for(
            response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
        )
    })
}

/// A 429's `Retry-After`, in seconds; absent or unparseable (an HTTP date)
/// means 1 s. Clamped to at least 1 s, so a `0` cannot spin, and at most
/// [`WAIT_DEADLINE`], so an absurd value cannot overflow the wait arithmetic.
fn shed_for(retry_after: Option<&str>) -> Duration {
    retry_after
        .and_then(|v| v.trim().parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(1))
        .clamp(Duration::from_secs(1), WAIT_DEADLINE)
}

/// 5xx and dropped, timed-out or stalled connections are retryable; 4xx are
/// the client's fault (spec 01). 429 never reaches here: it is a [`Shed`].
fn is_retryable(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<Rejected>()
        .is_some_and(|r| r.status.is_server_error())
        || error.is::<Stalled>()
        || error
            .downcast_ref::<reqwest::Error>()
            .is_some_and(|e| e.is_timeout() || e.is_connect() || connection_dropped(e))
}

/// The server answers `exists`, `in-progress` and 429 sheds *before* reading
/// the body (spec 01), and hyper then closes the connection with request bytes
/// still unread — which the kernel turns into an RST. A client mid-way through
/// writing the body races that RST: sometimes it reads the response first,
/// sometimes the write dies with a broken pipe and the response is lost. That
/// loss is indistinguishable from a network drop, and both are safe to retry:
/// negotiation makes every push idempotent.
pub fn connection_dropped(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut cause = Some(error);
    while let Some(e) = cause {
        if let Some(io) = e.downcast_ref::<std::io::Error>() {
            return matches!(
                io.kind(),
                std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::UnexpectedEof
            );
        }
        // hyper's IncompleteMessage carries no io::Error: a reused idle
        // connection the server closed under us, equally retryable.
        if e.to_string()
            .contains("connection closed before message completed")
        {
            return true;
        }
        cause = e.source();
    }
    false
}

/// Jitter without a rand dependency: sub-second clock noise, capped at the
/// delay. Microseconds, not nanoseconds: macOS clocks tick in whole
/// microseconds, so nanoseconds modulo a whole second would always be 0.
fn fastrand_millis(delay: Duration) -> u64 {
    let micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_micros() as u64)
        .unwrap_or(0);
    micros % delay.as_millis().max(1) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_object_form_of_path_info() {
        let json = br#"{"/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-x":
            {"narHash":"sha256:h","narSize":12,"references":[],"deriver":null,"ca":null}}"#;
        let infos = parse_path_info(json).unwrap();
        assert_eq!(infos.len(), 1);
        assert_eq!(
            infos[0].path,
            "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-x"
        );
        assert_eq!(infos[0].nar_size, 12);
    }

    #[test]
    fn paths_missing_from_the_store_are_named() {
        let object = br#"{"/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-x":null,
            "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-y":
            {"narHash":"sha256:h","narSize":12,"references":[]}}"#;
        let list = br#"[{"path":"/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-x","valid":false}]"#;
        for json in [&object[..], &list[..]] {
            let err = parse_path_info(json).unwrap_err().to_string();
            assert!(err.contains("not valid in the local store"), "{err}");
            assert!(err.contains("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-x"), "{err}");
            assert!(!err.contains("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-y"), "{err}");
        }
    }

    #[test]
    fn parses_the_list_form_of_path_info() {
        let json = br#"[{"path":"/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-x",
            "narHash":"sha256:h","narSize":12,"references":[]}]"#;
        let infos = parse_path_info(json).unwrap();
        assert_eq!(infos[0].nar_hash, "sha256:h");
        assert!(infos[0].deriver.is_none());
        // Absent signatures (older nix, unsigned paths) parse as empty.
        assert!(infos[0].signatures.is_empty());
    }

    #[test]
    fn parses_signatures_when_nix_reports_them() {
        let json = br#"[{"path":"/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-x",
            "narHash":"sha256:h","narSize":12,"references":[],
            "signatures":["cache.nixos.org-1:sig","garret-1:sig"]}]"#;
        let infos = parse_path_info(json).unwrap();
        assert_eq!(
            infos[0].signatures,
            ["cache.nixos.org-1:sig", "garret-1:sig"]
        );
    }

    fn info(path: &str, signatures: &[&str]) -> PathInfo {
        PathInfo {
            path: path.into(),
            nar_hash: "sha256:h".into(),
            nar_size: 1,
            references: vec![],
            deriver: None,
            ca: None,
            signatures: signatures.iter().map(|s| (*s).to_owned()).collect(),
        }
    }

    #[test]
    fn upstream_signed_paths_are_partitioned_out() {
        let keys = vec!["cache.nixos.org-1".to_owned()];
        let closure = vec![
            info("/nix/store/aaa-ours", &["garret-1:sig"]),
            info("/nix/store/bbb-toolchain", &["cache.nixos.org-1:sig"]),
            info("/nix/store/ccc-unsigned", &[]),
            // A key whose name merely contains an upstream key must not match.
            info("/nix/store/ddd-lookalike", &["not-cache.nixos.org-1x:sig"]),
        ];
        let (ours, upstream) = partition_upstream(closure, &keys);
        assert_eq!(
            ours.iter().map(|p| p.path.as_str()).collect::<Vec<_>>(),
            [
                "/nix/store/aaa-ours",
                "/nix/store/ccc-unsigned",
                "/nix/store/ddd-lookalike"
            ]
        );
        assert_eq!(upstream.len(), 1);
        assert_eq!(upstream[0].path, "/nix/store/bbb-toolchain");
    }

    #[test]
    fn the_negotiated_line_mentions_upstream_only_when_nonzero() {
        assert_eq!(
            Report::negotiated_line(2, 0, 2),
            "2 path(s) in closure, 2 missing"
        );
        assert_eq!(
            Report::negotiated_line(5, 3, 1),
            "5 path(s) in closure, 3 served upstream, 1 missing"
        );
    }

    #[test]
    fn tally_counts_each_outcome_and_names_the_failures() {
        let outcomes = vec![
            ("/nix/store/a".to_owned(), Outcome::Pushed),
            ("/nix/store/b".to_owned(), Outcome::Failed("boom".into())),
            ("/nix/store/c".to_owned(), Outcome::Deduped),
            ("/nix/store/d".to_owned(), Outcome::Pushed),
            ("/nix/store/e".to_owned(), Outcome::Upstream),
            ("/nix/store/f".to_owned(), Outcome::Failed("bang".into())),
        ];
        let (summary, failed) = tally(outcomes, 42);
        assert_eq!(
            summary,
            Summary {
                pushed: 2,
                deduped: 1,
                failed: 2,
                nar_bytes: 42,
            }
        );
        assert_eq!(failed, ["/nix/store/b", "/nix/store/f"]);
    }

    #[test]
    fn path_events_carry_the_status_and_only_a_failure_carries_an_error() {
        let path = info("/nix/store/aaa-x", &[]);
        let failed = path_event(&path, &Outcome::Failed("uploading NAR: reset".into()));
        assert_eq!(failed["event"], "path");
        assert_eq!(failed["path"], "/nix/store/aaa-x");
        assert_eq!(failed["status"], "failed");
        assert_eq!(failed["error"], "uploading NAR: reset");
        assert_eq!(failed["nar_size"], 1);
        for (outcome, status) in [
            (Outcome::Pushed, "pushed"),
            (Outcome::Deduped, "deduped"),
            (Outcome::Upstream, "upstream"),
        ] {
            let event = path_event(&path, &outcome);
            assert_eq!(event["status"], status);
            assert!(event.get("error").is_none(), "{event}");
        }
    }

    fn shed(secs: u64) -> anyhow::Error {
        Shed(Duration::from_secs(secs)).into()
    }

    fn rejected(status: u16) -> anyhow::Error {
        let status = StatusCode::from_u16(status).unwrap();
        Rejected {
            status,
            body: "nope".into(),
        }
        .into()
    }

    /// Six CI legs against sixteen upload slots is the normal load: a path
    /// can be shed for minutes, and none of that may count against the
    /// budget for real failures.
    #[tokio::test(start_paused = true)]
    async fn shedding_waits_out_retry_after_and_never_spends_the_error_budget() {
        let mut backoff = Backoff::new(2);
        for _ in 0..100 {
            let wait = backoff.next(&shed(3)).expect("well inside the deadline");
            assert!(
                (Duration::from_secs(3)..Duration::from_secs(6)).contains(&wait),
                "{wait:?}"
            );
            tokio::time::advance(wait).await;
        }
        assert!(backoff.next(&rejected(503)).is_some());
        assert!(backoff.next(&rejected(503)).is_some());
        // A shed in between resets the budget: it counts consecutive faults,
        // not every reply lost to the connection race over a long queue.
        assert!(backoff.next(&shed(1)).is_some());
        assert!(backoff.next(&rejected(503)).is_some());
        assert!(backoff.next(&rejected(503)).is_some());
        assert!(backoff.next(&rejected(503)).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn shedding_gives_up_at_the_deadline() {
        let mut backoff = Backoff::new(5);
        tokio::time::advance(WAIT_DEADLINE - Duration::from_secs(10)).await;
        assert!(backoff.next(&shed(1)).is_some());
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(backoff.next(&shed(1)).is_none());
    }

    /// The header is the server's (or a proxy's) to set: a `0` must not spin
    /// and an absurd value must not overflow the wait, which would panic.
    #[test]
    fn retry_after_is_held_between_a_second_and_the_deadline() {
        let second = Duration::from_secs(1);
        assert_eq!(shed_for(Some("7")), Duration::from_secs(7));
        assert_eq!(shed_for(Some("0")), second);
        assert_eq!(shed_for(None), second);
        assert_eq!(shed_for(Some("Wed, 21 Oct 2015 07:28:00 GMT")), second);
        let absurd = shed_for(Some("18446744073709551615"));
        assert_eq!(absurd, WAIT_DEADLINE);
        let mut backoff = Backoff::new(0);
        assert!(backoff.next(&Shed(absurd).into()).is_none());
    }

    /// Another pusher's upload may yet fail, so it is waited out — polling
    /// less and less often — rather than counted as pushed, and the wait
    /// spends none of the budget for real failures.
    #[tokio::test(start_paused = true)]
    async fn another_pushers_upload_is_polled_off_the_error_budget_until_the_deadline() {
        let mut backoff = Backoff::new(1);
        for base in [1, 2, 4, 8, 16, 30, 30, 30] {
            let wait = backoff.in_progress().expect("inside the deadline");
            let base = Duration::from_secs(base);
            assert!((base..base * 2).contains(&wait), "{wait:?} for {base:?}");
            tokio::time::advance(wait).await;
        }
        assert!(backoff.next(&rejected(503)).is_some());
        assert!(backoff.next(&rejected(503)).is_none());
        tokio::time::advance(WAIT_DEADLINE).await;
        assert!(backoff.in_progress().is_none());
    }

    fn node(name: &str, references: &[&str]) -> PathInfo {
        PathInfo {
            references: references
                .iter()
                .map(|r| format!("/nix/store/{r}"))
                .collect(),
            ..info(&format!("/nix/store/{name}"), &[])
        }
    }

    /// Every path starts after the paths in the batch it references, so a
    /// sequential run cut short leaves whole closures behind.
    #[test]
    fn references_are_pushed_before_their_referrers() {
        let batch = vec![
            node("root", &["root", "tool", "lib"]),
            node("tool", &["lib", "elsewhere"]),
            node("other", &[]),
            node("lib", &["lib", "elsewhere"]),
        ];
        let order = references_first(batch.clone());
        let at = |path: &str| order.iter().position(|p| p.path == path).unwrap();
        assert_eq!(order.len(), batch.len());
        for info in &batch {
            for reference in &info.references {
                if reference != &info.path && batch.iter().any(|p| &p.path == reference) {
                    assert!(
                        at(reference) < at(&info.path),
                        "{reference} after {}",
                        info.path
                    );
                }
            }
        }
        // Nix forbids cycles, but one must not drop a path.
        assert_eq!(
            references_first(vec![node("a", &["b"]), node("b", &["a"])]).len(),
            2
        );
    }

    #[test]
    fn only_a_server_status_is_retryable_never_a_body_that_looks_like_one() {
        let body_with_50 = Rejected {
            status: StatusCode::BAD_REQUEST,
            body: "bad nar 500 bytes".into(),
        };
        assert!(!is_retryable(&body_with_50.into()));
        assert!(is_retryable(&rejected(503)));
        assert!(!is_retryable(&Refused("no".into()).into()));
        assert!(!is_retryable(&rejected(403)));
    }

    #[test]
    fn server_errors_back_off_doubling_and_client_errors_fail_at_once() {
        let mut backoff = Backoff::new(5);
        let first = backoff.next(&rejected(503)).unwrap();
        let second = backoff.next(&rejected(502)).unwrap();
        assert!((Duration::from_millis(250)..Duration::from_millis(500)).contains(&first));
        assert!((Duration::from_millis(500)..Duration::from_millis(1000)).contains(&second));
        assert!(backoff.next(&rejected(400)).is_none());
        assert!(backoff.next(&rejected(401)).is_none());
        // A 403 is permanent: no retry, no wait.
        assert!(backoff.next(&Refused("no".into()).into()).is_none());
        assert!(backoff.next(&rejected(403)).is_none());
    }

    /// An idle limit, not a total one: an upload that keeps moving may take
    /// many times `idle`.
    #[tokio::test(start_paused = true)]
    async fn a_slow_upload_that_keeps_moving_is_not_a_stall() {
        let idle = Duration::from_secs(10);
        let progress = Progress::new();
        let ticks = progress.clone();
        let upload = async move {
            for _ in 0..10 {
                tokio::time::sleep(idle / 2).await;
                ticks.tick();
            }
        };
        assert!(unless_stalled(upload, &progress, idle).await.is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn going_idle_is_a_retryable_stall() {
        let progress = Progress::new();
        let stalled = unless_stalled(
            std::future::pending::<()>(),
            &progress,
            Duration::from_secs(10),
        )
        .await
        .unwrap_err();
        // As `upload` returns it: wrapped in context.
        assert!(is_retryable(
            &anyhow::Error::from(stalled).context("uploading NAR")
        ));
    }

    /// The watchdog reads progress off the body stream, so it relies on hyper
    /// pulling no more chunks once the socket stops draining. A server that
    /// accepts and never reads is exactly that stall.
    #[tokio::test]
    async fn a_server_that_stops_reading_stalls_the_upload() {
        static CHUNK: [u8; 65536] = [0; 65536];
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _held_open = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        let endless = stream::repeat_with(|| Ok(bytes::Bytes::from_static(&CHUNK)));
        let request = reqwest::Client::new().put(format!("http://{addr}/api/v1/nar/x"));
        let watched = send_watched(request, endless, Duration::from_millis(500));
        let error = tokio::time::timeout(Duration::from_secs(30), watched)
            .await
            .expect("an unwatched upload hangs here forever")
            .unwrap_err();
        assert!(error.is::<Stalled>(), "{error:#}");
    }

    /// Stand-in for reqwest's wrapping: the io::Error sits at the bottom of a
    /// source chain, not at the top.
    #[derive(Debug)]
    struct Wrapped(std::io::Error);

    impl std::fmt::Display for Wrapped {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "error writing a body to connection")
        }
    }

    impl std::error::Error for Wrapped {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn a_dropped_connection_is_recognised_through_the_source_chain() {
        for kind in [
            std::io::ErrorKind::BrokenPipe,
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::UnexpectedEof,
        ] {
            assert!(connection_dropped(&Wrapped(std::io::Error::from(kind))));
        }
        assert!(!connection_dropped(&Wrapped(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied
        ))));
        // No io::Error anywhere in the chain: not a drop.
        assert!(!connection_dropped(&std::fmt::Error));
    }

    #[test]
    fn jitter_never_exceeds_the_delay() {
        for ms in [1u64, 250, 4000] {
            let delay = Duration::from_millis(ms);
            assert!(fastrand_millis(delay) < ms.max(1));
        }
    }

    fn sh(script: &str) -> Command {
        let mut command = Command::new("sh");
        command.args(["-c", script]);
        command
    }

    #[tokio::test]
    async fn a_failed_dump_fails_the_body_instead_of_ending_it() {
        let items: Vec<_> = compressed_nar(
            sh("printf 'partial nar'; echo 'path is not valid' >&2; exit 3"),
            &Report::plain(),
            3,
            0,
        )
        .unwrap()
        .collect()
        .await;
        let (last, streamed) = items.split_last().unwrap();
        assert!(streamed.iter().all(Result::is_ok));
        let error = last.as_ref().unwrap_err().to_string();
        assert!(error.contains("path is not valid"), "{error}");

        let items: Vec<_> = compressed_nar(sh("printf nar"), &Report::plain(), 3, 0)
            .unwrap()
            .collect()
            .await;
        assert!(items.iter().all(Result::is_ok));
    }

    /// The Negotiation answers with hashes; only the batch's own entries that
    /// come back are pushed, in batch order.
    #[test]
    fn only_entries_named_missing_are_kept_in_order() {
        let a = "a".repeat(32);
        let b = "b".repeat(32);
        let c = "c".repeat(32);
        let infos = vec![
            info(&format!("/nix/store/{a}-one"), &[]),
            info(&format!("/nix/store/{b}-two"), &[]),
            info(&format!("/nix/store/{c}-three"), &[]),
        ];
        let missing: HashSet<String> = [c.clone(), a.clone(), "d".repeat(32)].into();
        let kept = retain_missing(&infos, &missing);
        assert_eq!(
            kept.iter().map(|p| p.path.as_str()).collect::<Vec<_>>(),
            [
                format!("/nix/store/{a}-one"),
                format!("/nix/store/{c}-three")
            ]
        );
    }

    /// Serves `status` with `body` to every request, counting them.
    async fn answering(
        status: u16,
        body: &'static str,
    ) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::Ordering;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = hits.clone();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = [0u8; 4096];
                let _ = socket.read(&mut buf).await;
                counter.fetch_add(1, Ordering::SeqCst);
                let reply = format!(
                    "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(reply.as_bytes()).await;
            }
        });
        (endpoint, hits)
    }

    fn pusher(endpoint: String) -> (Pusher, Arc<std::sync::atomic::AtomicUsize>) {
        let (tokens, mints) = crate::auth::TokenSource::counting();
        let pusher = Pusher {
            http: reqwest::Client::new(),
            endpoint,
            tokens,
            jobs: 1,
            zstd_level: 3,
            max_retries: 0,
            upload_idle: Duration::from_secs(5),
        };
        (pusher, mints)
    }

    /// 403 is a permanent refusal: no renewal, one request, typed `Refused`.
    #[tokio::test]
    async fn a_403_negotiation_is_refused_without_renewing_the_token() {
        use std::sync::atomic::Ordering;
        let (endpoint, requests) = answering(403, r#"{"error":"not authorized"}"#).await;
        let (pusher, mints) = pusher(endpoint);
        let error = pusher.negotiate(&["h"]).await.unwrap_err();
        let refused = error.downcast_ref::<Refused>().expect("typed Refused");
        assert!(refused.0.contains("not authorized"), "{}", refused.0);
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert_eq!(mints.load(Ordering::SeqCst), 1);
    }

    /// A 401 renews the token once; if it persists it is an ordinary failure,
    /// not `Refused`, so the watcher keeps the paths (transient causes).
    #[tokio::test]
    async fn a_persistent_401_renews_once_and_is_not_a_refusal() {
        use std::sync::atomic::Ordering;
        let (endpoint, requests) = answering(401, r#"{"error":"invalid token"}"#).await;
        let (pusher, mints) = pusher(endpoint);
        let error = pusher.negotiate(&["h"]).await.unwrap_err();
        assert!(!error.is::<Refused>());
        assert!(format!("{error:#}").contains("401"), "{error:#}");
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        assert_eq!(mints.load(Ordering::SeqCst), 2);
    }
}
