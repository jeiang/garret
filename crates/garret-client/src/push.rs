//! Pushing closures: one negotiation round-trip, then parallel streamed PUTs
//! (spec 01-push-protocol, 06-client).

use std::{process::Stdio, time::Duration};

use anyhow::{Context, Result, bail};
use async_compression::{Level, tokio::bufread::ZstdEncoder};
use futures::{StreamExt, stream};
use garret_common::{Preamble, hash_of_store_path};
use indicatif::{MultiProgress, ProgressBar, ProgressBarIter, ProgressStyle};
use reqwest::{Body, StatusCode};
use serde::Deserialize;
use serde_json::json;
use tokio::{io::AsyncRead, io::BufReader, process::Command};
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
}

impl Report {
    /// Announces the negotiation result and opens the bar. Called once, after
    /// `missing`, because that is the first moment the byte total is known.
    pub fn start(json: bool, mp: &MultiProgress, closure: usize, missing: &[PathInfo]) -> Self {
        let nar_bytes: u64 = missing.iter().map(|p| p.nar_size.max(0) as u64).sum();
        let mut report = Self {
            json,
            mp: mp.clone(),
            bar: None,
        };
        if json {
            report.event(json!({
                "event": "negotiated",
                "closure": closure,
                "missing": missing.len(),
                "nar_bytes": nar_bytes,
            }));
            return report;
        }
        report.out(&format!(
            "{closure} path(s) in closure, {} missing",
            missing.len()
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

    /// The daemon's reporter: per-path lines to the journal, no bar (the
    /// journal is not a TTY anyway), no events.
    pub fn plain() -> Self {
        Self {
            json: false,
            mp: MultiProgress::with_draw_target(indicatif::ProgressDrawTarget::hidden()),
            bar: None,
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
    fn path(&self, info: &PathInfo, status: &str, error: Option<&str>) {
        if self.json {
            let mut event = json!({
                "event": "path",
                "path": info.path,
                "status": status,
                "nar_size": info.nar_size,
            });
            if let Some(error) = error {
                event["error"] = error.into();
            }
            self.event(event);
        } else if let Some(error) = error {
            self.out(&format!("  failed   {}: {error}", info.path));
        } else {
            self.out(&format!("  {status:<8} {}", info.path));
        }
    }

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
                "done: {} pushed, {} deduped, {} failed",
                summary.pushed, summary.deduped, summary.failed
            ));
        }
    }

    /// Counts NAR bytes as they are read, *before* compression — the units the
    /// bar's total is denominated in.
    ///
    /// A retried path is re-read and so counted twice, nudging the bar past its
    /// total on a rare failure. That is cosmetic, and correcting it would mean
    /// threading a per-attempt counter through the body stream to decrement on
    /// error; the bar clamps its display, so it is left alone.
    fn wrap<R: AsyncRead + Unpin>(&self, reader: R) -> ProgressBarIter<R> {
        match &self.bar {
            Some(bar) => bar.clone().wrap_async_read(reader),
            None => ProgressBar::hidden().wrap_async_read(reader),
        }
    }
}

/// What a push run did. `deduped` means the bytes were uploaded and *then*
/// found redundant server-side — not that they were skipped.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Summary {
    pub pushed: usize,
    pub deduped: usize,
    pub failed: usize,
    pub nar_bytes: u64,
}

pub struct Pusher {
    pub http: reqwest::Client,
    pub endpoint: String,
    pub token: String,
    pub jobs: usize,
    pub zstd_level: i32,
    pub max_retries: u32,
}

/// The subset of `nix path-info --json` garret needs. Shelling out to nix
/// beats re-implementing its database: the client already requires nix.
#[derive(Debug, Deserialize, Clone)]
pub struct PathInfo {
    pub path: String,
    #[serde(rename = "narHash")]
    pub nar_hash: String,
    #[serde(rename = "narSize")]
    pub nar_size: i64,
    #[serde(default)]
    pub references: Vec<String>,
    pub deriver: Option<String>,
    pub ca: Option<String>,
}

/// Whole closure of the given installables, roots included.
pub async fn closure(paths: &[String]) -> Result<Vec<PathInfo>> {
    let output = Command::new("nix")
        .args(["path-info", "--recursive", "--json", "--json-format", "1"])
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
/// either rather than pinning a nix version.
fn parse_path_info(stdout: &[u8]) -> Result<Vec<PathInfo>> {
    let value: serde_json::Value =
        serde_json::from_slice(stdout).context("parsing nix path-info output")?;
    match value {
        serde_json::Value::Array(items) => Ok(serde_json::from_value(items.into())?),
        serde_json::Value::Object(map) => map
            .into_iter()
            .map(|(path, mut info)| {
                if info.get("path").is_none() {
                    info["path"] = path.into();
                }
                Ok(serde_json::from_value(info)?)
            })
            .collect(),
        other => bail!("unexpected nix path-info output: {other}"),
    }
}

impl Pusher {
    /// The one pre-upload round-trip (spec 01): ask for the whole batch at once.
    pub async fn missing(&self, closure: &[PathInfo]) -> Result<Vec<PathInfo>> {
        let hashes: Vec<&str> = closure
            .iter()
            .map(|p| hash_of_store_path(&p.path))
            .collect();
        let response = self
            .http
            .post(format!("{}/api/v1/missing-paths", self.endpoint))
            .bearer_auth(&self.token)
            .json(&hashes)
            .send()
            .await
            .context("negotiating missing paths")?;

        // `error_for_status` drops the body, which is where the server explains
        // itself — a bare "401 Unauthorized" says nothing a log can act on.
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("negotiation rejected with {status}: {}", body.trim());
        }
        let missing: Vec<String> = response
            .json()
            .await
            .context("parsing the negotiation response")?;

        Ok(closure
            .iter()
            .filter(|p| missing.iter().any(|h| h == hash_of_store_path(&p.path)))
            .cloned()
            .collect())
    }

    /// Uploads every path with at most `jobs` in flight.
    ///
    /// A failure does not abort the run: every path is attempted and reported,
    /// and the caller decides the exit code from `Summary::failed`. That way a
    /// `--json` consumer always receives the terminating `done` event.
    pub async fn push_all(&self, paths: Vec<PathInfo>, report: &Report) -> Summary {
        let nar_bytes: u64 = paths.iter().map(|p| p.nar_size.max(0) as u64).sum();
        let statuses = stream::iter(paths)
            .map(|info| async move {
                match self.push_one(&info, report).await {
                    Ok(status) => {
                        report.path(&info, status, None);
                        status
                    }
                    Err(e) => {
                        report.path(&info, "failed", Some(&format!("{e:#}")));
                        "failed"
                    }
                }
            })
            .buffer_unordered(self.jobs)
            .collect::<Vec<_>>()
            .await;

        Summary {
            pushed: statuses.iter().filter(|s| **s == "pushed").count(),
            deduped: statuses.iter().filter(|s| **s == "deduped").count(),
            failed: statuses.iter().filter(|s| **s == "failed").count(),
            nar_bytes,
        }
    }

    async fn push_one(&self, info: &PathInfo, report: &Report) -> Result<&'static str> {
        let mut delay = Duration::from_millis(250);
        for attempt in 0..=self.max_retries {
            match self.attempt(info, report).await {
                Ok(status) => return Ok(status),
                Err(e) if attempt < self.max_retries && is_retryable(&e) => {
                    // Jitter so a fleet of pushers doesn't retry in lockstep.
                    let jitter = Duration::from_millis(fastrand_millis(delay));
                    tokio::time::sleep(delay + jitter).await;
                    delay *= 2;
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("loop returns on the final attempt")
    }

    async fn attempt(&self, info: &PathInfo, report: &Report) -> Result<&'static str> {
        let preamble = Preamble {
            store_path: info.path.clone(),
            nar_hash: info.nar_hash.clone(),
            nar_size: info.nar_size,
            references: info.references.clone(),
            deriver: info.deriver.clone(),
            ca: info.ca.clone(),
        };

        // `nix nar dump-path` → zstd → the wire, never landing whole in memory.
        let mut child = Command::new("nix")
            .args(["nar", "dump-path", &info.path])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .context("running `nix nar dump-path`")?;
        let stdout = child.stdout.take().expect("stdout was piped");
        // Counted here, before the encoder, so the ticks are in the same units
        // as the bar's total.
        let encoder = ZstdEncoder::with_quality(
            BufReader::new(report.wrap(stdout)),
            Level::Precise(self.zstd_level),
        );

        let framed = preamble.to_framed()?;
        let body = stream::once(async move { Ok::<_, std::io::Error>(bytes::Bytes::from(framed)) })
            .chain(ReaderStream::new(encoder));

        let response = self
            .http
            .put(format!(
                "{}/api/v1/nar/{}",
                self.endpoint,
                hash_of_store_path(&info.path)
            ))
            .bearer_auth(&self.token)
            .body(Body::wrap_stream(body))
            .send()
            .await
            .context("uploading NAR")?;

        let status = response.status();
        if let Some(after) = retry_after(status, &response) {
            bail!("server is shedding load (429), retry after {after:?}");
        }
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("upload rejected with {status}: {}", body.trim());
        }

        #[derive(Deserialize)]
        struct Ack {
            status: String,
        }
        let ack: Ack = response.json().await.context("parsing upload response")?;
        Ok(match ack.status.as_str() {
            // First writer wins; a concurrent pusher finishing it is success.
            // "deduped", not "skipped": the bytes were uploaded and only then
            // found redundant, so the bandwidth was spent either way.
            "exists" | "in-progress" => "deduped",
            _ => "pushed",
        })
    }
}

fn retry_after(status: StatusCode, response: &reqwest::Response) -> Option<Duration> {
    (status == StatusCode::TOO_MANY_REQUESTS).then(|| {
        response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(1))
    })
}

/// 429 and 5xx are retryable; other 4xx are the client's fault (spec 01).
fn is_retryable(error: &anyhow::Error) -> bool {
    let text = error.to_string();
    text.contains("429")
        || text.contains("shedding load")
        || text.contains(" 50")
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

/// Jitter without a rand dependency: nanosecond noise, capped at the delay.
fn fastrand_millis(delay: Duration) -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    nanos % delay.as_millis().max(1) as u64
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
    fn parses_the_list_form_of_path_info() {
        let json = br#"[{"path":"/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-x",
            "narHash":"sha256:h","narSize":12,"references":[]}]"#;
        let infos = parse_path_info(json).unwrap();
        assert_eq!(infos[0].nar_hash, "sha256:h");
        assert!(infos[0].deriver.is_none());
    }

    #[test]
    fn only_load_shedding_and_server_errors_are_retried() {
        assert!(is_retryable(&anyhow::anyhow!(
            "server is shedding load (429)"
        )));
        assert!(is_retryable(&anyhow::anyhow!("upload rejected with 503")));
        assert!(!is_retryable(&anyhow::anyhow!("upload rejected with 400")));
        assert!(!is_retryable(&anyhow::anyhow!("upload rejected with 401")));
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
}
