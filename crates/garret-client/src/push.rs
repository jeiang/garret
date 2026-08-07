//! Pushing closures: one negotiation round-trip, then parallel streamed PUTs
//! (spec 01-push-protocol, 06-client).

use std::{process::Stdio, time::Duration};

use anyhow::{Context, Result, bail};
use async_compression::{Level, tokio::bufread::ZstdEncoder};
use futures::{StreamExt, stream};
use garret_common::{Preamble, hash_of_store_path};
use reqwest::{Body, StatusCode};
use serde::Deserialize;
use tokio::{io::BufReader, process::Command};
use tokio_util::io::ReaderStream;

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
        let missing: Vec<String> = self
            .http
            .post(format!("{}/api/v1/missing-paths", self.endpoint))
            .bearer_auth(&self.token)
            .json(&hashes)
            .send()
            .await?
            .error_for_status()
            .context("negotiating missing paths")?
            .json()
            .await?;

        Ok(closure
            .iter()
            .filter(|p| missing.iter().any(|h| h == hash_of_store_path(&p.path)))
            .cloned()
            .collect())
    }

    /// Uploads every path with at most `jobs` in flight. Returns how many were
    /// newly created; already-present paths count as success.
    pub async fn push_all(&self, paths: Vec<PathInfo>) -> Result<usize> {
        let results = stream::iter(paths)
            .map(|info| async move {
                let name = info.path.clone();
                match self.push_one(&info).await {
                    Ok(status) => {
                        println!("  {status:<8} {name}");
                        Ok(())
                    }
                    Err(e) => {
                        eprintln!("  failed   {name}: {e:#}");
                        Err(e)
                    }
                }
            })
            .buffer_unordered(self.jobs)
            .collect::<Vec<_>>()
            .await;

        let failed = results.iter().filter(|r| r.is_err()).count();
        if failed > 0 {
            bail!("{failed} path(s) failed to push");
        }
        Ok(results.len())
    }

    async fn push_one(&self, info: &PathInfo) -> Result<&'static str> {
        let mut delay = Duration::from_millis(250);
        for attempt in 0..=self.max_retries {
            match self.attempt(info).await {
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

    async fn attempt(&self, info: &PathInfo) -> Result<&'static str> {
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
        let encoder =
            ZstdEncoder::with_quality(BufReader::new(stdout), Level::Precise(self.zstd_level));

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
            "exists" | "in-progress" => "skipped",
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
            .is_some_and(|e| e.is_timeout() || e.is_connect())
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

    #[test]
    fn jitter_never_exceeds_the_delay() {
        for ms in [1u64, 250, 4000] {
            let delay = Duration::from_millis(ms);
            assert!(fastrand_millis(delay) < ms.max(1));
        }
    }
}
