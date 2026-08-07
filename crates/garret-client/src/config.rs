//! Client config: TOML with env/flag overrides (spec 06-client).

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Pusher base URL.
    pub endpoint: String,
    pub oidc: Oidc,
    #[serde(default = "jobs")]
    pub jobs: usize,
    #[serde(default = "zstd_level")]
    pub zstd_level: i32,
    #[serde(default = "max_retries")]
    pub max_retries: u32,
    /// Puller base URL — `list` and `tree` query the browse API, not the Pusher.
    pub puller_endpoint: Option<String>,
    #[serde(default)]
    pub watch: Watch,
}

/// Store watcher settings; only the daemon reads these.
#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Watch {
    /// `client_id:client_secret` for this machine's confidential client.
    pub credentials_file: Option<String>,
    #[serde(default = "nix_db")]
    pub nix_db: String,
    #[serde(default = "cursor_path")]
    pub cursor_path: String,
    #[serde(default = "poll_interval")]
    pub poll_interval_secs: u64,
    #[serde(default = "upstream_keys")]
    pub upstream_keys: Vec<String>,
    #[serde(default)]
    pub exclude_patterns: Vec<String>,
    #[serde(default = "max_attempts")]
    pub max_attempts: u32,
}

fn nix_db() -> String {
    "/nix/var/nix/db/db.sqlite".into()
}

fn cursor_path() -> String {
    "/var/lib/garret/watcher-cursor".into()
}

fn poll_interval() -> u64 {
    30
}

fn upstream_keys() -> Vec<String> {
    vec!["cache.nixos.org-1".into()]
}

fn max_attempts() -> u32 {
    5
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Oidc {
    pub issuer: String,
    pub client_id: String,
    /// RFC 8707 resource indicator identifying garret to the issuer.
    pub audience: String,
}

fn jobs() -> usize {
    // Uploads are network-bound, so this is not a core count.
    8
}

fn zstd_level() -> i32 {
    3
}

fn max_retries() -> u32 {
    5
}

pub fn path() -> Result<std::path::PathBuf> {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| std::path::PathBuf::from(h).join(".config")))
        .context("neither XDG_CONFIG_HOME nor HOME is set")?;
    Ok(base.join("garret").join("config.toml"))
}

pub fn load(explicit: Option<&str>) -> Result<Config> {
    let path = match explicit {
        Some(p) => std::path::PathBuf::from(p),
        None => path()?,
    };
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("reading client config {}", path.display()))?;
    let mut cfg: Config = toml::from_str(&text).context("parsing client config")?;
    if let Ok(endpoint) = std::env::var("GARRET_ENDPOINT") {
        cfg.endpoint = endpoint;
    }
    Ok(cfg)
}
