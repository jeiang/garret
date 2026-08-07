//! Client config: TOML with env/flag overrides (spec 06-client).

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
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
}

#[derive(Debug, Deserialize)]
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
