//! TOML config, one file per service. Secrets are paths or env, never inline
//! in the nix store (spec 10-packaging).

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub endpoint_url: Option<String>,
    pub region: Option<String>,
    #[serde(default = "yes")]
    pub path_style: bool,
    /// Omitted in production: the NixOS module supplies AWS_* via EnvironmentFile.
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

fn yes() -> bool {
    true
}

fn store_dir() -> String {
    "/nix/store".into()
}

fn pusher_listen() -> String {
    // Loopback until M2 wires OIDC — see the bind check in garret-pusher.
    "127.0.0.1:8080".into()
}

#[derive(Debug, Deserialize)]
pub struct PusherConfig {
    #[serde(default = "pusher_listen")]
    pub listen: String,
    pub db_path: String,
    pub s3: S3Config,
    #[serde(default = "store_dir")]
    pub store_dir: String,
    pub signing_key_files: Vec<String>,
    /// Bodies above this are refused until multipart lands (M3).
    #[serde(default = "max_body")]
    pub max_body_bytes: u64,
}

fn max_body() -> u64 {
    100 * 1024 * 1024
}

#[derive(Debug, Deserialize)]
pub struct PullerConfig {
    #[serde(default = "puller_listen")]
    pub listen: String,
    pub db_path: String,
    pub s3: S3Config,
    #[serde(default = "store_dir")]
    pub store_dir: String,
    #[serde(default = "presign_ttl")]
    pub presign_ttl_secs: u64,
}

fn puller_listen() -> String {
    "127.0.0.1:8081".into()
}

fn presign_ttl() -> u64 {
    3600
}

pub fn load<T: serde::de::DeserializeOwned>(path: &str) -> Result<T> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {path}"))?;
    toml::from_str(&text).with_context(|| format!("parsing config {path}"))
}
