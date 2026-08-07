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
    "127.0.0.1:8080".into()
}

/// One trusted OIDC issuer. Pocket ID and GitHub Actions differ only in the
/// authorization fields they populate (spec 04-auth).
#[derive(Debug, Deserialize, Clone)]
pub struct IssuerConfig {
    pub issuer: String,
    pub audience: String,
    /// Skips discovery. An `http(s)` URL, or a path to a static JWKS file —
    /// the sanctioned local-dev override.
    pub jwks_url: Option<String>,
    /// GitHub only: the immutable numeric owner id this issuer is scoped to.
    pub github_owner_id: Option<String>,
    /// GitHub only, optional: `refs/heads/main`, `refs/tags/*`, …
    #[serde(default)]
    pub ref_patterns: Vec<String>,
    /// Defense-in-depth, default off — group membership lives in Pocket ID.
    #[serde(default)]
    pub allowed_groups: Vec<String>,
}

/// Backpressure caps (spec 01-push-protocol). Server memory is bounded by
/// these, not by how many clients show up.
#[derive(Debug, Deserialize, Clone)]
pub struct Limits {
    #[serde(default = "max_concurrent_uploads")]
    pub max_concurrent_uploads: usize,
    #[serde(default = "max_in_flight_bytes")]
    pub max_in_flight_bytes: u64,
    #[serde(default = "part_size")]
    pub part_size: usize,
    #[serde(default = "max_parts_in_flight")]
    pub max_parts_in_flight: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_concurrent_uploads: max_concurrent_uploads(),
            max_in_flight_bytes: max_in_flight_bytes(),
            part_size: part_size(),
            max_parts_in_flight: max_parts_in_flight(),
        }
    }
}

fn max_concurrent_uploads() -> usize {
    32
}

fn max_in_flight_bytes() -> u64 {
    2 * 1024 * 1024 * 1024
}

fn part_size() -> usize {
    64 * 1024 * 1024
}

fn max_parts_in_flight() -> usize {
    4
}

fn pusher_metrics_listen() -> String {
    "127.0.0.1:9091".into()
}

fn puller_metrics_listen() -> String {
    "127.0.0.1:9092".into()
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
    /// At least one is required; there is no auth-disable flag (spec 04).
    pub oidc: Vec<IssuerConfig>,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default = "pusher_metrics_listen")]
    pub metrics_listen: String,
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
    #[serde(default = "puller_metrics_listen")]
    pub metrics_listen: String,
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
