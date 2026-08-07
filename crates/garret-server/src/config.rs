//! TOML config, one file per service. Secrets are paths or env, never inline
//! in the nix store (spec 10-packaging).

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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

/// Quota and eviction policy (spec 05-gc).
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct GcConfig {
    pub quota_bytes: u64,
    #[serde(default = "high_watermark")]
    pub high_watermark: f64,
    #[serde(default = "low_watermark")]
    pub low_watermark: f64,
    #[serde(default = "gc_interval")]
    pub interval_secs: u64,
    /// How long a blob with no row, or an idle multipart, must persist before
    /// the sweep removes it — long enough that no live upload is caught.
    #[serde(default = "orphan_grace")]
    pub orphan_grace_secs: u64,
}

impl GcConfig {
    /// Eviction starts here.
    pub fn high(&self) -> i64 {
        (self.quota_bytes as f64 * self.high_watermark) as i64
    }

    /// And runs until here, so passes are infrequent rather than constant.
    pub fn low(&self) -> i64 {
        (self.quota_bytes as f64 * self.low_watermark) as i64
    }
}

fn high_watermark() -> f64 {
    0.95
}

fn low_watermark() -> f64 {
    0.85
}

fn gc_interval() -> u64 {
    300
}

fn orphan_grace() -> u64 {
    24 * 60 * 60
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Absent means no quota is enforced and GC never evicts.
    pub gc: Option<GcConfig>,
    /// Root-only unix socket for garret-admin. Absent means no admin surface.
    pub admin_socket: Option<String>,
    #[serde(default = "pusher_metrics_listen")]
    pub metrics_listen: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Browse routes require Pocket ID; narinfo/NAR stay anonymous (spec 04).
    /// Absent means the browse API is not served at all.
    pub browse_oidc: Option<IssuerConfig>,
    /// Skip a last-accessed write if the stored value is newer than this.
    #[serde(default = "bump_debounce")]
    pub bump_debounce_secs: i64,
    /// How long to wait for the Pusher to create the database at boot before
    /// giving up. Until then the Puller serves 503s and `/ready` says so.
    #[serde(default = "db_wait_timeout")]
    pub db_wait_timeout_secs: u64,
}

fn db_wait_timeout() -> u64 {
    300
}

fn bump_debounce() -> i64 {
    24 * 60 * 60
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermarks_are_fractions_of_the_quota() {
        let cfg = GcConfig {
            quota_bytes: 1000,
            high_watermark: 0.95,
            low_watermark: 0.85,
            interval_secs: 300,
            orphan_grace_secs: 86400,
        };
        assert_eq!(cfg.high(), 950);
        assert_eq!(cfg.low(), 850);
        // Nothing is evicted while usage stays below the high watermark.
        assert!(949 < cfg.high());
        // And a pass always frees a margin, so passes stay infrequent.
        assert!(cfg.low() < cfg.high());
    }
}

#[cfg(test)]
mod strictness_tests {
    use super::*;

    /// A misplaced or mistyped key must fail loudly. Silently ignoring one is
    /// how `admin_socket` ended up inside `[limits]` and did nothing.
    #[test]
    fn unknown_keys_are_rejected() {
        let good = r#"
            db_path = "/tmp/x.db"
            signing_key_files = ["/tmp/k"]
            [s3]
            bucket = "b"
            [[oidc]]
            issuer = "https://i"
            audience = "a"
        "#;
        assert!(toml::from_str::<PusherConfig>(good).is_ok());

        let typo = good.replace("db_path", "database_path");
        assert!(toml::from_str::<PusherConfig>(&typo).is_err());

        let misplaced = format!("{good}\n[limits]\nadmin_socket = \"/run/x.sock\"\n");
        assert!(
            toml::from_str::<PusherConfig>(&misplaced).is_err(),
            "a root key inside [limits] must not be silently swallowed"
        );
    }
}
