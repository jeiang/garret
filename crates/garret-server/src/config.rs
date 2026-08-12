//! TOML config, one file per service. Secrets are paths or env, never inline
//! in the nix store (spec 10-packaging).

use anyhow::{Context, Result};
use serde::Deserialize;

/// The `[s3]` section: where blobs live (MEGA S4 in production, Garage
/// locally, spec 03-storage).
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct S3Config {
    /// Bucket holding the `nar/` blobs.
    pub bucket: String,
    /// Non-AWS endpoint (S4, Garage). Absent means the AWS default.
    pub endpoint_url: Option<String>,
    /// Defaults to `us-east-1`, which S3-compatible stores accept.
    pub region: Option<String>,
    /// Path-style addressing, default on — S4 and Garage both accept it, and
    /// it avoids DNS games for dotted buckets.
    #[serde(default = "yes")]
    pub path_style: bool,
    /// Omitted in production: the NixOS module supplies AWS_* via EnvironmentFile.
    pub access_key_id: Option<String>,
    /// See [`S3Config::access_key_id`] — set both or neither.
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
    /// Issuer URL, matched exactly against the token's `iss` claim.
    pub issuer: String,
    /// Required `aud` claim (RFC 8707 audience).
    pub audience: String,
    /// The public client id `garret login` should use for the device flow.
    /// Lives here rather than in a top-level `[discovery]` section so it cannot
    /// drift from the issuer the Pusher actually validates against, and so it
    /// self-selects: only the human device-flow issuer has one, never the
    /// GitHub Actions issuer. Discovery advertises the first issuer that sets it.
    pub client_id: Option<String>,
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
    /// Uploads admitted at once; the rest wait. Default 32.
    #[serde(default = "max_concurrent_uploads")]
    pub max_concurrent_uploads: usize,
    /// Global cap on buffered upload bytes across all uploads. Default 2 GiB.
    #[serde(default = "max_in_flight_bytes")]
    pub max_in_flight_bytes: u64,
    /// S3 part size, and the single-`PutObject` threshold. Default 64 MiB.
    #[serde(default = "part_size")]
    pub part_size: usize,
    /// Concurrent part uploads per NAR. Default 4.
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
    /// The Quota: the storage budget eviction defends.
    pub quota_bytes: u64,
    /// Fraction of quota at which eviction starts. Default 0.95.
    #[serde(default = "high_watermark")]
    pub high_watermark: f64,
    /// Fraction of quota eviction frees down to. Default 0.85.
    #[serde(default = "low_watermark")]
    pub low_watermark: f64,
    /// Seconds between GC timer ticks (each tick is a cheap counter check).
    /// Default 300.
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

/// The Pusher's config file (the upload service; not exposed publicly).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PusherConfig {
    /// Listen address. Default `127.0.0.1:8080`.
    #[serde(default = "pusher_listen")]
    pub listen: String,
    /// SQLite path. The Pusher creates and migrates it; the Puller waits.
    pub db_path: String,
    /// Blob storage.
    pub s3: S3Config,
    /// Store prefix used in signed fingerprints. Default `/nix/store`.
    #[serde(default = "store_dir")]
    pub store_dir: String,
    /// Nix secret-key files; every object is signed with all of them
    /// (multi-key overlap rotation, spec 10-packaging).
    pub signing_key_files: Vec<String>,
    /// Advertised by `/api/v1/discovery` so `garret login` can write a whole
    /// client config from one URL. Absent means discovery omits it and the
    /// client keeps its existing "set `puller_endpoint`" error.
    pub puller_endpoint: Option<String>,
    /// At least one is required; there is no auth-disable flag (spec 04).
    pub oidc: Vec<IssuerConfig>,
    /// Backpressure caps; every field has a default.
    #[serde(default)]
    pub limits: Limits,
    /// Absent means no quota is enforced and GC never evicts.
    pub gc: Option<GcConfig>,
    /// Root-only unix socket for garret-admin. Absent means no admin surface.
    pub admin_socket: Option<String>,
    /// Internal metrics listener (spec 08). Default `127.0.0.1:9091`.
    #[serde(default = "pusher_metrics_listen")]
    pub metrics_listen: String,
}

/// The Puller's config file (the public substituter frontend; read-only).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullerConfig {
    /// Listen address. Default `127.0.0.1:8081`.
    #[serde(default = "puller_listen")]
    pub listen: String,
    /// SQLite path — the same file the Pusher writes.
    pub db_path: String,
    /// Blob storage, used only to presign GETs (ADR-0005).
    pub s3: S3Config,
    /// Store prefix; advertised in `nix-cache-info`. Default `/nix/store`.
    #[serde(default = "store_dir")]
    pub store_dir: String,
    /// Lifetime of presigned NAR URLs. Default 3600.
    #[serde(default = "presign_ttl")]
    pub presign_ttl_secs: u64,
    /// Internal metrics listener (spec 08). Default `127.0.0.1:9092`.
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

/// Parses a TOML config file. Unknown or misplaced keys are errors
/// (`deny_unknown_fields`), so typos fail loudly instead of doing nothing.
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
