//! TOML config, one file per service. Secrets are paths or env, never inline
//! in the nix store (spec 10-packaging).

use anyhow::{Context, Result, bail};
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
    /// Path-style addressing, on unless set — S4 and Garage both accept it,
    /// and it avoids DNS games for dotted buckets.
    #[serde(default = "yes")]
    pub path_style: bool,
    /// Omitted in production: the NixOS module supplies AWS_* via EnvironmentFile.
    pub access_key_id: Option<String>,
    /// See [`S3Config::access_key_id`] — set both or neither.
    pub secret_access_key: Option<String>,
    /// Overall deadline for one S3 call, SDK retries included (ticket 27).
    /// Without it a stalled upstream holds an upload permit and its
    /// in-flight bytes forever. Raise on slow uplinks or with larger parts.
    #[serde(default = "s3_operation_timeout")]
    pub operation_timeout_secs: u64,
    /// Deadline for each attempt inside the overall one. Unset, a
    /// hung attempt uses the whole overall deadline and nothing retries. Set
    /// it above the time to send `part_size` on the slowest uplink and below
    /// `operation_timeout_secs`.
    #[serde(default)]
    pub operation_attempt_timeout_secs: Option<u64>,
    /// SigV4-sign upload bodies (a SHA-256 pass over every stored byte).
    /// Off unless set: uploads send `UNSIGNED-PAYLOAD` with a CRC32 checksum
    /// S3 verifies, and the blob's own SHA-256 is the FileHash nix checks
    /// on download (spec 03). Turn on for a store that refuses unsigned
    /// payloads.
    #[serde(default)]
    pub sign_payload: bool,
}

impl S3Config {
    /// Rejects a half-set credential pair and timeouts that never fire or
    /// leave no room for a retry. The S3 client uses explicit credentials
    /// only when both are present and otherwise falls back to the AWS
    /// default chain, so one alone would be silently ignored.
    pub fn validate(&self) -> Result<()> {
        if self.access_key_id.is_some() != self.secret_access_key.is_some() {
            bail!("s3.access_key_id and s3.secret_access_key must be set together or both omitted");
        }
        if self.operation_timeout_secs == 0 {
            bail!("s3.operation_timeout_secs must be at least 1");
        }
        if let Some(attempt) = self.operation_attempt_timeout_secs
            && (attempt == 0 || attempt >= self.operation_timeout_secs)
        {
            bail!(
                "s3.operation_attempt_timeout_secs ({attempt}) must be at least 1 and below s3.operation_timeout_secs ({})",
                self.operation_timeout_secs
            );
        }
        Ok(())
    }
}

fn yes() -> bool {
    true
}

fn s3_operation_timeout() -> u64 {
    60
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
    /// Further `aud` values accepted besides `audience`, which stays the only
    /// one discovery advertises. For machine clients (Pocket ID
    /// client_credentials) whose tokens carry their own client id as `aud`.
    #[serde(default)]
    pub extra_audiences: Vec<String>,
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
    /// GitHub only, optional: require a protected (`true`) or unprotected
    /// (`false`) triggering ref.
    #[serde(default)]
    pub ref_protected: Option<bool>,
    /// GitHub only, optional: immutable numeric `repository_id`s allowed to
    /// push. Names are renameable, so ids are the only stable scope.
    #[serde(default)]
    pub repository_ids: Vec<String>,
    /// GitHub only, optional: allowed `event_name`s, e.g. `push`. Excludes
    /// `pull_request_target` and `workflow_run`, whose ref is the default
    /// branch even when a stranger triggered them.
    #[serde(default)]
    pub event_names: Vec<String>,
    /// GitHub only, optional: allowed `job_workflow_ref`s — the workflow file
    /// the job actually runs, `owner/repo/.github/workflows/f.yml@refs/heads/main`.
    /// Same trailing-`*` globs as `ref_patterns`.
    #[serde(default)]
    pub job_workflow_refs: Vec<String>,
    /// Defense-in-depth, default off — group membership lives in Pocket ID.
    #[serde(default)]
    pub allowed_groups: Vec<String>,
}

/// The GitHub Actions OIDC issuer, which any repository can mint tokens from.
pub(crate) const GITHUB_ISSUER: &str = "https://token.actions.githubusercontent.com";

impl IssuerConfig {
    /// Rejects an issuer that could never verify a token or that would trust
    /// too much (spec 04), naming the issuer.
    pub fn validate(&self) -> Result<()> {
        let issuer = &self.issuer;
        if self.audience.is_empty() {
            bail!("issuer {issuer}: audience must not be empty");
        }
        if *issuer == GITHUB_ISSUER
            && self.github_owner_id.is_none()
            && self.repository_ids.is_empty()
        {
            bail!(
                "issuer {GITHUB_ISSUER} needs github_owner_id or repository_ids: \
                 any GitHub repository can mint a token for this audience"
            );
        }
        match &self.jwks_url {
            Some(url) if is_remote_url(url) => check_remote_url(url),
            // A local JWKS file: the sanctioned dev-issuer override.
            Some(_) => Ok(()),
            // Discovery is fetched from the issuer URL itself.
            None => check_remote_url(issuer),
        }
        .with_context(|| format!("issuer {issuer}"))
    }
}

/// Whether a JWKS location is a URL to fetch rather than a local file path.
pub(crate) fn is_remote_url(url: &str) -> bool {
    url.starts_with("http://") || url.starts_with("https://")
}

/// Remote JWKS and discovery URLs must be https; plain http is for a local
/// dev issuer on a loopback host only.
pub(crate) fn check_remote_url(url: &str) -> Result<()> {
    let parsed = reqwest::Url::parse(url).with_context(|| format!("invalid URL {url}"))?;
    match (parsed.scheme(), parsed.host_str()) {
        ("https", Some(_)) => Ok(()),
        ("http", Some("localhost" | "127.0.0.1" | "[::1]")) => Ok(()),
        _ => bail!("{url} must be https (http only for loopback hosts)"),
    }
}

/// S3's smallest accepted part size; `limits.part_size` must be at least this.
pub const MIN_PART_SIZE: usize = 5 * 1024 * 1024;
/// S3's largest accepted part size; `limits.part_size` must be at most this.
pub const MAX_PART_SIZE: usize = 5 * 1024 * 1024 * 1024;

/// Backpressure caps (spec 01-push-protocol). Server memory is bounded by
/// these, not by how many clients show up.
#[derive(Debug, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    /// Uploads admitted at once; the rest wait.
    pub max_concurrent_uploads: usize,
    /// Global cap on buffered upload bytes across all uploads.
    pub max_in_flight_bytes: u64,
    /// S3 part size, and the single-`PutObject` threshold; within
    /// [`MIN_PART_SIZE`]..=[`MAX_PART_SIZE`].
    pub part_size: usize,
    /// Concurrent part uploads per NAR.
    pub max_parts_in_flight: usize,
    /// Seconds an upload body may send nothing before the upload is
    /// abandoned (spec 01). A stalled body holds an upload slot, its
    /// in-flight claim and, mid-NAR, a part slot, so without a bound a few
    /// quiet senders starve every other push. Idle, not total: a slow
    /// sender that keeps moving is fine. Raising it holds upload slots longer.
    pub body_idle_timeout_secs: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_concurrent_uploads: 16,
            max_in_flight_bytes: 256 * 1024 * 1024,
            part_size: 16 * 1024 * 1024,
            max_parts_in_flight: 4,
            body_idle_timeout_secs: 60,
        }
    }
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
    /// Fraction of quota at which eviction starts.
    #[serde(default = "high_watermark")]
    pub high_watermark: f64,
    /// Fraction of quota eviction frees down to.
    #[serde(default = "low_watermark")]
    pub low_watermark: f64,
    /// Seconds between GC timer ticks (each tick is a cheap counter check).
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
    /// Listen address.
    #[serde(default = "pusher_listen")]
    pub listen: String,
    /// SQLite path. The Pusher creates and migrates it; the Puller waits.
    pub db_path: String,
    /// Blob storage.
    pub s3: S3Config,
    /// Store prefix used in signed fingerprints.
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
    /// Unix socket for garret-admin, mode 0600 under the Pusher's user. Absent
    /// means no admin surface.
    pub admin_socket: Option<String>,
    /// Internal metrics listener (spec 08).
    #[serde(default = "pusher_metrics_listen")]
    pub metrics_listen: String,
}

impl ServiceConfig for PusherConfig {
    /// Rejects values that would wedge or silently disable the service
    /// (a part size S3 refuses, a zero permit count, a watermark that never
    /// triggers), naming the key so the operator can fix it at startup.
    fn validate(&self) -> Result<()> {
        let l = &self.limits;
        if !(MIN_PART_SIZE..=MAX_PART_SIZE).contains(&l.part_size) {
            bail!(
                "limits.part_size must be between {} MiB and {} GiB (S3's part size range), got {}",
                MIN_PART_SIZE >> 20,
                MAX_PART_SIZE >> 30,
                l.part_size
            );
        }
        if l.max_in_flight_bytes < l.part_size as u64 {
            bail!(
                "limits.max_in_flight_bytes ({}) must be at least limits.part_size ({})",
                l.max_in_flight_bytes,
                l.part_size
            );
        }
        if l.max_concurrent_uploads == 0 {
            bail!("limits.max_concurrent_uploads must be at least 1");
        }
        if l.max_parts_in_flight == 0 {
            bail!("limits.max_parts_in_flight must be at least 1");
        }
        self.s3.validate()?;
        if l.body_idle_timeout_secs == 0 {
            bail!("limits.body_idle_timeout_secs must be at least 1");
        }
        if let Some(gc) = &self.gc {
            // A zero quota puts every Blob over the high watermark, so GC would
            // evict the whole cache.
            if gc.quota_bytes == 0 {
                bail!("gc.quota_bytes must be at least 1 (omit [gc] to disable the quota)");
            }
            if gc.interval_secs == 0 {
                bail!("gc.interval_secs must be at least 1");
            }
            let in_range = |w: f64| w > 0.0 && w <= 1.0;
            if !in_range(gc.high_watermark) {
                bail!(
                    "gc.high_watermark must be in (0, 1], got {}",
                    gc.high_watermark
                );
            }
            if !in_range(gc.low_watermark) {
                bail!(
                    "gc.low_watermark must be in (0, 1], got {}",
                    gc.low_watermark
                );
            }
            if gc.low_watermark >= gc.high_watermark {
                bail!(
                    "gc.low_watermark ({}) must be below gc.high_watermark ({})",
                    gc.low_watermark,
                    gc.high_watermark
                );
            }
        }
        if self.oidc.is_empty() {
            bail!(
                "oidc: at least one issuer is required; garret has no auth-disable mode (spec 04)"
            );
        }
        for issuer in &self.oidc {
            issuer.validate().context("oidc")?;
        }
        Ok(())
    }

    fn listen(&self) -> &str {
        &self.listen
    }

    fn metrics_listen(&self) -> &str {
        &self.metrics_listen
    }
}

/// The Puller's config file (the public substituter frontend; read-only).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullerConfig {
    /// Listen address.
    #[serde(default = "puller_listen")]
    pub listen: String,
    /// SQLite path — the same file the Pusher writes.
    pub db_path: String,
    /// Blob storage, used only to presign GETs (ADR-0005).
    pub s3: S3Config,
    /// Store prefix; advertised in `nix-cache-info`.
    #[serde(default = "store_dir")]
    pub store_dir: String,
    /// Lifetime of presigned NAR URLs.
    #[serde(default = "presign_ttl")]
    pub presign_ttl_secs: u64,
    /// Internal metrics listener (spec 08).
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
    /// Budget for a pull-path database read before the request degrades to a
    /// miss (ticket 25). Measured p99 is ~1.6 ms, so a trip means something
    /// is genuinely wrong.
    #[serde(default = "db_read_budget")]
    pub db_read_budget_ms: u64,
    /// Budget for presigning a NAR redirect before the request degrades to a
    /// miss (ticket 25). Measured p99 is ~2.4 ms.
    #[serde(default = "presign_budget")]
    pub presign_budget_ms: u64,
}

impl ServiceConfig for PullerConfig {
    /// Rejects values that would break every pull (a zero or over-long presign
    /// lifetime, a zero budget that degrades every request to a miss) naming
    /// the key, at startup.
    fn validate(&self) -> Result<()> {
        self.s3.validate()?;
        // 7 days is the SigV4 presigned-URL maximum.
        if !(1..=604_800).contains(&self.presign_ttl_secs) {
            bail!(
                "presign_ttl_secs must be between 1 and 604800, got {}",
                self.presign_ttl_secs
            );
        }
        if self.db_read_budget_ms == 0 {
            bail!("db_read_budget_ms must be at least 1");
        }
        if self.presign_budget_ms == 0 {
            bail!("presign_budget_ms must be at least 1");
        }
        if self.bump_debounce_secs < 0 {
            bail!("bump_debounce_secs must not be negative");
        }
        if self.db_wait_timeout_secs == 0 {
            bail!("db_wait_timeout_secs must be at least 1");
        }
        if let Some(issuer) = &self.browse_oidc {
            issuer.validate().context("browse_oidc")?;
        }
        Ok(())
    }

    fn listen(&self) -> &str {
        &self.listen
    }

    fn metrics_listen(&self) -> &str {
        &self.metrics_listen
    }
}

fn db_wait_timeout() -> u64 {
    300
}

fn db_read_budget() -> u64 {
    250
}

fn presign_budget() -> u64 {
    250
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

/// A service's config file: what [`load`] parses and validates, and what
/// `runtime::start` binds.
pub trait ServiceConfig: serde::de::DeserializeOwned {
    /// Rejects values that would break the service, naming the key.
    fn validate(&self) -> Result<()>;
    /// Public listen address.
    fn listen(&self) -> &str;
    /// Internal metrics listen address (spec 08).
    fn metrics_listen(&self) -> &str;
}

/// Parses and validates a TOML config file. Unknown or misplaced keys are
/// errors (`deny_unknown_fields`), so typos fail loudly instead of doing
/// nothing.
pub fn load<T: ServiceConfig>(path: &str) -> Result<T> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {path}"))?;
    let config: T = toml::from_str(&text).with_context(|| format!("parsing config {path}"))?;
    config.validate()?;
    Ok(config)
}

#[cfg(test)]
mod validate_tests {
    use super::*;

    fn cfg() -> PusherConfig {
        toml::from_str(
            r#"
            db_path = "/tmp/x.db"
            signing_key_files = ["/tmp/k"]
            [s3]
            bucket = "b"
            [[oidc]]
            issuer = "https://i"
            audience = "a"
            [gc]
            quota_bytes = 1000
        "#,
        )
        .unwrap()
    }

    /// The defaults validate, and `edit` alone makes them fail, naming `key`.
    fn rejects(edit: impl FnOnce(&mut PusherConfig), key: &str) {
        let mut c = cfg();
        c.validate().unwrap();
        edit(&mut c);
        let e = c.validate().unwrap_err();
        assert!(format!("{e:#}").contains(key), "{e:#}");
    }

    #[test]
    fn part_size_stays_within_s3_limits() {
        const MIB: usize = 1024 * 1024;
        let mut c = cfg();
        c.limits.part_size = 5 * MIB;
        c.limits.max_in_flight_bytes = (5 * MIB) as u64;
        c.validate().unwrap();

        rejects(|c| c.limits.part_size = 5 * MIB - 1, "part_size");
        rejects(|c| c.limits.part_size = 5 * 1024 * MIB + 1, "part_size");
    }

    #[test]
    fn attempt_timeout_must_fit_inside_the_overall_one() {
        let mut c = cfg();
        c.s3.operation_attempt_timeout_secs = Some(c.s3.operation_timeout_secs - 1);
        c.validate().unwrap();
        rejects(
            |c| c.s3.operation_attempt_timeout_secs = Some(0),
            "attempt_timeout",
        );
        rejects(
            |c| c.s3.operation_attempt_timeout_secs = Some(c.s3.operation_timeout_secs),
            "attempt_timeout",
        );
    }

    /// A zero quota or a watermark outside (0, 1] would evict the whole
    /// cache or never evict at all.
    #[test]
    fn gc_quota_and_watermarks_are_sane() {
        rejects(|c| c.gc.as_mut().unwrap().quota_bytes = 0, "quota_bytes");
        rejects(
            |c| c.gc.as_mut().unwrap().high_watermark = 0.0,
            "high_watermark",
        );
        rejects(
            |c| c.gc.as_mut().unwrap().high_watermark = 1.01,
            "high_watermark",
        );
        rejects(
            |c| c.gc.as_mut().unwrap().low_watermark = f64::NAN,
            "low_watermark",
        );
        rejects(
            |c| c.gc.as_mut().unwrap().low_watermark = c.gc.as_ref().unwrap().high_watermark,
            "low_watermark",
        );
        let mut c = cfg();
        c.gc.as_mut().unwrap().high_watermark = 1.0;
        c.validate().unwrap();
        // No [gc] section means no GC rules apply.
        c.gc = None;
        c.validate().unwrap();
    }

    #[test]
    fn s3_credentials_are_all_or_nothing() {
        rejects(
            |c| c.s3.access_key_id = Some("id".into()),
            "s3.access_key_id and s3.secret_access_key",
        );
        rejects(
            |c| c.s3.secret_access_key = Some("secret".into()),
            "s3.access_key_id and s3.secret_access_key",
        );
        let mut c = cfg();
        c.s3.access_key_id = Some("id".into());
        c.s3.secret_access_key = Some("secret".into());
        c.validate().unwrap();
    }

    /// Issuer mistakes fail at startup, not at the first token (spec 04).
    #[test]
    fn issuers_are_validated_at_load() {
        rejects(|c| c.oidc.clear(), "no auth-disable mode");
        rejects(|c| c.oidc[0].audience.clear(), "audience");
        rejects(
            |c| c.oidc[0].issuer = "http://id.example".into(),
            "must be https",
        );
        rejects(
            |c| c.oidc[0].jwks_url = Some("http://id.example/jwks".into()),
            "must be https",
        );
        rejects(
            |c| c.oidc[0].issuer = GITHUB_ISSUER.into(),
            "github_owner_id",
        );

        let mut c = cfg();
        c.oidc[0].issuer = GITHUB_ISSUER.into();
        c.oidc[0].repository_ids = vec!["42".into()];
        c.validate().unwrap();
        c.oidc[0].repository_ids.clear();
        c.oidc[0].github_owner_id = Some("1234".into());
        c.validate().unwrap();

        // The dev overrides: a JWKS file under any issuer URL, or http on a
        // loopback host.
        let mut c = cfg();
        c.oidc[0].issuer = "http://dev.garret.test".into();
        c.oidc[0].jwks_url = Some("/etc/garret/jwks.json".into());
        c.validate().unwrap();
        c.oidc[0].jwks_url = Some("http://127.0.0.1:8090/jwks".into());
        c.validate().unwrap();
    }

    #[test]
    fn remote_jwks_urls_must_be_https_or_loopback() {
        assert!(check_remote_url("https://id.example/jwks").is_ok());
        assert!(check_remote_url("http://127.0.0.1:8080/jwks").is_ok());
        assert!(check_remote_url("http://localhost/jwks").is_ok());
        assert!(check_remote_url("http://[::1]/jwks").is_ok());
        assert!(check_remote_url("http://id.example/jwks").is_err());
        assert!(check_remote_url("http://127.0.0.1.evil.example/jwks").is_err());
        assert!(check_remote_url("ftp://id.example/jwks").is_err());
    }
}

#[cfg(test)]
mod strictness_tests {
    use super::*;

    /// A misplaced or mistyped key must fail loudly, not be ignored.
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

#[cfg(test)]
mod puller_validate_tests {
    use super::*;

    fn cfg() -> PullerConfig {
        toml::from_str(
            r#"
            db_path = "/tmp/x.db"
            [s3]
            bucket = "b"
        "#,
        )
        .unwrap()
    }

    /// The defaults validate, and `edit` alone makes them fail, naming `key`.
    fn rejects(edit: impl FnOnce(&mut PullerConfig), key: &str) {
        let mut c = cfg();
        c.validate().unwrap();
        edit(&mut c);
        let e = c.validate().unwrap_err();
        assert!(format!("{e:#}").contains(key), "{e:#}");
    }

    /// 7 days is the SigV4 presigned-URL maximum.
    #[test]
    fn presign_ttl_stays_within_the_sigv4_maximum() {
        rejects(|c| c.presign_ttl_secs = 0, "presign_ttl_secs");
        rejects(|c| c.presign_ttl_secs = 604_801, "presign_ttl_secs");
        let mut c = cfg();
        c.presign_ttl_secs = 604_800;
        c.validate().unwrap();
    }

    #[test]
    fn s3_settings_are_validated_for_the_puller_too() {
        rejects(
            |c| c.s3.operation_timeout_secs = 0,
            "operation_timeout_secs",
        );
    }

    #[test]
    fn browse_oidc_is_validated_at_load() {
        let issuer = |c: &mut PullerConfig| {
            c.browse_oidc = Some(crate::testing::dev_issuer("/etc/garret/jwks.json"));
        };
        let mut c = cfg();
        issuer(&mut c);
        c.validate().unwrap();
        rejects(
            |c| {
                issuer(c);
                c.browse_oidc.as_mut().unwrap().audience.clear();
            },
            "browse_oidc",
        );
    }
}
