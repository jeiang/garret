//! Multi-issuer OIDC validation (spec 04-auth, ADR-0003). Garret issues no
//! tokens; it validates them against each configured issuer's JWKS.
//!
//! Hand-rolled rather than `jwt-authorizer` only because that crate is pinned
//! to axum 0.7 and ADR-0004 pins us to 0.8.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use axum::{
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, jwk::JwkSet};
use parking_lot::{Mutex, RwLock};
use serde::Deserialize;

use crate::config::{IssuerConfig, check_remote_url};

/// Clock skew allowance, per spec 04-auth.
const LEEWAY_SECS: u64 = 60;
/// Floor between the end of one JWKS fetch attempt and the start of the
/// next, successful or not, so neither an unknown-kid flood nor a down issuer
/// turns into a stream of fetches.
const MIN_REFRESH: Duration = Duration::from_secs(10);
/// Cached keys are refetched once this old, so a key the issuer has removed
/// (say, after a compromise) stops being trusted even if no unknown kid ever
/// forces a refresh.
const KEY_TTL: Duration = Duration::from_secs(3600);
/// Bound how long a slow or hung issuer can hold a fetch attempt.
const JWKS_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const JWKS_TIMEOUT: Duration = Duration::from_secs(10);

/// RS256-pinned validation for one issuer: its `audience` plus every
/// `extra_audiences` entry is accepted.
fn validation_for(cfg: &IssuerConfig) -> Validation {
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&[&cfg.issuer]);
    let audiences: Vec<&str> = std::iter::once(cfg.audience.as_str())
        .chain(cfg.extra_audiences.iter().map(String::as_str))
        .collect();
    validation.set_audience(&audiences);
    validation.leeway = LEEWAY_SECS;
    // `aud` and `iss` are only checked when present unless required, so a
    // token omitting them would otherwise skip the check.
    validation.set_required_spec_claims(&["exp", "aud", "iss"]);
    validation.validate_nbf = true;
    validation
}

#[derive(Debug, Deserialize)]
struct Claims {
    iss: String,
    sub: String,
    #[serde(default)]
    groups: Vec<String>,
    /// GitHub Actions: whether the triggering ref is protected.
    #[serde(default)]
    ref_protected: Option<String>,
    /// GitHub Actions: the immutable owner id (names are renameable, spec 04).
    #[serde(default)]
    repository_owner_id: Option<String>,
    #[serde(rename = "ref", default)]
    git_ref: Option<String>,
    /// GitHub Actions: the immutable repository id.
    #[serde(default)]
    repository_id: Option<String>,
    /// GitHub Actions: the triggering event, e.g. `push`.
    #[serde(default)]
    event_name: Option<String>,
    /// GitHub Actions: the workflow file the job runs (the called file, for a
    /// reusable workflow).
    #[serde(default)]
    job_workflow_ref: Option<String>,
    /// GitHub Actions: `owner/repo` the job runs in (the caller, for a
    /// reusable workflow).
    #[serde(default)]
    repository: Option<String>,
}

/// One JWKS fetch attempt, shared by every caller waiting on it. It runs as
/// its own task, so a caller that disconnects mid-fetch can't abort it.
type Attempt = Shared<BoxFuture<'static, Result<(), Arc<anyhow::Error>>>>;

struct Issuer {
    cfg: IssuerConfig,
    keys: RwLock<KeyCache>,
    refresh: Mutex<RefreshState>,
}

#[derive(Default)]
struct KeyCache {
    by_kid: HashMap<String, DecodingKey>,
    /// Last successful fetch + `KEY_TTL`; `None` until the first one.
    expires_at: Option<Instant>,
}

#[derive(Default)]
struct RefreshState {
    /// Single-flight: the attempt in progress, if any.
    in_flight: Option<Attempt>,
    /// Last completed attempt + `MIN_REFRESH`.
    next_attempt: Option<Instant>,
}

/// Validates bearer tokens against a fixed set of trusted issuers, caching
/// each issuer's JWKS in memory and refetching on key rotation.
pub struct Authenticator {
    issuers: Vec<Arc<Issuer>>,
    http: reqwest::Client,
}

/// The authenticated caller as `<issuer>#<sub>`; stored as `pushed_by` for audit.
#[derive(Debug, Clone)]
pub struct Subject(pub String);

impl Authenticator {
    /// Trusts `issuers` as given: [`IssuerConfig::validate`] is the gate,
    /// run when the config loads.
    pub fn new(issuers: Vec<IssuerConfig>) -> Result<Self> {
        Ok(Self {
            issuers: issuers
                .into_iter()
                .map(|cfg| {
                    Arc::new(Issuer {
                        cfg,
                        keys: RwLock::default(),
                        refresh: Mutex::default(),
                    })
                })
                .collect(),
            http: reqwest::Client::builder()
                .connect_timeout(JWKS_CONNECT_TIMEOUT)
                .timeout(JWKS_TIMEOUT)
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        })
    }

    /// Validates a bearer token and returns its subject, or the [`Refusal`]
    /// naming only what the caller may safely learn.
    async fn authenticate(&self, token: &str) -> Result<Subject, Refusal> {
        let (issuer, verdict) = self.check(token).await;
        let outcome = match &verdict {
            Ok(_) => Outcome::Accepted,
            Err(refusal) => refusal.outcome,
        };
        metrics::counter!(
            "garret_auth_validations_total",
            "issuer" => issuer, "outcome" => outcome.as_str(),
        )
        .increment(1);
        verdict
    }

    /// The verdict and the issuer label: the configured issuer's URL, never
    /// the token's own `iss`, which anyone can set; labels must stay bounded
    /// (spec 08).
    async fn check(&self, token: &str) -> (String, Result<Subject, Refusal>) {
        match self.route(token) {
            Ok(issuer) => (issuer.cfg.issuer.clone(), self.verify(issuer, token).await),
            Err(refusal) => ("unknown".to_owned(), Err(refusal)),
        }
    }

    /// Unverified read of `iss` picks which issuer to verify against. It
    /// decides routing only; every claim is re-read after verification.
    fn route(&self, token: &str) -> Result<&Arc<Issuer>, Refusal> {
        let issuer_name =
            unverified_issuer(token).map_err(|e| Refusal::new(Outcome::Malformed, e))?;
        self.issuers
            .iter()
            .find(|i| i.cfg.issuer == issuer_name)
            .ok_or_else(|| Refusal::new(Outcome::UntrustedIssuer, anyhow!("untrusted issuer")))
    }

    async fn verify(&self, issuer: &Arc<Issuer>, token: &str) -> Result<Subject, Refusal> {
        let kid = jsonwebtoken::decode_header(token)
            .context("malformed token header")
            .and_then(|h| h.kid.ok_or_else(|| anyhow!("token has no kid")))
            .map_err(|e| Refusal::new(Outcome::Malformed, e))?;

        let (key, expired) = issuer.cached_key(&kid);
        let key = match key {
            Some(key) => {
                if expired {
                    // Validate against the cached set while a background
                    // refetch runs: a removed key drops out when it lands,
                    // and a slow issuer stalls no one.
                    issuer.refresh(&self.http);
                }
                key
            }
            None => {
                // Unknown kid means rotation: wait for the shared,
                // rate-limited refetch.
                if let Some(attempt) = issuer.refresh(&self.http) {
                    attempt
                        .await
                        .map_err(|e| Refusal::new(Outcome::JwksUnavailable, anyhow!("{e:#}")))?;
                }
                issuer.cached_key(&kid).0.ok_or_else(|| {
                    Refusal::new(Outcome::UnknownKey, anyhow!("no signing key for kid {kid}"))
                })?
            }
        };

        let validation = validation_for(&issuer.cfg);
        let claims = jsonwebtoken::decode::<Claims>(token, &key, &validation)
            .context("token failed validation")
            .map_err(|e| Refusal::new(Outcome::Invalid, e))?
            .claims;

        authorize(&issuer.cfg, &claims).map_err(|(gate, e)| {
            // The gate name is a fixed tag; the claim value goes in the log only.
            metrics::counter!(
                "garret_auth_unauthorized_total",
                "issuer" => issuer.cfg.issuer.clone(), "claim" => gate.as_str(),
            )
            .increment(1);
            Refusal::new(Outcome::Unauthorized(gate), e)
        })?;
        Ok(Subject(format!("{}#{}", claims.iss, claims.sub)))
    }
}

/// Why [`bearer`] refused a request.
#[derive(Debug)]
pub enum Rejection {
    /// No `Authorization: Bearer` header.
    Missing,
    /// A token that failed verification; some causes are transient (a JWKS
    /// outage), so the client may renew or retry.
    Invalid(String),
    /// A verified token an issuer policy gate refused: the identity is known
    /// and stays refused.
    Refused(String),
}

impl Rejection {
    /// Why the token was refused, escaped and bounded for the log (spec 08);
    /// `None` when no token was sent.
    pub fn reason(&self) -> Option<&str> {
        match self {
            Rejection::Missing => None,
            Rejection::Invalid(reason) | Rejection::Refused(reason) => Some(reason),
        }
    }

    /// `403` for [`Rejection::Refused`], so clients neither renew nor retry;
    /// `401` with `WWW-Authenticate: Bearer` for everything else (spec 04).
    /// `body` is the service's own error format.
    pub fn respond(&self, body: impl IntoResponse) -> Response {
        match self {
            Rejection::Refused(_) => (StatusCode::FORBIDDEN, body).into_response(),
            Rejection::Missing | Rejection::Invalid(_) => (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, "Bearer")],
                body,
            )
                .into_response(),
        }
    }
}

/// Authenticates a request by its `Authorization: Bearer` header.
pub async fn bearer(auth: &Authenticator, headers: &HeaderMap) -> Result<Subject, Rejection> {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(Rejection::Missing)?;
    auth.authenticate(token)
        .await
        .map_err(|refusal| match refusal.outcome {
            Outcome::Unauthorized(_) => Rejection::Refused(refusal.to_string()),
            _ => Rejection::Invalid(refusal.to_string()),
        })
}

/// The `outcome` label of `garret_auth_validations_total` (spec 08).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Accepted,
    Malformed,
    UntrustedIssuer,
    UnknownKey,
    JwksUnavailable,
    Invalid,
    /// Verified, but refused by an issuer policy gate.
    Unauthorized(Gate),
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Outcome::Accepted => "accepted",
            Outcome::Malformed => "malformed",
            Outcome::UntrustedIssuer => "untrusted_issuer",
            Outcome::UnknownKey => "unknown_key",
            Outcome::JwksUnavailable => "jwks_unavailable",
            Outcome::Invalid => "invalid",
            Outcome::Unauthorized(_) => "unauthorized",
        }
    }
}

/// The failed policy gate: the `claim` label of
/// `garret_auth_unauthorized_total` (spec 08).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Gate {
    Owner,
    Ref,
    RefProtected,
    RepositoryId,
    EventName,
    JobWorkflowRef,
    Group,
}

impl Gate {
    fn as_str(self) -> &'static str {
        match self {
            Gate::Owner => "owner",
            Gate::Ref => "ref",
            Gate::RefProtected => "ref_protected",
            Gate::RepositoryId => "repository_id",
            Gate::EventName => "event_name",
            Gate::JobWorkflowRef => "job_workflow_ref",
            Gate::Group => "group",
        }
    }
}

/// A refused token: its outcome label and the reason for the log.
#[derive(Debug)]
struct Refusal {
    outcome: Outcome,
    error: anyhow::Error,
}

impl Refusal {
    fn new(outcome: Outcome, error: anyhow::Error) -> Self {
        Self { outcome, error }
    }
}

/// The reason, escaped and bounded for the log via [`loggable`].
impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&loggable(&self.error))
    }
}

/// Renders an authentication error for the log. The chain can carry
/// attacker-controlled token text (the kid, header fields echoed by parse
/// errors), so it is escaped, leaving no way to forge a log line, and
/// bounded.
fn loggable(e: &anyhow::Error) -> String {
    const MAX_CHARS: usize = 256;
    let rendered = format!("{e:#}");
    let mut escaped = rendered.escape_debug();
    let mut out: String = escaped.by_ref().take(MAX_CHARS).collect();
    if escaped.next().is_some() {
        out.push('…');
    }
    out
}

async fn fetch_keys(
    http: &reqwest::Client,
    cfg: &IssuerConfig,
) -> Result<HashMap<String, DecodingKey>> {
    let url = jwks_url(http, cfg).await?;
    let is_remote = url.starts_with("http://") || url.starts_with("https://");
    let body = if is_remote {
        check_remote_url(&url)?;
        http.get(&url)
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .with_context(|| format!("fetching JWKS from {url}"))?
            .text()
            .await?
    } else if cfg.jwks_url.is_some() {
        // A local path is the sanctioned dev-issuer override (spec 04), and
        // only as the configured `jwks_url`: a discovery document must never
        // point us at a file.
        std::fs::read_to_string(&url).with_context(|| format!("reading JWKS file {url}"))?
    } else {
        bail!("discovery returned a non-http jwks_uri");
    };

    let set: JwkSet = serde_json::from_str(&body).context("malformed JWKS")?;
    Ok(set
        .keys
        .iter()
        .filter_map(|jwk| {
            let kid = jwk.common.key_id.clone()?;
            match DecodingKey::from_jwk(jwk) {
                Ok(key) => Some((kid, key)),
                Err(e) => {
                    tracing::warn!("skipping unusable JWK {kid} from {url}: {e}");
                    None
                }
            }
        })
        .collect())
}

async fn jwks_url(http: &reqwest::Client, cfg: &IssuerConfig) -> Result<String> {
    if let Some(url) = &cfg.jwks_url {
        return Ok(url.clone());
    }
    #[derive(Deserialize)]
    struct Discovery {
        jwks_uri: String,
    }
    let url = format!(
        "{}/.well-known/openid-configuration",
        cfg.issuer.trim_end_matches('/')
    );
    // The issuer URL passed `check_remote_url` when the config loaded.
    Ok(http
        .get(&url)
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .with_context(|| format!("OIDC discovery at {url}"))?
        .json::<Discovery>()
        .await?
        .jwks_uri)
}

impl Issuer {
    /// The key for `kid`, if cached, and whether the cached set has expired.
    fn cached_key(&self, kid: &str) -> (Option<DecodingKey>, bool) {
        let cache = self.keys.read();
        let expired = cache.expires_at.is_none_or(|t| Instant::now() >= t);
        (cache.by_kid.get(kid).cloned(), expired)
    }

    /// Joins the in-flight JWKS fetch or starts one; `None` while the floor
    /// since the last completed attempt holds.
    fn refresh(self: &Arc<Self>, http: &reqwest::Client) -> Option<Attempt> {
        let mut state = self.refresh.lock();
        if let Some(attempt) = &state.in_flight {
            return Some(attempt.clone());
        }
        if state.next_attempt.is_some_and(|t| Instant::now() < t) {
            return None;
        }
        let issuer = Arc::clone(self);
        let http = http.clone();
        // The task can't finish before `in_flight` is set: it needs the
        // state lock held here to clear it.
        let task = tokio::spawn(async move {
            let label = issuer.cfg.issuer.clone();
            metrics::counter!("garret_jwks_refreshes_total", "issuer" => label.clone())
                .increment(1);
            let outcome = match fetch_keys(&http, &issuer.cfg).await {
                Ok(by_kid) => {
                    let mut cache = issuer.keys.write();
                    cache.by_kid = by_kid;
                    cache.expires_at = Some(Instant::now() + KEY_TTL);
                    Ok(())
                }
                Err(e) => {
                    metrics::counter!("garret_jwks_refresh_failures_total", "issuer" => label)
                        .increment(1);
                    tracing::warn!("JWKS refresh for {} failed: {e:#}", issuer.cfg.issuer);
                    Err(Arc::new(e))
                }
            };
            let mut state = issuer.refresh.lock();
            state.in_flight = None;
            state.next_attempt = Some(Instant::now() + MIN_REFRESH);
            outcome
        });
        let attempt = async move { task.await.unwrap_or_else(|e| Err(Arc::new(e.into()))) }
            .boxed()
            .shared();
        state.in_flight = Some(attempt.clone());
        Some(attempt)
    }
}

fn unverified_issuer(token: &str) -> Result<String> {
    #[derive(Deserialize)]
    struct OnlyIss {
        iss: String,
    }
    let payload = token
        .split('.')
        .nth(1)
        .ok_or_else(|| anyhow!("malformed token"))?;
    let bytes = base64_url(payload)?;
    Ok(serde_json::from_slice::<OnlyIss>(&bytes)
        .context("token payload has no issuer")?
        .iss)
}

fn base64_url(s: &str) -> Result<Vec<u8>> {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    Ok(URL_SAFE_NO_PAD.decode(s)?)
}

/// Authorization, which lives at the issuer wherever possible (ADR-0003):
/// Pocket ID grants by audience alone unless groups are configured; GitHub
/// grants by immutable owner id and optional ref, repository, event and
/// workflow constraints. A refusal carries the failed [`Gate`] for
/// `garret_auth_unauthorized_total`.
fn authorize(cfg: &IssuerConfig, claims: &Claims) -> Result<(), (Gate, anyhow::Error)> {
    if let Some(expected) = &cfg.github_owner_id {
        let owner = claims
            .repository_owner_id
            .as_deref()
            .ok_or_else(|| (Gate::Owner, anyhow!("token has no repository_owner_id")))?;
        if owner != expected {
            return Err((
                Gate::Owner,
                anyhow!("repository owner {owner:?} is not authorized"),
            ));
        }
    }
    allowlisted(
        &cfg.ref_patterns,
        claims.git_ref.as_deref(),
        Gate::Ref,
        ref_matches,
    )?;
    if let Some(expected) = cfg.ref_protected {
        let actual = claims
            .ref_protected
            .as_deref()
            .ok_or_else(|| (Gate::RefProtected, anyhow!("token has no ref_protected")))?;
        let expected = if expected { "true" } else { "false" };
        if actual != expected {
            return Err((
                Gate::RefProtected,
                anyhow!("ref protection status {actual:?} is not authorized"),
            ));
        }
    }
    allowlisted(
        &cfg.repository_ids,
        claims.repository_id.as_deref(),
        Gate::RepositoryId,
        |a, c| a == c,
    )?;
    allowlisted(
        &cfg.event_names,
        claims.event_name.as_deref(),
        Gate::EventName,
        |a, c| a == c,
    )?;
    allowlisted(
        &cfg.job_workflow_refs,
        claims.job_workflow_ref.as_deref(),
        Gate::JobWorkflowRef,
        ref_matches,
    )?;
    // A reusable workflow called from another repository carries the callee's
    // job_workflow_ref but the caller's repository, so the ref alone doesn't
    // prove whose run this is.
    if let (false, Some(jwr)) = (cfg.job_workflow_refs.is_empty(), &claims.job_workflow_ref) {
        let repository = claims
            .repository
            .as_deref()
            .ok_or_else(|| (Gate::JobWorkflowRef, anyhow!("token has no repository")))?;
        if jwr
            .strip_prefix(repository)
            .and_then(|rest| rest.strip_prefix('/'))
            .is_none()
        {
            return Err((
                Gate::JobWorkflowRef,
                anyhow!("job_workflow_ref {jwr:?} is not from the token's repository"),
            ));
        }
    }
    if !cfg.allowed_groups.is_empty()
        && !claims.groups.iter().any(|g| cfg.allowed_groups.contains(g))
    {
        return Err((Gate::Group, anyhow!("subject is not in an allowed group")));
    }
    Ok(())
}

/// An empty allowlist is off; a configured one fails closed on an absent
/// claim, so a token from another issuer shape can't slip past it.
fn allowlisted(
    allowed: &[String],
    claim: Option<&str>,
    gate: Gate,
    matches: impl Fn(&str, &str) -> bool,
) -> Result<(), (Gate, anyhow::Error)> {
    if allowed.is_empty() {
        return Ok(());
    }
    let name = gate.as_str();
    let value = claim.ok_or_else(|| (gate, anyhow!("token has no {name}")))?;
    if !allowed.iter().any(|a| matches(a, value)) {
        return Err((gate, anyhow!("{name} {value:?} is not authorized")));
    }
    Ok(())
}

/// Trailing-`*` globs only — enough for `refs/heads/main` and `refs/tags/*`,
/// and it cannot surprise anyone the way a regex can.
fn ref_matches(pattern: &str, git_ref: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => git_ref.starts_with(prefix),
        None => pattern == git_ref,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use crate::config::GITHUB_ISSUER;
    use crate::testing::{DEV_ISSUER, dev_issuer};

    fn github(owner: &str, refs: &[&str]) -> IssuerConfig {
        IssuerConfig {
            issuer: GITHUB_ISSUER.into(),
            jwks_url: None,
            github_owner_id: Some(owner.into()),
            ref_patterns: refs.iter().map(|r| (*r).to_owned()).collect(),
            ..dev_issuer("unused")
        }
    }

    fn claims(owner: Option<&str>, git_ref: Option<&str>, groups: &[&str]) -> Claims {
        Claims {
            iss: "https://token.actions.githubusercontent.com".into(),
            sub: "repo:me/thing:ref:refs/heads/main".into(),
            groups: groups.iter().map(|g| (*g).to_owned()).collect(),
            ref_protected: None,
            repository_owner_id: owner.map(str::to_owned),
            git_ref: git_ref.map(str::to_owned),
            repository_id: None,
            event_name: None,
            job_workflow_ref: None,
            repository: None,
        }
    }

    #[test]
    fn github_owner_must_match() {
        assert!(authorize(&github("1234", &[]), &claims(Some("1234"), None, &[])).is_ok());
        assert!(authorize(&github("1234", &[]), &claims(Some("9999"), None, &[])).is_err());
        // A token with no owner claim must not pass an owner-scoped issuer.
        assert!(authorize(&github("1234", &[]), &claims(None, None, &[])).is_err());
    }

    #[test]
    fn ref_patterns_gate_when_configured() {
        let cfg = github("1234", &["refs/heads/main", "refs/tags/*"]);
        assert!(authorize(&cfg, &claims(Some("1234"), Some("refs/heads/main"), &[])).is_ok());
        assert!(authorize(&cfg, &claims(Some("1234"), Some("refs/tags/v1"), &[])).is_ok());
        assert!(authorize(&cfg, &claims(Some("1234"), Some("refs/heads/wip"), &[])).is_err());
        // Absent ref claim must fail closed, not match the empty string.
        assert!(authorize(&cfg, &claims(Some("1234"), None, &[])).is_err());
    }

    #[test]
    fn ref_protected_gates_when_configured() {
        let mut cfg = github("1234", &["refs/heads/main"]);
        cfg.ref_protected = Some(true);

        let mut protected_main = claims(Some("1234"), Some("refs/heads/main"), &[]);
        protected_main.ref_protected = Some("true".into());
        assert!(authorize(&cfg, &protected_main).is_ok());

        let mut unprotected_main = claims(Some("1234"), Some("refs/heads/main"), &[]);
        unprotected_main.ref_protected = Some("false".into());
        assert!(authorize(&cfg, &unprotected_main).is_err());

        // An absent claim must fail closed when protection is required.
        assert!(authorize(&cfg, &claims(Some("1234"), Some("refs/heads/main"), &[])).is_err());

        // The inverse setting remains useful for explicitly unprotected refs.
        cfg.ref_protected = Some(false);
        assert!(authorize(&cfg, &unprotected_main).is_ok());
        assert!(authorize(&cfg, &protected_main).is_err());
    }

    #[test]
    fn repository_ids_gate_when_configured() {
        let mut cfg = github("1234", &[]);
        cfg.repository_ids = vec!["42".into(), "43".into()];
        let mut c = claims(Some("1234"), None, &[]);
        c.repository_id = Some("43".into());
        assert!(authorize(&cfg, &c).is_ok());
        // Another repo of the same owner is exactly what this narrows away.
        c.repository_id = Some("99".into());
        assert!(authorize(&cfg, &c).is_err());
        c.repository_id = None;
        assert!(authorize(&cfg, &c).is_err());
    }

    #[test]
    fn refusal_names_the_value_and_the_failed_claim() {
        let mut cfg = github("1234", &["refs/heads/main"]);
        cfg.repository_ids = vec!["42".into()];
        let mut c = claims(Some("1234"), Some("refs/heads/main"), &[]);
        c.repository_id = Some("99".into());
        let (gate, e) = authorize(&cfg, &c).unwrap_err();
        assert_eq!(gate, Gate::RepositoryId);
        assert!(format!("{e:#}").contains("\"99\""));

        let (gate, e) = authorize(&github("1234", &[]), &claims(Some("7"), None, &[])).unwrap_err();
        assert_eq!(
            (gate, format!("{e:#}").contains("\"7\"")),
            (Gate::Owner, true)
        );

        let mut cfg = github("1234", &[]);
        cfg.allowed_groups = vec!["builders".into()];
        let (gate, _) = authorize(&cfg, &claims(Some("1234"), None, &["others"])).unwrap_err();
        assert_eq!(gate, Gate::Group);
    }

    #[test]
    fn event_names_gate_when_configured() {
        let mut cfg = github("1234", &["refs/heads/main"]);
        cfg.event_names = vec!["push".into(), "workflow_dispatch".into()];
        let with_event = |event: Option<&str>| {
            let mut c = claims(Some("1234"), Some("refs/heads/main"), &[]);
            c.event_name = event.map(str::to_owned);
            authorize(&cfg, &c)
        };
        assert!(with_event(Some("push")).is_ok());
        assert!(with_event(Some("workflow_dispatch")).is_ok());
        // Both run on the default branch even when a stranger's PR set them
        // off, so ref_patterns alone admits them.
        assert!(with_event(Some("pull_request_target")).is_err());
        assert!(with_event(Some("workflow_run")).is_err());
        assert!(with_event(None).is_err());
    }

    #[test]
    fn job_workflow_ref_must_come_from_the_tokens_own_repository() {
        let mut cfg = github("1234", &[]);
        cfg.job_workflow_refs =
            vec!["jeiang/garret/.github/workflows/ci.yml@refs/heads/main".into()];
        let with = |repository: Option<&str>, jwr: &str| {
            let mut c = claims(Some("1234"), None, &[]);
            c.repository = repository.map(str::to_owned);
            c.job_workflow_ref = Some(jwr.to_owned());
            authorize(&cfg, &c)
        };
        let ci = "jeiang/garret/.github/workflows/ci.yml@refs/heads/main";
        assert!(with(Some("jeiang/garret"), ci).is_ok());
        // Another repo's job calling garret's reusable workflow carries
        // garret's job_workflow_ref but its own repository.
        assert!(with(Some("ripper/evil"), ci).is_err());
        assert!(with(None, ci).is_err());
    }

    #[test]
    fn ref_patterns_gate_without_an_owner_id() {
        let mut cfg = github("1234", &["refs/heads/main"]);
        cfg.github_owner_id = None;
        cfg.repository_ids = vec!["42".into()];
        let with_ref = |r: Option<&str>| {
            let mut c = claims(None, r, &[]);
            c.repository_id = Some("42".into());
            authorize(&cfg, &c)
        };
        assert!(with_ref(Some("refs/heads/main")).is_ok());
        assert!(with_ref(Some("refs/heads/wip")).is_err());
        assert!(with_ref(None).is_err());
    }

    #[tokio::test]
    async fn a_discovered_jwks_uri_is_never_read_as_a_file() {
        let app = axum::Router::new().route(
            "/.well-known/openid-configuration",
            axum::routing::get(|| async { r#"{"jwks_uri":"/etc/hostname"}"# }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let issuer = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut cfg = dev_issuer("unused");
        cfg.issuer = issuer;
        cfg.jwks_url = None;
        let http = reqwest::Client::new();
        let e = fetch_keys(&http, &cfg).await.err().unwrap();
        assert!(format!("{e:#}").contains("non-http jwks_uri"), "{e:#}");
    }

    #[test]
    fn a_token_without_aud_is_rejected() {
        let cfg = dev_issuer("unused");
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(Algorithm::HS256),
            &serde_json::json!({"iss": cfg.issuer, "sub": "ci", "exp": 4_102_444_800_u64}),
            &jsonwebtoken::EncodingKey::from_secret(b"k"),
        )
        .unwrap();
        let mut validation = validation_for(&cfg);
        validation.algorithms = vec![Algorithm::HS256];
        assert!(
            jsonwebtoken::decode::<Claims>(&token, &DecodingKey::from_secret(b"k"), &validation)
                .is_err()
        );
    }

    #[test]
    fn job_workflow_refs_gate_when_configured() {
        let mut cfg = github("1234", &[]);
        cfg.job_workflow_refs = vec![
            "me/thing/.github/workflows/ci.yml@refs/heads/main".into(),
            "me/other/.github/workflows/release.yml@refs/tags/v*".into(),
        ];
        let with_ref = |r: Option<&str>| {
            let mut c = claims(Some("1234"), None, &[]);
            c.repository = r.map(|r| r.split('/').take(2).collect::<Vec<_>>().join("/"));
            c.job_workflow_ref = r.map(str::to_owned);
            authorize(&cfg, &c)
        };
        assert!(with_ref(Some("me/thing/.github/workflows/ci.yml@refs/heads/main")).is_ok());
        assert!(
            with_ref(Some(
                "me/other/.github/workflows/release.yml@refs/tags/v1.2"
            ))
            .is_ok()
        );
        // Another workflow in an allowed repo, and a third-party reusable
        // workflow called from one, both carry the caller's other claims.
        assert!(with_ref(Some("me/thing/.github/workflows/lint.yml@refs/heads/main")).is_err());
        assert!(
            with_ref(Some(
                "them/tools/.github/workflows/build.yml@refs/heads/main"
            ))
            .is_err()
        );
        assert!(with_ref(None).is_err());
    }

    #[test]
    fn recommended_policy_reads_real_github_claim_names() {
        let mut cfg = github("31970261", &["refs/heads/main"]);
        cfg.ref_protected = Some(true);
        cfg.repository_ids = vec!["1324491067".into()];
        cfg.event_names = vec!["push".into(), "workflow_dispatch".into()];
        cfg.job_workflow_refs =
            vec!["jeiang/garret/.github/workflows/ci.yml@refs/heads/main".into()];
        // Shape of a GitHub Actions ID token payload: every claim a string.
        let payload = |event: &str| {
            serde_json::from_value::<Claims>(serde_json::json!({
                "iss": "https://token.actions.githubusercontent.com",
                "sub": "repo:jeiang/garret:ref:refs/heads/main",
                "aud": "garret",
                "ref": "refs/heads/main",
                "ref_protected": "true",
                "repository": "jeiang/garret",
                "repository_id": "1324491067",
                "repository_owner_id": "31970261",
                "event_name": event,
                "workflow_ref": "jeiang/garret/.github/workflows/ci.yml@refs/heads/main",
                "job_workflow_ref": "jeiang/garret/.github/workflows/ci.yml@refs/heads/main",
            }))
            .unwrap()
        };
        assert!(authorize(&cfg, &payload("push")).is_ok());
        assert!(authorize(&cfg, &payload("pull_request_target")).is_err());
    }

    #[test]
    fn pocket_id_grants_on_audience_alone_unless_groups_are_set() {
        let mut cfg = IssuerConfig {
            issuer: "https://id.example".into(),
            jwks_url: None,
            ..dev_issuer("unused")
        };
        assert!(authorize(&cfg, &claims(None, None, &[])).is_ok());

        cfg.allowed_groups = vec!["builders".into()];
        assert!(authorize(&cfg, &claims(None, None, &["builders"])).is_ok());
        assert!(authorize(&cfg, &claims(None, None, &["others"])).is_err());
        assert!(authorize(&cfg, &claims(None, None, &[])).is_err());
    }

    /// Decodes an HS256 token under `validation_for`'s rules (only the
    /// algorithm swapped, so no RSA key is needed).
    fn decodes_with_aud(cfg: &IssuerConfig, aud: &str) -> bool {
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(Algorithm::HS256),
            &serde_json::json!({
                "iss": cfg.issuer,
                "sub": "ci",
                "aud": aud,
                "exp": 4_102_444_800_u64,
            }),
            &jsonwebtoken::EncodingKey::from_secret(b"k"),
        )
        .unwrap();
        let mut validation = validation_for(cfg);
        validation.algorithms = vec![Algorithm::HS256];
        jsonwebtoken::decode::<Claims>(&token, &DecodingKey::from_secret(b"k"), &validation).is_ok()
    }

    #[test]
    fn extra_audiences_are_accepted_alongside_the_primary() {
        let mut cfg = dev_issuer("unused");
        cfg.extra_audiences = vec!["ci-builder".into()];
        assert!(decodes_with_aud(&cfg, "garret"));
        assert!(decodes_with_aud(&cfg, "ci-builder"));
    }

    #[test]
    fn an_unknown_audience_is_rejected_even_with_extra_audiences() {
        let mut cfg = dev_issuer("unused");
        assert!(!decodes_with_aud(&cfg, "ci-builder"));
        cfg.extra_audiences = vec!["ci-builder".into()];
        assert!(!decodes_with_aud(&cfg, "someone-else"));
    }

    #[test]
    fn issuer_is_read_from_the_payload_for_routing() {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
        let payload = URL_SAFE_NO_PAD.encode(br#"{"iss":"https://id.example"}"#);
        assert_eq!(
            unverified_issuer(&format!("header.{payload}.sig")).unwrap(),
            "https://id.example"
        );
        assert!(unverified_issuer("nope").is_err());
    }

    /// Routes to `iss` and names `kid`, with a signature nothing verifies.
    fn token_from(iss: &str, kid: &str) -> String {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
        let header = URL_SAFE_NO_PAD.encode(format!(r#"{{"alg":"RS256","kid":"{kid}"}}"#));
        let payload = URL_SAFE_NO_PAD.encode(format!(r#"{{"iss":"{iss}"}}"#));
        format!("{header}.{payload}.c2ln")
    }

    fn token_with_kid(kid: &str) -> String {
        token_from(DEV_ISSUER, kid)
    }
    /// A key the test issuer publishes. No test token is signed by it, so
    /// reaching signature validation is as far as any of them gets.
    const ROTATED_IN: &str = r#"{"keys":[{"kty":"RSA","kid":"new","n":"AQAB","e":"AQAB"}]}"#;

    /// An issuer serving `keys` with `status` after `delay`, counting fetches.
    async fn jwks_server(
        status: u16,
        keys: &'static str,
        delay: Duration,
    ) -> (String, Arc<AtomicUsize>) {
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        let app = axum::Router::new().route(
            "/jwks",
            axum::routing::get(move || async move {
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(delay).await;
                (axum::http::StatusCode::from_u16(status).unwrap(), keys)
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/jwks", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, hits)
    }

    /// Slow enough that concurrent callers overlap.
    const SLOW: Duration = Duration::from_millis(50);

    /// Waits out any in-flight attempt, e.g. one started in the background.
    async fn settle(auth: &Authenticator) {
        let pending = auth.issuers[0].refresh.lock().in_flight.clone();
        if let Some(attempt) = pending {
            let _ = attempt.await;
        }
    }

    fn preload(auth: &Authenticator, kid: &str, expires_at: Instant) {
        let mut cache = auth.issuers[0].keys.write();
        cache
            .by_kid
            .insert(kid.into(), DecodingKey::from_secret(b"k"));
        cache.expires_at = Some(expires_at);
    }

    #[tokio::test]
    async fn an_unknown_kid_flood_fetches_the_jwks_once() {
        let (url, hits) = jwks_server(200, r#"{"keys":[]}"#, SLOW).await;
        let auth = Authenticator::new(vec![dev_issuer(&url)]).unwrap();
        let token = token_with_kid("forged");
        let flood = (0..20).map(|_| auth.authenticate(&token));
        assert!(
            futures::future::join_all(flood)
                .await
                .iter()
                .all(Result::is_err)
        );
        assert!(auth.authenticate(&token).await.is_err());
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn callers_during_a_rotation_wait_for_the_one_fetch() {
        let (url, hits) = jwks_server(200, ROTATED_IN, SLOW).await;
        let auth = Authenticator::new(vec![dev_issuer(&url)]).unwrap();
        let token = token_with_kid("new");
        let burst = (0..20).map(|_| auth.authenticate(&token));
        // Every caller got the rotated-in key and failed only on the (fake)
        // signature, not on a missing key.
        for outcome in futures::future::join_all(burst).await {
            let e = outcome.unwrap_err();
            assert!(format!("{e:#}").contains("failed validation"), "{e:#}");
        }
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_caller_dropped_mid_fetch_does_not_abort_it() {
        let (url, hits) = jwks_server(200, ROTATED_IN, Duration::from_millis(200)).await;
        let auth = Authenticator::new(vec![dev_issuer(&url)]).unwrap();
        let token = token_with_kid("new");
        // Like a client that sends the token and hangs up.
        let dropped =
            tokio::time::timeout(Duration::from_millis(20), auth.authenticate(&token)).await;
        assert!(
            dropped.is_err(),
            "the first caller must be cancelled mid-fetch"
        );
        let e = auth.authenticate(&token).await.unwrap_err();
        assert!(format!("{e:#}").contains("failed validation"), "{e:#}");
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_failed_fetch_is_not_retried_within_the_floor() {
        let (url, hits) = jwks_server(500, r#"{"keys":[]}"#, SLOW).await;
        let auth = Authenticator::new(vec![dev_issuer(&url)]).unwrap();
        for _ in 0..5 {
            assert!(auth.authenticate(&token_with_kid("forged")).await.is_err());
        }
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn expired_keys_are_refetched_and_removed_keys_dropped() {
        let (url, hits) = jwks_server(200, ROTATED_IN, SLOW).await;
        let auth = Authenticator::new(vec![dev_issuer(&url)]).unwrap();

        preload(&auth, "old", Instant::now() + KEY_TTL);
        let _ = auth.authenticate(&token_with_kid("old")).await;
        settle(&auth).await;
        assert_eq!(hits.load(Ordering::SeqCst), 0, "fresh keys need no fetch");

        preload(&auth, "old", Instant::now());
        let _ = auth.authenticate(&token_with_kid("old")).await;
        settle(&auth).await;
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        // The issuer no longer publishes it, so it is no longer trusted.
        assert!(auth.issuers[0].cached_key("old").0.is_none());
    }

    #[tokio::test]
    async fn a_failed_refresh_keeps_expired_keys() {
        let (url, hits) = jwks_server(500, r#"{"keys":[]}"#, SLOW).await;
        let auth = Authenticator::new(vec![dev_issuer(&url)]).unwrap();
        preload(&auth, "old", Instant::now());
        let _ = auth.authenticate(&token_with_kid("old")).await;
        settle(&auth).await;
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert!(auth.issuers[0].cached_key("old").0.is_some());
    }

    /// Real clock, and so ~10 s: under a paused clock the virtual timeouts
    /// can fire before the connection is even made, hiding what this checks.
    #[tokio::test]
    async fn a_hung_issuer_costs_one_timeout_not_one_per_caller() {
        // Accepts connections and never answers.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/jwks", listener.local_addr().unwrap());
        let connections = Arc::new(AtomicUsize::new(0));
        let counter = connections.clone();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                held.push(socket);
            }
        });
        let auth = Authenticator::new(vec![dev_issuer(&url)]).unwrap();
        let token = token_with_kid("k");
        let started = Instant::now();
        let callers = futures::future::join_all((0..3).map(|_| auth.authenticate(&token)));
        let outcome = tokio::time::timeout(JWKS_TIMEOUT * 2, callers).await;
        let outcomes = outcome.expect("the fetch must give up on its own");
        assert!(outcomes.iter().all(Result::is_err));
        assert!(started.elapsed() <= JWKS_TIMEOUT + Duration::from_secs(1));
        assert_eq!(connections.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn refusals_are_classified_for_the_metric() {
        let (url, _) = jwks_server(200, ROTATED_IN, SLOW).await;
        let auth = Authenticator::new(vec![dev_issuer(&url)]).unwrap();
        let outcome = async |token: &str| match auth.check(token).await {
            (_, Ok(_)) => "accepted",
            (_, Err(refusal)) => refusal.outcome.as_str(),
        };
        let elsewhere = token_from("https://elsewhere.example", "new");
        assert_eq!(outcome("not-a-jwt").await, "malformed");
        assert_eq!(outcome(&elsewhere).await, "untrusted_issuer");
        assert_eq!(outcome(&token_with_kid("forged")).await, "unknown_key");
        assert_eq!(outcome(&token_with_kid("new")).await, "invalid");
        // The token's own `iss` never becomes a label.
        assert_eq!(auth.check(&elsewhere).await.0, "unknown");

        let (down, _) = jwks_server(500, ROTATED_IN, SLOW).await;
        let auth = Authenticator::new(vec![dev_issuer(&down)]).unwrap();
        let (issuer, verdict) = auth.check(&token_with_kid("new")).await;
        assert_eq!(issuer, DEV_ISSUER);
        assert_eq!(verdict.unwrap_err().outcome, Outcome::JwksUnavailable);
    }

    /// Metric label values are a public contract (spec 08): a renamed one
    /// breaks every dashboard and alert keyed on it.
    #[test]
    fn outcome_and_gate_labels_match_spec_08() {
        let outcomes = [
            (Outcome::Accepted, "accepted"),
            (Outcome::Malformed, "malformed"),
            (Outcome::UntrustedIssuer, "untrusted_issuer"),
            (Outcome::UnknownKey, "unknown_key"),
            (Outcome::JwksUnavailable, "jwks_unavailable"),
            (Outcome::Invalid, "invalid"),
            (Outcome::Unauthorized(Gate::Owner), "unauthorized"),
        ];
        for (outcome, label) in outcomes {
            assert_eq!(outcome.as_str(), label);
        }
        let gates = [
            (Gate::Owner, "owner"),
            (Gate::Ref, "ref"),
            (Gate::RefProtected, "ref_protected"),
            (Gate::RepositoryId, "repository_id"),
            (Gate::EventName, "event_name"),
            (Gate::JobWorkflowRef, "job_workflow_ref"),
            (Gate::Group, "group"),
        ];
        for (gate, label) in gates {
            assert_eq!(gate.as_str(), label);
        }
    }

    #[test]
    fn logged_errors_cannot_forge_lines_and_are_bounded() {
        let forged = anyhow!("no signing key for kid x\nINFO accepted token for admin");
        let logged = loggable(&forged);
        assert!(!logged.contains('\n'), "{logged}");
        assert!(logged.contains(r"x\nINFO"), "{logged}");

        let flood = anyhow!("no signing key for kid {}", "k".repeat(10_000));
        assert!(loggable(&flood).chars().count() <= 257);
    }

    /// Only a verified token a policy gate refuses is a 403 without
    /// `WWW-Authenticate`; a missing, malformed or unverifiable token is a 401
    /// asking for a bearer token (spec 04).
    #[tokio::test]
    async fn bearer_answers_403_only_for_a_policy_refusal() {
        let jwks = crate::testing::jwks_file();
        let mut cfg = dev_issuer(jwks.path());
        cfg.repository_ids = vec!["42".into()];
        let auth = Authenticator::new(vec![cfg]).unwrap();
        let headers = |authorization: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(header::AUTHORIZATION, authorization.parse().unwrap());
            headers
        };
        let token = |repository_id: &str| {
            let token = crate::testing::sign(&format!(
                r#"{{"iss":"{DEV_ISSUER}","sub":"ci","aud":"garret","exp":4102444800,"repository_id":"{repository_id}"}}"#
            ));
            headers(&format!("Bearer {token}"))
        };

        let subject = bearer(&auth, &token("42")).await.unwrap();
        assert_eq!(subject.0, format!("{DEV_ISSUER}#ci"));

        let refused = bearer(&auth, &token("99")).await.unwrap_err();
        assert!(matches!(refused, Rejection::Refused(_)), "{refused:?}");
        assert!(refused.reason().unwrap().contains("repository_id"));
        let res = refused.respond(());
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
        assert!(res.headers().get(header::WWW_AUTHENTICATE).is_none());

        for sent in [
            HeaderMap::new(),
            headers("Basic x"),
            headers("Bearer not-a-jwt"),
            headers(&format!("Bearer {}", token_with_kid("forged"))),
        ] {
            let rejected = bearer(&auth, &sent).await.unwrap_err();
            assert!(
                !matches!(rejected, Rejection::Refused(_)),
                "{sent:?}: {rejected:?}"
            );
            let res = rejected.respond(());
            assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
            assert_eq!(res.headers()[header::WWW_AUTHENTICATE], "Bearer");
        }
    }
}
