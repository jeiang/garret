//! Multi-issuer OIDC validation (spec 04-auth, ADR-0003). Garret issues no
//! tokens; it validates them against each configured issuer's JWKS.
//!
//! Hand-rolled rather than `jwt-authorizer` only because that crate is pinned
//! to axum 0.7 and ADR-0004 pins us to 0.8.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, jwk::JwkSet};
use serde::Deserialize;

use crate::config::IssuerConfig;

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
    /// Fails on an empty issuer list: garret has no auth-disable mode (spec 04).
    pub fn new(issuers: Vec<IssuerConfig>) -> Result<Self> {
        if issuers.is_empty() {
            bail!("no OIDC issuers configured — garret has no auth-disable mode (spec 04)");
        }
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
                .build()?,
        })
    }

    /// Validates a bearer token and returns its subject, or an error naming
    /// only what the caller may safely learn (log it via [`loggable`]).
    pub async fn authenticate(&self, token: &str) -> Result<Subject> {
        let (issuer, verdict) = self.check(token).await;
        let outcome = match &verdict {
            Ok(_) => "accepted",
            Err((outcome, _)) => *outcome,
        };
        metrics::counter!(
            "garret_auth_validations_total",
            "issuer" => issuer, "outcome" => outcome,
        )
        .increment(1);
        verdict.map_err(|(_, e)| e)
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
        let issuer_name = unverified_issuer(token).map_err(|e| ("malformed", e))?;
        self.issuers
            .iter()
            .find(|i| i.cfg.issuer == issuer_name)
            .ok_or_else(|| ("untrusted_issuer", anyhow!("untrusted issuer")))
    }

    async fn verify(&self, issuer: &Arc<Issuer>, token: &str) -> Result<Subject, Refusal> {
        let kid = jsonwebtoken::decode_header(token)
            .context("malformed token header")
            .and_then(|h| h.kid.ok_or_else(|| anyhow!("token has no kid")))
            .map_err(|e| ("malformed", e))?;

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
                        .map_err(|e| ("jwks_unavailable", anyhow!("{e:#}")))?;
                }
                issuer
                    .cached_key(&kid)
                    .0
                    .ok_or_else(|| ("unknown_key", anyhow!("no signing key for kid {kid}")))?
            }
        };

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[&issuer.cfg.issuer]);
        validation.set_audience(&[&issuer.cfg.audience]);
        validation.leeway = LEEWAY_SECS;
        validation.validate_nbf = true;
        let claims = jsonwebtoken::decode::<Claims>(token, &key, &validation)
            .context("token failed validation")
            .map_err(|e| ("invalid", e))?
            .claims;

        authorize(&issuer.cfg, &claims).map_err(|e| ("unauthorized", e))?;
        Ok(Subject(format!("{}#{}", claims.iss, claims.sub)))
    }
}

/// A refused token: the bounded `outcome` label for
/// `garret_auth_validations_total`, and the reason for the log.
type Refusal = (&'static str, anyhow::Error);

/// Renders an authentication error for the log. The chain can carry
/// attacker-controlled token text (the kid, header fields echoed by parse
/// errors), so it is escaped, leaving no way to forge a log line, and
/// bounded.
pub fn loggable(e: &anyhow::Error) -> String {
    const MAX_CHARS: usize = 256;
    let rendered = format!("{e:#}");
    let mut escaped = rendered.escape_debug();
    let mut out: String = escaped.by_ref().take(MAX_CHARS).collect();
    if escaped.next().is_some() {
        out.push('…');
    }
    out
}
/// Fetches and parses an issuer's current key set.
async fn fetch_keys(
    http: &reqwest::Client,
    cfg: &IssuerConfig,
) -> Result<HashMap<String, DecodingKey>> {
    let url = jwks_url(http, cfg).await?;
    // A local path is the sanctioned dev-issuer override (spec 04): test
    // keys on disk, never an auth-disable flag.
    let body = if url.starts_with("http") {
        http.get(&url)
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .with_context(|| format!("fetching JWKS from {url}"))?
            .text()
            .await?
    } else {
        std::fs::read_to_string(&url).with_context(|| format!("reading JWKS file {url}"))?
    };

    let set: JwkSet = serde_json::from_str(&body).context("malformed JWKS")?;
    Ok(set
        .keys
        .iter()
        .filter_map(|jwk| {
            let kid = jwk.common.key_id.clone()?;
            DecodingKey::from_jwk(jwk).ok().map(|k| (kid, k))
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
        let cache = self.keys.read().unwrap();
        let expired = cache.expires_at.is_none_or(|t| Instant::now() >= t);
        (cache.by_kid.get(kid).cloned(), expired)
    }

    /// Joins the in-flight JWKS fetch or starts one; `None` while the floor
    /// since the last completed attempt holds.
    fn refresh(self: &Arc<Self>, http: &reqwest::Client) -> Option<Attempt> {
        let mut state = self.refresh.lock().unwrap();
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
                    let mut cache = issuer.keys.write().unwrap();
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
            let mut state = issuer.refresh.lock().unwrap();
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
/// workflow constraints.
fn authorize(cfg: &IssuerConfig, claims: &Claims) -> Result<()> {
    if let Some(expected) = &cfg.github_owner_id {
        let owner = claims
            .repository_owner_id
            .as_deref()
            .ok_or_else(|| anyhow!("token has no repository_owner_id"))?;
        if owner != expected {
            bail!("repository owner is not authorized");
        }
        if !cfg.ref_patterns.is_empty() {
            let git_ref = claims.git_ref.as_deref().unwrap_or_default();
            if !cfg.ref_patterns.iter().any(|p| ref_matches(p, git_ref)) {
                bail!("ref {git_ref} is not authorized");
            }
        }
    }
    if let Some(expected) = cfg.ref_protected {
        let actual = claims
            .ref_protected
            .as_deref()
            .ok_or_else(|| anyhow!("token has no ref_protected"))?;
        let expected = if expected { "true" } else { "false" };
        if actual != expected {
            bail!("ref protection status is not authorized");
        }
    }
    allowlisted(
        &cfg.repository_ids,
        claims.repository_id.as_deref(),
        "repository_id",
        |a, c| a == c,
    )?;
    allowlisted(
        &cfg.event_names,
        claims.event_name.as_deref(),
        "event_name",
        |a, c| a == c,
    )?;
    allowlisted(
        &cfg.job_workflow_refs,
        claims.job_workflow_ref.as_deref(),
        "job_workflow_ref",
        ref_matches,
    )?;
    if !cfg.allowed_groups.is_empty()
        && !claims.groups.iter().any(|g| cfg.allowed_groups.contains(g))
    {
        bail!("subject is not in an allowed group");
    }
    Ok(())
}

/// An empty allowlist is off; a configured one fails closed on an absent
/// claim, so a token from another issuer shape can't slip past it.
fn allowlisted(
    allowed: &[String],
    claim: Option<&str>,
    name: &str,
    matches: impl Fn(&str, &str) -> bool,
) -> Result<()> {
    if allowed.is_empty() {
        return Ok(());
    }
    let value = claim.ok_or_else(|| anyhow!("token has no {name}"))?;
    if !allowed.iter().any(|a| matches(a, value)) {
        bail!("{name} is not authorized");
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

    fn github(owner: &str, refs: &[&str]) -> IssuerConfig {
        IssuerConfig {
            issuer: "https://token.actions.githubusercontent.com".into(),
            audience: "garret".into(),
            client_id: None,
            jwks_url: None,
            github_owner_id: Some(owner.into()),
            ref_patterns: refs.iter().map(|r| (*r).to_owned()).collect(),
            ref_protected: None,
            repository_ids: vec![],
            event_names: vec![],
            job_workflow_refs: vec![],
            allowed_groups: vec![],
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
    fn job_workflow_refs_gate_when_configured() {
        let mut cfg = github("1234", &[]);
        cfg.job_workflow_refs = vec![
            "me/thing/.github/workflows/ci.yml@refs/heads/main".into(),
            "me/other/.github/workflows/release.yml@refs/tags/v*".into(),
        ];
        let with_ref = |r: Option<&str>| {
            let mut c = claims(Some("1234"), None, &[]);
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
            audience: "garret".into(),
            client_id: None,
            jwks_url: None,
            github_owner_id: None,
            ref_patterns: vec![],
            ref_protected: None,
            repository_ids: vec![],
            event_names: vec![],
            job_workflow_refs: vec![],
            allowed_groups: vec![],
        };
        assert!(authorize(&cfg, &claims(None, None, &[])).is_ok());

        cfg.allowed_groups = vec!["builders".into()];
        assert!(authorize(&cfg, &claims(None, None, &["builders"])).is_ok());
        assert!(authorize(&cfg, &claims(None, None, &["others"])).is_err());
        assert!(authorize(&cfg, &claims(None, None, &[])).is_err());
    }

    #[test]
    fn an_authenticator_with_no_issuers_is_refused() {
        assert!(Authenticator::new(vec![]).is_err());
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

    const DEV_ISSUER: &str = "https://issuer.example";

    fn dev_issuer(jwks_url: &str) -> IssuerConfig {
        IssuerConfig {
            issuer: DEV_ISSUER.into(),
            audience: "garret".into(),
            client_id: None,
            jwks_url: Some(jwks_url.into()),
            github_owner_id: None,
            ref_patterns: vec![],
            ref_protected: None,
            repository_ids: vec![],
            event_names: vec![],
            job_workflow_refs: vec![],
            allowed_groups: vec![],
        }
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
        let pending = auth.issuers[0].refresh.lock().unwrap().in_flight.clone();
        if let Some(attempt) = pending {
            let _ = attempt.await;
        }
    }

    fn preload(auth: &Authenticator, kid: &str, expires_at: Instant) {
        let mut cache = auth.issuers[0].keys.write().unwrap();
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
            (_, Err((outcome, _))) => outcome,
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
        assert_eq!(verdict.unwrap_err().0, "jwks_unavailable");
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
}
