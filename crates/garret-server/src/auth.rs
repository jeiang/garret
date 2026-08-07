//! Multi-issuer OIDC validation (spec 04-auth, ADR-0003). Garret issues no
//! tokens; it validates them against each configured issuer's JWKS.
//!
//! Hand-rolled rather than `jwt-authorizer` only because that crate is pinned
//! to axum 0.7 and ADR-0004 pins us to 0.8.

use std::{
    collections::HashMap,
    sync::RwLock,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, jwk::JwkSet};
use serde::Deserialize;

use crate::config::IssuerConfig;

/// Clock skew allowance, per spec 04-auth.
const LEEWAY_SECS: u64 = 60;
/// Floor between JWKS refetches, so an unknown-kid flood can't hammer the issuer.
const MIN_REFRESH: Duration = Duration::from_secs(10);

#[derive(Debug, Deserialize)]
struct Claims {
    iss: String,
    sub: String,
    #[serde(default)]
    groups: Vec<String>,
    /// GitHub Actions: the immutable owner id (names are renameable, spec 04).
    #[serde(default)]
    repository_owner_id: Option<String>,
    #[serde(rename = "ref", default)]
    git_ref: Option<String>,
}

struct Issuer {
    cfg: IssuerConfig,
    keys: RwLock<KeyCache>,
}

#[derive(Default)]
struct KeyCache {
    by_kid: HashMap<String, DecodingKey>,
    fetched_at: Option<Instant>,
}

pub struct Authenticator {
    issuers: Vec<Issuer>,
    http: reqwest::Client,
}

/// The authenticated caller; stored as `pushed_by` for audit.
#[derive(Debug, Clone)]
pub struct Subject(pub String);

impl Authenticator {
    pub fn new(issuers: Vec<IssuerConfig>) -> Result<Self> {
        if issuers.is_empty() {
            bail!("no OIDC issuers configured — garret has no auth-disable mode (spec 04)");
        }
        Ok(Self {
            issuers: issuers
                .into_iter()
                .map(|cfg| Issuer {
                    cfg,
                    keys: RwLock::new(KeyCache::default()),
                })
                .collect(),
            http: reqwest::Client::new(),
        })
    }

    /// Validates a bearer token and returns its subject, or an error naming
    /// only what the caller may safely learn.
    pub async fn authenticate(&self, token: &str) -> Result<Subject> {
        // Unverified read of `iss` picks which issuer to verify against. It
        // decides routing only; every claim is re-read after verification.
        let issuer_name = unverified_issuer(token)?;
        let issuer = self
            .issuers
            .iter()
            .find(|i| i.cfg.issuer == issuer_name)
            .ok_or_else(|| anyhow!("untrusted issuer"))?;

        let kid = jsonwebtoken::decode_header(token)
            .context("malformed token header")?
            .kid
            .ok_or_else(|| anyhow!("token has no kid"))?;

        let mut key = issuer.cached_key(&kid);
        if key.is_none() {
            // Unknown kid means rotation: refetch once, rate-limited.
            self.refresh_jwks(issuer).await?;
            key = issuer.cached_key(&kid);
        }
        let key = key.ok_or_else(|| anyhow!("no signing key for kid {kid}"))?;

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[&issuer.cfg.issuer]);
        validation.set_audience(&[&issuer.cfg.audience]);
        validation.leeway = LEEWAY_SECS;
        let claims = jsonwebtoken::decode::<Claims>(token, &key, &validation)
            .context("token failed validation")?
            .claims;

        authorize(&issuer.cfg, &claims)?;
        Ok(Subject(format!("{}#{}", claims.iss, claims.sub)))
    }

    async fn refresh_jwks(&self, issuer: &Issuer) -> Result<()> {
        if let Some(at) = issuer.keys.read().unwrap().fetched_at
            && at.elapsed() < MIN_REFRESH
        {
            return Ok(());
        }
        let url = self.jwks_url(issuer).await?;
        // A local path is the sanctioned dev-issuer override (spec 04): test
        // keys on disk, never an auth-disable flag.
        let body = if url.starts_with("http") {
            self.http
                .get(&url)
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
        let mut cache = issuer.keys.write().unwrap();
        cache.by_kid = set
            .keys
            .iter()
            .filter_map(|jwk| {
                let kid = jwk.common.key_id.clone()?;
                DecodingKey::from_jwk(jwk).ok().map(|k| (kid, k))
            })
            .collect();
        cache.fetched_at = Some(Instant::now());
        Ok(())
    }

    async fn jwks_url(&self, issuer: &Issuer) -> Result<String> {
        if let Some(url) = &issuer.cfg.jwks_url {
            return Ok(url.clone());
        }
        #[derive(Deserialize)]
        struct Discovery {
            jwks_uri: String,
        }
        let url = format!(
            "{}/.well-known/openid-configuration",
            issuer.cfg.issuer.trim_end_matches('/')
        );
        Ok(self
            .http
            .get(&url)
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .with_context(|| format!("OIDC discovery at {url}"))?
            .json::<Discovery>()
            .await?
            .jwks_uri)
    }
}

impl Issuer {
    fn cached_key(&self, kid: &str) -> Option<DecodingKey> {
        self.keys.read().unwrap().by_kid.get(kid).cloned()
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
/// grants by immutable owner id.
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
    if !cfg.allowed_groups.is_empty()
        && !claims.groups.iter().any(|g| cfg.allowed_groups.contains(g))
    {
        bail!("subject is not in an allowed group");
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
    use super::*;

    fn github(owner: &str, refs: &[&str]) -> IssuerConfig {
        IssuerConfig {
            issuer: "https://token.actions.githubusercontent.com".into(),
            audience: "garret".into(),
            jwks_url: None,
            github_owner_id: Some(owner.into()),
            ref_patterns: refs.iter().map(|r| (*r).to_owned()).collect(),
            allowed_groups: vec![],
        }
    }

    fn claims(owner: Option<&str>, git_ref: Option<&str>, groups: &[&str]) -> Claims {
        Claims {
            iss: "https://token.actions.githubusercontent.com".into(),
            sub: "repo:me/thing:ref:refs/heads/main".into(),
            groups: groups.iter().map(|g| (*g).to_owned()).collect(),
            repository_owner_id: owner.map(str::to_owned),
            git_ref: git_ref.map(str::to_owned),
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
    fn pocket_id_grants_on_audience_alone_unless_groups_are_set() {
        let mut cfg = IssuerConfig {
            issuer: "https://id.example".into(),
            audience: "garret".into(),
            jwks_url: None,
            github_owner_id: None,
            ref_patterns: vec![],
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
}
