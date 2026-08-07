//! Getting a bearer token, by whichever flow the caller's environment offers
//! (spec 04-auth). Garret issues no tokens; these all come from an issuer.

use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredToken {
    pub refresh_token: String,
    pub issuer: String,
    pub client_id: String,
}

pub fn token_path() -> Result<PathBuf> {
    Ok(crate::config::config_home()?
        .join("garret")
        .join("token.json"))
}

/// `garret logout`. Idempotent: a missing token file is the desired state, not
/// an error. The config stays — `rm` exists, and re-logging-in is the common case.
pub fn logout() -> Result<PathBuf> {
    let path = token_path()?;
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(path),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(path),
        Err(e) => Err(e).with_context(|| format!("removing {}", path.display())),
    }
}

/// The `sub`, `aud` and `exp` a token carries, for `garret whoami`.
#[derive(Debug, Default, Deserialize)]
pub struct Claims {
    pub sub: Option<String>,
    pub exp: Option<i64>,
    /// `aud` is a string or an array of strings depending on the issuer.
    #[serde(default)]
    pub aud: serde_json::Value,
}

/// Reads a JWT's payload **without verifying its signature**. Verification is
/// the server's job and needs the issuer's JWKS; the client is only reporting
/// what it is holding, and a forged token would be rejected by the liveness
/// probe `whoami` runs anyway. Never use this to make an access decision.
pub fn peek_claims(token: &str) -> Result<Claims> {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64URL};
    let payload = token
        .split('.')
        .nth(1)
        .context("token is not a JWT: expected three dot-separated segments")?;
    let bytes = B64URL
        .decode(payload)
        .context("token payload is not base64url")?;
    serde_json::from_slice(&bytes).context("token payload is not JSON")
}

pub fn save_token(token: &StoredToken) -> Result<()> {
    let path = token_path()?;
    std::fs::create_dir_all(path.parent().unwrap())?;
    std::fs::write(&path, serde_json::to_vec_pretty(token)?)?;
    restrict(&path)?;
    Ok(())
}

/// Mode 0600 — a refresh token is a credential (spec 04-auth).
#[cfg(unix)]
fn restrict(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn restrict(_path: &std::path::Path) -> Result<()> {
    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct Endpoints {
    pub device_authorization_endpoint: String,
    pub token_endpoint: String,
}

pub async fn discover(http: &reqwest::Client, issuer: &str) -> Result<Endpoints> {
    let url = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );
    Ok(http
        .get(&url)
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .with_context(|| format!("OIDC discovery at {url}"))?
        .json()
        .await?)
}

#[derive(Debug, Deserialize)]
struct DeviceGrant {
    device_code: String,
    user_code: String,
    verification_uri: String,
    verification_uri_complete: Option<String>,
    #[serde(default = "default_interval")]
    interval: u64,
}

fn default_interval() -> u64 {
    5
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    /// Optional because the device-flow poll returns `authorization_pending`
    /// with no token at all. A required field made the first poll fail to
    /// deserialize and abort the login, so `garret login` only ever worked if
    /// the human approved within the poll interval.
    access_token: Option<String>,
    refresh_token: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

fn missing_token() -> anyhow::Error {
    anyhow!("token response contained no access_token")
}

/// Appends the RFC 8707 `resource` parameter only when one is configured.
/// Always sending it breaks issuers that require each resource to be
/// registered in advance (Pocket ID answers `invalid_target`), and the
/// parameter is not needed when the issuer already puts the client id in `aud`.
fn with_resource<'a>(
    mut form: Vec<(&'a str, &'a str)>,
    resource: Option<&'a str>,
) -> Vec<(&'a str, &'a str)> {
    if let Some(resource) = resource {
        form.push(("resource", resource));
    }
    form
}

/// Device flow: the human approves in a browser (passkey, in Pocket ID's case)
/// while this polls. Returns the access token and stores the refresh token.
pub async fn device_login(
    http: &reqwest::Client,
    issuer: &str,
    client_id: &str,
    resource: Option<&str>,
) -> Result<String> {
    let endpoints = discover(http, issuer).await?;
    let grant: DeviceGrant = http
        .post(&endpoints.device_authorization_endpoint)
        .form(&with_resource(
            vec![("client_id", client_id), ("scope", "openid offline_access")],
            resource,
        ))
        .send()
        .await?
        .error_for_status()
        .context("requesting a device code")?
        .json()
        .await?;

    println!(
        "\nOpen {} and enter code: {}",
        grant
            .verification_uri_complete
            .as_deref()
            .unwrap_or(&grant.verification_uri),
        grant.user_code
    );
    println!("Waiting for approval…");

    loop {
        tokio::time::sleep(Duration::from_secs(grant.interval)).await;
        let response: TokenResponse = http
            .post(&endpoints.token_endpoint)
            .form(&with_resource(
                vec![
                    ("client_id", client_id),
                    ("device_code", &grant.device_code),
                    ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ],
                resource,
            ))
            .send()
            .await?
            .json()
            .await?;

        match response.error.as_deref() {
            // Both are "keep waiting"; anything else is terminal.
            Some("authorization_pending") | Some("slow_down") => continue,
            Some(other) => bail!("device authorization failed: {other}"),
            // No error and no token is not success -- keep polling rather than
            // reporting a login that did not happen.
            None if response.access_token.is_none() => continue,
            None => {
                let TokenResponse {
                    access_token,
                    refresh_token,
                    ..
                } = response;
                if let Some(refresh_token) = refresh_token {
                    save_token(&StoredToken {
                        refresh_token,
                        issuer: issuer.to_owned(),
                        client_id: client_id.to_owned(),
                    })?;
                }
                return access_token.ok_or_else(missing_token);
            }
        }
    }
}

async fn refresh(
    http: &reqwest::Client,
    stored: &StoredToken,
    resource: Option<&str>,
) -> Result<String> {
    let endpoints = discover(http, &stored.issuer).await?;
    let response: TokenResponse = http
        .post(&endpoints.token_endpoint)
        .form(&with_resource(
            vec![
                ("client_id", &stored.client_id as &str),
                ("refresh_token", &stored.refresh_token),
                ("grant_type", "refresh_token"),
            ],
            resource,
        ))
        .send()
        .await?
        .error_for_status()
        .context("refreshing the stored token")?
        .json()
        .await?;

    // Rotating refresh tokens: persist the new one or the next run is locked out.
    let TokenResponse {
        access_token,
        refresh_token,
        ..
    } = response;
    if let Some(refresh_token) = refresh_token {
        save_token(&StoredToken {
            refresh_token,
            issuer: stored.issuer.clone(),
            client_id: stored.client_id.clone(),
        })?;
    }
    access_token.ok_or_else(missing_token)
}

#[derive(Debug, Deserialize)]
struct GithubToken {
    value: String,
}

/// GitHub Actions: mint a fresh runner token per push rather than caching one
/// (ADR-0003 — the 5-minute TTL is handled client-side, not by an exchange).
/// Minting per invocation satisfies the spec's ">4 minutes old" rule by
/// construction, so there is no cache and no staleness check to get wrong.
async fn github_token(http: &reqwest::Client, audience: &str) -> Result<Option<String>> {
    let (Ok(url), Ok(request_token)) = (
        std::env::var("ACTIONS_ID_TOKEN_REQUEST_URL"),
        std::env::var("ACTIONS_ID_TOKEN_REQUEST_TOKEN"),
    ) else {
        return Ok(None);
    };
    let token: GithubToken = http
        .get(&url)
        .query(&[("audience", audience)])
        .bearer_auth(request_token)
        .send()
        .await?
        .error_for_status()
        .context("requesting a GitHub Actions OIDC token")?
        .json()
        .await?;
    Ok(Some(token.value))
}

/// Watcher daemons: a per-machine confidential client, secret in a root-owned
/// file wired by the NixOS module (spec 04-auth).
pub async fn client_credentials(
    http: &reqwest::Client,
    issuer: &str,
    client_id: &str,
    client_secret: &str,
    resource: Option<&str>,
) -> Result<String> {
    let endpoints = discover(http, issuer).await?;
    let response: TokenResponse = http
        .post(&endpoints.token_endpoint)
        .form(&with_resource(
            vec![
                ("client_id", client_id),
                ("client_secret", client_secret),
                ("grant_type", "client_credentials"),
            ],
            resource,
        ))
        .send()
        .await?
        .error_for_status()
        .context("requesting a client_credentials token")?
        .json()
        .await?;
    response.access_token.ok_or_else(missing_token)
}

/// Reads `id:secret` from a mode-0600 file rather than taking it on argv,
/// where every process on the box could read it.
pub fn read_client_credentials(path: &str) -> Result<(String, String)> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading client credentials {path}"))?;
    let (id, secret) = text
        .trim()
        .split_once(':')
        .context("credentials file must contain `client_id:client_secret`")?;
    Ok((id.to_owned(), secret.to_owned()))
}

/// Resolves a bearer token from whatever this environment provides.
pub async fn bearer_token(
    http: &reqwest::Client,
    audience: &str,
    resource: Option<&str>,
) -> Result<String> {
    if let Ok(token) = std::env::var("GARRET_TOKEN") {
        return Ok(token);
    }
    if let Some(token) = github_token(http, audience).await? {
        return Ok(token);
    }
    let path = token_path()?;
    let stored: StoredToken = std::fs::read(&path)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .ok_or_else(|| anyhow!("not logged in — run `garret login` (looked in {path:?})"))?;
    refresh(http, &stored, resource).await
}
