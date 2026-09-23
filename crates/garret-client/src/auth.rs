//! Getting a bearer token, by whichever flow the caller's environment offers
//! (spec 04-auth). Garret issues no tokens; these all come from an issuer.

use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

/// What `garret login` persists: enough to mint fresh access tokens without
/// re-running the device flow. Never the access token itself — that is
/// short-lived by design and re-minted as it ages (see [`TokenSource`]).
#[derive(Debug, Serialize, Deserialize)]
pub struct StoredToken {
    /// The rotating refresh token. Each refresh may replace it, and the
    /// replacement must be persisted or the next run is locked out.
    pub refresh_token: String,
    /// Issuer URL the token came from; refresh goes back to the same one.
    pub issuer: String,
    /// The public client id the device flow authenticated as.
    pub client_id: String,
}

/// `$XDG_CONFIG_HOME/garret/token.json` — where the refresh token lives.
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

/// The claims the client reads from a token it holds: `sub`, `aud` and `exp`
/// for `garret whoami`, `iat` and `exp` for when [`TokenSource`] re-mints.
#[derive(Debug, Default, Deserialize)]
pub struct Claims {
    /// Subject — who the issuer says this token belongs to.
    pub sub: Option<String>,
    /// Expiry as a Unix timestamp; `None` if the issuer omitted it.
    pub exp: Option<i64>,
    /// Issue time as a Unix timestamp; `None` if the issuer omitted it.
    pub iat: Option<i64>,
    /// `aud` is a string or an array of strings depending on the issuer.
    #[serde(default)]
    pub aud: serde_json::Value,
}

/// Reads a JWT's payload **without verifying its signature**. Verification is
/// the server's job and needs the issuer's JWKS; the client only reads what it
/// is holding — to report it, and to time a re-mint — and a forged token would
/// be rejected by the server anyway. Never use this to make an access decision.
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

/// Writes the token file mode 0600, creating `~/.config/garret/` if needed.
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

/// The two OIDC endpoints the client uses, out of everything discovery offers.
#[derive(Debug, Deserialize)]
pub struct Endpoints {
    /// Where the device flow asks for a user code (RFC 8628).
    pub device_authorization_endpoint: String,
    /// Where every grant type is exchanged for tokens.
    pub token_endpoint: String,
}

/// Fetches the issuer's `/.well-known/openid-configuration`.
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

/// GitHub Actions: a fresh runner token for `audience` (ADR-0003 — the
/// 5-minute TTL is handled client-side by [`TokenSource`], not by an exchange).
async fn github_token(
    http: &reqwest::Client,
    url: &str,
    request_token: &str,
    audience: &str,
) -> Result<String> {
    let token: GithubToken = http
        .get(url)
        .query(&[("audience", audience)])
        .bearer_auth(request_token)
        .send()
        .await?
        .error_for_status()
        .context("requesting a GitHub Actions OIDC token")?
        .json()
        .await?;
    Ok(token.value)
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

/// One token from whatever this environment provides — for commands that make
/// a single request. Anything longer-lived holds a [`TokenSource`].
pub async fn bearer_token(
    http: &reqwest::Client,
    audience: &str,
    resource: Option<&str>,
) -> Result<String> {
    TokenSource::from_env(http, audience, resource).get().await
}

/// Tokens are re-minted this long before their declared lifetime ends. For a
/// GitHub runner token (five minutes) that is spec 04-auth's "re-mint once
/// the cached one is >4 min old".
const EXPIRY_MARGIN_SECS: i64 = 60;

/// Where a [`TokenSource`] gets its next token.
enum Mint {
    /// `GARRET_TOKEN`: handed in, so nothing can replace it.
    Given(String),
    /// A GitHub Actions runner's OIDC endpoint.
    Github {
        url: String,
        request_token: String,
        audience: String,
    },
    /// The refresh token `garret login` stored, re-read per mint because each
    /// refresh rotates it.
    Stored { resource: Option<String> },
    /// A watcher daemon's confidential client.
    ClientCredentials {
        issuer: String,
        client_id: String,
        client_secret: String,
        resource: Option<String>,
    },
    /// Stand-in issuer for the aging and renewal tests.
    #[cfg(test)]
    Fake(Box<dyn Fn() -> String + Send + Sync>),
}

/// A bearer token that stays valid for as long as a run lasts.
///
/// A push can outlive any one token — GitHub's live five minutes, Pocket ID's
/// an hour, and the watcher runs for weeks — so callers ask for a token per
/// request instead of holding one. [`get`](Self::get) re-mints once the held
/// token has aged out; [`renew`](Self::renew) replaces it after a 401, for
/// whatever aging cannot see (revocation, a token with no declared lifetime).
pub struct TokenSource {
    http: reqwest::Client,
    mint: Mint,
    /// Locked across a mint, so concurrent requests share one — which rotating
    /// refresh tokens require: a second refresh racing the first would spend
    /// the refresh token the first just rotated out.
    held: tokio::sync::Mutex<Option<Held>>,
}

struct Held {
    token: String,
    minted: Instant,
    /// `None`: no declared lifetime, so only a 401 replaces it.
    max_age: Option<Duration>,
}

impl TokenSource {
    fn new(http: &reqwest::Client, mint: Mint) -> Self {
        Self {
            http: http.clone(),
            mint,
            held: tokio::sync::Mutex::new(None),
        }
    }

    /// Whatever this environment provides, in order: `GARRET_TOKEN`, a GitHub
    /// Actions runner, then the stored refresh token (failing on first use
    /// with "run `garret login`" when none of the three applies).
    pub fn from_env(http: &reqwest::Client, audience: &str, resource: Option<&str>) -> Self {
        let mint = if let Ok(token) = std::env::var("GARRET_TOKEN") {
            Mint::Given(token)
        } else if let (Ok(url), Ok(request_token)) = (
            std::env::var("ACTIONS_ID_TOKEN_REQUEST_URL"),
            std::env::var("ACTIONS_ID_TOKEN_REQUEST_TOKEN"),
        ) {
            Mint::Github {
                url,
                request_token,
                audience: audience.to_owned(),
            }
        } else {
            Mint::Stored {
                resource: resource.map(str::to_owned),
            }
        };
        Self::new(http, mint)
    }

    /// Watcher daemons authenticate as themselves: a per-machine confidential
    /// client, secret in a root-owned file wired by the NixOS module (spec
    /// 04-auth).
    pub fn client_credentials(
        http: &reqwest::Client,
        issuer: &str,
        client_id: &str,
        client_secret: &str,
        resource: Option<&str>,
    ) -> Self {
        Self::new(
            http,
            Mint::ClientCredentials {
                issuer: issuer.to_owned(),
                client_id: client_id.to_owned(),
                client_secret: client_secret.to_owned(),
                resource: resource.map(str::to_owned),
            },
        )
    }

    /// The token to send now, minted first if none is held or it has aged out.
    pub async fn get(&self) -> Result<String> {
        let mut held = self.held.lock().await;
        if let Some(current) = &*held
            && current
                .max_age
                .is_none_or(|max| current.minted.elapsed() < max)
        {
            return Ok(current.token.clone());
        }
        self.mint_into(&mut held).await
    }

    /// Replaces `rejected` after the server answered 401 to it, returning the
    /// token to retry with — the held one as-is when a concurrent request
    /// already replaced it. `None` when nothing can mint a replacement
    /// (`GARRET_TOKEN`), so the 401 stands.
    pub async fn renew(&self, rejected: &str) -> Result<Option<String>> {
        if matches!(self.mint, Mint::Given(_)) {
            return Ok(None);
        }
        let mut held = self.held.lock().await;
        if let Some(current) = &*held
            && current.token != rejected
        {
            return Ok(Some(current.token.clone()));
        }
        self.mint_into(&mut held).await.map(Some)
    }

    async fn mint_into(&self, held: &mut Option<Held>) -> Result<String> {
        let http = &self.http;
        let token = match &self.mint {
            Mint::Given(token) => token.clone(),
            Mint::Github {
                url,
                request_token,
                audience,
            } => github_token(http, url, request_token, audience).await?,
            Mint::Stored { resource } => {
                let path = token_path()?;
                let stored: StoredToken = std::fs::read(&path)
                    .ok()
                    .and_then(|b| serde_json::from_slice(&b).ok())
                    .ok_or_else(|| {
                        anyhow!("not logged in — run `garret login` (looked in {path:?})")
                    })?;
                refresh(http, &stored, resource.as_deref()).await?
            }
            Mint::ClientCredentials {
                issuer,
                client_id,
                client_secret,
                resource,
            } => {
                client_credentials(http, issuer, client_id, client_secret, resource.as_deref())
                    .await?
            }
            #[cfg(test)]
            Mint::Fake(next) => next(),
        };
        *held = Some(Held {
            max_age: declared_max_age(&token),
            token: token.clone(),
            minted: Instant::now(),
        });
        Ok(token)
    }
}

/// How long a token may be held: the lifetime its own claims declare, less a
/// margin. `exp - iat` are both issuer-clock, so local clock skew cannot
/// stretch it. `None` for a token that declares no lifetime.
fn declared_max_age(token: &str) -> Option<Duration> {
    let claims = peek_claims(token).ok()?;
    let usable = claims.exp? - claims.iat? - EXPIRY_MARGIN_SECS;
    Some(Duration::from_secs(usable.max(0) as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    /// A JWT-shaped token declaring `lifetime` seconds; `n` tells mints apart.
    fn jwt(n: usize, lifetime: Option<i64>) -> String {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64URL};
        let claims = match lifetime {
            Some(l) => serde_json::json!({"n": n, "iat": 1_000, "exp": 1_000 + l}),
            None => serde_json::json!({"n": n}),
        };
        format!("h.{}.s", B64URL.encode(claims.to_string()))
    }

    /// A source over a fake issuer, and a count of how often it was asked.
    fn source(lifetime: Option<i64>) -> (TokenSource, Arc<AtomicUsize>) {
        let mints = Arc::new(AtomicUsize::new(0));
        let counter = mints.clone();
        let mint = Mint::Fake(Box::new(move || {
            jwt(counter.fetch_add(1, Ordering::SeqCst) + 1, lifetime)
        }));
        (TokenSource::new(&reqwest::Client::new(), mint), mints)
    }

    #[tokio::test(start_paused = true)]
    async fn a_github_length_token_is_reminted_once_it_is_four_minutes_old() {
        let (tokens, _) = source(Some(300));
        let first = tokens.get().await.unwrap();
        tokio::time::advance(Duration::from_secs(239)).await;
        assert_eq!(tokens.get().await.unwrap(), first);
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(tokens.get().await.unwrap(), jwt(2, Some(300)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_token_without_a_declared_lifetime_is_held_until_rejected() {
        let (tokens, _) = source(None);
        let first = tokens.get().await.unwrap();
        tokio::time::advance(Duration::from_secs(86_400)).await;
        assert_eq!(tokens.get().await.unwrap(), first);
        assert_eq!(tokens.renew(&first).await.unwrap(), Some(jwt(2, None)));
        assert_eq!(tokens.get().await.unwrap(), jwt(2, None));
    }

    /// Every in-flight request sent with the old token sees the 401: the first
    /// to report it mints, the rest retry with that replacement. Minting per
    /// report would spend a rotating refresh token the first mint rotated out.
    #[tokio::test]
    async fn concurrent_401s_for_one_token_mint_one_replacement() {
        let (tokens, mints) = source(None);
        let first = tokens.get().await.unwrap();
        for _ in 0..3 {
            assert_eq!(tokens.renew(&first).await.unwrap(), Some(jwt(2, None)));
        }
        assert_eq!(mints.load(Ordering::SeqCst), 2);
        // The replacement rejected in turn is a new 401, and mints again.
        assert_eq!(
            tokens.renew(&jwt(2, None)).await.unwrap(),
            Some(jwt(3, None))
        );
    }
}
