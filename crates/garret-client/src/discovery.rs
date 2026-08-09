//! The Pusher's public discovery document (spec 06-client): everything
//! `garret login` needs to write a whole client config from one URL.
//!
//! There is no `pusher_endpoint` field — the client dialled the Pusher to ask,
//! so it already knows it, and behind a reverse proxy the server's own `listen`
//! address is a loopback that would be wrong to advertise.

use anyhow::{Context, Result, bail};
use reqwest::StatusCode;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Discovery {
    /// Absent when the Pusher has no `puller_endpoint` configured.
    pub puller_endpoint: Option<String>,
    /// Public halves of the cache's signing keys — plural, because every object
    /// is signed with every configured key and a rotation runs several at once.
    #[serde(default)]
    pub public_keys: Vec<String>,
    /// Absent when no issuer sets a `client_id`, in which case the device flow
    /// has nothing to identify itself as.
    pub oidc: Option<Oidc>,
}

#[derive(Debug, Deserialize)]
pub struct Oidc {
    pub issuer: String,
    pub audience: String,
    pub client_id: Option<String>,
}

pub async fn fetch(http: &reqwest::Client, endpoint: &str) -> Result<Discovery> {
    let url = format!("{}/api/v1/discovery", endpoint.trim_end_matches('/'));
    let response = http
        .get(&url)
        .send()
        .await
        .with_context(|| format!("fetching {url}"))?;

    // The one failure worth naming precisely: a client newer than its server.
    // Without this it surfaces as a bare 404 and looks like a typo'd URL.
    //
    // 401 lands here too. Discovery is anonymous by design, so a server that
    // demands a token on it is one whose auth layer still wraps the whole
    // router — the shape the Pusher had before this endpoint existed. Left to
    // the generic arm below it reads as "your credentials are wrong", which
    // sends you off chasing an OIDC problem that isn't there.
    let status = response.status();
    if status == StatusCode::NOT_FOUND || status == StatusCode::UNAUTHORIZED {
        bail!(
            "{url} returned {status} — this Pusher predates `garret login <url>` \
             (discovery needs no token, so a 401 here means its auth layer \
             covers every route).\n\
             Upgrade the server, or write the config by hand."
        );
    }
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!("discovery failed with {status}: {}", body.trim());
    }
    response
        .json()
        .await
        .context("parsing the discovery document")
}
