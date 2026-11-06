//! The Pusher's public discovery document (spec 06-client): everything
//! `garret login` needs to write a whole client config from one URL.
//!
//! There is no `pusher_endpoint` field — the client dialled the Pusher to ask,
//! so it already knows it, and behind a reverse proxy the server's own `listen`
//! address is a loopback that would be wrong to advertise.

use anyhow::{Context, Result, bail};
use garret_common::wire::{self, Discovery};
use reqwest::StatusCode;

/// Fetches `GET /api/v1/discovery` from the Pusher at `endpoint` (no trailing
/// `/`), anonymously, and refuses a document that [`check`] rejects.
///
/// Distinguishes one failure precisely: a 404 *or* 401 means a server that
/// predates the discovery endpoint (discovery needs no token, so a 401 can
/// only be an auth layer wrapping every route), and says so rather than
/// letting it read as a typo'd URL or bad credentials.
pub async fn fetch(http: &reqwest::Client, endpoint: &str) -> Result<Discovery> {
    check_url("the Pusher URL", endpoint)?;
    let url = format!("{endpoint}{}", wire::DISCOVERY);
    let response = http
        .get(&url)
        .send()
        .await
        .with_context(|| format!("fetching {url}"))?;

    let status = response.status();
    if status == StatusCode::NOT_FOUND || status == StatusCode::UNAUTHORIZED {
        bail!(
            "{url} returned {status} — this Pusher predates `garret login <url>` \
             (discovery needs no token, so a 401 here means its auth layer \
             covers every route).\n\
             Upgrade the server, or write the config by hand."
        );
    }
    let discovery: Discovery = crate::http::ensure_success(response, "discovery failed")
        .await?
        .json()
        .await
        .context("parsing the discovery document")?;
    check(&discovery).with_context(|| format!("refusing the discovery document from {url}"))?;
    Ok(discovery)
}

/// Every advertised value ends up somewhere that trusts it: the puller URL
/// and keys in nix.conf, where a trusted key applies to *every* substituter
/// and a newline would start a new setting (`post-build-hook`, say); the
/// issuer as the place the device flow sends the human to sign in.
pub fn check(discovery: &Discovery) -> Result<()> {
    if let Some(puller) = &discovery.puller_endpoint {
        check_url("puller_endpoint", puller)?;
    }
    for key in &discovery.public_keys {
        check_key(key)?;
    }
    if let Some(oidc) = &discovery.oidc {
        check_url("oidc.issuer", &oidc.issuer)?;
        for (what, value) in [
            ("oidc.audience", Some(&oidc.audience)),
            ("oidc.client_id", oidc.client_id.as_ref()),
        ] {
            if let Some(value) = value
                && (value.is_empty()
                    || !value
                        .chars()
                        .all(|c| c.is_ascii_graphic() && c != '"' && c != '\\'))
            {
                bail!(
                    "{what} {value:?} is empty or holds whitespace, quotes or control characters"
                );
            }
        }
    }
    Ok(())
}

/// Refuses a URL that is not `https`, or plain `http` to a loopback host (a
/// local test server, where nothing crosses a network), or that carries any
/// character that could escape the nix.conf line or Nix string it is written
/// into. The raw string is checked, not the parsed form: the URL parser quietly
/// strips tabs and newlines, and it is the raw string that gets written.
pub fn check_url(what: &str, raw: &str) -> Result<()> {
    if let Some(c) = raw
        .chars()
        .find(|c| !c.is_ascii_graphic() || matches!(c, '"' | '\\' | '{' | '}' | '`'))
    {
        bail!("{what} {raw:?} contains {c:?}; URLs here must be plain printable ASCII");
    }
    let url = reqwest::Url::parse(raw).with_context(|| format!("{what} {raw:?} is not a URL"))?;
    let host = url.host_str().unwrap_or_default();
    let loopback = host == "localhost"
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    match url.scheme() {
        "https" => Ok(()),
        "http" if loopback => Ok(()),
        scheme => bail!(
            "{what} {raw:?} uses {scheme}://; only https is accepted (plain http only to \
             localhost) — tokens and trusted signing keys must not cross the network in the clear"
        ),
    }
}

/// Refuses anything but Nix's `name:base64` public-key form — no spaces, which
/// would split one key into two in `trusted-public-keys`, and no newlines.
pub fn check_key(key: &str) -> Result<()> {
    let valid = key.split_once(':').is_some_and(|(name, value)| {
        let data = value.trim_end_matches('=');
        !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
            && !data.is_empty()
            && data
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '/'))
    });
    if !valid {
        bail!("public key {key:?} is not of the form `name:base64`");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_must_be_https_except_to_loopback() {
        for ok in [
            "https://cache.example",
            "https://cache.example:8443/sub/",
            "http://127.0.0.1:8080",
            "http://localhost:3000",
            "http://[::1]:80",
        ] {
            check_url("u", ok).unwrap_or_else(|e| panic!("{ok}: {e:#}"));
        }
        for bad in [
            "http://cache.example",
            "http://127.0.0.1.evil.example",
            "http://localhost.evil.example",
            "ftp://cache.example",
            "cache.example",
            "",
        ] {
            assert!(check_url("u", bad).is_err(), "{bad} was accepted");
        }
    }

    /// The injection the check exists for: a newline starts a new nix.conf
    /// setting, and the URL parser would have silently dropped it.
    #[test]
    fn urls_must_not_smuggle_nix_conf_or_nix_syntax() {
        for bad in [
            "https://cache.example\npost-build-hook = /tmp/x",
            "https://cache.example\tx",
            "https://cache.example other",
            "https://cache.example/\"; x = \"",
            "https://cache.example/${x}",
        ] {
            assert!(check_url("u", bad).is_err(), "{bad:?} was accepted");
        }
    }

    #[test]
    fn keys_must_be_name_colon_base64() {
        for ok in [
            "garret-1:AAAA+bb/cc=",
            "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=",
        ] {
            check_key(ok).unwrap_or_else(|e| panic!("{ok}: {e:#}"));
        }
        for bad in [
            "garret-1:AAAA other:BBBB",
            "garret-1:AAAA\npost-build-hook = /tmp/x",
            "garret-1:",
            ":AAAA",
            "garret-1",
            "garret 1:AAAA",
            "garret-1:AA=A",
            "garret-1:AAAA\"",
        ] {
            assert!(check_key(bad).is_err(), "{bad:?} was accepted");
        }
    }

    #[test]
    fn a_document_with_one_bad_value_is_refused() {
        let good = crate::testing::discovery;
        check(&good()).unwrap();

        let mut d = good();
        d.puller_endpoint = Some("http://cache.example".into());
        assert!(check(&d).is_err());
        let mut d = good();
        d.public_keys
            .push("evil:AAAA\nextra-substituters = http://x".into());
        assert!(check(&d).is_err());
        let mut d = good();
        d.oidc.as_mut().unwrap().issuer = "http://id.example".into();
        assert!(check(&d).is_err());
        let mut d = good();
        d.oidc.as_mut().unwrap().client_id = Some("a\nb".into());
        assert!(check(&d).is_err());
    }
}
