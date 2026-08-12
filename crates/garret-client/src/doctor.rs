//! `garret doctor` — one-command diagnosis of the client→cache pipeline
//! (spec 06-client). Every check is one of the curls an operator would
//! eventually try by hand; the value is running them all, in layer order,
//! and naming the broken layer instead of letting a push fail three layers
//! deeper with a message about the wrong one.

use garret_common::hash_of_store_path;

use crate::{auth, config, discovery};

/// The hash probed when no path argument is given: certainly absent, so the
/// Puller answering 404 for it proves the narinfo route is served without
/// depending on the cache holding anything.
const ABSENT_HASH: &str = "00000000000000000000000000000000";

/// A check's outcome. `Skip` marks a check whose prerequisite already
/// failed: no discovery document leaves the drift and signing-key checks
/// nothing to compare against, and no accepted token means the cached-path
/// question cannot be asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// The layer works.
    Pass,
    /// The layer is broken; the detail says how and names the fix.
    Fail,
    /// Not checked, because an earlier layer failed.
    Skip,
}

impl Status {
    /// The lowercase word used in both the human lines and `--json` output.
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Pass => "pass",
            Status::Fail => "fail",
            Status::Skip => "skip",
        }
    }
}

/// One layer's verdict.
#[derive(Debug)]
pub struct Check {
    /// The layer: `discovery`, `config`, `keys`, `auth`, `pull` or `path`.
    pub name: &'static str,
    /// Pass, fail, or skipped.
    pub status: Status,
    /// What was probed; on failure, what is broken and how to fix it.
    pub detail: String,
}

impl Check {
    fn pass(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            status: Status::Pass,
            detail: detail.into(),
        }
    }

    fn fail(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            status: Status::Fail,
            detail: detail.into(),
        }
    }

    fn skip(name: &'static str) -> Self {
        Self {
            name,
            status: Status::Skip,
            detail: "skipped: an earlier layer failed".into(),
        }
    }
}

/// Runs every check in layer order and returns all the verdicts. Never an
/// `Err` — a broken layer is a `Fail` verdict, not an early return — so the
/// caller always gets the whole picture before deciding the exit code.
pub async fn run(cfg: &config::Config, http: &reqwest::Client, path: Option<&str>) -> Vec<Check> {
    let mut checks = Vec::new();

    let discovered = match discovery::fetch(http, &cfg.endpoint).await {
        Ok(d) => {
            checks.push(Check::pass(
                "discovery",
                format!("the Pusher at {} answered", cfg.endpoint),
            ));
            Some(d)
        }
        Err(e) => {
            checks.push(Check::fail("discovery", format!("{e:#}")));
            None
        }
    };

    match &discovered {
        Some(d) => {
            let drifted = drift(cfg, d);
            checks.push(match drifted.is_empty() {
                true => Check::pass("config", "config matches the server's discovery document"),
                false => Check::fail(
                    "config",
                    format!(
                        "config drift — re-run `garret login --force`: {}",
                        drifted.join("; ")
                    ),
                ),
            });
            let (status, detail) = keys_verdict(&cfg.public_keys, &d.public_keys);
            checks.push(Check {
                name: "keys",
                status,
                detail,
            });
        }
        None => {
            checks.push(Check::skip("config"));
            checks.push(Check::skip("keys"));
        }
    }

    let token = auth_check(cfg, http, &mut checks).await;

    checks.push(pull_check(cfg, http, path).await);

    match (path, token) {
        (Some(path), Some(token)) => checks.push(path_check(cfg, http, &token, path).await),
        (Some(_), None) => checks.push(Check::skip("path")),
        (None, _) => {}
    }

    checks
}

/// Token acquisition plus the liveness probe `whoami` uses: an empty
/// Negotiation batch, valid and near-free, exercising exactly the
/// authentication path `push` crosses. Returns the token — only when the
/// Pusher accepted it — so the `path` check can reuse it.
async fn auth_check(
    cfg: &config::Config,
    http: &reqwest::Client,
    checks: &mut Vec<Check>,
) -> Option<String> {
    let token =
        match auth::bearer_token(http, &cfg.oidc.audience, cfg.oidc.resource.as_deref()).await {
            Ok(token) => token,
            Err(e) => {
                checks.push(Check::fail("auth", format!("no token: {e:#}")));
                return None;
            }
        };

    let response = http
        .post(format!("{}/api/v1/missing-paths", cfg.endpoint))
        .bearer_auth(&token)
        .json(&Vec::<String>::new())
        .send()
        .await;
    match response {
        Ok(r) if r.status().is_success() => {
            let subject = auth::peek_claims(&token)
                .ok()
                .and_then(|c| c.sub)
                .unwrap_or_else(|| "unknown".into());
            checks.push(Check::pass(
                "auth",
                format!("the Pusher accepted the token (subject {subject})"),
            ));
            Some(token)
        }
        Ok(r) => {
            let status = r.status();
            let body = r.text().await.unwrap_or_default();
            checks.push(Check::fail(
                "auth",
                format!(
                    "the Pusher rejected the token with {status}: {}",
                    body.trim()
                ),
            ));
            None
        }
        Err(e) => {
            checks.push(Check::fail("auth", format!("probing the Pusher: {e:#}")));
            None
        }
    }
}

/// The substituter side: `/nix-cache-info` must be served, and the narinfo
/// route must answer. Hit and miss both pass — whether a *specific* path is
/// cached is the `path` check's question, asked of the Pusher; here only a
/// dead connection or an error status is a broken pull layer.
async fn pull_check(cfg: &config::Config, http: &reqwest::Client, path: Option<&str>) -> Check {
    let Some(puller) = cfg.puller_endpoint.as_deref() else {
        return Check::fail(
            "pull",
            "no `puller_endpoint` in the config — re-run `garret login --force` to pick it up \
             from the server",
        );
    };

    let url = format!("{puller}/nix-cache-info");
    let body = match http.get(&url).send().await {
        Ok(r) if r.status().is_success() => r.text().await.unwrap_or_default(),
        Ok(r) => return Check::fail("pull", format!("{url} answered {}", r.status())),
        Err(e) => return Check::fail("pull", format!("fetching {url}: {e:#}")),
    };
    if !cache_info_ok(&body) {
        return Check::fail(
            "pull",
            format!("{url} did not return a nix-cache-info document — is this really the Puller?"),
        );
    }

    let probe = path.and_then(|p| probe_hash(p).ok()).unwrap_or(ABSENT_HASH);
    let narinfo_url = format!("{puller}/{probe}.narinfo");
    match http.get(&narinfo_url).send().await {
        Ok(r)
            if r.status() == reqwest::StatusCode::OK
                || r.status() == reqwest::StatusCode::NOT_FOUND =>
        {
            Check::pass(
                "pull",
                format!(
                    "the Puller serves nix-cache-info; {probe}.narinfo answered {}",
                    r.status().as_u16()
                ),
            )
        }
        Ok(r) => Check::fail("pull", format!("{narinfo_url} answered {}", r.status())),
        Err(e) => Check::fail("pull", format!("fetching {narinfo_url}: {e:#}")),
    }
}

/// One Negotiation round for a single hash: the authoritative answer to
/// "is this path cached", straight from the Pusher. An uncached path is a
/// `Fail` so the exit code carries the answer for scripts.
async fn path_check(
    cfg: &config::Config,
    http: &reqwest::Client,
    token: &str,
    path: &str,
) -> Check {
    let hash = match probe_hash(path) {
        Ok(hash) => hash,
        Err(why) => return Check::fail("path", why),
    };

    let response = match http
        .post(format!("{}/api/v1/missing-paths", cfg.endpoint))
        .bearer_auth(token)
        .json(&vec![hash])
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return Check::fail("path", format!("Negotiation failed: {e:#}")),
    };
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Check::fail(
            "path",
            format!("Negotiation rejected with {status}: {}", body.trim()),
        );
    }
    let missing: Vec<String> = match response.json().await {
        Ok(missing) => missing,
        Err(e) => {
            return Check::fail("path", format!("parsing the Negotiation response: {e:#}"));
        }
    };

    match missing.iter().any(|h| h == hash) {
        true => Check::fail(
            "path",
            format!(
                "not cached — Negotiation lists {hash} as missing; `garret push {path}` would upload it"
            ),
        ),
        false => Check::pass(
            "path",
            format!("cached — Negotiation does not list {hash} as missing"),
        ),
    }
}

/// The fields `garret login` writes that the server still advertises. Only
/// advertised fields count — a server that omits its `puller_endpoint` is
/// sparse, not drifted. `public_keys` is deliberately excluded: the
/// signing-key match is its own check.
fn drift(cfg: &config::Config, d: &discovery::Discovery) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(advertised) = &d.puller_endpoint
        && cfg.puller_endpoint.as_deref() != Some(advertised.as_str())
    {
        out.push(format!(
            "puller_endpoint is {}, server advertises {advertised}",
            cfg.puller_endpoint.as_deref().unwrap_or("unset")
        ));
    }
    if let Some(oidc) = &d.oidc {
        if cfg.oidc.issuer != oidc.issuer {
            out.push(format!(
                "oidc.issuer is {}, server advertises {}",
                cfg.oidc.issuer, oidc.issuer
            ));
        }
        if cfg.oidc.audience != oidc.audience {
            out.push(format!(
                "oidc.audience is {}, server advertises {}",
                cfg.oidc.audience, oidc.audience
            ));
        }
        if let Some(client_id) = &oidc.client_id
            && cfg.oidc.client_id.as_deref() != Some(client_id.as_str())
        {
            out.push(format!(
                "oidc.client_id is {}, server advertises {client_id}",
                cfg.oidc.client_id.as_deref().unwrap_or("unset")
            ));
        }
    }
    out
}

/// Signing-key match: every key the config trusts must still be one the
/// server signs with, or nix verifies against a key that signs nothing and
/// rejects every path. Newer server keys the config lacks still pass — that
/// is a rotation in progress — but the detail says to re-login before the
/// old key is retired.
fn keys_verdict(configured: &[String], advertised: &[String]) -> (Status, String) {
    if configured.is_empty() {
        return (
            Status::Fail,
            "no `public_keys` in the config — nix would reject every path as unsigned; \
             re-run `garret login --force`"
                .into(),
        );
    }
    let stale: Vec<&str> = configured
        .iter()
        .filter(|k| !advertised.contains(k))
        .map(String::as_str)
        .collect();
    if !stale.is_empty() {
        return (
            Status::Fail,
            format!(
                "configured key(s) the server no longer advertises: {} — re-run \
                 `garret login --force`",
                stale.join(", ")
            ),
        );
    }
    let newer = advertised
        .iter()
        .filter(|k| !configured.contains(k))
        .count();
    match newer {
        0 => (
            Status::Pass,
            format!(
                "{} configured public key(s), all advertised by the server",
                configured.len()
            ),
        ),
        n => (
            Status::Pass,
            format!(
                "configured keys match; the server also advertises {n} newer key(s) — \
                 re-run `garret login --force` to trust them"
            ),
        ),
    }
}

/// The one line every substituter must serve; anything else at that URL is a
/// reverse proxy answering for a Puller that is not there.
fn cache_info_ok(body: &str) -> bool {
    body.lines().any(|l| l.starts_with("StoreDir:"))
}

/// The 32-character store path hash of a path (or bare hash) argument, or an
/// explanation of why the argument cannot name one.
fn probe_hash(path: &str) -> Result<&str, String> {
    let hash = hash_of_store_path(path);
    match hash.len() == 32 && hash.chars().all(|c| c.is_ascii_alphanumeric()) {
        true => Ok(hash),
        false => Err(format!(
            "`{path}` does not look like a store path or 32-character hash"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::{Discovery, Oidc};

    fn cfg() -> config::Config {
        config::Config {
            endpoint: "https://push.example".into(),
            oidc: config::Oidc {
                issuer: "https://id.example".into(),
                client_id: Some("garret-cli".into()),
                audience: "garret".into(),
                resource: None,
            },
            jobs: 8,
            zstd_level: 3,
            max_retries: 5,
            puller_endpoint: Some("https://cache.example".into()),
            public_keys: vec!["garret-1:AAAA=".into()],
            upstream_keys: vec!["cache.nixos.org-1".into()],
            watch: Default::default(),
        }
    }

    fn advertised() -> Discovery {
        Discovery {
            puller_endpoint: Some("https://cache.example".into()),
            public_keys: vec!["garret-1:AAAA=".into()],
            oidc: Some(Oidc {
                issuer: "https://id.example".into(),
                audience: "garret".into(),
                client_id: Some("garret-cli".into()),
            }),
        }
    }

    #[test]
    fn a_matching_config_has_no_drift() {
        assert!(drift(&cfg(), &advertised()).is_empty());
    }

    #[test]
    fn every_drifted_field_is_named() {
        let d = Discovery {
            puller_endpoint: Some("https://cache2.example".into()),
            oidc: Some(Oidc {
                issuer: "https://id2.example".into(),
                audience: "garret2".into(),
                client_id: Some("garret-cli-2".into()),
            }),
            ..advertised()
        };
        let drifted = drift(&cfg(), &d);
        assert_eq!(drifted.len(), 4, "{drifted:?}");
        assert!(drifted[0].contains("puller_endpoint"));
    }

    /// A sparse server advertises less, not different: no drift.
    #[test]
    fn unadvertised_fields_are_not_drift() {
        let sparse = Discovery {
            puller_endpoint: None,
            oidc: None,
            ..advertised()
        };
        assert!(drift(&cfg(), &sparse).is_empty());
    }

    #[test]
    fn matching_keys_pass() {
        let (status, detail) = keys_verdict(&["a:1".into()], &["a:1".into()]);
        assert_eq!(status, Status::Pass);
        assert!(detail.contains("all advertised"));
    }

    #[test]
    fn no_configured_keys_fail() {
        let (status, _) = keys_verdict(&[], &["a:1".into()]);
        assert_eq!(status, Status::Fail);
    }

    #[test]
    fn a_stale_configured_key_fails_and_is_named() {
        let (status, detail) = keys_verdict(&["old:1".into()], &["new:2".into()]);
        assert_eq!(status, Status::Fail);
        assert!(detail.contains("old:1"));
    }

    /// Mid-rotation the server signs with more keys than the config trusts;
    /// pulls still verify, so it passes — with the re-login nudge.
    #[test]
    fn newer_server_keys_pass_with_a_nudge() {
        let (status, detail) = keys_verdict(&["a:1".into()], &["a:1".into(), "b:2".into()]);
        assert_eq!(status, Status::Pass);
        assert!(detail.contains("garret login --force"));
    }

    #[test]
    fn cache_info_is_recognised_by_its_store_dir() {
        assert!(cache_info_ok(
            "StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 40\n"
        ));
        assert!(!cache_info_ok("<html>404 not found</html>"));
    }

    #[test]
    fn probe_hash_accepts_paths_and_bare_hashes() {
        assert_eq!(
            probe_hash("/nix/store/v2xb3fmd3qd34s7q7sngajj8rjl5k25j-payload"),
            Ok("v2xb3fmd3qd34s7q7sngajj8rjl5k25j")
        );
        assert_eq!(
            probe_hash("v2xb3fmd3qd34s7q7sngajj8rjl5k25j"),
            Ok("v2xb3fmd3qd34s7q7sngajj8rjl5k25j")
        );
        assert!(probe_hash("hello").is_err());
        assert!(probe_hash("/nix/store/short-name").is_err());
    }
}
