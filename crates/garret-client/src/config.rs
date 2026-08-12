//! Client config: TOML with env/flag overrides (spec 06-client).

use anyhow::{Context, Result};
use serde::Deserialize;

/// The whole client config, `~/.config/garret/config.toml` as written by
/// `garret login`. Unknown keys are rejected so a typo'd option fails loudly
/// instead of silently meaning its default.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Pusher base URL.
    pub endpoint: String,
    /// How to get a token — every command that talks to a server needs one.
    pub oidc: Oidc,
    /// Concurrent uploads. Network-bound, so the default is not a core count.
    #[serde(default = "jobs")]
    pub jobs: usize,
    /// zstd level for NAR compression (client-side, per the push protocol).
    #[serde(default = "zstd_level")]
    pub zstd_level: i32,
    /// Upload attempts per path beyond the first, for 429/5xx failures only.
    #[serde(default = "max_retries")]
    pub max_retries: u32,
    /// Puller base URL — `list` and `tree` query the browse API, not the Pusher.
    pub puller_endpoint: Option<String>,
    /// The cache's signing keys, public halves, as `garret use` writes them into
    /// `trusted-public-keys`. Plural because the Pusher signs every object with
    /// every configured key, so a rotation has more than one live at once.
    /// Written by `garret login` from the server's discovery document.
    #[serde(default)]
    pub public_keys: Vec<String>,
    /// Store watcher settings; defaulted so laptop configs never mention them.
    #[serde(default)]
    pub watch: Watch,
}

/// Store watcher settings; only the daemon reads these.
#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Watch {
    /// `client_id:client_secret` for this machine's confidential client.
    pub credentials_file: Option<String>,
    /// The Nix database the watcher polls for newly validated paths.
    #[serde(default = "nix_db")]
    pub nix_db: String,
    /// Where the Watcher Cursor persists across restarts.
    #[serde(default = "cursor_path")]
    pub cursor_path: String,
    /// Seconds between database polls when the backlog is empty.
    #[serde(default = "poll_interval")]
    pub poll_interval_secs: u64,
    /// Key names whose signatures mean "someone else already serves this
    /// path" — such paths are not pushed. Defaults to cache.nixos.org's.
    #[serde(default = "upstream_keys")]
    pub upstream_keys: Vec<String>,
    /// Substring patterns; a matching store path is never pushed.
    #[serde(default)]
    pub exclude_patterns: Vec<String>,
    /// Failed pushes per path before it lands on the skip-list and the
    /// watcher moves on.
    #[serde(default = "max_attempts")]
    pub max_attempts: u32,
}

fn nix_db() -> String {
    "/nix/var/nix/db/db.sqlite".into()
}

fn cursor_path() -> String {
    "/var/lib/garret/watcher-cursor".into()
}

fn poll_interval() -> u64 {
    30
}

fn upstream_keys() -> Vec<String> {
    vec!["cache.nixos.org-1".into()]
}

fn max_attempts() -> u32 {
    5
}

/// Which issuer to get tokens from, and what to ask it for (spec 04-auth).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Oidc {
    /// Issuer base URL; OIDC discovery hangs off it.
    pub issuer: String,
    /// Only the device flow needs one; GitHub Actions and client_credentials
    /// callers identify themselves other ways.
    pub client_id: Option<String>,
    /// The `aud` claim garret validates the resulting token against.
    pub audience: String,
    /// RFC 8707 resource indicator, sent only when set. Off by default
    /// because not every issuer implements RFC 8707, and those that do
    /// generally require each resource to be registered first — Pocket ID
    /// rejects any unregistered value with `invalid_target`, which made the
    /// device flow unusable when this was always sent. Omitting it falls
    /// back to the issuer's own audience behaviour, which for Pocket ID
    /// already includes the client id in `aud`.
    #[serde(default)]
    pub resource: Option<String>,
}

fn jobs() -> usize {
    // Uploads are network-bound, so this is not a core count.
    8
}

fn zstd_level() -> i32 {
    3
}

fn max_retries() -> u32 {
    5
}

/// `$XDG_CONFIG_HOME`, or `~/.config`. Shared by the client config, the stored
/// token and the nix.conf `garret use` writes.
pub fn config_home() -> Result<std::path::PathBuf> {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| std::path::PathBuf::from(h).join(".config")))
        .context("neither XDG_CONFIG_HOME nor HOME is set")
}

/// The default config location: `$XDG_CONFIG_HOME/garret/config.toml`.
pub fn path() -> Result<std::path::PathBuf> {
    Ok(config_home()?.join("garret").join("config.toml"))
}

/// Loads the config from `explicit` (the `--config` flag) or the default
/// path, then applies the `GARRET_ENDPOINT` override. A missing file is an
/// error that names `garret login` as the fix.
pub fn load(explicit: Option<&str>) -> Result<Config> {
    let path = match explicit {
        Some(p) => std::path::PathBuf::from(p),
        None => path()?,
    };
    // `login` is the only way to create a config, so a missing one has exactly
    // one fix and this says it rather than reporting a bare ENOENT.
    if !path.exists() {
        anyhow::bail!(
            "no config at {} — run `garret login <pusher-url>` to create one",
            path.display()
        );
    }
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("reading client config {}", path.display()))?;
    let mut cfg: Config = toml::from_str(&text).context("parsing client config")?;
    if let Ok(endpoint) = std::env::var("GARRET_ENDPOINT") {
        cfg.endpoint = endpoint;
    }
    Ok(cfg)
}

/// TOML-quotes a string. Base64 signing keys carry `+/=` and issuer URLs carry
/// `:/`, none of which need escaping — but borrowing toml's own quoting is
/// free and removes the question entirely.
fn q(value: &str) -> String {
    toml::Value::from(value).to_string()
}

/// Renders the config `login` writes: the discovered subset, commented.
///
/// Deliberately not `#[derive(Serialize)]`. Serializing `Config` would emit
/// every default and the entire `[watch]` section — nix-db paths, cursor paths,
/// poll intervals, all daemon-only — into a laptop's config, with no comments
/// to say which of them matter. The point is to write a subset, so the subset
/// is written by hand. `renders_a_parseable_config` guards the drift.
pub fn render(endpoint: &str, discovery: &crate::discovery::Discovery) -> Result<String> {
    let oidc = discovery.oidc.as_ref().context(
        "the server's discovery document has no oidc section: no issuer sets a \
         `client_id`, so the device flow has nothing to identify itself as",
    )?;
    let client_id = oidc.client_id.as_deref().context(
        "the server advertised an issuer with no `client_id`, which `garret login` requires",
    )?;

    let mut out = String::new();
    out.push_str("# Written by `garret login`. Re-run with --force to regenerate.\n");
    out.push_str(&format!("endpoint = {}\n", q(endpoint)));
    match &discovery.puller_endpoint {
        Some(puller) => out.push_str(&format!("puller_endpoint = {}\n", q(puller))),
        // Named rather than omitted: `use`, `list` and `tree` all fail without
        // it, and a commented key is a better clue than an absent one.
        None => out.push_str(
            "# puller_endpoint — the server advertised none; `use`/`list`/`tree` need it\n",
        ),
    }
    if discovery.public_keys.is_empty() {
        out.push_str("# public_keys — the server advertised none; `garret use` needs them\n");
    } else {
        let keys: Vec<String> = discovery.public_keys.iter().map(|k| q(k)).collect();
        out.push_str(&format!("public_keys = [{}]\n", keys.join(", ")));
    }
    out.push_str(&format!(
        "\n[oidc]\nissuer = {}\nclient_id = {}\naudience = {}\n",
        q(&oidc.issuer),
        q(client_id),
        q(&oidc.audience),
    ));
    out.push_str("\n# Optional: jobs = 8, zstd_level = 3, max_retries = 5\n");
    Ok(out)
}

/// Writes the config, creating `~/.config/garret/` if it is not there — the
/// whole point being that a fresh machine needs no manual setup.
pub fn write(path: &std::path::Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    std::fs::write(path, contents).with_context(|| format!("writing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::{Discovery, Oidc};

    fn discovery() -> Discovery {
        Discovery {
            puller_endpoint: Some("https://cache.example".into()),
            public_keys: vec!["garret-1:AAAA+bb/cc=".into()],
            oidc: Some(Oidc {
                issuer: "https://id.example".into(),
                audience: "garret".into(),
                client_id: Some("garret-cli".into()),
            }),
        }
    }

    /// The template and the struct can drift; this is what stops them. It also
    /// catches `deny_unknown_fields` violations, which are otherwise only
    /// discovered by a user whose brand-new config refuses to load.
    #[test]
    fn renders_a_parseable_config() {
        let text = render("https://push.example", &discovery()).unwrap();
        let cfg: Config = toml::from_str(&text).unwrap();
        assert_eq!(cfg.endpoint, "https://push.example");
        assert_eq!(
            cfg.puller_endpoint.as_deref(),
            Some("https://cache.example")
        );
        assert_eq!(cfg.public_keys, ["garret-1:AAAA+bb/cc="]);
        assert_eq!(cfg.oidc.client_id.as_deref(), Some("garret-cli"));
        assert_eq!(cfg.oidc.audience, "garret");
        // Defaults survive being absent from the template.
        assert_eq!(cfg.jobs, jobs());
    }

    /// A server with no `puller_endpoint` and no keys still yields a config
    /// that loads — the affected commands fail later, with their own messages.
    #[test]
    fn renders_a_parseable_config_when_the_server_advertises_little() {
        let sparse = Discovery {
            puller_endpoint: None,
            public_keys: vec![],
            ..discovery()
        };
        let text = render("https://push.example", &sparse).unwrap();
        let cfg: Config = toml::from_str(&text).unwrap();
        assert!(cfg.puller_endpoint.is_none());
        assert!(cfg.public_keys.is_empty());
    }

    #[test]
    fn rendering_refuses_a_server_that_advertises_no_client_id() {
        let no_oidc = Discovery {
            oidc: None,
            ..discovery()
        };
        assert!(render("https://push.example", &no_oidc).is_err());
    }
}
