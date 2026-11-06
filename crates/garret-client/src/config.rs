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
    /// Upload attempts per path beyond the first, for 5xx answers and dropped
    /// connections. A 429 does not spend these: it waits out `Retry-After`.
    #[serde(default = "max_retries")]
    pub max_retries: u32,
    /// Seconds an upload may go without progress (no body chunk taken, or no
    /// answer once sent) before it is abandoned and retried. Keep it above
    /// the Pusher's `limits.body_idle_timeout_secs`, or the client gives up
    /// on an upload the server is still willing to wait for.
    #[serde(default = "upload_idle_secs")]
    pub upload_idle_secs: u64,
    /// Puller base URL — `list` and `tree` query the browse API, not the Pusher.
    pub puller_endpoint: Option<String>,
    /// The cache's signing keys, public halves, as `garret use` writes them into
    /// `trusted-public-keys`. Plural because the Pusher signs every object with
    /// every configured key, so a rotation has more than one live at once.
    /// Written by `garret login` from the server's discovery document.
    #[serde(default)]
    pub public_keys: Vec<String>,
    /// Key names whose signatures mean "an upstream cache already serves this
    /// path" — `push` drops such paths from the closure before the
    /// Negotiation, and the watcher skips them (unless `[watch]` overrides the
    /// list). Defaults to cache.nixos.org's key; `--no-upstream-filter`
    /// bypasses the filter for one push.
    #[serde(default = "upstream_keys")]
    pub upstream_keys: Vec<String>,
    /// Store watcher settings; defaulted so laptop configs never mention them.
    #[serde(default)]
    pub watch: Watch,
}

/// Store watcher settings; only the daemon reads these.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Watch {
    /// The Nix database the watcher polls for newly validated paths.
    #[serde(default = "nix_db")]
    pub nix_db: String,
    /// Where the Watcher Cursor persists across restarts.
    #[serde(default = "cursor_path")]
    pub cursor_path: String,
    /// Seconds between database polls when the backlog is empty.
    #[serde(default = "poll_interval")]
    pub poll_interval_secs: u64,
    /// Per-daemon override of the top-level `upstream_keys`; unset means the
    /// watcher shares the client-wide list.
    #[serde(default)]
    pub upstream_keys: Option<Vec<String>>,
    /// Substring patterns; a matching store path is never pushed.
    #[serde(default)]
    pub exclude_patterns: Vec<String>,
    /// Push attempts per path in one watcher process, the first included,
    /// before it stops retrying and leaves the path on the failed list for a
    /// restart or a drain.
    #[serde(default = "max_attempts")]
    pub max_attempts: u32,
    /// Where the wake socket listens; `garret enqueue` (the post-build-hook
    /// stub) pokes it so a fresh build is polled for immediately instead of
    /// after `poll_interval_secs`.
    #[serde(default = "socket_path")]
    pub socket_path: String,
}

/// An absent `[watch]` table must equal an empty one, not zeroed fields.
impl Default for Watch {
    fn default() -> Self {
        toml::from_str("").expect("every Watch field has a serde default")
    }
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

/// The default wake-socket path, shared with `garret enqueue`'s `--socket`
/// default so both ends agree with zero configuration.
pub const DEFAULT_SOCKET_PATH: &str = "/run/garret/watch.sock";

fn socket_path() -> String {
    DEFAULT_SOCKET_PATH.into()
}

/// Which issuer to get tokens from, and what to ask it for (spec 04-auth).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Oidc {
    /// Issuer base URL; OIDC discovery hangs off it. Needed only to log in
    /// and to use `client_secret_file`: a `GARRET_TOKEN` or GitHub Actions
    /// config has no use for one, and refreshing a stored login uses the
    /// issuer recorded in the token file.
    pub issuer: Option<String>,
    /// Only the device flow and `client_secret_file` need one; GitHub Actions
    /// callers identify themselves another way.
    pub client_id: Option<String>,
    /// A file holding only this machine's client secret (`client_id` names the
    /// client). Set, the client authenticates with the client_credentials
    /// grant instead of a human's stored login — for daemons and CI.
    #[serde(default)]
    pub client_secret_file: Option<std::path::PathBuf>,
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

fn upload_idle_secs() -> u64 {
    5 * 60
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
    anyhow::ensure!(
        cfg.upload_idle_secs > 0,
        "upload_idle_secs must be at least 1"
    );
    cfg.validate()?;
    if let Ok(endpoint) = std::env::var("GARRET_ENDPOINT") {
        cfg.endpoint = endpoint;
    }
    crate::discovery::check_url("endpoint", &cfg.endpoint)?;
    if let Some(issuer) = &cfg.oidc.issuer {
        crate::discovery::check_url("oidc.issuer", issuer)?;
    }
    Ok(cfg)
}

impl Config {
    /// Rejects zero values that would deadlock the uploader or spin the
    /// watcher, naming the key.
    fn validate(&self) -> Result<()> {
        anyhow::ensure!(self.jobs >= 1, "jobs must be at least 1");
        anyhow::ensure!(
            self.watch.poll_interval_secs >= 1,
            "watch.poll_interval_secs must be at least 1"
        );
        anyhow::ensure!(
            self.watch.max_attempts >= 1,
            "watch.max_attempts must be at least 1"
        );
        Ok(())
    }

    /// How this machine authenticates, `GARRET_TOKEN` aside: that variable
    /// overrides the token but never changes what kind of config this is.
    pub fn credential_kind(&self, github: Option<GithubActions>) -> CredentialKind<'_> {
        match (&self.oidc.client_secret_file, github) {
            (Some(path), _) => CredentialKind::ClientSecret(path),
            (None, Some(github)) => CredentialKind::GithubActions(github),
            (None, None) => CredentialKind::DeviceFlow,
        }
    }
}

/// Which credential a config uses, first match wins (spec 06).
#[derive(Debug)]
pub enum CredentialKind<'a> {
    /// `oidc.client_secret_file`: the client_credentials grant.
    ClientSecret(&'a std::path::Path),
    /// A GitHub Actions runner's OIDC token.
    GithubActions(GithubActions),
    /// The refresh token `garret login` stored.
    DeviceFlow,
}

/// A GitHub Actions runner's request for an OIDC token.
#[derive(Debug)]
pub struct GithubActions {
    /// `ACTIONS_ID_TOKEN_REQUEST_URL`.
    pub url: String,
    /// `ACTIONS_ID_TOKEN_REQUEST_TOKEN`, the bearer for that URL.
    pub request_token: String,
}

impl GithubActions {
    /// The runner's OIDC request variables, or `None` off a runner.
    pub fn from_env() -> Option<Self> {
        Self::from_vars(
            std::env::var("ACTIONS_ID_TOKEN_REQUEST_URL").ok(),
            std::env::var("ACTIONS_ID_TOKEN_REQUEST_TOKEN").ok(),
        )
    }

    /// Both variables or nothing: a token cannot be requested with only one.
    pub fn from_vars(url: Option<String>, request_token: Option<String>) -> Option<Self> {
        Some(Self {
            url: url?,
            request_token: request_token?,
        })
    }
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
    out.push_str(
        "\n# Optional: jobs = 8, zstd_level = 3, max_retries = 5,\n\
         # upstream_keys = [\"cache.nixos.org-1\"] (paths these keys sign are not pushed)\n",
    );
    Ok(out)
}

/// Writes the config, creating `~/.config/garret/` if it is not there — the
/// whole point being that a fresh machine needs no manual setup.
pub fn write(path: &std::path::Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        create_private_dir(parent)?;
    }
    std::fs::write(path, contents).with_context(|| format!("writing {}", path.display()))
}

/// Creates `dir` and any missing parents mode 0700. `~/.config/garret/` holds
/// the refresh token beside the config, and `login` writes the config first,
/// so whichever write comes first must create it private (spec 04-auth). An
/// existing directory is left as it is.
pub fn create_private_dir(dir: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .with_context(|| format!("creating {}", dir.display()))
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

    #[test]
    fn validate_rejects_zero_values() {
        let base = render("https://push.example", &discovery()).unwrap();
        let ok: Config = toml::from_str(&base).unwrap();
        ok.validate().unwrap();
        for (edit, key) in [
            ("jobs = 0\n", "jobs"),
            ("[watch]\npoll_interval_secs = 0\n", "poll_interval_secs"),
            ("[watch]\nmax_attempts = 0\n", "max_attempts"),
        ] {
            // Top-level keys must precede the first table.
            let text = if edit.starts_with('[') {
                format!("{base}\n{edit}")
            } else {
                format!("{edit}{base}")
            };
            let c: Config = toml::from_str(&text).unwrap();
            let e = c.validate().unwrap_err();
            assert!(format!("{e:#}").contains(key), "{e:#}");
        }
    }

    #[test]
    fn upload_idle_defaults_to_five_minutes() {
        let text = render("https://push.example", &discovery()).unwrap();
        let cfg: Config = toml::from_str(&text).unwrap();
        assert_eq!(cfg.upload_idle_secs, 300);
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
        assert_eq!(cfg.upstream_keys, ["cache.nixos.org-1"]);
        // Unset in [watch] means "share the top-level list".
        assert!(cfg.watch.upstream_keys.is_none());
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

    #[test]
    fn a_machine_config_needs_no_issuer_and_may_name_a_secret_file() {
        let cfg: Config = toml::from_str(
            "endpoint = \"https://push.example\"\n\
             [oidc]\naudience = \"garret\"\nclient_id = \"builder\"\n\
             client_secret_file = \"/run/secrets/garret\"\n",
        )
        .unwrap();
        assert!(cfg.oidc.issuer.is_none());
        assert_eq!(
            cfg.oidc.client_secret_file.as_deref(),
            Some(std::path::Path::new("/run/secrets/garret"))
        );
    }
}
