//! `garret` — the CLI that drives the Pusher (spec 06-client).

use std::io::IsTerminal;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{CommandFactory, Parser, Subcommand};
use garret_client::{auth, browse, config, discovery, doctor, http, nixconf, push, watcher};
use garret_common::wire;
use indicatif::MultiProgress;
use serde_json::json;

#[derive(Parser)]
#[command(
    name = "garret",
    about = "Push store paths to a garret binary cache",
    after_help = "Environment:\n  \
                  GARRET_TOKEN     Bearer token to use instead of any login or client secret\n  \
                  GARRET_ENDPOINT  Pusher URL, overriding `endpoint` in the config"
)]
struct Cli {
    /// Config file (defaults to $XDG_CONFIG_HOME/garret/config.toml)
    #[arg(long, global = true)]
    config: Option<String>,
    /// Machine-readable output on stdout; everything human goes to stderr
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Log in via OIDC device flow, creating the config from the server
    Login {
        /// Pusher URL. Omit to re-authenticate against the configured one.
        url: Option<String>,
        /// Overwrite an existing config with what the server advertises
        #[arg(long)]
        force: bool,
    },
    /// Forget the stored refresh token
    Logout,
    /// Show who this machine is logged in as, and whether the server agrees
    Whoami,
    /// Diagnose the client→cache pipeline layer by layer
    Doctor {
        /// Store path (or bare 32-character hash) to ask "is this cached?"
        path: Option<String>,
    },
    /// Point the local Nix at the cache as a substituter
    Use {
        /// Print the configuration instead of writing it
        #[arg(long)]
        print: bool,
    },
    /// Push store paths and their closure
    Push {
        /// Store paths to push, with their closure; read from stdin,
        /// whitespace-separated, when none are given and stdin is not a terminal
        paths: Vec<String>,
        /// Concurrent uploads
        #[arg(long, short)]
        jobs: Option<usize>,
        /// Negotiate and report what would be uploaded, then stop
        #[arg(long)]
        dry_run: bool,
        /// Push paths even when a configured upstream cache already signs them
        #[arg(long)]
        no_upstream_filter: bool,
    },
    /// Watch the local nix store and push newly-built paths
    WatchStore {
        /// Walk the whole store history instead of starting at the newest path
        #[arg(long)]
        full_sync: bool,
        /// Push everything built since the cursor, and earlier failures, then
        /// exit; non-zero if any path failed. CI's end-of-job step, run after
        /// stopping the watch-store that pushed during the build.
        #[arg(long)]
        drain: bool,
        /// Persist the Watcher Cursor here instead of `[watch] cursor_path`
        #[arg(long)]
        cursor: Option<String>,
        /// Listen for wake datagrams here instead of `[watch] socket_path`
        #[arg(long)]
        socket: Option<String>,
    },
    /// Wake a running watch-store so it polls now (the post-build-hook stub).
    /// Exits 0 unconditionally: a hook must never block or fail the build.
    Enqueue {
        /// Store paths; omitted means $OUT_PATHS, as nix passes to the hook
        paths: Vec<String>,
        /// The watch-store wake socket
        #[arg(long, default_value = garret_client::config::DEFAULT_SOCKET_PATH)]
        socket: String,
    },
    /// Search the cache contents
    List {
        /// Substring to match against store path names; omit to list everything
        query: Option<String>,
        /// Maximum number of Objects to print
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// Print an object's dependency tree
    Tree {
        /// Store hash (32 characters) or store path of the Object
        hash: String,
    },
    /// List GC-exempt pins (set with `garret-admin pin`)
    Pins,
    /// Emit a shell completion script
    Completions {
        /// Shell to generate the script for
        shell: clap_complete::Shell,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // One MultiProgress shared by the bar and by tracing's writer, so a log line
    // suspends the bar instead of shredding it. Both draw to stderr, and
    // indicatif hides itself when stderr is not a terminal — which is why CI
    // needs no flag and gets the per-path lines as its progress indication.
    let progress = MultiProgress::new();
    tracing_subscriber::fmt()
        .with_writer(tracing_indicatif::writer::IndicatifWriter::<
            tracing_indicatif::writer::Stderr,
        >::new(progress.clone()))
        .init();

    // Only the arms that use a config load it, first thing. `login` must run
    // before a config exists — that is the whole point of it — and
    // `completions`/`logout` have nothing to read one for. `enqueue` runs
    // inside the post-build-hook as whatever user nix says, so it must not
    // depend on a readable config either — the socket default is its contract.
    let load = || config::load(cli.config.as_deref());
    // A server that never accepts becomes an error instead of a hang. A
    // connection that stalls once open is `push`'s to catch: reqwest's read
    // timeout cannot tell a slow upload from a stuck one.
    let http = http::client_builder()
        .connect_timeout(Duration::from_secs(30))
        // The servers close connections idle for 30 s; retire pooled ones
        // first so an upload is never sent on a connection mid-close.
        .pool_idle_timeout(Duration::from_secs(20))
        .build()?;

    match cli.command {
        Command::Completions { shell } => {
            clap_complete::generate(shell, &mut Cli::command(), "garret", &mut std::io::stdout());
        }

        Command::Login { url, force } => login(&http, cli.config.as_deref(), url, force).await?,

        Command::Logout => {
            let path = auth::logout()?;
            println!("logged out; removed {}", path.display());
        }

        Command::Whoami => whoami(&load()?, &http, cli.json).await?,

        Command::Doctor { path } => {
            let cfg = load()?;
            // A diagnostic that hangs is a diagnostic that failed: every probe
            // gets a hard timeout, unlike the push path, which gives up only
            // on a stalled connection, never on a slow one.
            let timed = http::client_builder()
                .timeout(Duration::from_secs(15))
                .build()?;
            let checks = doctor::run(&cfg, &timed, path.as_deref()).await;
            let failed = checks
                .iter()
                .filter(|c| c.status == doctor::Status::Fail)
                .count();
            if cli.json {
                println!(
                    "{}",
                    json!({
                        "ok": failed == 0,
                        "checks": checks.iter().map(|c| json!({
                            "name": c.name,
                            "status": c.status.as_str(),
                            "detail": c.detail,
                        })).collect::<Vec<_>>(),
                    })
                );
            } else {
                for check in &checks {
                    println!(
                        "{:<4} {:<9} {}",
                        check.status.as_str(),
                        check.name,
                        check.detail
                    );
                }
            }
            // Reported first, then the exit code carries the failure — the
            // same order push uses, so a --json consumer always gets the data.
            if failed > 0 {
                anyhow::bail!("{failed} of {} check(s) failed", checks.len());
            }
        }

        Command::Use { print } => use_cache(&load()?, print)?,

        Command::Push {
            paths,
            jobs,
            dry_run,
            no_upstream_filter,
        } => {
            // Before stdin is read, so a missing config fails without waiting on it.
            let cfg = load()?;
            let stdin = match paths.is_empty() && !std::io::stdin().is_terminal() {
                true => Some(
                    std::io::read_to_string(std::io::stdin())
                        .context("reading store paths from stdin")?,
                ),
                false => None,
            };
            let paths = paths_or_fallback(paths, stdin.as_deref());
            if paths.is_empty() {
                anyhow::bail!("nothing to push");
            }
            let mut pusher = push::Pusher::from_config(&cfg, &http)?;
            // The config's `jobs` is validated on load; the flag only here.
            if let Some(jobs) = jobs {
                anyhow::ensure!(jobs >= 1, "--jobs must be at least 1");
                pusher.jobs = jobs;
            }
            let closure = push::closure(&paths).await?;
            let closure_len = closure.len();
            let (own, upstream) = match no_upstream_filter {
                true => (closure, Vec::new()),
                false => push::partition_upstream(closure, &cfg.upstream_keys),
            };
            let missing = pusher.missing(&own).await?;

            if dry_run {
                push::Report::dry_run(cli.json, &progress, closure_len, &upstream, &missing);
                return Ok(());
            }

            let report = push::Report::start(cli.json, &progress, closure_len, &upstream, &missing);
            let pushed_any = !missing.is_empty();
            let mut summary = pusher.push_all(missing, &report).await;
            // A closure is only substitutable whole, and a path the Negotiation
            // found present can be gone by the end of the run (evicted, deleted,
            // or another pusher's upload that never finished). One more
            // Negotiation after a clean run catches it, and pushes it.
            let mut recheck = Ok(());
            if pushed_any && summary.failed == 0 {
                match pusher.missing(&own).await {
                    Ok(gone) if gone.is_empty() => {}
                    Ok(gone) => {
                        report.rechecked(&gone);
                        summary += pusher.push_all(gone, &report).await;
                    }
                    Err(e) => recheck = Err(e.context("re-checking the closure after pushing")),
                }
            }
            report.finish(&summary);
            // Reported first, so a --json consumer always sees `done`, then the
            // exit code carries the failure.
            recheck?;
            if summary.failed > 0 {
                anyhow::bail!("{} path(s) failed to push", summary.failed);
            }
        }

        Command::WatchStore {
            full_sync,
            drain,
            cursor,
            socket,
        } => {
            let cfg = load()?;
            let pusher = push::Pusher::from_config(&cfg, &http)?;
            // Minted once up front so a daemon that cannot authenticate exits
            // saying so, rather than failing every path it sees.
            pusher.tokens.get().await?;
            let watcher = watcher::Watcher::from_config(&cfg, cursor, socket);
            if !drain {
                watcher.run(&pusher, full_sync).await?;
            } else {
                let failed = watcher.drain(&pusher, full_sync).await?;
                if !failed.is_empty() {
                    anyhow::bail!(
                        "{} path(s) failed to push and stay on the failed list:\n  {}",
                        failed.len(),
                        failed.join("\n  ")
                    );
                }
                eprintln!("drained: nothing left to push");
            }
        }

        Command::Enqueue { paths, socket } => enqueue(paths, &socket).await,

        Command::List { query, limit } => {
            let cfg = load()?;
            let (puller, token) = browse_target(&cfg, &http).await?;
            let raw = browse::list(&http, &puller, &token, query.as_deref(), limit).await?;
            if cli.json {
                println!("{raw}");
                return Ok(());
            }
            let page: wire::Page =
                serde_json::from_value(raw).context("parsing the object list")?;
            print!("{}", browse::render_page(&page));
        }

        Command::Tree { hash } => {
            let cfg = load()?;
            let hash = garret_common::parse_store_hash(&hash).with_context(|| {
                format!("`{hash}` is not a 32-character store hash or a store path")
            })?;
            let (puller, token) = browse_target(&cfg, &http).await?;
            let raw = browse::tree(&http, &puller, &token, hash).await?;
            if cli.json {
                println!("{raw}");
                return Ok(());
            }
            let tree: wire::TreeNode =
                serde_json::from_value(raw).context("parsing the dependency tree")?;
            let mut out = String::new();
            browse::render(&tree, 0, &mut out);
            print!("{out}");
        }

        Command::Pins => {
            let cfg = load()?;
            let (puller, token) = browse_target(&cfg, &http).await?;
            let raw = browse::pins(&http, &puller, &token).await?;
            if cli.json {
                println!("{raw}");
                return Ok(());
            }
            let pins: Vec<wire::Pin> =
                serde_json::from_value(raw).context("parsing the pin list")?;
            print!("{}", browse::render_pins(&pins, garret_common::unix_now()));
            if pins.is_empty() {
                eprintln!("no pins");
            }
        }
    }
    Ok(())
}

/// CLI paths, or the whitespace-separated `fallback` when there are none:
/// nix's `$OUT_PATHS` for enqueue (`post-build-hook` runs its program with no
/// arguments), stdin for push.
fn paths_or_fallback(args: Vec<String>, fallback: Option<&str>) -> Vec<String> {
    if !args.is_empty() {
        return args;
    }
    fallback
        .unwrap_or_default()
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

/// `garret enqueue` — one datagram per invocation, then exit 0 no matter what.
/// The wake socket is only an optimization: on any failure the watcher's next
/// poll still pushes the paths, so a loud stderr line is the whole story.
async fn enqueue(args: Vec<String>, socket: &str) {
    let paths = paths_or_fallback(args, std::env::var("OUT_PATHS").ok().as_deref());
    if paths.is_empty() {
        eprintln!("garret enqueue: no paths given and $OUT_PATHS is empty; nothing to do");
        return;
    }
    let payload = paths.join(" ");
    let send = async {
        tokio::net::UnixDatagram::unbound()?
            .send_to(payload.as_bytes(), socket)
            .await
    };
    if let Err(e) = send.await {
        eprintln!(
            "garret enqueue: could not wake watch-store at {socket}: {e}; \
             the paths will be picked up on its next poll"
        );
    }
}

/// `garret login` — the only command that can create a config, which is why it
/// is also the only one allowed to run without one.
async fn login(
    http: &reqwest::Client,
    explicit: Option<&str>,
    url: Option<String>,
    force: bool,
) -> Result<()> {
    let cfg = match url {
        // Pure re-authentication. The config is not touched, so a token that
        // has expired or been revoked costs nothing to replace. Without a
        // config there is nothing to re-authenticate against, and `load`
        // says to pass a URL.
        None => config::load(explicit)?,
        Some(url) => {
            let path = config::resolve_path(explicit)?;
            let url = url.trim_end_matches('/');
            let discovered = discovery::fetch(http, url).await?;
            let rendered = config::render(url, &discovered)?;
            let current = garret_client::files::read_optional(&path)?;
            match current {
                // Re-running against the server that wrote this config: nothing
                // to replace, so nothing to refuse.
                Some(current) if current == rendered => {
                    eprintln!("{} is already up to date", path.display());
                }
                Some(current) if !force => anyhow::bail!(
                    "{} already exists. Re-run with --force to replace it with:\n\n{}\n\
                     (hand-edited `jobs`/`zstd_level` would be lost; current contents:\n\n{})",
                    path.display(),
                    rendered.trim_end(),
                    current.trim_end(),
                ),
                _ => {
                    config::write(&path, &rendered)?;
                    eprintln!("wrote {}", path.display());
                }
            }
            if !discovered.public_keys.is_empty() {
                eprintln!(
                    "`garret use` will make nix trust these signing keys: {}",
                    discovered.public_keys.join(" ")
                );
            }
            config::load(explicit)?
        }
    };

    let client_id = cfg
        .oidc
        .client_id
        .as_deref()
        .context("`oidc.client_id` is required for `garret login`")?;
    let issuer = cfg
        .oidc
        .issuer
        .as_deref()
        .context("`oidc.issuer` is required to log in")?;
    let stored = auth::device_login(http, issuer, client_id, cfg.oidc.resource.as_deref()).await?;
    match stored {
        true => println!(
            "Logged in; refresh token stored at {}",
            auth::token_path()?.display()
        ),
        false => eprintln!(
            "warning: logged in, but the issuer returned no refresh token, so nothing was \
             stored and later commands cannot get a token; let client `{client_id}` use \
             refresh tokens (the `offline_access` scope) at the issuer"
        ),
    }
    Ok(())
}

/// Proves the token is not merely present but accepted, with the same empty
/// Negotiation `doctor` sends: it crosses exactly the authentication path
/// `push` uses.
async fn whoami(cfg: &config::Config, http: &reqwest::Client, as_json: bool) -> Result<()> {
    let pusher = push::Pusher::from_config(cfg, http)?;
    pusher.negotiate(&[]).await?;
    // After the Negotiation, which may have replaced a rejected token.
    let claims = auth::peek_claims(&pusher.tokens.get().await?).unwrap_or_default();

    let issuer = cfg.oidc.issuer.as_deref();
    let expires_at = claims.exp.map(|exp| {
        let remaining = exp - garret_common::unix_now();
        format!("{exp} ({})", human_duration(remaining))
    });

    if as_json {
        println!(
            "{}",
            json!({
                "endpoint": cfg.endpoint,
                "puller_endpoint": cfg.puller_endpoint,
                "issuer": issuer,
                "audience": cfg.oidc.audience,
                "subject": claims.sub,
                "expires_at": claims.exp,
            })
        );
        return Ok(());
    }
    println!("endpoint   {}", cfg.endpoint);
    println!(
        "puller     {}",
        cfg.puller_endpoint.as_deref().unwrap_or("(unset)")
    );
    println!("issuer     {}", issuer.unwrap_or("(unset)"));
    println!("audience   {}", cfg.oidc.audience);
    println!(
        "subject    {}",
        claims.sub.as_deref().unwrap_or("(unknown)")
    );
    println!(
        "expires    {}",
        expires_at.as_deref().unwrap_or("(unknown)")
    );
    println!("authenticated: the Pusher accepted this token");
    Ok(())
}

fn use_cache(cfg: &config::Config, print: bool) -> Result<()> {
    let puller = cfg.puller_endpoint.as_deref().context(
        "set `puller_endpoint` in the config: `use` points nix at the Puller, not the Pusher",
    )?;
    if cfg.public_keys.is_empty() {
        anyhow::bail!(
            "no `public_keys` in the config — nix would fetch from the cache and then \
             refuse every path as unsigned.\nRe-run `garret login --force {}` to pick them \
             up from the server.",
            cfg.endpoint
        );
    }
    // Checked again here, not only at login: the config may predate the check
    // or be hand-edited, and this is the write that makes nix trust the values.
    discovery::check_url("puller_endpoint", puller)?;
    for key in &cfg.public_keys {
        discovery::check_key(key)?;
    }

    if print {
        print!("{}", nixconf::lines(puller, &cfg.public_keys));
        println!("\n# Or, on NixOS:\n");
        print!("{}", nixconf::nixos_snippet(puller, &cfg.public_keys));
        return Ok(());
    }

    match nixconf::apply(puller, &cfg.public_keys)? {
        nixconf::Outcome::AlreadyPresent(path) => {
            println!("{} already names {puller}", path.display())
        }
        nixconf::Outcome::Added(path) => println!("added {puller} to {}", path.display()),
    }

    // The silent failure this command exists to prevent: the snippet is
    // written, but nix will not read it, so the cache is not in use. A script
    // running `garret use` must see that as a failure.
    if nixconf::honoured(puller) == Some(false) {
        anyhow::bail!(
            "nix does not list {puller} as a substituter, so it is ignoring your user \
             nix.conf.\nThat happens when your user is not in `trusted-users`. \
             On NixOS, add instead:\n\n{}",
            nixconf::nixos_snippet(puller, &cfg.public_keys)
        );
    }
    Ok(())
}

async fn browse_target(cfg: &config::Config, http: &reqwest::Client) -> Result<(String, String)> {
    let puller = cfg
        .puller_endpoint
        .clone()
        .context("set `puller_endpoint` in the config: list/tree query the Puller")?;
    let token = auth::TokenSource::for_config(http, cfg)?.get().await?;
    Ok((puller, token))
}

/// Relative time for a token expiry; the JSON output keeps the raw epoch.
fn human_duration(seconds: i64) -> String {
    if seconds <= 0 {
        return "expired".into();
    }
    match seconds {
        s if s < 90 => format!("in {s}s"),
        s if s < 5400 => format!("in {}m", s / 60),
        s if s < 172800 => format!("in {}h", s / 3600),
        s => format!("in {}d", s / 86400),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argv_wins_over_the_whitespace_separated_fallback() {
        let argv = vec!["/nix/store/aaa-x".to_owned()];
        assert_eq!(
            paths_or_fallback(argv.clone(), Some("/nix/store/bbb-y")),
            argv
        );
        assert_eq!(
            paths_or_fallback(vec![], Some(" /nix/store/aaa-x  /nix/store/bbb-y ")),
            ["/nix/store/aaa-x", "/nix/store/bbb-y"]
        );
        assert_eq!(
            paths_or_fallback(vec![], Some("/nix/store/aaa-x\n/nix/store/bbb-y\n")),
            ["/nix/store/aaa-x", "/nix/store/bbb-y"]
        );
        assert!(paths_or_fallback(vec![], None).is_empty());
        assert!(paths_or_fallback(vec![], Some("  ")).is_empty());
    }
}
