//! `garret` — the CLI that drives the Pusher (spec 06-client).

use std::time::Duration;

use anyhow::{Context, Result};
use clap::{CommandFactory, Parser, Subcommand};
use garret_client::{auth, browse, config, discovery, doctor, nixconf, push, watcher};
use indicatif::MultiProgress;
use serde_json::json;

#[derive(Parser)]
#[command(name = "garret", about = "Push store paths to a garret binary cache")]
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
        query: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// Print an object's dependency tree
    Tree { hash: String },
    /// List GC-exempt pins (set with `garret-admin pin`)
    Pins,
    /// Emit a shell completion script
    Completions { shell: clap_complete::Shell },
}

impl Command {
    /// `login` must run before a config exists — that is the whole point of it —
    /// and `completions`/`logout` have nothing to read one for. `enqueue` runs
    /// inside the post-build-hook as whatever user nix says, so it must not
    /// depend on a readable config either — the socket default is its contract.
    /// Everything else still requires a config, and says so.
    fn needs_config(&self) -> bool {
        !matches!(
            self,
            Command::Login { .. }
                | Command::Logout
                | Command::Completions { .. }
                | Command::Enqueue { .. }
        )
    }
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

    let cfg = match cli.command.needs_config() {
        true => Some(config::load(cli.config.as_deref())?),
        false => None,
    };
    let http = reqwest::Client::new();

    match cli.command {
        Command::Completions { shell } => {
            clap_complete::generate(shell, &mut Cli::command(), "garret", &mut std::io::stdout());
        }

        Command::Login { url, force } => login(&http, cli.config.as_deref(), url, force).await?,

        Command::Logout => {
            let path = auth::logout()?;
            println!("logged out; removed {}", path.display());
        }

        Command::Whoami => whoami(&cfg.unwrap(), &http, cli.json).await?,

        Command::Doctor { path } => {
            // A diagnostic that hangs is a diagnostic that failed: every probe
            // gets a hard timeout, unlike the push path where a slow server is
            // the client's problem to wait out.
            let timed = reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()?;
            let checks = doctor::run(&cfg.unwrap(), &timed, path.as_deref()).await;
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

        Command::Use { print } => use_cache(&cfg.unwrap(), print)?,

        Command::Push {
            paths,
            jobs,
            dry_run,
            no_upstream_filter,
        } => {
            if paths.is_empty() {
                anyhow::bail!("nothing to push");
            }
            let cfg = cfg.unwrap();
            let pusher = pusher(&cfg, &http, jobs).await?;
            let closure = push::closure(&paths).await?;
            let closure_len = closure.len();
            // Paths an upstream cache already signs never enter the
            // Negotiation; they are reported as `upstream` instead.
            let (own, upstream) = match no_upstream_filter {
                true => (closure, Vec::new()),
                false => push::partition_upstream(closure, &cfg.upstream_keys),
            };
            let missing = pusher.missing(&own).await?;

            if dry_run {
                return dry_run_report(cli.json, closure_len, &upstream, &missing);
            }

            let report = push::Report::start(cli.json, &progress, closure_len, &upstream, &missing);
            let summary = pusher.push_all(missing, &report).await;
            report.finish(&summary);
            // Reported first, so a --json consumer always sees `done`, then the
            // exit code carries the failure.
            if summary.failed > 0 {
                anyhow::bail!("{} path(s) failed to push", summary.failed);
            }
        }

        Command::WatchStore { full_sync } => {
            let cfg = cfg.unwrap();
            // A daemon authenticates as itself, not as whoever last logged in.
            let token = match &cfg.watch.credentials_file {
                Some(path) => {
                    let (id, secret) = auth::read_client_credentials(path)?;
                    auth::client_credentials(
                        &http,
                        &cfg.oidc.issuer,
                        &id,
                        &secret,
                        cfg.oidc.resource.as_deref(),
                    )
                    .await?
                }
                None => {
                    auth::bearer_token(&http, &cfg.oidc.audience, cfg.oidc.resource.as_deref())
                        .await?
                }
            };
            let pusher = push::Pusher {
                token,
                http: http.clone(),
                endpoint: cfg.endpoint.clone(),
                jobs: cfg.jobs,
                zstd_level: cfg.zstd_level,
                max_retries: cfg.max_retries,
            };
            let watcher = watcher::Watcher {
                nix_db: cfg.watch.nix_db.clone(),
                cursor_path: cfg.watch.cursor_path.clone().into(),
                poll_interval: Duration::from_secs(cfg.watch.poll_interval_secs),
                filters: watcher::Filters {
                    // `[watch]` may override the client-wide upstream list.
                    upstream_keys: cfg
                        .watch
                        .upstream_keys
                        .clone()
                        .unwrap_or_else(|| cfg.upstream_keys.clone()),
                    exclude_patterns: cfg.watch.exclude_patterns.clone(),
                },
                max_attempts: cfg.watch.max_attempts,
                socket_path: cfg.watch.socket_path.clone().into(),
            };
            watcher.run(&pusher, full_sync).await?;
        }

        Command::Enqueue { paths, socket } => enqueue(paths, &socket).await,

        Command::List { query, limit } => {
            let cfg = cfg.unwrap();
            let (puller, token) = browse_target(&cfg, &http).await?;
            let raw = browse::list(&http, &puller, &token, query.as_deref(), limit).await?;
            if cli.json {
                println!("{raw}");
                return Ok(());
            }
            let page: browse::Page =
                serde_json::from_value(raw).context("parsing the object list")?;
            for object in &page.objects {
                println!(
                    "{}  {:<40} {:>12} on disk",
                    &object.hash[..8.min(object.hash.len())],
                    object.name,
                    human(object.file_size),
                );
            }
            if page.next_cursor.is_some() {
                println!("… more results; narrow the query or raise --limit");
            }
        }

        Command::Tree { hash } => {
            let cfg = cfg.unwrap();
            let (puller, token) = browse_target(&cfg, &http).await?;
            let raw = browse::tree(&http, &puller, &token, &hash).await?;
            if cli.json {
                println!("{raw}");
                return Ok(());
            }
            let tree: browse::TreeNode =
                serde_json::from_value(raw).context("parsing the dependency tree")?;
            let mut out = String::new();
            browse::render(&tree, 0, &mut out);
            print!("{out}");
        }

        Command::Pins => {
            let cfg = cfg.unwrap();
            let (puller, token) = browse_target(&cfg, &http).await?;
            let raw = browse::pins(&http, &puller, &token).await?;
            if cli.json {
                println!("{raw}");
                return Ok(());
            }
            let pins: Vec<browse::Pin> =
                serde_json::from_value(raw).context("parsing the pin list")?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            for pin in &pins {
                let expiry = match pin.expires_at {
                    None => "permanent".to_owned(),
                    Some(t) if t <= now => "expired".to_owned(),
                    // Unix seconds, not a formatted date: no date dependency
                    // for one column, and --json is the machine surface.
                    Some(t) => format!("expires at {t}"),
                };
                println!(
                    "{:<20} {}  {} ({expiry})",
                    pin.name,
                    &pin.store_path_hash[..8.min(pin.store_path_hash.len())],
                    pin.store_path,
                );
            }
            if pins.is_empty() {
                eprintln!("no pins");
            }
        }
    }
    Ok(())
}

/// CLI paths, or nix's `$OUT_PATHS` (space-separated) when there are none —
/// `post-build-hook` runs its program with no arguments.
fn enqueue_paths(args: Vec<String>, out_paths: Option<&str>) -> Vec<String> {
    if !args.is_empty() {
        return args;
    }
    out_paths
        .unwrap_or_default()
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

/// `garret enqueue` — one datagram per invocation, then exit 0 no matter what.
/// The wake socket is only an optimization: on any failure the watcher's next
/// poll still pushes the paths, so a loud stderr line is the whole story.
async fn enqueue(args: Vec<String>, socket: &str) {
    let paths = enqueue_paths(args, std::env::var("OUT_PATHS").ok().as_deref());
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
    let path = match explicit {
        Some(p) => std::path::PathBuf::from(p),
        None => config::path()?,
    };
    let exists = path.exists();

    let cfg = match (&url, exists) {
        // Nothing to go on: no config to re-authenticate against, no URL to
        // bootstrap from.
        (None, false) => anyhow::bail!(
            "no config at {} — run `garret login <pusher-url>` to create one",
            path.display()
        ),
        // Pure re-authentication. The config is not touched, so a token that
        // has expired or been revoked costs nothing to replace.
        (None, true) => config::load(explicit)?,
        (Some(url), _) => {
            let discovered = discovery::fetch(http, url).await?;
            let rendered = config::render(url.trim_end_matches('/'), &discovered)?;
            if exists {
                let current = std::fs::read_to_string(&path).unwrap_or_default();
                if !force {
                    anyhow::bail!(
                        "{} already exists. Re-run with --force to replace it with:\n\n{}\n\
                         (hand-edited `jobs`/`zstd_level` would be lost; current contents:\n\n{})",
                        path.display(),
                        rendered.trim_end(),
                        current.trim_end(),
                    );
                }
            }
            config::write(&path, &rendered)?;
            eprintln!("wrote {}", path.display());
            config::load(explicit)?
        }
    };

    let client_id = cfg
        .oidc
        .client_id
        .as_deref()
        .context("`oidc.client_id` is required for `garret login`")?;
    auth::device_login(
        http,
        &cfg.oidc.issuer,
        client_id,
        cfg.oidc.resource.as_deref(),
    )
    .await?;
    println!(
        "Logged in; refresh token stored at {}",
        auth::token_path()?.display()
    );
    Ok(())
}

/// Proves the token is not merely present but accepted: an empty
/// `missing-paths` batch is a valid, near-free request that exercises exactly
/// the authentication path `push` uses.
async fn whoami(cfg: &config::Config, http: &reqwest::Client, as_json: bool) -> Result<()> {
    let token = auth::bearer_token(http, &cfg.oidc.audience, cfg.oidc.resource.as_deref()).await?;
    let claims = auth::peek_claims(&token).unwrap_or_default();

    let response = http
        .post(format!("{}/api/v1/missing-paths", cfg.endpoint))
        .bearer_auth(&token)
        .json(&Vec::<String>::new())
        .send()
        .await
        .context("probing the Pusher")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!(
            "the Pusher rejected this token with {status}: {}",
            body.trim()
        );
    }

    let expires_at = claims.exp.map(|exp| {
        let remaining = exp - now();
        format!("{exp} ({})", human_duration(remaining))
    });

    if as_json {
        println!(
            "{}",
            json!({
                "endpoint": cfg.endpoint,
                "puller_endpoint": cfg.puller_endpoint,
                "issuer": cfg.oidc.issuer,
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
    println!("issuer     {}", cfg.oidc.issuer);
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

    // The silent failure this command exists to prevent.
    if nixconf::honoured(puller) == Some(false) {
        eprintln!(
            "\nwarning: nix does not list {puller} as a substituter, so it is ignoring \
             your user nix.conf.\nThat happens when your user is not in `trusted-users`. \
             On NixOS, add instead:\n\n{}",
            nixconf::nixos_snippet(puller, &cfg.public_keys)
        );
    }
    Ok(())
}

/// `--dry-run` reuses the push schema exactly — same events, same field names —
/// so a consumer needs one parser rather than two. Upstream-filtered paths keep
/// their real `upstream` status: the filter runs identically either way.
fn dry_run_report(
    as_json: bool,
    closure: usize,
    upstream: &[push::PathInfo],
    missing: &[push::PathInfo],
) -> Result<()> {
    let nar_bytes: u64 = missing.iter().map(|p| p.nar_size.max(0) as u64).sum();
    if as_json {
        println!(
            "{}",
            json!({"event":"negotiated","closure":closure,"upstream":upstream.len(),"missing":missing.len(),"nar_bytes":nar_bytes})
        );
        for info in upstream {
            println!(
                "{}",
                json!({"event":"path","path":info.path,"status":"upstream","nar_size":info.nar_size})
            );
        }
        for info in missing {
            println!(
                "{}",
                json!({"event":"path","path":info.path,"status":"would-push","nar_size":info.nar_size})
            );
        }
        println!(
            "{}",
            json!({"event":"done","pushed":0,"deduped":0,"failed":0,"nar_bytes":nar_bytes})
        );
        return Ok(());
    }
    println!(
        "{}",
        push::Report::negotiated_line(closure, upstream.len(), missing.len())
    );
    for info in upstream {
        println!("  upstream   {}", info.path);
    }
    for info in missing {
        println!("  would-push {}", info.path);
    }
    println!("{} to upload (uncompressed)", human(nar_bytes as i64));
    Ok(())
}

async fn pusher(
    cfg: &config::Config,
    http: &reqwest::Client,
    jobs: Option<usize>,
) -> Result<push::Pusher> {
    Ok(push::Pusher {
        token: auth::bearer_token(http, &cfg.oidc.audience, cfg.oidc.resource.as_deref()).await?,
        http: http.clone(),
        endpoint: cfg.endpoint.clone(),
        jobs: jobs.unwrap_or(cfg.jobs),
        zstd_level: cfg.zstd_level,
        max_retries: cfg.max_retries,
    })
}

async fn browse_target(cfg: &config::Config, http: &reqwest::Client) -> Result<(String, String)> {
    let puller = cfg
        .puller_endpoint
        .clone()
        .context("set `puller_endpoint` in the config: list/tree query the Puller")?;
    Ok((
        puller,
        auth::bearer_token(http, &cfg.oidc.audience, cfg.oidc.resource.as_deref()).await?,
    ))
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
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

/// Byte sizes for humans; the API keeps the exact number.
fn human(bytes: i64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{size:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enqueue_prefers_argv_and_falls_back_to_out_paths() {
        let argv = vec!["/nix/store/aaa-x".to_owned()];
        assert_eq!(enqueue_paths(argv.clone(), Some("/nix/store/bbb-y")), argv);
        assert_eq!(
            enqueue_paths(vec![], Some(" /nix/store/aaa-x  /nix/store/bbb-y ")),
            ["/nix/store/aaa-x", "/nix/store/bbb-y"]
        );
        assert!(enqueue_paths(vec![], None).is_empty());
        assert!(enqueue_paths(vec![], Some("  ")).is_empty());
    }
}
