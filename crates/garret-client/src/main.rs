//! `garret` — the CLI that drives the Pusher (spec 06-client).

use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use garret_client::{auth, browse, config, push, watcher};

#[derive(Parser)]
#[command(name = "garret", about = "Push store paths to a garret binary cache")]
struct Cli {
    /// Config file (defaults to $XDG_CONFIG_HOME/garret/config.toml)
    #[arg(long, global = true)]
    config: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Log in via OIDC device flow and store a refresh token
    Login,
    /// Push store paths and their closure
    Push {
        paths: Vec<String>,
        /// Concurrent uploads
        #[arg(long, short)]
        jobs: Option<usize>,
    },
    /// Watch the local nix store and push newly-built paths
    WatchStore {
        /// Walk the whole store history instead of starting at the newest path
        #[arg(long)]
        full_sync: bool,
    },
    /// Search the cache contents
    List {
        query: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
    },
    /// Print an object's dependency tree
    Tree { hash: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let cfg = config::load(cli.config.as_deref())?;
    let http = reqwest::Client::new();

    match cli.command {
        Command::Login => {
            auth::device_login(
                &http,
                &cfg.oidc.issuer,
                &cfg.oidc.client_id,
                cfg.oidc.resource.as_deref(),
            )
            .await?;
            println!(
                "Logged in; refresh token stored at {}",
                auth::token_path()?.display()
            );
        }

        Command::Push { paths, jobs } => {
            if paths.is_empty() {
                anyhow::bail!("nothing to push");
            }
            let pusher = pusher(&cfg, &http, jobs).await?;
            let closure = push::closure(&paths).await?;
            let missing = pusher.missing(&closure).await?;
            println!(
                "{} path(s) in closure, {} missing",
                closure.len(),
                missing.len()
            );
            let pushed = pusher.push_all(missing).await?;
            println!("done: {pushed} path(s) uploaded");
        }

        Command::WatchStore { full_sync } => {
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
                    upstream_keys: cfg.watch.upstream_keys.clone(),
                    exclude_patterns: cfg.watch.exclude_patterns.clone(),
                },
                max_attempts: cfg.watch.max_attempts,
            };
            watcher.run(&pusher, full_sync).await?;
        }

        Command::List { query, limit } => {
            let (puller, token) = browse_target(&cfg, &http).await?;
            let page = browse::list(&http, &puller, &token, query.as_deref(), limit).await?;
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
            let (puller, token) = browse_target(&cfg, &http).await?;
            let tree = browse::tree(&http, &puller, &token, &hash).await?;
            let mut out = String::new();
            browse::render(&tree, 0, &mut out);
            print!("{out}");
        }
    }
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
