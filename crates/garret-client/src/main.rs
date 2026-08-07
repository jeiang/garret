//! `garret` — the CLI that drives the Pusher (spec 06-client).

use anyhow::Result;
use clap::{Parser, Subcommand};
use garret_client::{auth, config, push};

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
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = config::load(cli.config.as_deref())?;
    let http = reqwest::Client::new();

    match cli.command {
        Command::Login => {
            auth::device_login(
                &http,
                &cfg.oidc.issuer,
                &cfg.oidc.client_id,
                &cfg.oidc.audience,
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
            let pusher = push::Pusher {
                token: auth::bearer_token(&http, &cfg.oidc.audience).await?,
                http,
                endpoint: cfg.endpoint.clone(),
                jobs: jobs.unwrap_or(cfg.jobs),
                zstd_level: cfg.zstd_level,
                max_retries: cfg.max_retries,
            };

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
    }
    Ok(())
}
