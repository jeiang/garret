//! `garret-admin` — local operator tool (spec 10-packaging).
//!
//! Key operations are offline file operations. Everything touching the DB goes
//! through the Pusher's admin socket, because the Pusher owns all writes: this
//! process never opens the database while the service is running.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::STANDARD as B64};
use clap::{Parser, Subcommand};
use garret_common::admin::{Request, Response};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
};

#[derive(Parser)]
#[command(name = "garret-admin", about = "Administer a garret cache")]
struct Cli {
    /// Pusher admin socket
    #[arg(long, default_value = "/run/garret/admin.sock", global = true)]
    socket: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Signing key management (offline — no running Pusher required)
    #[command(subcommand)]
    Key(KeyCommand),
    /// Object count, usage against quota, uploads in flight
    Status,
    /// Trigger a GC pass now
    #[command(name = "gc")]
    Gc {
        #[command(subcommand)]
        command: GcCommand,
    },
    /// Re-sign every object with the currently configured keys
    Resign,
    /// Remove objects by store-path hash, row and blob
    Delete {
        /// Store-path hashes (the 32 characters before the first `-`)
        #[arg(required = true)]
        hashes: Vec<String>,
    },
}

#[derive(Subcommand)]
enum KeyCommand {
    /// Write a new nix-format secret key
    Generate {
        /// Key name, as it appears in narinfo signatures
        name: String,
        /// Where to write the secret key (mode 0600)
        out: PathBuf,
    },
    /// Print the public key for nix.conf's trusted-public-keys
    Show { secret_key_file: PathBuf },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Key(KeyCommand::Generate { name, out }) => {
            if out.exists() {
                bail!(
                    "{} already exists — refusing to overwrite a signing key",
                    out.display()
                );
            }
            let key = ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng);
            // Nix's format: name:base64(seed ++ public), the whole 64 bytes.
            let mut material = key.to_bytes().to_vec();
            material.extend_from_slice(&key.verifying_key().to_bytes());
            std::fs::write(&out, format!("{name}:{}\n", B64.encode(&material)))?;
            restrict(&out)?;
            println!("wrote {}", out.display());
            println!(
                "public key: {name}:{}",
                B64.encode(key.verifying_key().to_bytes())
            );
        }

        Command::Key(KeyCommand::Show { secret_key_file }) => {
            let text = std::fs::read_to_string(&secret_key_file)
                .with_context(|| format!("reading {}", secret_key_file.display()))?;
            let (name, b64) = text
                .trim()
                .split_once(':')
                .context("malformed key file: expected `name:base64`")?;
            let bytes = B64.decode(b64).context("key is not valid base64")?;
            let seed: [u8; 32] = bytes
                .get(..32)
                .and_then(|s| s.try_into().ok())
                .context("key is too short")?;
            let key = ed25519_dalek::SigningKey::from_bytes(&seed);
            println!("{name}:{}", B64.encode(key.verifying_key().to_bytes()));
        }

        Command::Status => match request(&cli.socket, Request::Status).await? {
            Response::Status {
                objects,
                total_bytes,
                quota_bytes,
                uploads_in_flight,
            } => {
                println!("objects:           {objects}");
                match quota_bytes {
                    Some(quota) => println!(
                        "usage:             {} of {} ({:.1}%)",
                        human(total_bytes),
                        human(quota as i64),
                        100.0 * total_bytes as f64 / quota as f64
                    ),
                    None => println!("usage:             {} (no quota)", human(total_bytes)),
                }
                println!("uploads in flight: {uploads_in_flight}");
            }
            other => print_unexpected(other),
        },

        Command::Gc {
            command: GcCommand::Run,
        } => match request(&cli.socket, Request::GcRun).await? {
            Response::Gc {
                evicted,
                bytes_freed,
                candidates_exhausted,
            } => {
                println!(
                    "evicted {evicted} object(s), freeing {}",
                    human(bytes_freed)
                );
                if candidates_exhausted {
                    println!(
                        "warning: still above the low watermark with nothing evictable — \
                         everything left is referenced"
                    );
                }
            }
            other => print_unexpected(other),
        },

        Command::Resign => match request(&cli.socket, Request::Resign).await? {
            Response::Resign { resigned } => println!("re-signed {resigned} object(s)"),
            other => print_unexpected(other),
        },

        Command::Delete { hashes } => match request(&cli.socket, Request::Delete { hashes }).await?
        {
            Response::Delete {
                deleted,
                bytes_freed,
                missing,
            } => {
                println!("deleted {deleted} object(s), {bytes_freed} byte(s) freed");
                // Reported, not swallowed: a mistyped hash would otherwise
                // look exactly like a successful delete.
                if !missing.is_empty() {
                    println!("not in the cache: {}", missing.join(", "));
                }
            }
            other => print_unexpected(other),
        },
    }
    Ok(())
}

#[derive(Subcommand)]
enum GcCommand {
    /// Run an eviction pass now
    Run,
}

async fn request(socket: &str, request: Request) -> Result<Response> {
    let stream = UnixStream::connect(socket)
        .await
        .with_context(|| format!("connecting to {socket} — is the Pusher running?"))?;
    let (read, mut write) = stream.into_split();
    write
        .write_all(format!("{}\n", serde_json::to_string(&request)?).as_bytes())
        .await?;
    let mut line = String::new();
    BufReader::new(read).read_line(&mut line).await?;
    if line.is_empty() {
        bail!("the Pusher closed the connection without answering");
    }
    serde_json::from_str(&line).context("parsing the admin response")
}

fn print_unexpected(response: Response) {
    match response {
        Response::Error { message } => eprintln!("error: {message}"),
        other => eprintln!("unexpected response: {other:?}"),
    }
}

#[cfg(unix)]
fn restrict(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

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
