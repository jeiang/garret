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
use serde_json::json;
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
    /// Pin an object's closure as a GC-exempt root (spec 05)
    Pin {
        /// Pin name (re-pinning a name replaces it)
        name: String,
        /// Store-path hash of the root (the 32 characters before the first `-`)
        hash: String,
        /// Protect only this long (e.g. `36h`, `30d`); permanent by default
        #[arg(long)]
        expires: Option<String>,
    },
    /// Remove a pin by name
    Unpin { name: String },
    /// Audit row ⇔ blob consistency (spec 02/03); dry-run by default
    Fsck {
        /// Delete dangling rows and size-mismatched rows
        #[arg(long)]
        repair: bool,
        /// Compare DB `file_size` against the S3 object's actual size
        #[arg(long)]
        verify_sizes: bool,
        /// Reject new pushes and wait for uploads to drain before repairing
        #[arg(long, requires = "repair")]
        quiesce: bool,
        /// Machine-readable output
        #[arg(long)]
        json: bool,
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

        Command::Pin {
            name,
            hash,
            expires,
        } => {
            let expires_at = expires
                .as_deref()
                .map(|d| {
                    parse_duration(d).map(|secs| {
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs() as i64
                            + secs
                    })
                })
                .transpose()?;
            match request(
                &cli.socket,
                Request::Pin {
                    name: name.clone(),
                    hash,
                    expires_at,
                },
            )
            .await?
            {
                Response::Pin => println!("pinned {name}"),
                other => print_unexpected(other),
            }
        }

        Command::Unpin { name } => match request(&cli.socket, Request::Unpin { name }).await? {
            Response::Unpin { removed: true } => println!("unpinned"),
            // Reported, not swallowed: a mistyped name would otherwise look
            // exactly like a successful unpin.
            Response::Unpin { removed: false } => {
                bail!("no pin has that name");
            }
            other => print_unexpected(other),
        },

        Command::Fsck {
            repair,
            verify_sizes,
            quiesce,
            json,
        } => match request(
            &cli.socket,
            Request::Fsck {
                repair,
                verify_sizes,
                quiesce,
            },
        )
        .await?
        {
            Response::Fsck {
                dangling,
                size_mismatches,
                orphans,
                repaired,
                quiesced,
                quiesce_drained,
            } => {
                // Orphans alone stay informational: the orphan sweep already
                // owns them on its own schedule.
                let ok = dangling.is_empty()
                    && size_mismatches.is_empty()
                    && !(quiesced && !quiesce_drained);

                if json {
                    println!(
                        "{}",
                        json!({
                            "ok": ok,
                            "dangling": dangling,
                            "size_mismatches": size_mismatches,
                            "orphans": orphans,
                            "repaired": repaired,
                            "quiesce_drained": quiesced.then_some(quiesce_drained),
                        })
                    );
                } else {
                    print_fsck_report(&dangling, &size_mismatches, &orphans, repair, repaired);
                    if quiesced && !quiesce_drained {
                        println!(
                            "quiesce timed out waiting for uploads to drain — no repair was performed"
                        );
                    }
                }

                if !ok {
                    std::process::exit(1);
                }
            }
            other => print_unexpected(other),
        },
    }
    Ok(())
}

fn print_fsck_report(
    dangling: &[garret_common::admin::FsckRow],
    size_mismatches: &[garret_common::admin::FsckSizeMismatch],
    orphans: &[String],
    repair: bool,
    repaired: usize,
) {
    if dangling.is_empty() {
        println!("dangling rows: none");
    } else {
        println!("dangling rows ({}):", dangling.len());
        for row in dangling {
            println!("  {} {}", row.store_path_hash, row.name);
        }
    }

    if size_mismatches.is_empty() {
        println!("size mismatches: none");
    } else {
        println!("size mismatches ({}):", size_mismatches.len());
        for row in size_mismatches {
            println!(
                "  {} {} (db {} bytes, s3 {} bytes)",
                row.store_path_hash, row.name, row.db_size, row.s3_size
            );
        }
    }

    if orphans.is_empty() {
        println!("orphan blobs: none");
    } else {
        println!(
            "orphan blobs ({}), informational only — handled by the orphan sweep:",
            orphans.len()
        );
        for key in orphans {
            println!("  {key}");
        }
    }

    if repair {
        println!("repaired {repaired} row(s)");
    }
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
    // A failed command must fail the process: scripts branch on the exit
    // code, and a printed error with status 0 reads as success.
    std::process::exit(1);
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

/// `<number><s|m|h|d>` → seconds. ponytail: four suffixes cover release
/// retention; reach for a duration crate only if operators ask for more.
fn parse_duration(text: &str) -> Result<i64> {
    let (digits, suffix) = text.split_at(text.len().saturating_sub(1));
    let scale = match suffix {
        "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86400,
        _ => bail!("bad duration {text:?} — use e.g. 90s, 30m, 36h, 30d"),
    };
    let n: i64 = digits
        .parse()
        .ok()
        .filter(|n| *n > 0)
        .with_context(|| format!("bad duration {text:?} — use e.g. 90s, 30m, 36h, 30d"))?;
    Ok(n * scale)
}

#[cfg(test)]
mod tests {
    use super::parse_duration;

    #[test]
    fn durations_parse_or_fail_loudly() {
        assert_eq!(parse_duration("90s").unwrap(), 90);
        assert_eq!(parse_duration("30m").unwrap(), 1800);
        assert_eq!(parse_duration("36h").unwrap(), 129600);
        assert_eq!(parse_duration("30d").unwrap(), 2_592_000);
        for bad in ["", "d", "30", "-1d", "0h", "1w"] {
            assert!(parse_duration(bad).is_err(), "{bad:?} should be rejected");
        }
    }
}
