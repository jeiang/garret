//! `garret-admin` — local operator tool (spec 10-packaging).
//!
//! Key operations are offline file operations. Everything touching the DB goes
//! through the Pusher's admin socket, because the Pusher owns all writes: this
//! process never opens the database while the service is running.

use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine, engine::general_purpose::STANDARD as B64};
use clap::{Parser, Subcommand};
use garret_common::{
    PUSH_GRACE,
    admin::{DEFAULT_ADMIN_SOCKET, FsckReport, Repair, Request, Response},
    human_bytes, unix_now,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
};

#[derive(Parser)]
#[command(name = "garret-admin", about = "Administer a garret cache")]
struct Cli {
    /// Pusher admin socket
    #[arg(long, default_value = DEFAULT_ADMIN_SOCKET, global = true)]
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
    /// Remove objects by store-path hash, row and blob; or, with
    /// `--pushed-by`, everything one subject pushed (dry-run unless `--apply`)
    Delete {
        /// Store-path hashes (the 32 characters before the first `-`), or
        /// the store paths themselves
        #[arg(
            required_unless_present = "pushed_by",
            conflicts_with = "pushed_by",
            value_parser = parse_store_hash
        )]
        hashes: Vec<String>,
        /// Select every object this subject pushed (`<issuer>#<sub>`, as the
        /// browse API's `pushed_by` shows it) instead of naming hashes
        #[arg(long)]
        pushed_by: Option<String>,
        /// Only objects first pushed at or after this: a UTC date
        /// (`2026-06-01`) or an age (`36h`)
        #[arg(long, requires = "pushed_by", conflicts_with = "hashes")]
        since: Option<String>,
        /// Delete; without this, only list what would go
        #[arg(long)]
        apply: bool,
    },
    /// Pin an object's closure as a GC-exempt root (spec 05)
    Pin {
        /// Pin name (re-pinning a name replaces it)
        name: String,
        /// The root: its store-path hash (the 32 characters before the
        /// first `-`), or the store path itself
        #[arg(value_parser = parse_store_hash)]
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
    /// Delete every closure last pushed before a cutoff, keeping anything a
    /// newer push or a live pin still needs (spec 05); dry-run by default
    Prune {
        /// Cutoff: a UTC date (`2026-06-01`) or an age (`90d`); older than
        /// the push grace (spec 05)
        #[arg(long)]
        before: String,
        /// Delete; without this, only list what would be deleted
        #[arg(long)]
        apply: bool,
    },
    /// Write a consistent copy of the database without stopping either
    /// service. The Pusher writes it, so the path must be writable by the
    /// Pusher; an existing file is never overwritten.
    Backup {
        /// Destination file (created with mode 0600)
        path: PathBuf,
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
            let key = ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng);
            // Nix's format: name:base64(seed ++ public), the whole 64 bytes.
            let mut material = key.to_bytes().to_vec();
            material.extend_from_slice(&key.verifying_key().to_bytes());
            write_secret(&out, &format!("{name}:{}\n", B64.encode(&material)))?;
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
                        human_bytes(total_bytes),
                        human_bytes(quota as i64),
                        100.0 * total_bytes as f64 / quota as f64
                    ),
                    None => println!("usage:             {} (no quota)", human_bytes(total_bytes)),
                }
                println!("uploads in flight: {uploads_in_flight}");
            }
            other => return Err(unexpected(other)),
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
                    human_bytes(bytes_freed)
                );
                if candidates_exhausted {
                    println!(
                        "warning: still above the low watermark with nothing evictable — \
                         everything left is referenced, pinned, or pushed in the last {} hours",
                        PUSH_GRACE / 3600
                    );
                }
            }
            other => return Err(unexpected(other)),
        },

        Command::Resign => match request(&cli.socket, Request::Resign).await? {
            Response::Resign { resigned, refused } => {
                println!("re-signed {resigned} object(s)");
                if refused > 0 {
                    println!(
                        "refused {refused} object(s): no existing signature verifies under a \
                         configured key (tampered or signed only by a key no longer configured)"
                    );
                }
            }
            other => return Err(unexpected(other)),
        },

        Command::Delete {
            pushed_by: Some(subject),
            since,
            apply,
            ..
        } => {
            let since = since
                .map(|since| parse_cutoff(&since, unix_now()))
                .transpose()?;
            let dry_run = !apply;
            match request(
                &cli.socket,
                Request::DeletePushedBy {
                    subject,
                    since,
                    dry_run,
                },
            )
            .await?
            {
                Response::DeletePushedBy {
                    objects,
                    bytes_freed,
                } => print_listing(dry_run, &objects, bytes_freed),
                other => return Err(unexpected(other)),
            }
        }

        Command::Delete { hashes, apply, .. } => {
            let dry_run = !apply;
            match request(&cli.socket, Request::Delete { hashes, dry_run }).await? {
                Response::Delete {
                    deleted,
                    bytes_freed,
                    missing,
                } => {
                    let verb = if dry_run { "would delete" } else { "deleted" };
                    println!("{verb} {deleted} object(s), {}", human_bytes(bytes_freed));
                    if !missing.is_empty() {
                        println!("not in the cache: {}", missing.join(", "));
                    }
                }
                other => return Err(unexpected(other)),
            }
        }

        Command::Pin {
            name,
            hash,
            expires,
        } => {
            let expires_at = expires
                .as_deref()
                .map(|d| {
                    parse_duration(d)?
                        .checked_add(unix_now())
                        .with_context(|| format!("--expires {d:?} is too far in the future"))
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
                other => return Err(unexpected(other)),
            }
        }

        Command::Unpin { name } => match request(&cli.socket, Request::Unpin { name }).await? {
            Response::Unpin { removed: true } => println!("unpinned"),
            Response::Unpin { removed: false } => {
                bail!("no pin has that name");
            }
            other => return Err(unexpected(other)),
        },

        Command::Fsck {
            repair,
            verify_sizes,
            quiesce,
            json,
        } => match request(
            &cli.socket,
            Request::Fsck {
                verify_sizes,
                repair: match (repair, quiesce) {
                    (false, _) => Repair::No,
                    (true, false) => Repair::Live,
                    (true, true) => Repair::Quiesced,
                },
            },
        )
        .await?
        {
            Response::Fsck(report) => {
                if json {
                    let mut value = serde_json::to_value(&report)?;
                    value["ok"] = report.ok().into();
                    println!("{value}");
                } else {
                    print_fsck_report(&report, repair);
                }
                if !report.ok() {
                    std::process::exit(1);
                }
            }
            other => return Err(unexpected(other)),
        },

        Command::Prune { before, apply } => {
            let before = parse_cutoff(&before, unix_now())?;
            let dry_run = !apply;
            match request(&cli.socket, Request::Prune { before, dry_run }).await? {
                Response::Prune {
                    pruned,
                    bytes_freed,
                } => print_listing(dry_run, &pruned, bytes_freed),
                other => return Err(unexpected(other)),
            }
        }

        Command::Backup { path } => {
            // The Pusher resolves the path, from its own working directory.
            let path = std::path::absolute(&path)?;
            let path = path
                .to_str()
                .context("backup path is not UTF-8")?
                .to_owned();
            match request(&cli.socket, Request::Backup { path: path.clone() }).await? {
                Response::Backup { bytes } => {
                    println!("wrote {path} ({})", human_bytes(bytes as i64))
                }
                other => return Err(unexpected(other)),
            }
        }
    }
    Ok(())
}

/// `would delete`/`deleted` per basename, then the total: `prune` and
/// `delete --pushed-by` report alike.
fn print_listing(dry_run: bool, basenames: &[String], bytes_freed: i64) {
    let verb = if dry_run { "would delete" } else { "deleted" };
    for basename in basenames {
        println!("{verb} {basename}");
    }
    println!(
        "{verb} {} object(s), {}",
        basenames.len(),
        human_bytes(bytes_freed)
    );
}

fn print_fsck_report(report: &FsckReport, repair: bool) {
    let FsckReport {
        dangling,
        size_mismatches,
        orphans,
        repaired,
        quiesce_drained,
    } = report;
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
    if *quiesce_drained == Some(false) {
        println!("quiesce timed out waiting for uploads to drain — no repair was performed");
    }
}

#[derive(Subcommand)]
enum GcCommand {
    /// Run an eviction pass now
    Run,
}

/// One request, one reply; a [`Response::Error`] comes back as `Err`.
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
    match serde_json::from_str(&line).context("parsing the admin response")? {
        Response::Error { message } => Err(anyhow!(message)),
        response => Ok(response),
    }
}

/// A reply of the wrong variant for the request sent.
fn unexpected(response: Response) -> anyhow::Error {
    anyhow!("unexpected response: {response:?}")
}

/// Creates `path` holding `contents`, mode 0600 from the moment it exists:
/// writing first and chmod-ing after leaves a window in which anyone can open
/// the key and keep the descriptor. `create_new` (O_EXCL) also refuses
/// whatever is already there -- a key, or a symlink planted to redirect the
/// write.
fn write_secret(path: &std::path::Path, contents: &str) -> Result<()> {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| match e.kind() {
            std::io::ErrorKind::AlreadyExists => anyhow::anyhow!(
                "{} already exists — refusing to overwrite a signing key",
                path.display()
            ),
            _ => anyhow::Error::new(e).context(format!("creating {}", path.display())),
        })?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

/// clap value parser over [`garret_common::parse_store_hash`]: a typo cannot
/// come back from the Pusher as "not in the cache".
fn parse_store_hash(text: &str) -> Result<String> {
    match garret_common::parse_store_hash(text) {
        Some(hash) => Ok(hash.to_owned()),
        None => bail!("{text:?} is not a store path or a 32-character store-path hash"),
    }
}

/// `<number><s|m|h|d>` → seconds.
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
    n.checked_mul(scale)
        .with_context(|| format!("duration {text:?} is too long"))
}

/// A prune cutoff: `YYYY-MM-DD` (UTC midnight) or an age understood by
/// [`parse_duration`], counted back from `now`.
fn parse_cutoff(text: &str, now: i64) -> Result<i64> {
    if !text.contains('-') {
        return now
            .checked_sub(parse_duration(text)?)
            .with_context(|| format!("age {text:?} reaches before the clock's range"));
    }
    let bad = || anyhow!("bad date {text:?} — use YYYY-MM-DD or an age like 90d");
    let mut parts = text.splitn(3, '-').map(|p| p.parse::<i64>().ok());
    let (Some(Some(y)), Some(Some(m)), Some(Some(d))) = (parts.next(), parts.next(), parts.next())
    else {
        return Err(bad());
    };
    if !(1..=9999).contains(&y) || !(1..=12).contains(&m) || d < 1 {
        return Err(bad());
    }
    let first = days_from_civil(y, m, 1);
    let next_first = if m == 12 {
        days_from_civil(y + 1, 1, 1)
    } else {
        days_from_civil(y, m + 1, 1)
    };
    if d > next_first - first {
        return Err(bad());
    }
    Ok((first + d - 1) * 86400)
}

/// Days since the epoch (Howard Hinnant's days_from_civil). Assumes a valid
/// date; out-of-range days roll into the next month.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::{parse_cutoff, parse_duration, write_secret};

    #[test]
    fn durations_parse_or_fail_loudly() {
        assert_eq!(parse_duration("90s").unwrap(), 90);
        assert_eq!(parse_duration("30m").unwrap(), 1800);
        assert_eq!(parse_duration("36h").unwrap(), 129600);
        assert_eq!(parse_duration("30d").unwrap(), 2_592_000);
        for bad in ["", "d", "30", "-1d", "0h", "1w", "106751991167301d"] {
            assert!(parse_duration(bad).is_err(), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn cutoffs_parse_dates_and_ages() {
        assert_eq!(parse_cutoff("1970-01-01", 0).unwrap(), 0);
        assert_eq!(parse_cutoff("2000-03-01", 0).unwrap(), 951_868_800);
        assert_eq!(parse_cutoff("2026-09-23", 0).unwrap(), 1_790_121_600);
        assert_eq!(
            parse_cutoff("30d", 3_000_000).unwrap(),
            3_000_000 - 2_592_000
        );
        assert_eq!(parse_cutoff("2024-02-29", 0).unwrap(), 1_709_164_800);
        assert_eq!(parse_cutoff("2026-12-31", 0).unwrap(), 1_798_675_200);
        for bad in [
            "2026-13-01",
            "2026-06",
            "2026-06-00",
            "2026-02-31",
            "2026-02-29",
            "2026-04-31",
            "yesterday",
            "-1d",
        ] {
            assert!(parse_cutoff(bad, 0).is_err(), "{bad:?} should be rejected");
        }
        assert!(parse_cutoff("1s", i64::MIN).is_err(), "underflow accepted");
    }

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("garret-admin-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn secrets_are_created_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = scratch("mode");
        let key = dir.join("key");
        write_secret(&key, "k:secret\n").unwrap();
        let mode = std::fs::metadata(&key).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        assert_eq!(std::fs::read_to_string(&key).unwrap(), "k:secret\n");
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// Existing keys and planted symlinks are both refused: following a
    /// dangling symlink would write the key wherever it points.
    #[test]
    fn secrets_never_replace_or_follow_what_is_there() {
        let dir = scratch("exists");
        let key = dir.join("key");
        std::fs::write(&key, "old").unwrap();
        assert!(write_secret(&key, "new").is_err());
        assert_eq!(std::fs::read_to_string(&key).unwrap(), "old");

        let link = dir.join("link");
        let target = dir.join("elsewhere");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(write_secret(&link, "new").is_err());
        assert!(!target.exists(), "the write followed the symlink");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
