//! Store watcher: pushes store paths as they become valid (spec 06-client).
//!
//! Source of truth is a persisted cursor over `ValidPaths.id` in the Nix
//! database. That column is AUTOINCREMENT — monotonic, never reused — so the
//! watcher is complete by construction: catch-up after downtime, the initial
//! scan and an offline backlog are all just "the cursor is old", not special
//! cases needing their own code.

use std::{collections::HashSet, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, params};
use tokio::net::UnixDatagram;

use crate::push::{PathInfo, Pusher};

/// The daemon's assembled settings; [`run`](Watcher::run) is its whole life.
pub struct Watcher {
    /// Path to the Nix database (normally `/nix/var/nix/db/db.sqlite`).
    pub nix_db: String,
    /// Where the Watcher Cursor persists across restarts.
    pub cursor_path: PathBuf,
    /// How long to sleep when a poll finds nothing new.
    pub poll_interval: Duration,
    /// What not to push.
    pub filters: Filters,
    /// Failures per path before it lands on the skip-list.
    pub max_attempts: u32,
    /// Where the wake socket listens; `garret enqueue` pokes it to collapse
    /// push latency from `poll_interval` to "right after the build".
    pub socket_path: PathBuf,
}

/// Why a newly-valid path might not be worth pushing (spec 06).
#[derive(Debug, Default, Clone)]
pub struct Filters {
    /// Signed by one of these upstreams already — someone else serves it.
    pub upstream_keys: Vec<String>,
    /// Substring matches, so an operator can exclude a whole family of paths.
    pub exclude_patterns: Vec<String>,
}

/// One newly-valid store path, as the Nix database describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidPath {
    /// `ValidPaths.id` — AUTOINCREMENT, so also the cursor value.
    pub id: i64,
    /// Full store path.
    pub path: String,
    /// Its signatures, `key-name:base64` each, for the upstream filter.
    pub sigs: Vec<String>,
}

impl Filters {
    /// `.drv` files are build recipes nix fetches from its own sources, and a
    /// path already signed upstream is already served by someone else. Keeps
    /// `-source` and fixed-output paths (spec 06).
    pub fn should_skip(&self, path: &ValidPath) -> Option<&'static str> {
        if path.path.ends_with(".drv") {
            return Some("drv");
        }
        if self.exclude_patterns.iter().any(|p| path.path.contains(p)) {
            return Some("excluded");
        }
        if crate::push::signed_upstream(&path.sigs, &self.upstream_keys) {
            return Some("upstream");
        }
        None
    }
}

/// Opens the Nix database read-only and checks the schema is the one we index
/// against — a silent schema change would make the cursor meaningless.
pub fn open_nix_db(path: &str) -> Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("opening the nix database at {path}"))?;
    let has_valid_paths: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='ValidPaths'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|n| n > 0)
        .unwrap_or(false);
    if !has_valid_paths {
        bail!("{path} has no ValidPaths table — is this a nix database?");
    }
    Ok(conn)
}

/// Paths validated after `cursor`, oldest first. Signatures come along so the
/// upstream filter needs no second query.
pub fn paths_after(conn: &Connection, cursor: i64, limit: usize) -> Result<Vec<ValidPath>> {
    let mut stmt = conn
        .prepare("SELECT id, path, sigs FROM ValidPaths WHERE id > ?1 ORDER BY id ASC LIMIT ?2")?;
    Ok(stmt
        .query_map(params![cursor, limit as i64], |row| {
            let sigs: Option<String> = row.get(2)?;
            Ok(ValidPath {
                id: row.get(0)?,
                path: row.get(1)?,
                sigs: sigs
                    .unwrap_or_default()
                    .split_whitespace()
                    .map(str::to_owned)
                    .collect(),
            })
        })?
        .collect::<rusqlite::Result<_>>()?)
}

/// The newest `ValidPaths.id`, or 0 in an empty store — where a fresh cursor
/// bootstraps so only paths validated from now on push.
pub fn max_id(conn: &Connection) -> Result<i64> {
    Ok(
        conn.query_row("SELECT COALESCE(MAX(id), 0) FROM ValidPaths", [], |r| {
            r.get(0)
        })?,
    )
}

/// Reads the persisted cursor; `None` (missing or unparseable file) means
/// first run and triggers the bootstrap in [`Watcher::run`].
pub fn read_cursor(path: &std::path::Path) -> Option<i64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// Persists the cursor, creating its directory on first write.
pub fn write_cursor(path: &std::path::Path, cursor: i64) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, cursor.to_string()).with_context(|| format!("writing cursor {path:?}"))
}

/// Binds the wake socket `garret enqueue` datagrams land on. A stale file from
/// an unclean shutdown is unlinked first, so the only warning-worthy failure
/// left is a real one (usually: the directory is not writable by this user).
pub fn bind_wake_socket(path: &std::path::Path) -> Result<UnixDatagram> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating socket directory {parent:?}"))?;
    }
    if path.exists() {
        std::fs::remove_file(path).with_context(|| format!("unlinking stale socket {path:?}"))?;
    }
    let socket =
        UnixDatagram::bind(path).with_context(|| format!("binding wake socket {path:?}"))?;
    // ponytail: 0666 on purpose — the socket carries no authority. A datagram
    // only makes the watcher poll its own cursor early, which the poll timer
    // does anyway, and single-user installs run the hook as the building user.
    // Tighten this the day a real command protocol lands here.
    std::fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o666))
        .with_context(|| format!("setting permissions on {path:?}"))?;
    Ok(socket)
}

/// Sleeps until `interval` elapses or a wake datagram arrives, whichever comes
/// first. Bursts coalesce: whatever queued is drained before returning, so a
/// hook firing fifty times costs one early poll.
pub async fn wait_for_wake(socket: Option<&UnixDatagram>, interval: Duration) {
    let Some(socket) = socket else {
        return tokio::time::sleep(interval).await;
    };
    let mut buf = [0u8; 4096];
    tokio::select! {
        _ = tokio::time::sleep(interval) => {}
        received = socket.recv(&mut buf) => {
            if let Ok(n) = received {
                tracing::debug!(payload = %String::from_utf8_lossy(&buf[..n]), "woken by enqueue");
            }
            while socket.try_recv(&mut buf).is_ok() {}
        }
    }
}

impl Watcher {
    /// Runs until killed. The cursor always advances: one poison path must
    /// never wedge the pipeline, so repeated failures land on a skip-list and
    /// the watcher moves on, loudly (spec 06).
    pub async fn run(&self, pusher: &Pusher, full_sync: bool) -> Result<()> {
        let conn = open_nix_db(&self.nix_db)?;
        let mut cursor = match read_cursor(&self.cursor_path) {
            Some(cursor) => cursor,
            // Bootstrap at the end of history: only new paths push, unless
            // --full-sync opts into walking everything.
            None if full_sync => 0,
            None => max_id(&conn)?,
        };
        tracing::info!(cursor, full_sync, "store watcher starting");

        // The socket is an optimization, never a reason the backstop won't
        // run: without it every path is still pushed, just up to poll_interval
        // later.
        let wake = match bind_wake_socket(&self.socket_path) {
            Ok(socket) => Some(socket),
            Err(e) => {
                tracing::warn!(
                    socket = ?self.socket_path,
                    "wake socket unavailable; `garret enqueue` will be a no-op \
                     and pushes trail builds by up to the poll interval: {e:#}"
                );
                None
            }
        };

        let mut skipped: HashSet<String> = HashSet::new();
        let mut attempts: std::collections::HashMap<String, u32> = Default::default();

        loop {
            let batch = paths_after(&conn, cursor, 500)?;
            if batch.is_empty() {
                wait_for_wake(wake.as_ref(), self.poll_interval).await;
                continue;
            }

            for entry in batch {
                cursor = entry.id;
                if skipped.contains(&entry.path) {
                    continue;
                }
                if let Some(reason) = self.filters.should_skip(&entry) {
                    tracing::debug!(path = entry.path, reason, "skipping");
                    continue;
                }
                match self.push_path(pusher, &entry.path).await {
                    Ok(()) => {
                        attempts.remove(&entry.path);
                    }
                    Err(e) => {
                        let count = attempts.entry(entry.path.clone()).or_default();
                        *count += 1;
                        if *count >= self.max_attempts {
                            tracing::error!(
                                path = entry.path,
                                attempts = *count,
                                "giving up on this path; adding to the skip list: {e:#}"
                            );
                            skipped.insert(entry.path.clone());
                        } else {
                            tracing::warn!(path = entry.path, "push failed, will retry: {e:#}");
                        }
                    }
                }
                // Advance regardless of outcome — an old cursor is a backlog,
                // but a stuck cursor is an outage.
                write_cursor(&self.cursor_path, cursor)?;
            }
        }
    }

    async fn push_path(&self, pusher: &Pusher, path: &str) -> Result<()> {
        let closure = match crate::push::closure(std::slice::from_ref(&path.to_owned())).await {
            Ok(closure) => closure,
            // Nix-GC'd between validation and push: nothing to do, not an error.
            Err(_) if !std::path::Path::new(path).exists() => {
                tracing::debug!(path, "path vanished before push; skipping");
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        // Bare paths: dependencies register before roots, so closures
        // self-assemble and the server never needs a closed closure (spec 06).
        let missing: Vec<PathInfo> = pusher
            .missing(&closure)
            .await?
            .into_iter()
            .filter(|p| p.path == path)
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        // The daemon reports per path into the journal and draws no bar. A
        // failure must surface as an error here, because the watcher's retry
        // and skip-list logic is what decides whether the cursor advances.
        let summary = pusher
            .push_all(missing, &crate::push::Report::plain())
            .await;
        if summary.failed > 0 {
            bail!("{} path(s) failed to push", summary.failed);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(p: &str, sigs: &[&str]) -> ValidPath {
        ValidPath {
            id: 1,
            path: p.into(),
            sigs: sigs.iter().map(|s| (*s).to_owned()).collect(),
        }
    }

    #[test]
    fn drvs_and_upstream_signed_paths_are_skipped() {
        let filters = Filters {
            upstream_keys: vec!["cache.nixos.org-1".into()],
            exclude_patterns: vec!["-secret".into()],
        };
        assert_eq!(
            filters.should_skip(&path("/nix/store/aaa-thing.drv", &[])),
            Some("drv")
        );
        assert_eq!(
            filters.should_skip(&path("/nix/store/aaa-thing", &["cache.nixos.org-1:sig"])),
            Some("upstream")
        );
        assert_eq!(
            filters.should_skip(&path("/nix/store/aaa-my-secret-key", &[])),
            Some("excluded")
        );
    }

    #[test]
    fn source_and_locally_signed_paths_are_kept() {
        let filters = Filters {
            upstream_keys: vec!["cache.nixos.org-1".into()],
            exclude_patterns: vec![],
        };
        // `-source` and fixed-output paths are exactly what we want to cache.
        assert_eq!(
            filters.should_skip(&path("/nix/store/aaa-thing-source", &[])),
            None
        );
        // Signed, but by us — still ours to push.
        assert_eq!(
            filters.should_skip(&path("/nix/store/aaa-thing", &["garret-1:sig"])),
            None
        );
        // A key whose name merely contains an upstream key must not match.
        assert_eq!(
            filters.should_skip(&path(
                "/nix/store/aaa-thing",
                &["not-cache.nixos.org-1x:sig"]
            )),
            None
        );
    }

    #[tokio::test]
    async fn a_wake_datagram_cuts_the_sleep_short() {
        let dir = std::env::temp_dir().join(format!("garret-wake-{}", std::process::id()));
        let sock_path = dir.join("watch.sock");
        let socket = bind_wake_socket(&sock_path).unwrap();

        // Silence: the full interval elapses.
        let start = std::time::Instant::now();
        wait_for_wake(Some(&socket), Duration::from_millis(50)).await;
        assert!(start.elapsed() >= Duration::from_millis(50));

        // A burst of wakes: returns long before the interval, drained to empty.
        let sender = UnixDatagram::unbound().unwrap();
        sender
            .send_to(b"/nix/store/aaa-x", &sock_path)
            .await
            .unwrap();
        sender
            .send_to(b"/nix/store/bbb-y", &sock_path)
            .await
            .unwrap();
        let start = std::time::Instant::now();
        wait_for_wake(Some(&socket), Duration::from_secs(30)).await;
        assert!(start.elapsed() < Duration::from_secs(5));
        let mut buf = [0u8; 16];
        assert!(socket.try_recv(&mut buf).is_err(), "burst must be drained");

        // Rebinding over the live socket file is the stale-socket dance.
        bind_wake_socket(&sock_path).unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_cursor_round_trips_through_disk() {
        let dir = std::env::temp_dir().join(format!("garret-cursor-{}", std::process::id()));
        let file = dir.join("cursor");
        assert_eq!(read_cursor(&file), None);
        write_cursor(&file, 42).unwrap();
        assert_eq!(read_cursor(&file), Some(42));
        std::fs::remove_dir_all(&dir).ok();
    }
}
