//! Store watcher: pushes store paths as they become valid (spec 06-client).
//!
//! Source of truth is a persisted cursor over `ValidPaths.id` in the Nix
//! database. That column is AUTOINCREMENT — monotonic, never reused — so the
//! watcher is complete by construction: catch-up after downtime, the initial
//! scan and an offline backlog are all just "the cursor is old", not special
//! cases needing their own code.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, params};
use tokio::net::UnixDatagram;

use garret_common::hash_of_store_path;

use crate::push::{PathInfo, Pusher, Refused, key_name, retain_missing, signed_by};

/// Paths per poll, drain chunk, and retry chunk: one `nix path-info` and one
/// Negotiation each, so argv and request bodies stay bounded.
const BATCH_SIZE: usize = 500;

/// The daemon's assembled settings; [`run`](Watcher::run) is its whole life.
pub struct Watcher {
    /// Path to the Nix database (normally `/nix/var/nix/db/db.sqlite`).
    nix_db: String,
    /// Where the Watcher Cursor persists across restarts.
    cursor_path: PathBuf,
    /// How long to sleep when a poll finds nothing new.
    poll_interval: Duration,
    /// What not to push.
    filters: Filters,
    /// Push attempts per path in one process, the first included, before the
    /// watcher leaves it on the failed list for a restart or a drain.
    max_attempts: u32,
    /// Where the wake socket listens; `garret enqueue` pokes it to collapse
    /// push latency from `poll_interval` to "right after the build".
    socket_path: PathBuf,
}

/// Why a newly-valid path might not be worth pushing (spec 06).
#[derive(Debug, Default, Clone)]
struct Filters {
    /// Signed by one of these upstreams already — someone else serves it.
    upstream_keys: Vec<String>,
    /// Substring matches, so an operator can exclude a whole family of paths.
    exclude_patterns: Vec<String>,
    /// The cache's own `public_keys` (`name:base64`); a path signed by one of
    /// these names came from the cache, so pushing it back is pointless.
    own_keys: Vec<String>,
}

/// One newly-valid store path, as the Nix database describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ValidPath {
    /// `ValidPaths.id` — AUTOINCREMENT, so also the cursor value.
    id: i64,
    /// Full store path.
    path: String,
    /// Its signatures, `key-name:base64` each, for the upstream filter.
    sigs: Vec<String>,
}

impl Filters {
    /// `.drv` files are build recipes nix fetches from its own sources, and a
    /// path already signed upstream is already served by someone else. Keeps
    /// `-source` and fixed-output paths (spec 06).
    fn should_skip(&self, path: &ValidPath) -> Option<&'static str> {
        if path.path.ends_with(".drv") {
            return Some("drv");
        }
        if self.exclude_patterns.iter().any(|p| path.path.contains(p)) {
            return Some("excluded");
        }
        if signed_by(&path.sigs, self.upstream_keys.iter().map(String::as_str)) {
            return Some("upstream");
        }
        // Our own signature means this path came from the cache (or was
        // already pushed to it): CI needs no upstream_keys entry for that.
        if signed_by(&path.sigs, self.own_keys.iter().map(|key| key_name(key))) {
            return Some("own");
        }
        None
    }

    /// The store paths of `batch` worth pushing, in order; the rest are
    /// skipped with their reason logged.
    fn select(&self, batch: &[ValidPath]) -> Vec<String> {
        batch
            .iter()
            .filter(|entry| match self.should_skip(entry) {
                Some(reason) => {
                    tracing::debug!(path = entry.path, reason, "skipping");
                    false
                }
                None => true,
            })
            .map(|entry| entry.path.clone())
            .collect()
    }
}

/// Opens the Nix database read-only and checks the schema is the one we index
/// against — a silent schema change would make the cursor meaningless.
fn open_nix_db(path: &str) -> Result<Connection> {
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
        .with_context(|| format!("reading the schema of the nix database at {path}"))?;
    if !has_valid_paths {
        bail!("{path} has no ValidPaths table — is this a nix database?");
    }
    Ok(conn)
}

/// Paths validated after `cursor`, oldest first. Signatures come along so the
/// upstream filter needs no second query.
fn paths_after(conn: &Connection, cursor: i64, limit: usize) -> Result<Vec<ValidPath>> {
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
fn max_id(conn: &Connection) -> Result<i64> {
    Ok(
        conn.query_row("SELECT COALESCE(MAX(id), 0) FROM ValidPaths", [], |r| {
            r.get(0)
        })?,
    )
}

/// Reads the persisted cursor; `None` (no file) means first run and triggers
/// the bootstrap in [`Watcher::run`]. A cursor that is there but unreadable is
/// an error: bootstrapping past it would silently skip the backlog.
fn read_cursor(path: &Path) -> Result<Option<i64>> {
    crate::files::read_optional(path)?
        .map(|text| {
            text.trim()
                .parse()
                .with_context(|| format!("cursor {path:?} holds {text:?}, not a ValidPaths id"))
        })
        .transpose()
}

/// Persists the cursor, replaced whole: the daemon is stopped by a signal (CI
/// kills it before draining), and a kill mid-write must not leave an empty
/// cursor that bootstraps past the backlog.
fn write_cursor(path: &Path, cursor: i64) -> Result<()> {
    crate::files::write_atomic(path, cursor.to_string().as_bytes(), 0o666)
}

/// The failed list, `<cursor>.failed` beside the cursor, one store path per
/// line, and the attempts this process has spent on each entry.
///
/// The cursor passes a path whether or not its push worked — one poison path
/// must never wedge the pipeline — so a failure is recorded here instead,
/// until a later poll, a restart or a drain pushes it. Attempts are not
/// persisted, so a restart or a drain gives every failed path a fresh budget.
struct FailedList {
    file: PathBuf,
    paths: BTreeSet<String>,
    attempts: HashMap<String, u32>,
}

impl FailedList {
    /// Reads the list beside `cursor_path`; a missing file is an empty list,
    /// an unreadable one an error, since the next save would erase it.
    fn load(cursor_path: &Path) -> Result<Self> {
        let mut name = cursor_path.file_name().unwrap_or_default().to_os_string();
        name.push(".failed");
        let file = cursor_path.with_file_name(name);
        let paths = crate::files::read_optional(&file)?
            .map(|text| {
                text.lines()
                    .filter(|l| !l.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        Ok(Self {
            file,
            paths,
            attempts: HashMap::new(),
        })
    }

    /// Puts `path` on the list; the attempts now spent on it in this process.
    fn record_failure(&mut self, path: &str) -> u32 {
        self.paths.insert(path.to_owned());
        let spent = self.attempts.entry(path.to_owned()).or_default();
        *spent += 1;
        *spent
    }

    /// Takes a pushed `path` off the list.
    fn record_success(&mut self, path: &str) {
        self.paths.remove(path);
        self.attempts.remove(path);
    }

    /// The listed paths this process has tried fewer than `max` times.
    fn due(&self, max: u32) -> Vec<String> {
        self.paths
            .iter()
            .filter(|p| self.attempts.get(*p).copied().unwrap_or(0) < max)
            .cloned()
            .collect()
    }

    /// Applies one pushed `chunk`: each path in `still_failed` is recorded
    /// and handed to `on_fail` with the attempts now spent on it, every other
    /// path comes off the list. Returns how many came off.
    fn apply(
        &mut self,
        chunk: &[String],
        still_failed: Vec<String>,
        mut on_fail: impl FnMut(&str, u32),
    ) -> usize {
        let still_failed: HashSet<String> = still_failed.into_iter().collect();
        let mut pushed = 0;
        for path in chunk {
            if still_failed.contains(path) {
                on_fail(path, self.record_failure(path));
            } else {
                self.record_success(path);
                pushed += 1;
            }
        }
        pushed
    }

    /// Persists the list, replaced whole, removing the file once it is empty.
    fn save(&self) -> Result<()> {
        if self.paths.is_empty() {
            return match std::fs::remove_file(&self.file) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                    Err(e).with_context(|| format!("removing {:?}", self.file))
                }
                _ => Ok(()),
            };
        }
        let text: String = self.paths.iter().map(|p| format!("{p}\n")).collect();
        crate::files::write_atomic(&self.file, text.as_bytes(), 0o666)
    }
}

/// Logs a failed push of `path` with `spent` of its `max` attempts used.
fn log_failure(path: &str, spent: u32, max: u32) {
    if spent >= max {
        tracing::error!(
            path,
            attempts = spent,
            "giving up on this path until the watcher restarts or \
             `garret watch-store --drain` runs"
        );
    } else if spent == 1 {
        tracing::warn!(path, "on the failed list, retried when idle");
    } else {
        tracing::warn!(path, attempts = spent, "retry failed");
    }
}

/// Binds the wake socket `garret enqueue` datagrams land on. A stale file from
/// an unclean shutdown is unlinked first, so the only warning-worthy failure
/// left is a real one (usually: the directory is not writable by this user).
fn bind_wake_socket(path: &std::path::Path) -> Result<UnixDatagram> {
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
async fn wait_for_wake(socket: Option<&UnixDatagram>, interval: Duration) {
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
    /// The Watcher `cfg`'s `[watch]` table describes; `cursor_path` and
    /// `socket_path` override it, as the `--cursor` and `--socket` flags do.
    pub fn from_config(
        cfg: &crate::config::Config,
        cursor_path: Option<String>,
        socket_path: Option<String>,
    ) -> Self {
        let watch = &cfg.watch;
        Self {
            nix_db: watch.nix_db.clone(),
            cursor_path: cursor_path
                .unwrap_or_else(|| watch.cursor_path.clone())
                .into(),
            poll_interval: Duration::from_secs(watch.poll_interval_secs),
            filters: Filters {
                upstream_keys: watch
                    .upstream_keys
                    .clone()
                    .unwrap_or_else(|| cfg.upstream_keys.clone()),
                exclude_patterns: watch.exclude_patterns.clone(),
                own_keys: cfg.public_keys.clone(),
            },
            max_attempts: watch.max_attempts,
            socket_path: socket_path
                .unwrap_or_else(|| watch.socket_path.clone())
                .into(),
        }
    }

    /// Runs until killed. The cursor always advances: one poison path must
    /// never wedge the pipeline, so a failed push goes on the failed list and
    /// the watcher moves on, retrying it on idle polls up to `max_attempts`
    /// times, then loudly giving up until a restart or a drain (spec 06).
    pub async fn run(&self, pusher: &Pusher, full_sync: bool) -> Result<()> {
        let conn = open_nix_db(&self.nix_db)?;
        let mut cursor = match read_cursor(&self.cursor_path)? {
            Some(cursor) => cursor,
            // Bootstrap at the end of history: only new paths push, unless
            // --full-sync opts into walking everything.
            None if full_sync => 0,
            None => max_id(&conn)?,
        };
        // Written at once, so a drain later in the same CI job knows where
        // the job began even if nothing is built before it runs.
        write_cursor(&self.cursor_path, cursor)?;
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

        let mut failed = FailedList::load(&self.cursor_path)?;

        loop {
            let batch = paths_after(&conn, cursor, BATCH_SIZE)?;
            let Some(newest) = batch.last().map(|entry| entry.id) else {
                self.retry_failed(pusher, &mut failed).await?;
                wait_for_wake(wake.as_ref(), self.poll_interval).await;
                continue;
            };

            for path in push_batch(pusher, &self.filters.select(&batch)).await {
                log_failure(&path, failed.record_failure(&path), self.max_attempts);
            }
            failed.save()?;
            // Advance regardless of outcome — an old cursor is a backlog,
            // but a stuck cursor is an outage. Written once the whole batch
            // is handled, so a crash mid-batch re-pushes it (Negotiation makes
            // that a no-op for what already landed).
            cursor = newest;
            write_cursor(&self.cursor_path, cursor)?;
        }
    }

    /// One more try for each failed path with attempts left in this process,
    /// in batches of `BATCH_SIZE`.
    async fn retry_failed(&self, pusher: &Pusher, failed: &mut FailedList) -> Result<()> {
        let due = failed.due(self.max_attempts);
        if due.is_empty() {
            return Ok(());
        }
        for chunk in due.chunks(BATCH_SIZE) {
            let still_failed = push_batch(pusher, chunk).await;
            let pushed = failed.apply(chunk, still_failed, |path, spent| {
                log_failure(path, spent, self.max_attempts)
            });
            if pushed > 0 {
                tracing::info!(pushed, "pushed on retry");
            }
        }
        failed.save()
    }

    /// Pushes everything validated after the cursor as of now, and every path
    /// on the failed list, then stops: CI's end-of-job step, after the daemon
    /// that pushed during the build is killed. Returns the paths that still
    /// failed, which stay on the list; the cursor ends at the newest path seen.
    pub async fn drain(&self, pusher: &Pusher, full_sync: bool) -> Result<Vec<String>> {
        let conn = open_nix_db(&self.nix_db)?;
        let target = max_id(&conn)?;
        let mut cursor = match read_cursor(&self.cursor_path)? {
            Some(cursor) => cursor,
            None if full_sync => 0,
            // Nothing recorded where to start, so "nothing to push" would be
            // a guess — and a green CI step that pushed nothing.
            None => bail!(
                "no Watcher Cursor at {:?}: start `garret watch-store` before building \
                 so it records where to drain from, or pass --full-sync",
                self.cursor_path
            ),
        };
        let mut failed = FailedList::load(&self.cursor_path)?;
        let mut todo: Vec<String> = failed.paths.iter().cloned().collect();
        while cursor < target {
            let mut batch = paths_after(&conn, cursor, BATCH_SIZE)?;
            // Paths validated after the drain began are the daemon's (or the
            // next drain's), not this one's.
            batch.retain(|entry| entry.id <= target);
            let Some(newest) = batch.last().map(|entry| entry.id) else {
                break;
            };
            cursor = newest;
            todo.extend(
                self.filters
                    .select(&batch)
                    .into_iter()
                    .filter(|path| !failed.paths.contains(path)),
            );
        }
        tracing::info!(cursor, paths = todo.len(), "draining");

        for chunk in todo.chunks(BATCH_SIZE) {
            let still_failed = push_batch(pusher, chunk).await;
            failed.apply(chunk, still_failed, |path, _| {
                tracing::error!(path, "push failed")
            });
        }
        failed.save()?;
        write_cursor(&self.cursor_path, cursor)?;
        Ok(failed.paths.into_iter().collect())
    }
}

/// Pushes a batch of paths with one `nix path-info` and one Negotiation,
/// returning the paths that failed (never an error: a failure puts paths on
/// the failed list, it does not stop the watcher).
async fn push_batch(pusher: &Pusher, paths: &[String]) -> Vec<String> {
    if paths.is_empty() {
        return Vec::new();
    }
    let (infos, mut failed) = describe(paths).await;
    failed.extend(push_described(pusher, infos).await);
    failed
}

/// The Negotiation and uploads for described paths; the paths that failed.
///
/// The Negotiation names the batch and its *direct* references, not whole
/// closures. Dependencies need no upload from this batch (they are pushed as
/// their own newly-valid paths), but a present one must still be named,
/// because naming it is what refreshes its `pushed_at`: until this batch
/// lands nothing in the cache refers to it, and without the refresh GC could
/// see it as an unreferenced old root (spec 05 `PUSH_GRACE`). Anything deeper
/// is kept by its referrer's `object_refs` edges, so closure-wide
/// Negotiations bought nothing but load.
async fn push_described(pusher: &Pusher, infos: Vec<PathInfo>) -> Vec<String> {
    if infos.is_empty() {
        return Vec::new();
    }
    let hashes = negotiation_hashes(&infos);
    let missing = match pusher.negotiate(&hashes).await {
        Ok(missing) => missing,
        // Every failure keeps the paths on the failed list. A 403 is a policy
        // refusal no retry fixes until the policy changes, so it is said
        // loudly with the Pusher's reason; `max_attempts` bounds the retries.
        // A 401 that survives renewal is transient (JWKS fetch failure, key
        // rotation) and stays a warning.
        Err(e) => {
            if e.is::<Refused>() {
                tracing::error!(paths = infos.len(), "negotiation refused: {e:#}");
            } else {
                tracing::warn!(paths = infos.len(), "negotiation failed: {e:#}");
            }
            return infos.into_iter().map(|info| info.path).collect();
        }
    };
    let to_push = retain_missing(&infos, &missing);
    tracing::debug!(
        batch = infos.len(),
        negotiated = hashes.len(),
        missing = to_push.len(),
        "negotiated"
    );
    if to_push.is_empty() {
        return Vec::new();
    }
    let (_, failed) = pusher
        .push_tracked(to_push, &crate::push::Report::plain())
        .await;
    failed
}

/// `nix path-info` for a batch, non-recursive. A path that errors poisons the
/// whole call, so on failure each path is described alone: one that nix-GC'd
/// since validation is dropped (not an error), and one that still fails comes
/// back in the second list.
async fn describe(paths: &[String]) -> (Vec<PathInfo>, Vec<String>) {
    if let Ok(infos) = crate::push::path_info(paths, false).await {
        return (infos, Vec::new());
    }
    let mut infos = Vec::new();
    let mut failed = Vec::new();
    for path in paths {
        match crate::push::path_info(std::slice::from_ref(path), false).await {
            Ok(mut one) => infos.append(&mut one),
            Err(_) if !Path::new(path).exists() => {
                tracing::debug!(path, "path vanished before push; skipping");
            }
            Err(e) => {
                tracing::warn!(path, "nix path-info failed: {e:#}");
                failed.push(path.clone());
            }
        }
    }
    (infos, failed)
}

/// The hashes one Negotiation covers: each batch path and each direct
/// reference, once, in first-seen order.
fn negotiation_hashes(infos: &[PathInfo]) -> Vec<&str> {
    let mut seen = HashSet::new();
    infos
        .iter()
        .flat_map(|info| std::iter::once(&info.path).chain(&info.references))
        .map(|path| hash_of_store_path(path))
        .filter(|hash| seen.insert(*hash))
        .collect()
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
            own_keys: vec![],
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
            own_keys: vec![],
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

    /// Real time: under a paused clock tokio auto-advances past the 30 s sleep
    /// before the IO driver reports the queued datagram.
    #[tokio::test]
    async fn a_wake_datagram_cuts_the_sleep_short() {
        let dir = crate::testing::scratch_dir();
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
    }

    #[test]
    fn the_cursor_round_trips_through_disk() {
        let dir = crate::testing::scratch_dir();
        let file = dir.join("cursor");
        assert_eq!(read_cursor(&file).unwrap(), None);
        write_cursor(&file, 42).unwrap();
        assert_eq!(read_cursor(&file).unwrap(), Some(42));
        // A cursor that is there but unreadable is not a first run.
        std::fs::write(&file, "").unwrap();
        assert!(read_cursor(&file).is_err());
    }

    /// The failed list is what a restart or a drain retries, so it must
    /// survive the process, and an empty one must not linger as a file.
    #[test]
    fn the_failed_list_lives_beside_the_cursor_and_goes_when_empty() {
        let dir = crate::testing::scratch_dir();
        let cursor = dir.join("watcher-cursor");
        let file = dir.join("watcher-cursor.failed");
        let mut list = FailedList::load(&cursor).unwrap();
        assert!(list.paths.is_empty());

        list.record_failure("/nix/store/bbb-y");
        list.record_failure("/nix/store/aaa-x");
        list.save().unwrap();
        assert_eq!(
            std::fs::read_to_string(&file).unwrap(),
            "/nix/store/aaa-x\n/nix/store/bbb-y\n"
        );
        assert_eq!(FailedList::load(&cursor).unwrap().paths, list.paths);

        list.record_success("/nix/store/aaa-x");
        list.record_success("/nix/store/bbb-y");
        list.save().unwrap();
        assert!(!file.exists());
        list.save().unwrap();
    }

    /// Each failure spends one of the path's attempts in this process; a
    /// success clears it, and a reload (a restart or a drain) starts afresh.
    #[test]
    fn failed_paths_are_due_until_their_attempts_run_out() {
        let dir = crate::testing::scratch_dir();
        let cursor = dir.join("watcher-cursor");
        let mut list = FailedList::load(&cursor).unwrap();
        assert_eq!(list.record_failure("/nix/store/aaa-x"), 1);
        // `max_attempts = 1`: the first failure was the only attempt.
        assert!(list.due(1).is_empty());
        assert_eq!(list.due(2), ["/nix/store/aaa-x"]);
        assert_eq!(list.record_failure("/nix/store/aaa-x"), 2);
        assert!(list.due(2).is_empty());
        list.save().unwrap();

        let mut reloaded = FailedList::load(&cursor).unwrap();
        assert_eq!(reloaded.due(1), ["/nix/store/aaa-x"]);
        reloaded.record_success("/nix/store/aaa-x");
        assert!(reloaded.due(5).is_empty());
        assert_eq!(reloaded.record_failure("/nix/store/aaa-x"), 1);
    }

    /// A pushed chunk takes its successes off the list and spends an attempt
    /// on each path that failed again, reporting it with the new count.
    #[test]
    fn applying_a_chunk_clears_successes_and_counts_failures() {
        let dir = crate::testing::scratch_dir();
        let mut list = FailedList::load(&dir.join("watcher-cursor")).unwrap();
        list.record_failure("/nix/store/aaa-x");
        list.record_failure("/nix/store/bbb-y");
        let chunk = ["/nix/store/aaa-x".to_owned(), "/nix/store/bbb-y".to_owned()];
        let mut reported = Vec::new();
        let pushed = list.apply(&chunk, vec!["/nix/store/bbb-y".into()], |path, spent| {
            reported.push((path.to_owned(), spent))
        });
        assert_eq!(pushed, 1);
        assert_eq!(reported, [("/nix/store/bbb-y".to_owned(), 2)]);
        assert_eq!(list.due(5), ["/nix/store/bbb-y"]);
    }

    /// A 403 Negotiation is a policy refusal: the whole batch stays on the
    /// failed list, to push once the policy is fixed.
    #[tokio::test]
    async fn a_refused_negotiation_keeps_the_batch_on_the_failed_list() {
        let (endpoint, _) = crate::testing::answering(403, r#"{"error":"not authorized"}"#).await;
        let (pusher, _) = crate::testing::pusher(endpoint);
        let a = format!("/nix/store/{}-a", "a".repeat(32));
        let b = format!("/nix/store/{}-b", "b".repeat(32));

        let failed = push_described(&pusher, vec![info(&a, &[]), info(&b, &[&a])]).await;
        assert_eq!(failed, [a.clone(), b.clone()]);

        let dir = crate::testing::scratch_dir();
        let mut list = FailedList::load(&dir.join("watcher-cursor")).unwrap();
        for path in &failed {
            list.record_failure(path);
        }
        list.save().unwrap();
        assert_eq!(
            FailedList::load(&dir.join("watcher-cursor"))
                .unwrap()
                .due(5),
            [a, b]
        );
    }

    #[test]
    fn the_caches_own_keys_are_skipped_by_name() {
        let filters = Filters {
            upstream_keys: vec!["cache.nixos.org-1".into()],
            own_keys: vec!["garret-1:AAAA".into(), "garret-2:BBBB".into()],
            exclude_patterns: vec![],
        };
        // Signed by any of the cache's keys, whatever the signature bytes.
        assert_eq!(
            filters.should_skip(&path("/nix/store/aaa-thing", &["garret-2:other-sig"])),
            Some("own")
        );
        // A lookalike name is not the cache's key.
        assert_eq!(
            filters.should_skip(&path("/nix/store/aaa-thing", &["garret-12:sig"])),
            None
        );
    }

    fn info(path: &str, references: &[&str]) -> PathInfo {
        PathInfo {
            path: path.into(),
            nar_hash: "sha256:h".into(),
            nar_size: 1,
            references: references.iter().map(|r| (*r).to_owned()).collect(),
            deriver: None,
            ca: None,
            signatures: vec![],
        }
    }

    /// One Negotiation per batch covers the batch and its direct references,
    /// each hash once — not a closure per path.
    #[test]
    fn a_batch_negotiates_its_paths_and_direct_references_once_each() {
        let (a, b, lib, dep) = (
            format!("/nix/store/{}-a", "a".repeat(32)),
            format!("/nix/store/{}-b", "b".repeat(32)),
            format!("/nix/store/{}-lib", "l".repeat(32)),
            format!("/nix/store/{}-dep", "d".repeat(32)),
        );
        let infos = vec![
            info(&a, &[&a, &lib, &b]),
            info(&b, &[&lib, &dep]),
            info(&lib, &[]),
        ];
        let hashes = negotiation_hashes(&infos);
        assert_eq!(
            hashes,
            [
                "a".repeat(32),
                "l".repeat(32),
                "b".repeat(32),
                "d".repeat(32)
            ]
        );
    }

    #[test]
    fn selection_skips_filtered_paths_and_keeps_order() {
        let filters = Filters {
            upstream_keys: vec!["cache.nixos.org-1".into()],
            own_keys: vec![],
            exclude_patterns: vec![],
        };
        let batch = [
            path("/nix/store/aaa-one", &[]),
            path("/nix/store/bbb-two.drv", &[]),
            path("/nix/store/ccc-three", &["cache.nixos.org-1:sig"]),
            path("/nix/store/ddd-four", &[]),
        ];
        assert_eq!(
            filters.select(&batch),
            ["/nix/store/aaa-one", "/nix/store/ddd-four"]
        );
    }
}
