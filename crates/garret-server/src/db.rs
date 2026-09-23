//! SQLite access. Schema per spec 02-database; the Pusher owns all writes.

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};

/// One cached store path: the unit of content in the cache, keyed by its
/// store path hash. A row exists if and only if its blob does (spec 02).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Object {
    /// 32-char nix-base32 store path hash — the object key, and the blob key
    /// via [`crate::storage::key_for`].
    pub store_path_hash: String,
    /// Full store path, e.g. `/nix/store/<hash>-<name>`.
    pub store_path: String,
    /// Basename after the hash (`hello-1.0`); indexed for browse search.
    pub name: String,
    /// Hash of the uncompressed NAR, `sha256:<nix-base32>` — client-claimed.
    pub nar_hash: String,
    /// Size of the uncompressed NAR in bytes.
    pub nar_size: i64,
    /// Hash of the stored zstd blob, computed server-side during upload.
    pub file_hash: String,
    /// Size of the stored zstd blob in bytes; what quota accounting counts.
    pub file_size: i64,
    /// Deriving `.drv` store path, when the client reported one.
    pub deriver: Option<String>,
    /// Content-address string for CA paths; input-addressed paths have none.
    pub ca: Option<String>,
    /// Reference basenames (`<hash>-<name>`), as narinfo prints them.
    pub references: Vec<String>,
    /// Ed25519 signatures (`<key-name>:<base64>`), computed on write.
    pub sigs: Vec<String>,
    /// OIDC subject that pushed this object, kept for audit.
    pub pushed_by: Option<String>,
}

/// `<hash>-<name>` → hash. Store path basenames are hash-prefixed by
/// construction; anything else (a row from before the Pusher validated its
/// input) yields a hash that matches nothing rather than a panic mid-browse.
pub fn hash_of(basename: &str) -> &str {
    basename.get(..32).unwrap_or(basename)
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS objects (
  store_path_hash   TEXT PRIMARY KEY,
  store_path        TEXT NOT NULL,
  name              TEXT NOT NULL,
  nar_hash          TEXT NOT NULL,
  nar_size          INTEGER NOT NULL,
  file_hash         TEXT NOT NULL,
  file_size         INTEGER NOT NULL,
  deriver           TEXT,
  ca                TEXT,
  sigs              TEXT NOT NULL,
  pushed_by         TEXT,
  created_at        INTEGER NOT NULL,
  last_accessed_at  INTEGER NOT NULL,
  pushed_at         INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS objects_name          ON objects(name);
CREATE INDEX IF NOT EXISTS objects_last_accessed ON objects(last_accessed_at);

CREATE TABLE IF NOT EXISTS object_refs (
  referrer  TEXT NOT NULL REFERENCES objects ON DELETE CASCADE,
  reference TEXT NOT NULL,          -- basename; may not be in the cache
  reference_hash TEXT GENERATED ALWAYS AS (substr(reference, 1, 32)) VIRTUAL,
  PRIMARY KEY (referrer, reference)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS object_refs_reference ON object_refs(reference_hash);

CREATE TABLE IF NOT EXISTS pins (
  name             TEXT PRIMARY KEY,
  store_path_hash  TEXT NOT NULL REFERENCES objects ON DELETE CASCADE,
  expires_at       INTEGER,          -- NULL = permanent
  created_at       INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS pins_hash ON pins(store_path_hash);

CREATE TABLE IF NOT EXISTS stats (
  id           INTEGER PRIMARY KEY CHECK (id = 1),
  total_bytes  INTEGER NOT NULL
);
INSERT OR IGNORE INTO stats (id, total_bytes) VALUES (1, 0);
"#;

/// Opens the database with WAL and the pragmas both services rely on.
///
/// `create` belongs to the Pusher alone — it owns the schema. The Puller opens
/// read-write (it does debounced last-accessed bumps, spec 02) but must never
/// conjure a database: an empty one at a typo'd path would serve silent 404s.
pub fn open(path: &str, create: bool) -> Result<Connection> {
    let mut flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    if create {
        flags |= OpenFlags::SQLITE_OPEN_CREATE;
    }
    let conn = Connection::open_with_flags(path, flags)
        .with_context(|| format!("opening database {path}"))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "busy_timeout", 5000)?;
    conn.pragma_update(None, "mmap_size", 512 * 1024 * 1024)?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    persist_wal(&conn)?;
    Ok(conn)
}

/// Keeps `-wal` and `-shm` on disk when the last connection closes. The Puller
/// runs as its own user and cannot create files in the database directory
/// (ADR-0009), so it can open the database only while they exist, and a
/// Pusher exiting with an error after opening would otherwise delete them on
/// its way out and take the Puller down with it.
fn persist_wal(conn: &Connection) -> Result<()> {
    let mut on: std::ffi::c_int = 1;
    // SAFETY: `conn` holds an open handle for the whole call, the schema name
    // is NUL-terminated, and PERSIST_WAL only reads the int behind the pointer,
    // during the call.
    let rc = unsafe {
        rusqlite::ffi::sqlite3_file_control(
            conn.handle(),
            c"main".as_ptr(),
            rusqlite::ffi::SQLITE_FCNTL_PERSIST_WAL,
            (&raw mut on).cast(),
        )
    };
    // An in-memory database has no file to keep.
    anyhow::ensure!(
        matches!(
            rc,
            rusqlite::ffi::SQLITE_OK | rusqlite::ffi::SQLITE_NOTFOUND
        ),
        "enabling persistent WAL: sqlite error {rc}"
    );
    Ok(())
}

/// How stale `pushed_at` must be before Negotiation rewrites it: a closure
/// pushed from several CI jobs in one run costs one write per path, not one
/// per job.
pub const PUSHED_AT_DEBOUNCE: i64 = 3600;

/// The newest cutoff [`prune`] accepts. A client that negotiated a path is
/// told it need not upload it, and relies on it until its push finishes; the
/// debounced `pushed_at` of such a path is at most [`PUSHED_AT_DEBOUNCE`]
/// behind the Negotiation, so any cutoff older than this leaves a push up to
/// 23 hours long room to finish against a complete closure.
pub const PRUNE_MIN_AGE: i64 = 86400;

/// Applies the schema (idempotent `IF NOT EXISTS`). Pusher-only, like every
/// other write.
pub fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(SCHEMA).context("applying schema")?;
    // Databases created before `pushed_at` existed: the best record of when
    // those objects were last pushed is when they were first stored.
    if conn
        .prepare("SELECT pushed_at FROM objects LIMIT 0")
        .is_err()
    {
        conn.execute_batch(
            "BEGIN;
             ALTER TABLE objects ADD COLUMN pushed_at INTEGER NOT NULL DEFAULT 0;
             UPDATE objects SET pushed_at = created_at;
             COMMIT;",
        )
        .context("adding objects.pushed_at")?;
    }
    Ok(())
}

/// The Pusher creates the database, so a Puller started first would otherwise
/// die on a missing file. Wait instead — including for the schema, since the
/// file appears (WAL, journal) before the Pusher has finished migrating.
/// Gives up after `timeout` so a typo'd `db_path` still fails loudly.
pub async fn open_when_ready(path: &str, timeout: std::time::Duration) -> Result<Connection> {
    use std::time::{Duration, Instant};

    let deadline = Instant::now() + timeout;
    let mut delay = Duration::from_millis(250);
    loop {
        match open(path, false).and_then(|conn| {
            conn.query_row(
                "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'objects'",
                [],
                |_| Ok(()),
            )
            .optional()
            .context("checking for schema")
            .map(|found| (conn, found.is_some()))
        }) {
            Ok((conn, true)) => return Ok(conn),
            Ok((_, false)) => tracing::info!("waiting for {path}: pusher has not migrated it yet"),
            Err(e) => tracing::info!("waiting for database: {e:#}"),
        }
        let now = Instant::now();
        anyhow::ensure!(
            now < deadline,
            "database {path} was not created within {timeout:?} — is the pusher running?"
        );
        tokio::time::sleep(delay.min(deadline - now)).await;
        delay = (delay * 2).min(Duration::from_secs(15));
    }
}

/// Inserts the object, its refs and the usage counter in one transaction —
/// only ever called after the blob is durable (row exists ⇒ blob exists).
///
/// IMMEDIATE, because it reads before it writes: a deferred transaction
/// would take a read lock first, and SQLite fails the later upgrade to a
/// write lock with SQLITE_BUSY at once, without waiting out `busy_timeout`,
/// whenever another connection (the Puller's bumps) holds the write lock.
pub fn insert_object(conn: &mut Connection, obj: &Object, now: i64) -> Result<()> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    // A re-push of an object we already hold must not count its bytes twice.
    let previous: i64 = tx
        .query_row(
            "SELECT file_size FROM objects WHERE store_path_hash = ?1",
            params![obj.store_path_hash],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(0);
    // An upsert, not INSERT OR REPLACE: REPLACE deletes the old row first,
    // and that delete cascades to the object's pins, so a re-push would
    // silently unpin it.
    tx.execute(
        "INSERT INTO objects (store_path_hash, store_path, name, nar_hash, nar_size,
             file_hash, file_size, deriver, ca, sigs, pushed_by, created_at, last_accessed_at,
             pushed_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?12,?12)
         ON CONFLICT (store_path_hash) DO UPDATE SET
             store_path = excluded.store_path, name = excluded.name,
             nar_hash = excluded.nar_hash, nar_size = excluded.nar_size,
             file_hash = excluded.file_hash, file_size = excluded.file_size,
             deriver = excluded.deriver, ca = excluded.ca, sigs = excluded.sigs,
             pushed_by = excluded.pushed_by, created_at = excluded.created_at,
             last_accessed_at = excluded.last_accessed_at, pushed_at = excluded.pushed_at",
        params![
            obj.store_path_hash,
            obj.store_path,
            obj.name,
            obj.nar_hash,
            obj.nar_size,
            obj.file_hash,
            obj.file_size,
            obj.deriver,
            obj.ca,
            serde_json::to_string(&obj.sigs)?,
            obj.pushed_by,
            now,
        ],
    )?;
    tx.execute(
        "DELETE FROM object_refs WHERE referrer = ?1",
        params![obj.store_path_hash],
    )?;
    {
        let mut stmt =
            tx.prepare("INSERT INTO object_refs (referrer, reference) VALUES (?1, ?2)")?;
        for r in &obj.references {
            // Self-references are stored, not dropped: the signature covers the
            // full reference list the pusher was handed, so a narinfo rendered
            // without them produces a fingerprint nix cannot match, and every
            // self-referential path -- which is most compiled packages -- fails
            // verification with "lacks a signature by a trusted key".
            //
            // The reason they were dropped (spec 02: a self-edge makes an object
            // permanently unevictable) is handled where it belongs, in
            // `evictable`, which ignores self-edges when deciding reachability.
            stmt.execute(params![obj.store_path_hash, r])?;
        }
    }
    tx.execute(
        "UPDATE stats SET total_bytes = total_bytes + ?1 WHERE id = 1",
        params![obj.file_size - previous],
    )?;
    tx.commit()?;
    Ok(())
}

/// Fetches an object with its references sorted, ready for narinfo rendering.
pub fn get_object(conn: &Connection, hash: &str) -> Result<Option<Object>> {
    Ok(get_object_and_last_accessed(conn, hash)?.map(|(obj, _)| obj))
}

/// [`get_object`] plus the row's `last_accessed_at`, read by the same query,
/// so the Puller decides whether a hit needs a bump without a second lookup
/// and without a write (spec 02).
pub fn get_object_and_last_accessed(
    conn: &Connection,
    hash: &str,
) -> Result<Option<(Object, i64)>> {
    let mut obj = conn
        .query_row(
            "SELECT store_path, name, nar_hash, nar_size, file_hash, file_size, deriver, ca,
                    sigs, pushed_by, last_accessed_at
             FROM objects WHERE store_path_hash = ?1",
            params![hash],
            |row| {
                let obj = Object {
                    store_path_hash: hash.to_owned(),
                    store_path: row.get(0)?,
                    name: row.get(1)?,
                    nar_hash: row.get(2)?,
                    nar_size: row.get(3)?,
                    file_hash: row.get(4)?,
                    file_size: row.get(5)?,
                    deriver: row.get(6)?,
                    ca: row.get(7)?,
                    references: Vec::new(),
                    sigs: serde_json::from_str::<Vec<String>>(&row.get::<_, String>(8)?)
                        .unwrap_or_default(),
                    pushed_by: row.get(9)?,
                };
                Ok((obj, row.get(10)?))
            },
        )
        .optional()?;

    if let Some((obj, _)) = obj.as_mut() {
        // Sorted: narinfo References order is part of what the signature covers.
        let mut stmt = conn
            .prepare("SELECT reference FROM object_refs WHERE referrer = ?1 ORDER BY reference")?;
        obj.references = stmt
            .query_map(params![hash], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?;
    }
    Ok(obj)
}

/// Whether the cache holds this store path hash — and, by the row ⇒ blob
/// invariant, its blob.
pub fn exists(conn: &Connection, hash: &str) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM objects WHERE store_path_hash = ?1",
            params![hash],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

/// Current cache usage from the maintained `stats` counter — a cheap read,
/// which is what lets every GC tick check quota without summing rows.
pub fn total_bytes(conn: &Connection) -> Result<i64> {
    Ok(
        conn.query_row("SELECT total_bytes FROM stats WHERE id = 1", [], |r| {
            r.get(0)
        })?,
    )
}

/// Re-derives the maintained counter from the rows themselves (spec 05: done
/// once per GC pass, so drift from any bug is corrected rather than compounded).
pub fn reconcile_total_bytes(conn: &Connection) -> Result<i64> {
    let actual: i64 =
        conn.query_row("SELECT COALESCE(SUM(file_size), 0) FROM objects", [], |r| {
            r.get(0)
        })?;
    conn.execute(
        "UPDATE stats SET total_bytes = ?1 WHERE id = 1",
        params![actual],
    )?;
    Ok(actual)
}

/// Objects no surviving object in the cache references, least-recently-accessed
/// first. Everything returned is evictable *together*: removing one unreferenced
/// object cannot make another referenced (spec 05).
///
/// Live pins are excluded. Only the pinned root needs excluding: eviction is
/// root-first, so while the pinned row survives, its `object_refs` keep every
/// closure member referenced and therefore out of this query — the whole
/// closure is protected without any walk. An expired pin simply stops
/// matching; no sweep runs (ticket 22).
pub fn evictable(conn: &Connection, limit: usize, now: i64) -> Result<Vec<(String, i64)>> {
    let mut stmt = conn.prepare(
        // `r.referrer != o.store_path_hash` skips self-edges: an object
        // referring to itself must not count as a referrer keeping itself
        // alive, or nothing would ever be evictable (spec 02).
        "SELECT o.store_path_hash, o.file_size FROM objects o
         WHERE NOT EXISTS (
             SELECT 1 FROM object_refs r
             WHERE r.reference_hash = o.store_path_hash
               AND r.referrer != o.store_path_hash
         )
         AND NOT EXISTS (
             SELECT 1 FROM pins p
             WHERE p.store_path_hash = o.store_path_hash
               AND (p.expires_at IS NULL OR p.expires_at > ?2)
         )
         ORDER BY o.last_accessed_at ASC, o.store_path_hash ASC
         LIMIT ?1",
    )?;
    Ok(stmt
        .query_map(params![limit, now], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?)
}

/// Creates or replaces a pin (idempotent, ncps semantics). Pinning a hash not
/// in the cache is a hard error, not a no-op (ticket 22).
pub fn pin(
    conn: &Connection,
    name: &str,
    hash: &str,
    expires_at: Option<i64>,
    now: i64,
) -> Result<()> {
    let exists: bool = conn
        .query_row(
            "SELECT 1 FROM objects WHERE store_path_hash = ?1",
            params![hash],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    anyhow::ensure!(exists, "{hash} is not in the cache — nothing to pin");
    conn.execute(
        "INSERT OR REPLACE INTO pins (name, store_path_hash, expires_at, created_at)
         VALUES (?1, ?2, ?3, ?4)",
        params![name, hash, expires_at, now],
    )?;
    Ok(())
}

/// Removes a pin; `false` means no pin had that name (reported, not swallowed,
/// so a typo does not look like a successful unpin).
pub fn unpin(conn: &Connection, name: &str) -> Result<bool> {
    Ok(conn.execute("DELETE FROM pins WHERE name = ?1", params![name])? > 0)
}

/// Removes the row and its refs, and debits the usage counter, in one
/// transaction. The blob is deleted after this returns — row-then-blob, so a
/// failure leaves an orphan for the sweep rather than a row with no blob.
/// IMMEDIATE for the same reason as [`insert_object`].
pub fn delete_object(conn: &mut Connection, hash: &str) -> Result<i64> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let size: i64 = tx
        .query_row(
            "SELECT file_size FROM objects WHERE store_path_hash = ?1",
            params![hash],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(0);
    tx.execute(
        "DELETE FROM objects WHERE store_path_hash = ?1",
        params![hash],
    )?;
    tx.execute(
        "UPDATE stats SET total_bytes = MAX(0, total_bytes - ?1) WHERE id = 1",
        params![size],
    )?;
    tx.commit()?;
    Ok(size)
}

/// Replaces an object's stored signatures (`garret-admin resign`).
pub fn update_sigs(conn: &mut Connection, hash: &str, sigs: &[String]) -> Result<()> {
    conn.execute(
        "UPDATE objects SET sigs = ?2 WHERE store_path_hash = ?1",
        params![hash, serde_json::to_string(sigs)?],
    )?;
    Ok(())
}

/// Every object key, for the orphan sweep to diff the bucket against.
pub fn all_hashes(conn: &Connection) -> Result<std::collections::HashSet<String>> {
    let mut stmt = conn.prepare("SELECT store_path_hash FROM objects")?;
    Ok(stmt
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?)
}

/// One row per object: hash, name, creation time and blob size — exactly
/// what `garret-admin fsck` needs to classify rows, without an N+1 fetch
/// through [`get_object`] per hash.
pub fn all_objects_brief(conn: &Connection) -> Result<Vec<(String, String, i64, i64)>> {
    let mut stmt =
        conn.prepare("SELECT store_path_hash, name, created_at, file_size FROM objects")?;
    Ok(stmt
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .collect::<rusqlite::Result<_>>()?)
}

/// Debounced last-accessed bumps (spec 02), one IMMEDIATE transaction per
/// batch: the write lock is taken up front, so a busy database fails within
/// the connection's busy timeout instead of partway through. Day granularity
/// is enough for LRU, so a row touched in the last `stale_after` seconds is
/// left alone.
pub fn bump_last_accessed<S: AsRef<str>>(
    conn: &mut Connection,
    hashes: impl IntoIterator<Item = S>,
    now: i64,
    stale_after: i64,
) -> Result<()> {
    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    {
        let mut stmt = tx.prepare(
            "UPDATE objects SET last_accessed_at = ?2
             WHERE store_path_hash = ?1 AND last_accessed_at < ?2 - ?3",
        )?;
        for hash in hashes {
            stmt.execute(params![hash.as_ref(), now, stale_after])?;
        }
    }
    tx.commit()?;
    Ok(())
}

/// Negotiation: the subset of `hashes` the cache does not hold. Every hash it
/// does hold has its `pushed_at` refreshed (debounced): the client will now
/// rely on that path instead of uploading it, so to [`prune`] it counts as
/// pushed now.
///
/// Reads first and takes the write lock only when a bump is due, so the
/// usual, debounced Negotiation stays read-only; the IMMEDIATE transaction
/// makes `busy_timeout` apply, where a deferred read-to-write upgrade would
/// fail at once under contention. [`prune`] cannot interleave: both run on
/// the Pusher's one writer connection, behind its mutex.
pub fn missing(conn: &mut Connection, hashes: &[String], now: i64) -> Result<Vec<String>> {
    let mut missing = Vec::new();
    let mut stale = Vec::new();
    {
        let mut pushed_at =
            conn.prepare_cached("SELECT pushed_at FROM objects WHERE store_path_hash = ?1")?;
        for hash in hashes {
            match pushed_at
                .query_row(params![hash], |r| r.get::<_, i64>(0))
                .optional()?
            {
                None => missing.push(hash.clone()),
                Some(at) if at < now - PUSHED_AT_DEBOUNCE => stale.push(hash),
                Some(_) => {}
            }
        }
    }
    if !stale.is_empty() {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        {
            let mut touch =
                tx.prepare("UPDATE objects SET pushed_at = ?2 WHERE store_path_hash = ?1")?;
            for hash in stale {
                touch.execute(params![hash, now])?;
            }
        }
        tx.commit()?;
    }
    Ok(missing)
}

/// One object [`prune`] removed (or would remove): hash, name, blob size.
pub type Pruned = (String, String, i64);

/// Rows [`prune`] deletes per write transaction, like GC's batches: the
/// Puller's last-accessed writes interleave between them instead of waiting
/// out one long transaction.
const PRUNE_BATCH: usize = 500;

/// Removes every object last pushed before `before` that no surviving
/// closure needs: mark from the roots that stay — objects pushed at or after
/// the cutoff and live pins — through their references, then delete what the
/// mark did not reach. Store paths form a DAG (self-edges aside), so this is
/// exactly the set that repeated root-first deletion of old objects would
/// reach. Anything unmarked is older than the cutoff by construction.
///
/// The mark is a plain read, so a dry run takes no write lock. Everything it
/// reads is written only by the Pusher, and the caller holds the Pusher's
/// connection for the whole call, so no Negotiation can refresh a doomed
/// path between mark and delete. Deletes run in short batches, referrers
/// before their references, so the cache is closed after every commit. The
/// caller deletes the blobs afterwards: row first, blob second (spec 05).
pub fn prune(conn: &mut Connection, before: i64, now: i64, dry_run: bool) -> Result<Vec<Pruned>> {
    anyhow::ensure!(
        before <= now - PRUNE_MIN_AGE,
        "the cutoff must be at least {} hours ago, so a push in progress keeps its closure",
        PRUNE_MIN_AGE / 3600
    );
    let doomed: Vec<Pruned> = conn
        .prepare(
            "WITH RECURSIVE keep(hash) AS (
                 SELECT store_path_hash FROM objects WHERE pushed_at >= ?1
                 UNION
                 SELECT store_path_hash FROM pins WHERE expires_at IS NULL OR expires_at > ?2
                 UNION
                 SELECT r.reference_hash FROM object_refs r JOIN keep ON r.referrer = keep.hash
             )
             SELECT store_path_hash, name, file_size FROM objects
             WHERE store_path_hash NOT IN (SELECT hash FROM keep)
             ORDER BY name, store_path_hash",
        )?
        .query_map(params![before, now], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?
        .collect::<rusqlite::Result<_>>()?;
    if dry_run {
        return Ok(doomed);
    }
    for batch in referrers_first(conn, &doomed)?.chunks(PRUNE_BATCH) {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        {
            let mut delete = tx.prepare("DELETE FROM objects WHERE store_path_hash = ?1")?;
            for &i in batch {
                delete.execute(params![doomed[i].0])?;
            }
        }
        let freed: i64 = batch.iter().map(|&i| doomed[i].2).sum();
        tx.execute(
            "UPDATE stats SET total_bytes = MAX(0, total_bytes - ?1) WHERE id = 1",
            params![freed],
        )?;
        tx.commit()?;
    }
    Ok(doomed)
}

/// Indexes into `doomed`, every referrer before the objects it references
/// (Kahn's algorithm over the edges inside the set; self-edges ignored).
fn referrers_first(conn: &Connection, doomed: &[Pruned]) -> Result<Vec<usize>> {
    let index: std::collections::HashMap<&str, usize> = doomed
        .iter()
        .enumerate()
        .map(|(i, (hash, _, _))| (hash.as_str(), i))
        .collect();
    let mut references = vec![Vec::new(); doomed.len()];
    let mut referrers = vec![0usize; doomed.len()];
    let mut stmt = conn.prepare("SELECT reference_hash FROM object_refs WHERE referrer = ?1")?;
    for (i, (hash, _, _)) in doomed.iter().enumerate() {
        for reference in stmt.query_map(params![hash], |row| row.get::<_, String>(0))? {
            if let Some(&j) = index.get(reference?.as_str())
                && j != i
            {
                references[i].push(j);
                referrers[j] += 1;
            }
        }
    }
    let mut ready: Vec<usize> = (0..doomed.len()).filter(|&i| referrers[i] == 0).collect();
    let mut order = Vec::with_capacity(doomed.len());
    while let Some(i) = ready.pop() {
        order.push(i);
        for &j in &references[i] {
            referrers[j] -= 1;
            if referrers[j] == 0 {
                ready.push(j);
            }
        }
    }
    anyhow::ensure!(
        order.len() == doomed.len(),
        "reference cycle among objects to prune; nothing deleted"
    );
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(hash: &str, refs: &[&str]) -> Object {
        Object {
            store_path_hash: hash.into(),
            store_path: format!("/nix/store/{hash}-thing"),
            name: "thing".into(),
            nar_hash: "sha256:x".into(),
            nar_size: 10,
            file_hash: "sha256:y".into(),
            file_size: 5,
            deriver: None,
            ca: None,
            references: refs.iter().map(|r| (*r).to_owned()).collect(),
            sigs: vec!["k:sig".into()],
            pushed_by: Some("someone".into()),
        }
    }

    fn db() -> Connection {
        let conn = open(":memory:", true).unwrap();
        migrate(&conn).unwrap();
        conn
    }

    #[test]
    fn hash_of_survives_a_multibyte_character_at_the_hash_boundary() {
        // Byte 32 falls inside `é`: slicing there used to panic the Puller's
        // browse tree while it held the connection mutex.
        let base = format!("{}é-x", "a".repeat(31));
        assert!(!crate::nix_base32::is_store_hash(hash_of(&base)));
        assert_eq!(hash_of(&format!("{}-x", "a".repeat(32))), "a".repeat(32));
    }

    #[test]
    fn round_trips_an_object_with_sorted_references() {
        let mut conn = db();
        let a = "a".repeat(32);
        let obj = object(
            &a,
            &[
                &format!("{}-zed", "c".repeat(32)),
                &format!("{}-abe", "b".repeat(32)),
            ],
        );
        insert_object(&mut conn, &obj, 100).unwrap();

        let got = get_object(&conn, &a).unwrap().unwrap();
        assert_eq!(
            got.references,
            vec![
                format!("{}-abe", "b".repeat(32)),
                format!("{}-zed", "c".repeat(32)),
            ]
        );
        assert_eq!(got.sigs, vec!["k:sig".to_string()]);
        assert!(exists(&conn, &a).unwrap());
        assert_eq!(
            missing(&mut conn, &[a.clone(), "nope".into()], 100).unwrap(),
            vec!["nope"]
        );
    }

    #[test]
    fn self_references_survive_the_round_trip_and_stay_evictable() {
        // Replaces an earlier `drops_self_references`, which pinned the
        // opposite behaviour: the signature covers the reference list as
        // pushed, self-reference included, so dropping it on write renders a
        // narinfo whose fingerprint nix cannot reproduce. Most compiled store
        // paths self-reference, so that failed verification for nearly
        // everything.
        let mut conn = db();
        let a = "a".repeat(32);
        let own = format!("{a}-thing");
        insert_object(&mut conn, &object(&a, &[&own]), 100).unwrap();

        assert_eq!(
            get_object(&conn, &a).unwrap().unwrap().references,
            vec![own],
            "self-reference must round-trip: it is covered by the signature"
        );

        // ...but a self-edge must not count as a referrer keeping the object
        // alive, or nothing would ever be evictable (spec 02).
        assert!(
            evictable(&conn, 10, 1000)
                .unwrap()
                .iter()
                .any(|(h, _)| h == &a),
            "self-reference made the object unevictable"
        );
    }

    #[test]
    fn reinserting_does_not_double_count_usage() {
        let mut conn = db();
        let a = "a".repeat(32);
        insert_object(&mut conn, &object(&a, &[]), 100).unwrap();
        insert_object(&mut conn, &object(&a, &[]), 200).unwrap();
        let total: i64 = conn
            .query_row("SELECT total_bytes FROM stats", [], |r| r.get(0))
            .unwrap();
        assert_eq!(total, 5);
    }

    #[test]
    fn only_unreferenced_objects_are_evictable_and_lru_first() {
        let mut conn = db();
        let (a, b, c) = ("a".repeat(32), "b".repeat(32), "c".repeat(32));
        // a → b, so b is pinned by a. c is loose.
        insert_object(&mut conn, &object(&a, &[&format!("{b}-dep")]), 300).unwrap();
        insert_object(&mut conn, &object(&b, &[]), 100).unwrap();
        insert_object(&mut conn, &object(&c, &[]), 200).unwrap();

        let candidates = evictable(&conn, 10, 1000).unwrap();
        let hashes: Vec<&str> = candidates.iter().map(|(h, _)| h.as_str()).collect();
        // b is referenced, so it must not appear at any price.
        assert!(
            !hashes.contains(&b.as_str()),
            "referenced object is evictable"
        );
        // c (200) is older than a (300), so it goes first.
        assert_eq!(hashes, vec![c.as_str(), a.as_str()]);

        // Evicting the root frees its dependency for the next pass — this is
        // what makes root-first eviction reclaim whole closures.
        delete_object(&mut conn, &a).unwrap();
        let after: Vec<String> = evictable(&conn, 10, 1000)
            .unwrap()
            .into_iter()
            .map(|(h, _)| h)
            .collect();
        assert!(after.contains(&b));
    }

    #[test]
    fn a_live_pin_protects_its_whole_closure_and_an_expired_one_does_not() {
        let mut conn = db();
        let (a, b) = ("a".repeat(32), "b".repeat(32));
        // a → b: pinning a must keep b too, via root-first eviction.
        insert_object(&mut conn, &object(&a, &[&format!("{b}-dep")]), 100).unwrap();
        insert_object(&mut conn, &object(&b, &[]), 100).unwrap();

        pin(&conn, "release", &a, None, 500).unwrap();
        assert!(
            evictable(&conn, 10, 1000).unwrap().is_empty(),
            "pinned closure appeared as an eviction candidate"
        );

        // Idempotent re-pin with an expiry; once past it, protection lapses
        // with no sweep — the pin simply stops matching.
        pin(&conn, "release", &a, Some(900), 500).unwrap();
        assert!(evictable(&conn, 10, 800).unwrap().is_empty());
        let after: Vec<String> = evictable(&conn, 10, 1000)
            .unwrap()
            .into_iter()
            .map(|(h, _)| h)
            .collect();
        assert_eq!(after, vec![a.clone()], "expired pin still protecting");

        assert!(unpin(&conn, "release").unwrap());
        assert!(
            !unpin(&conn, "release").unwrap(),
            "double unpin reported ok"
        );
    }

    #[test]
    fn pinning_an_unknown_hash_is_a_hard_error() {
        let conn = db();
        assert!(pin(&conn, "nope", &"f".repeat(32), None, 100).is_err());
    }

    #[test]
    fn re_pushing_a_pinned_object_keeps_it_pinned() {
        let mut conn = db();
        let (a, b) = ("a".repeat(32), "b".repeat(32));
        let dep = format!("{b}-dep");
        insert_object(&mut conn, &object(&a, &[&dep]), 100).unwrap();
        insert_object(&mut conn, &object(&b, &[]), 100).unwrap();
        pin(&conn, "release", &a, None, 100).unwrap();

        insert_object(&mut conn, &object(&a, &[&dep]), 200).unwrap();
        assert!(
            evictable(&conn, 10, 1000).unwrap().is_empty(),
            "re-push dropped the pin: the pinned closure became evictable"
        );
        assert_eq!(
            get_object(&conn, &a).unwrap().unwrap().references,
            vec![dep]
        );
    }

    #[test]
    fn deleting_a_pinned_object_drops_the_pin() {
        // `garret-admin delete` is the explicit operator override; the pin
        // must not linger pointing at nothing (FK cascade).
        let mut conn = db();
        let a = "a".repeat(32);
        insert_object(&mut conn, &object(&a, &[]), 100).unwrap();
        pin(&conn, "release", &a, None, 100).unwrap();
        delete_object(&mut conn, &a).unwrap();
        let pins: i64 = conn
            .query_row("SELECT COUNT(*) FROM pins", [], |r| r.get(0))
            .unwrap();
        assert_eq!(pins, 0);
    }

    #[test]
    fn deleting_debits_usage_and_reconciles() {
        let mut conn = db();
        let (a, b) = ("a".repeat(32), "b".repeat(32));
        insert_object(&mut conn, &object(&a, &[]), 100).unwrap();
        insert_object(&mut conn, &object(&b, &[]), 100).unwrap();
        assert_eq!(total_bytes(&conn).unwrap(), 10);

        assert_eq!(delete_object(&mut conn, &a).unwrap(), 5);
        assert_eq!(total_bytes(&conn).unwrap(), 5);

        // Drift in the counter is corrected from the rows, not compounded.
        conn.execute("UPDATE stats SET total_bytes = 9999", [])
            .unwrap();
        assert_eq!(reconcile_total_bytes(&conn).unwrap(), 5);
        assert_eq!(total_bytes(&conn).unwrap(), 5);
    }

    #[test]
    fn pusher_writes_wait_out_another_connections_write_lock() {
        // The Puller's bumps take the WAL write lock on its own connection.
        // A read-then-write transaction that began deferred could not wait
        // for it: SQLite fails the read-to-write upgrade with SQLITE_BUSY at
        // once, ignoring busy_timeout, so uploads and GC failed under pull
        // load. Needs a real file: WAL does not apply to `:memory:`.
        let dir = std::env::temp_dir().join(format!("garret-db-busy-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("garret.sqlite");
        let path = path.to_str().unwrap();
        let mut conn = open(path, true).unwrap();
        migrate(&conn).unwrap();
        let hold_write_lock = || {
            let other = open(path, false).unwrap();
            other.execute_batch("BEGIN IMMEDIATE").unwrap();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(200));
                other.execute_batch("COMMIT").unwrap();
            })
        };
        let a = "a".repeat(32);

        let holder = hold_write_lock();
        insert_object(&mut conn, &object(&a, &[]), 100).unwrap();
        holder.join().unwrap();

        let holder = hold_write_lock();
        assert_eq!(delete_object(&mut conn, &a).unwrap(), 5);
        holder.join().unwrap();

        drop(conn);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn prune_removes_old_closures_but_keeps_what_survivors_need() {
        const DAY: i64 = 86400;
        let mut conn = db();
        let [a, b, s, n, p, q, c] =
            ['a', 'b', 's', 'n', 'p', 'q', 'c'].map(|x| x.to_string().repeat(32));
        let dep = |h: &str| format!("{h}-dep");
        // Old root a (self-referencing) → b (exclusive) and s (shared with
        // the new root n). Old root p is pinned, keeping q. Old c is
        // re-negotiated today, which is a push as far as prune is concerned.
        insert_object(&mut conn, &object(&a, &[&dep(&a), &dep(&b), &dep(&s)]), 100).unwrap();
        for h in [&b, &s, &q, &c] {
            insert_object(&mut conn, &object(h, &[]), 100).unwrap();
        }
        insert_object(&mut conn, &object(&p, &[&dep(&q)]), 100).unwrap();
        insert_object(&mut conn, &object(&n, &[&dep(&s)]), 10 * DAY).unwrap();
        pin(&conn, "release", &p, None, 100).unwrap();
        let now = 10 * DAY + 1;
        missing(&mut conn, std::slice::from_ref(&c), now).unwrap();

        assert!(
            prune(&mut conn, now - 1, now, true).is_err(),
            "a cutoff inside the in-progress-push window was accepted"
        );

        let expected = vec![
            (a.clone(), "thing".into(), 5),
            (b.clone(), "thing".into(), 5),
        ];
        assert_eq!(prune(&mut conn, 5 * DAY, now, true).unwrap(), expected);
        assert_eq!(
            total_bytes(&conn).unwrap(),
            35,
            "a dry run deleted something"
        );

        assert_eq!(prune(&mut conn, 5 * DAY, now, false).unwrap(), expected);
        for h in [&a, &b] {
            assert!(!exists(&conn, h).unwrap(), "{h} survived the prune");
        }
        for h in [&s, &n, &p, &q, &c] {
            assert!(exists(&conn, h).unwrap(), "{h} was pruned");
        }
        assert_eq!(total_bytes(&conn).unwrap(), 25);
    }

    #[test]
    fn prune_deletes_referrers_before_their_references() {
        // Batched deletes leave the cache closed after each commit only if no
        // batch removes a dependency while its referrer survives.
        let mut conn = db();
        let [a, b, c, d] = ['a', 'b', 'c', 'd'].map(|x| x.to_string().repeat(32));
        let dep = |h: &str| format!("{h}-dep");
        // c ← b ← a, c ← d, and b refers to itself.
        insert_object(&mut conn, &object(&c, &[]), 100).unwrap();
        insert_object(&mut conn, &object(&b, &[&dep(&b), &dep(&c)]), 100).unwrap();
        insert_object(&mut conn, &object(&a, &[&dep(&b)]), 100).unwrap();
        insert_object(&mut conn, &object(&d, &[&dep(&c)]), 100).unwrap();
        let doomed: Vec<Pruned> = [&c, &b, &a, &d]
            .map(|h| (h.clone(), "thing".into(), 5))
            .into();
        let order: Vec<&str> = referrers_first(&conn, &doomed)
            .unwrap()
            .into_iter()
            .map(|i| doomed[i].0.as_str())
            .collect();
        let at = |h: &str| order.iter().position(|x| *x == h).unwrap();
        assert_eq!(order.len(), 4);
        assert!(
            at(&a) < at(&b) && at(&b) < at(&c) && at(&d) < at(&c),
            "{order:?}"
        );
    }

    #[test]
    fn migrate_seeds_pushed_at_from_created_at() {
        let conn = db();
        conn.execute_batch("ALTER TABLE objects DROP COLUMN pushed_at")
            .unwrap();
        conn.execute(
            "INSERT INTO objects (store_path_hash, store_path, name, nar_hash, nar_size,
                 file_hash, file_size, sigs, created_at, last_accessed_at)
             VALUES ('a', '/nix/store/a-x', 'x', 'h', 1, 'f', 1, '[]', 42, 42)",
            [],
        )
        .unwrap();
        migrate(&conn).unwrap();
        let pushed: i64 = conn
            .query_row("SELECT pushed_at FROM objects", [], |r| r.get(0))
            .unwrap();
        assert_eq!(pushed, 42);
    }

    #[test]
    fn last_accessed_bumps_are_debounced() {
        let mut conn = db();
        let (a, b) = ("a".repeat(32), "b".repeat(32));
        insert_object(&mut conn, &object(&a, &[]), 1000).unwrap();
        insert_object(&mut conn, &object(&b, &[]), 1000).unwrap();

        // Within the debounce window: the row stays put.
        bump_last_accessed(&mut conn, [&a], 1001, 86400).unwrap();
        assert_eq!(last_accessed(&conn, &a), 1000);

        // Past it: every row in the batch moves, as the pull path reads it.
        bump_last_accessed(&mut conn, [&a, &b], 1000 + 86401, 86400).unwrap();
        assert_eq!(last_accessed(&conn, &a), 1000 + 86401);
        assert_eq!(last_accessed(&conn, &b), 1000 + 86401);
        assert_eq!(
            get_object_and_last_accessed(&conn, &a).unwrap().unwrap().1,
            1000 + 86401
        );
    }

    fn last_accessed(conn: &Connection, hash: &str) -> i64 {
        conn.query_row(
            "SELECT last_accessed_at FROM objects WHERE store_path_hash = ?1",
            params![hash],
            |r| r.get(0),
        )
        .unwrap()
    }

    #[test]
    fn brief_listing_carries_what_fsck_needs() {
        let mut conn = db();
        let (a, b) = ("a".repeat(32), "b".repeat(32));
        insert_object(&mut conn, &object(&a, &[]), 100).unwrap();
        insert_object(&mut conn, &object(&b, &[]), 200).unwrap();

        let mut rows = all_objects_brief(&conn).unwrap();
        rows.sort_by(|x, y| x.0.cmp(&y.0));
        assert_eq!(
            rows,
            vec![
                (a, "thing".to_string(), 100, 5),
                (b, "thing".to_string(), 200, 5),
            ]
        );
    }

    #[test]
    fn reverse_lookup_finds_referrers_by_hash() {
        let mut conn = db();
        let (a, b) = ("a".repeat(32), "b".repeat(32));
        insert_object(&mut conn, &object(&a, &[&format!("{b}-dep")]), 100).unwrap();
        let referrers: Vec<String> = conn
            .prepare("SELECT referrer FROM object_refs WHERE reference_hash = ?1")
            .unwrap()
            .query_map(params![b], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(referrers, vec![a]);
    }

    /// A database and its WAL sidecars, which outlive every connection.
    fn remove_db(path: &str) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
    }

    #[test]
    fn closing_leaves_the_wal_sidecars_for_the_puller() {
        let path = std::env::temp_dir().join("garret-persist-wal.sqlite");
        let path = path.to_str().unwrap().to_owned();
        remove_db(&path);

        let conn = open(&path, true).unwrap();
        migrate(&conn).unwrap();
        drop(conn);
        // The Puller cannot create them (spec 10), so the last close must not
        // remove them.
        for suffix in ["-wal", "-shm"] {
            assert!(
                std::path::Path::new(&format!("{path}{suffix}")).exists(),
                "{suffix} was removed on close"
            );
        }
        remove_db(&path);
    }

    #[tokio::test]
    async fn open_when_ready_waits_for_the_pusher_to_create_the_schema() {
        let path = std::env::temp_dir().join("garret-open-when-ready.sqlite");
        let path = path.to_str().unwrap().to_owned();
        // Sidecars outlive every connection, so clear an earlier run's too.
        remove_db(&path);

        let waiter = tokio::spawn({
            let path = path.clone();
            async move { open_when_ready(&path, std::time::Duration::from_secs(30)).await }
        });
        // Not ready while the file is absent, nor while it exists unmigrated.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!waiter.is_finished());
        let created = open(&path, true).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!waiter.is_finished(), "an empty database is not ready");

        migrate(&created).unwrap();
        let conn = tokio::time::timeout(std::time::Duration::from_secs(10), waiter)
            .await
            .expect("open_when_ready should return once the schema exists")
            .unwrap()
            .unwrap();
        assert!(!exists(&conn, "nope").unwrap());
        remove_db(&path);
    }

    #[tokio::test]
    async fn open_when_ready_gives_up_on_a_database_that_never_appears() {
        let err = open_when_ready(
            "/nonexistent-dir/garret.sqlite",
            std::time::Duration::from_millis(600),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("was not created"), "{err:#}");
    }
}
