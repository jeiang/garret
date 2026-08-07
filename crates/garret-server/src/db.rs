//! SQLite access. Schema per spec 02-database; the Pusher owns all writes.

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Object {
    pub store_path_hash: String,
    pub store_path: String,
    pub name: String,
    pub nar_hash: String,
    pub nar_size: i64,
    pub file_hash: String,
    pub file_size: i64,
    pub deriver: Option<String>,
    pub ca: Option<String>,
    /// Reference basenames (`<hash>-<name>`), as narinfo prints them.
    pub references: Vec<String>,
    pub sigs: Vec<String>,
    pub pushed_by: Option<String>,
}

/// `<hash>-<name>` → hash. Store path basenames are hash-prefixed by construction.
pub fn hash_of(basename: &str) -> &str {
    &basename[..basename.len().min(32)]
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
  last_accessed_at  INTEGER NOT NULL
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

CREATE TABLE IF NOT EXISTS stats (
  id           INTEGER PRIMARY KEY CHECK (id = 1),
  total_bytes  INTEGER NOT NULL
);
INSERT OR IGNORE INTO stats (id, total_bytes) VALUES (1, 0);
"#;

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
    Ok(conn)
}

pub fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(SCHEMA).context("applying schema")
}

/// Inserts the object, its refs and the usage counter in one transaction —
/// only ever called after the blob is durable (row exists ⇒ blob exists).
pub fn insert_object(conn: &mut Connection, obj: &Object, now: i64) -> Result<()> {
    let tx = conn.transaction()?;
    // A re-push of an object we already hold must not count its bytes twice.
    let previous: i64 = tx
        .query_row(
            "SELECT file_size FROM objects WHERE store_path_hash = ?1",
            params![obj.store_path_hash],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(0);
    tx.execute(
        "INSERT OR REPLACE INTO objects (store_path_hash, store_path, name, nar_hash, nar_size,
             file_hash, file_size, deriver, ca, sigs, pushed_by, created_at, last_accessed_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?12)",
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
            // Self-references would make the object permanently unevictable (spec 02).
            if hash_of(r) != obj.store_path_hash {
                stmt.execute(params![obj.store_path_hash, r])?;
            }
        }
    }
    tx.execute(
        "UPDATE stats SET total_bytes = total_bytes + ?1 WHERE id = 1",
        params![obj.file_size - previous],
    )?;
    tx.commit()?;
    Ok(())
}

pub fn get_object(conn: &Connection, hash: &str) -> Result<Option<Object>> {
    let mut obj = conn
        .query_row(
            "SELECT store_path, name, nar_hash, nar_size, file_hash, file_size, deriver, ca,
                    sigs, pushed_by
             FROM objects WHERE store_path_hash = ?1",
            params![hash],
            |row| {
                Ok(Object {
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
                })
            },
        )
        .optional()?;

    if let Some(obj) = obj.as_mut() {
        // Sorted: narinfo References order is part of what the signature covers.
        let mut stmt = conn
            .prepare("SELECT reference FROM object_refs WHERE referrer = ?1 ORDER BY reference")?;
        obj.references = stmt
            .query_map(params![hash], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?;
    }
    Ok(obj)
}

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

/// Negotiation: the subset of `hashes` the cache does not hold.
pub fn missing(conn: &Connection, hashes: &[String]) -> Result<Vec<String>> {
    hashes
        .iter()
        .filter(|h| !matches!(exists(conn, h), Ok(true)))
        .map(|h| Ok(h.clone()))
        .collect()
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
            missing(&conn, &[a.clone(), "nope".into()]).unwrap(),
            vec!["nope"]
        );
    }

    #[test]
    fn drops_self_references() {
        let mut conn = db();
        let a = "a".repeat(32);
        insert_object(&mut conn, &object(&a, &[&format!("{a}-thing")]), 100).unwrap();
        assert!(
            get_object(&conn, &a)
                .unwrap()
                .unwrap()
                .references
                .is_empty()
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
}
