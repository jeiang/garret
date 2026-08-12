//! Browse queries behind the Puller's `/api/v1` routes (spec 07-browse-api).
//! Served entirely by ticket 07's indices: name, PK, and the reverse-ref index.

use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;

/// One object in a listing: the fields the browse UI shows, nothing heavier.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Summary {
    /// Store path hash — the object key.
    pub hash: String,
    /// Basename after the hash (`hello-1.0`).
    pub name: String,
    /// Full store path.
    pub store_path: String,
    /// Uncompressed NAR size in bytes.
    pub nar_size: i64,
    /// Stored (compressed) blob size in bytes.
    pub file_size: i64,
    /// Unix time the object was pushed.
    pub created_at: i64,
}

/// One page of listing results.
#[derive(Debug, Serialize)]
pub struct Page {
    /// The page's objects, newest first.
    pub objects: Vec<Summary>,
    /// Opaque; pass back as `cursor` for the next page. Absent means the end.
    pub next_cursor: Option<String>,
}

/// One node of a dependency tree.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct TreeNode {
    /// Store path hash of this reference.
    pub hash: String,
    /// Reference basename for children; the object's `name` at the root.
    pub name: String,
    /// Absent from the cache: shown, but marked (spec 07).
    pub missing: bool,
    /// Already expanded elsewhere in this tree; children are omitted.
    pub truncated: bool,
    /// This node's references, empty when missing or truncated.
    pub children: Vec<TreeNode>,
}

/// Keyset pagination, newest first. The cursor is `created_at:hash` so the
/// pair is strictly ordered even when timestamps collide — offset pagination
/// would skip or repeat rows as objects are pushed underneath it.
pub fn list(
    conn: &Connection,
    query: Option<&str>,
    limit: usize,
    cursor: Option<&str>,
) -> Result<Page> {
    let limit = limit.clamp(1, 500);
    let (after_time, after_hash) = match cursor.and_then(|c| c.split_once(':')) {
        Some((time, hash)) => (time.parse::<i64>().unwrap_or(i64::MAX), hash.to_owned()),
        None => (i64::MAX, String::new()),
    };
    let pattern = query.map(|q| format!("%{q}%"));

    let mut stmt = conn.prepare(
        "SELECT store_path_hash, name, store_path, nar_size, file_size, created_at
         FROM objects
         WHERE (?1 IS NULL OR name LIKE ?1)
           AND (created_at < ?2 OR (created_at = ?2 AND store_path_hash > ?3))
         ORDER BY created_at DESC, store_path_hash ASC
         LIMIT ?4",
    )?;
    let mut objects: Vec<Summary> = stmt
        .query_map(
            params![pattern, after_time, after_hash, limit as i64 + 1],
            |row| {
                Ok(Summary {
                    hash: row.get(0)?,
                    name: row.get(1)?,
                    store_path: row.get(2)?,
                    nar_size: row.get(3)?,
                    file_size: row.get(4)?,
                    created_at: row.get(5)?,
                })
            },
        )?
        .collect::<rusqlite::Result<_>>()?;

    // One extra row was fetched purely to learn whether another page exists.
    let next_cursor = (objects.len() > limit).then(|| {
        objects.truncate(limit);
        let last = objects.last().expect("limit is at least 1");
        format!("{}:{}", last.created_at, last.hash)
    });
    Ok(Page {
        objects,
        next_cursor,
    })
}

/// Dependency tree: first occurrence expands, repeats truncate, references
/// missing from the cache are shown but marked (spec 07).
pub fn tree(conn: &Connection, hash: &str, depth_limit: usize) -> Result<Option<TreeNode>> {
    let Some(name) = name_of(conn, hash)? else {
        return Ok(None);
    };
    let mut seen = std::collections::HashSet::new();
    seen.insert(hash.to_owned());
    Ok(Some(TreeNode {
        hash: hash.to_owned(),
        name,
        missing: false,
        truncated: false,
        children: children_of(conn, hash, &mut seen, depth_limit)?,
    }))
}

fn children_of(
    conn: &Connection,
    hash: &str,
    seen: &mut std::collections::HashSet<String>,
    depth: usize,
) -> Result<Vec<TreeNode>> {
    if depth == 0 {
        return Ok(Vec::new());
    }
    let mut stmt =
        conn.prepare("SELECT reference FROM object_refs WHERE referrer = ?1 ORDER BY reference")?;
    let references: Vec<String> = stmt
        .query_map(params![hash], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;

    let mut out = Vec::new();
    for reference in references {
        let child_hash = crate::db::hash_of(&reference).to_owned();
        // Self-references are excluded at insert, but a cycle through two
        // paths would still loop forever without the seen set.
        let truncated = !seen.insert(child_hash.clone());
        let missing = name_of(conn, &child_hash)?.is_none();
        out.push(TreeNode {
            name: reference,
            children: if truncated || missing {
                Vec::new()
            } else {
                children_of(conn, &child_hash, seen, depth - 1)?
            },
            hash: child_hash,
            missing,
            truncated,
        });
    }
    Ok(out)
}

/// Reverse dependencies, via the reverse-ref index.
pub fn referrers(conn: &Connection, hash: &str) -> Result<Vec<Summary>> {
    let mut stmt = conn.prepare(
        "SELECT o.store_path_hash, o.name, o.store_path, o.nar_size, o.file_size, o.created_at
         FROM object_refs r
         JOIN objects o ON o.store_path_hash = r.referrer
         WHERE r.reference_hash = ?1 AND r.referrer != ?1
         ORDER BY o.name",
    )?;
    Ok(stmt
        .query_map(params![hash], |row| {
            Ok(Summary {
                hash: row.get(0)?,
                name: row.get(1)?,
                store_path: row.get(2)?,
                nar_size: row.get(3)?,
                file_size: row.get(4)?,
                created_at: row.get(5)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?)
}

/// One GC-exempt root (ticket 22), as `GET /api/v1/pins` reports it.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Pin {
    /// Operator-chosen pin name.
    pub name: String,
    /// Store path hash of the pinned root.
    pub store_path_hash: String,
    /// Full store path of the pinned root.
    pub store_path: String,
    /// Unix time the pin stops protecting; `None` = permanent.
    pub expires_at: Option<i64>,
    /// Unix time the pin was created.
    pub created_at: i64,
}

/// All pins, expired ones included — the listing is for auditing what the
/// operator set up, and hiding an expired pin would hide the answer to
/// "why did my release get evicted?".
pub fn pins(conn: &Connection) -> Result<Vec<Pin>> {
    let mut stmt = conn.prepare(
        "SELECT p.name, p.store_path_hash, o.store_path, p.expires_at, p.created_at
         FROM pins p JOIN objects o ON o.store_path_hash = p.store_path_hash
         ORDER BY p.name",
    )?;
    Ok(stmt
        .query_map([], |row| {
            Ok(Pin {
                name: row.get(0)?,
                store_path_hash: row.get(1)?,
                store_path: row.get(2)?,
                expires_at: row.get(3)?,
                created_at: row.get(4)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?)
}

fn name_of(conn: &Connection, hash: &str) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT name FROM objects WHERE store_path_hash = ?1",
            params![hash],
            |row| row.get(0),
        )
        .optional()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{self, Object};

    fn object(hash: &str, name: &str, refs: &[&str]) -> Object {
        Object {
            store_path_hash: hash.into(),
            store_path: format!("/nix/store/{hash}-{name}"),
            name: name.into(),
            nar_hash: "sha256:x".into(),
            nar_size: 10,
            file_size: 5,
            file_hash: "sha256:y".into(),
            deriver: None,
            ca: None,
            references: refs.iter().map(|r| (*r).to_owned()).collect(),
            sigs: vec![],
            pushed_by: None,
        }
    }

    fn h(c: char) -> String {
        std::iter::repeat_n(c, 32).collect()
    }

    fn db() -> Connection {
        let conn = db::open(":memory:", true).unwrap();
        db::migrate(&conn).unwrap();
        conn
    }

    #[test]
    fn listing_pages_without_skipping_or_repeating() {
        let mut conn = db();
        for (i, c) in ['a', 'b', 'c', 'd'].iter().enumerate() {
            db::insert_object(&mut conn, &object(&h(*c), "thing", &[]), 100 + i as i64).unwrap();
        }

        let first = list(&conn, None, 2, None).unwrap();
        assert_eq!(first.objects.len(), 2);
        // Newest first.
        assert_eq!(first.objects[0].hash, h('d'));
        assert!(first.next_cursor.is_some());

        let second = list(&conn, None, 2, first.next_cursor.as_deref()).unwrap();
        let seen: Vec<&str> = first
            .objects
            .iter()
            .chain(second.objects.iter())
            .map(|o| o.hash.as_str())
            .collect();
        assert_eq!(seen, vec![h('d'), h('c'), h('b'), h('a')]);
        assert!(second.next_cursor.is_none(), "last page must end the walk");
    }

    #[test]
    fn search_filters_by_name() {
        let mut conn = db();
        db::insert_object(&mut conn, &object(&h('a'), "hello-1.0", &[]), 100).unwrap();
        db::insert_object(&mut conn, &object(&h('b'), "ripgrep", &[]), 101).unwrap();

        let page = list(&conn, Some("hell"), 10, None).unwrap();
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].name, "hello-1.0");
        assert!(
            list(&conn, Some("nothing"), 10, None)
                .unwrap()
                .objects
                .is_empty()
        );
    }

    #[test]
    fn pin_listing_joins_the_store_path_and_keeps_expired_pins_visible() {
        let mut conn = db();
        db::insert_object(&mut conn, &object(&h('a'), "hello-1.0", &[]), 100).unwrap();
        db::pin(&conn, "release", &h('a'), Some(50), 10).unwrap();

        let got = pins(&conn).unwrap();
        assert_eq!(
            got,
            vec![Pin {
                name: "release".into(),
                store_path_hash: h('a'),
                store_path: format!("/nix/store/{}-hello-1.0", h('a')),
                expires_at: Some(50),
                created_at: 10,
            }]
        );
    }

    #[test]
    fn tree_marks_missing_references_and_truncates_repeats() {
        let mut conn = db();
        // root → (mid, missing); mid → shared; root → shared as well.
        let (root, mid, shared, gone) = (h('a'), h('b'), h('c'), h('d'));
        db::insert_object(
            &mut conn,
            &object(
                &root,
                "root",
                &[&format!("{mid}-mid"), &format!("{shared}-shared")],
            ),
            100,
        )
        .unwrap();
        db::insert_object(
            &mut conn,
            &object(
                &mid,
                "mid",
                &[&format!("{shared}-shared"), &format!("{gone}-gone")],
            ),
            100,
        )
        .unwrap();
        db::insert_object(&mut conn, &object(&shared, "shared", &[]), 100).unwrap();

        let tree = tree(&conn, &root, 10).unwrap().unwrap();
        let mid_node = &tree.children[0];
        assert_eq!(mid_node.hash, mid);

        // The reference nix knows nothing about is shown, but flagged.
        let gone_node = mid_node.children.iter().find(|c| c.hash == gone).unwrap();
        assert!(gone_node.missing);
        assert!(gone_node.children.is_empty());

        // `shared` is reached twice; the second occurrence does not re-expand.
        let shared_under_mid = mid_node.children.iter().find(|c| c.hash == shared).unwrap();
        let shared_under_root = tree.children.iter().find(|c| c.hash == shared).unwrap();
        assert!(shared_under_mid.truncated ^ shared_under_root.truncated);

        assert!(tree_of_unknown_hash(&conn));
    }

    fn tree_of_unknown_hash(conn: &Connection) -> bool {
        tree(conn, "nope", 10).unwrap().is_none()
    }

    #[test]
    fn referrers_are_the_reverse_of_references() {
        let mut conn = db();
        let (root, dep) = (h('a'), h('b'));
        db::insert_object(
            &mut conn,
            &object(&root, "root", &[&format!("{dep}-dep")]),
            100,
        )
        .unwrap();
        db::insert_object(&mut conn, &object(&dep, "dep", &[]), 100).unwrap();

        let found = referrers(&conn, &dep).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].hash, root);
        assert!(referrers(&conn, &root).unwrap().is_empty());
    }
}
