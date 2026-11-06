//! Browse queries behind the Puller's `/api/v1` routes (spec 07-browse-api).
//! Served entirely by indices: name, creation order, PK, and the reverse-ref
//! index.

use anyhow::Result;
use garret_common::wire::{ObjectDetail, Page, Pin, Summary, TreeNode};
use rusqlite::{Connection, OptionalExtension, params};

use crate::db;

/// The listing query. Its order is exactly the `objects_created` index, so a
/// page walks the index and stops, instead of sorting every row per request.
const LIST: &str = "SELECT store_path_hash, name, store_path, nar_size, file_size, created_at
     FROM objects
     WHERE (?1 IS NULL OR name LIKE ?1)
       AND (created_at < ?2 OR (created_at = ?2 AND store_path_hash > ?3))
     ORDER BY created_at DESC, store_path_hash ASC
     LIMIT ?4";

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

    let mut stmt = conn.prepare(LIST)?;
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

/// One object's detail. Every [`db::Object`] field is named here, so a
/// schema change cannot silently change the public API.
pub fn object(conn: &Connection, hash: &str) -> Result<Option<ObjectDetail>> {
    Ok(db::get_object(conn, hash)?.map(|object| {
        let db::Object {
            store_path_hash,
            store_path,
            name,
            nar_hash,
            nar_size,
            file_hash,
            file_size,
            deriver,
            ca,
            references,
            sigs,
            pushed_by,
        } = object;
        ObjectDetail {
            store_path_hash,
            store_path,
            name,
            nar_hash,
            nar_size,
            file_hash,
            file_size,
            deriver,
            ca,
            references,
            sigs,
            pushed_by,
        }
    }))
}

/// Dependency tree: first occurrence expands, repeats truncate,
/// self-references are skipped, references missing from the cache are shown
/// but marked (spec 07).
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

/// A node's child edges with whether each child is cached: one PK range on
/// `object_refs` (already in `reference` order) and one `objects` PK lookup
/// per edge. The hash is the generated `reference_hash` that referrers and
/// GC join on; a malformed reference just matches no object. Self-edges are
/// stored because the signature covers them, but a path is not its own
/// dependency: showing it would add a truncated self-child to nearly every
/// node, since most compiled packages refer to themselves.
const CHILDREN: &str = "SELECT r.reference, r.reference_hash, o.store_path_hash IS NULL
     FROM object_refs r
     LEFT JOIN objects o ON o.store_path_hash = r.reference_hash
     WHERE r.referrer = ?1 AND r.reference_hash != ?1
     ORDER BY r.reference";

fn children_of(
    conn: &Connection,
    hash: &str,
    seen: &mut std::collections::HashSet<String>,
    depth: usize,
) -> Result<Vec<TreeNode>> {
    if depth == 0 {
        return Ok(Vec::new());
    }
    // Collected before recursing, so the cached statement is back in the
    // cache for the children's queries.
    let edges: Vec<(String, String, bool)> = conn
        .prepare_cached(CHILDREN)?
        .query_map(params![hash], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?
        .collect::<rusqlite::Result<_>>()?;

    let mut out = Vec::with_capacity(edges.len());
    for (reference, child_hash, missing) in edges {
        // A cycle through two paths would still loop forever without the
        // seen set.
        let truncated = !seen.insert(child_hash.clone());
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
    use crate::db;
    use crate::testing::object;

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
        // `b` and `c` share a timestamp, and the page boundary falls between
        // them: the hash tiebreak must carry the walk across it.
        for (c, at) in [('a', 100), ('b', 101), ('c', 101), ('d', 102)] {
            db::insert_object(&mut conn, &object(&h(c)), at).unwrap();
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
        assert_eq!(seen, vec![h('d'), h('b'), h('c'), h('a')]);
        assert!(second.next_cursor.is_none(), "last page must end the walk");
    }

    /// The default listing pages newest-first over the whole cache on every
    /// browse visit; sorting every row for each page is what made it slow.
    #[test]
    fn the_unfiltered_listing_walks_an_index_instead_of_sorting() {
        let conn = db();
        let plan: Vec<String> = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {LIST}"))
            .unwrap()
            .query_map(params![None::<String>, i64::MAX, "", 51], |row| row.get(3))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert!(
            !plan.iter().any(|step| step.contains("TEMP B-TREE")),
            "{plan:?}"
        );
    }

    #[test]
    fn search_filters_by_name() {
        let mut conn = db();
        db::insert_object(&mut conn, &object(&h('a')).named("hello-1.0"), 100).unwrap();
        db::insert_object(&mut conn, &object(&h('b')).named("ripgrep"), 101).unwrap();

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
        db::insert_object(&mut conn, &object(&h('a')).named("hello-1.0"), 100).unwrap();
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
            &object(&root)
                .named("root")
                .referencing(&[&format!("{mid}-mid"), &format!("{shared}-shared")]),
            100,
        )
        .unwrap();
        db::insert_object(
            &mut conn,
            &object(&mid)
                .named("mid")
                .referencing(&[&format!("{shared}-shared"), &format!("{gone}-gone")]),
            100,
        )
        .unwrap();
        db::insert_object(&mut conn, &object(&shared).named("shared"), 100).unwrap();

        let tree = tree(&conn, &root, 10).unwrap().unwrap();
        let mid_node = &tree.children[0];
        assert_eq!(mid_node.hash, mid);

        // The reference nix knows nothing about is shown, but flagged.
        let gone_node = mid_node.children.iter().find(|c| c.hash == gone).unwrap();
        assert!(gone_node.missing);
        assert!(gone_node.children.is_empty());

        // `shared` is reached twice. Depth-first in reference order, the copy
        // under `mid` comes first and expands; the later one is truncated.
        let shared_under_mid = mid_node.children.iter().find(|c| c.hash == shared).unwrap();
        let shared_under_root = tree.children.iter().find(|c| c.hash == shared).unwrap();
        assert!(!shared_under_mid.truncated);
        assert!(shared_under_root.truncated);
    }

    #[test]
    fn tree_of_an_unknown_hash_is_none() {
        assert!(tree(&db(), "nope", 10).unwrap().is_none());
    }

    #[test]
    fn tree_skips_self_references() {
        let mut conn = db();
        let (root, dep) = (h('a'), h('b'));
        let (root_ref, dep_ref) = (format!("{root}-root"), format!("{dep}-dep"));
        db::insert_object(
            &mut conn,
            &object(&root)
                .named("root")
                .referencing(&[&root_ref, &dep_ref]),
            100,
        )
        .unwrap();
        db::insert_object(
            &mut conn,
            &object(&dep).named("dep").referencing(&[&dep_ref]),
            100,
        )
        .unwrap();

        let tree = tree(&conn, &root, 10).unwrap().unwrap();
        let hashes: Vec<&str> = tree.children.iter().map(|c| c.hash.as_str()).collect();
        assert_eq!(hashes, vec![dep.as_str()]);
        assert!(!tree.children[0].truncated);
        assert!(tree.children[0].children.is_empty());
    }

    /// A tree query runs once per expanded node on the single browse
    /// connection, so each must be index lookups: no scan of either table,
    /// and no sort (the primary key already orders edges by reference).
    #[test]
    fn tree_children_are_index_lookups() {
        let conn = db();
        let plan: Vec<String> = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {CHILDREN}"))
            .unwrap()
            .query_map(params![h('a')], |row| row.get(3))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(plan.len(), 2, "{plan:?}");
        assert!(
            plan[0].starts_with("SEARCH r USING PRIMARY KEY (referrer=?)"),
            "{plan:?}"
        );
        assert!(
            plan[1].starts_with("SEARCH o USING")
                && plan[1].contains("INDEX sqlite_autoindex_objects_1 (store_path_hash=?)"),
            "{plan:?}"
        );
    }

    #[test]
    fn tree_truncates_a_cycle_instead_of_looping() {
        let mut conn = db();
        let (a, b) = (h('a'), h('b'));
        let (a_ref, b_ref) = (format!("{a}-a"), format!("{b}-b"));
        db::insert_object(
            &mut conn,
            &object(&a).named("a").referencing(&[&b_ref]),
            100,
        )
        .unwrap();
        db::insert_object(
            &mut conn,
            &object(&b).named("b").referencing(&[&a_ref]),
            100,
        )
        .unwrap();

        let tree = tree(&conn, &a, 10).unwrap().unwrap();
        let b_node = &tree.children[..];
        assert_eq!(b_node.len(), 1);
        assert_eq!(
            (b_node[0].hash.as_str(), b_node[0].truncated),
            (b.as_str(), false)
        );
        let a_again = &b_node[0].children[..];
        assert_eq!(a_again.len(), 1);
        assert_eq!(a_again[0].hash, a);
        assert_eq!(a_again[0].name, a_ref);
        assert!(a_again[0].truncated && !a_again[0].missing);
        assert!(a_again[0].children.is_empty());
    }

    #[test]
    fn referrers_are_the_reverse_of_references() {
        let mut conn = db();
        let (root, dep) = (h('a'), h('b'));
        db::insert_object(
            &mut conn,
            &object(&root)
                .named("root")
                .referencing(&[&format!("{dep}-dep")]),
            100,
        )
        .unwrap();
        db::insert_object(&mut conn, &object(&dep).named("dep"), 100).unwrap();

        let found = referrers(&conn, &dep).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].hash, root);
        assert!(referrers(&conn, &root).unwrap().is_empty());
    }

    /// `GET /objects/{hash}` serves this JSON; pinned so a schema change
    /// cannot alter the public API unnoticed.
    #[test]
    fn object_detail_json_is_stable() {
        let mut conn = db();
        let (a, b) = (h('a'), h('b'));
        let obj = db::Object {
            deriver: Some(format!("{b}-hello.drv")),
            sigs: vec!["k:c2ln".into()],
            pushed_by: Some("https://id.example#me".into()),
            ..object(&a)
                .named("hello")
                .referencing(&[&format!("{b}-dep")])
        };
        db::insert_object(&mut conn, &obj, 100).unwrap();
        let json = serde_json::to_string(&super::object(&conn, &a).unwrap().unwrap()).unwrap();
        assert_eq!(
            json,
            format!(
                r#"{{"store_path_hash":"{a}","store_path":"/nix/store/{a}-hello","name":"hello","nar_hash":"sha256:x","nar_size":10,"file_hash":"sha256:y","file_size":5,"deriver":"{b}-hello.drv","ca":null,"references":["{b}-dep"],"sigs":["k:c2ln"],"pushed_by":"https://id.example#me"}}"#
            )
        );
        assert!(super::object(&conn, &b).unwrap().is_none());
    }
}
