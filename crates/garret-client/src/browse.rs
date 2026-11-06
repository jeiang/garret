//! `garret list` / `garret tree` / `garret pins` — thin clients over the
//! Puller's browse API (spec 07). Rendering only; every query is the server's
//! to answer.

use anyhow::{Context, Result};
use garret_common::{
    human_bytes,
    wire::{self, TreeNode},
};

use crate::http::ensure_success;

/// Each query returns the server's JSON verbatim, so `--json` passes it
/// straight through; human rendering deserializes it into the
/// [`garret_common::wire`] types the server serialises.
pub async fn list(
    http: &reqwest::Client,
    puller: &str,
    token: &str,
    query: Option<&str>,
    limit: usize,
) -> Result<serde_json::Value> {
    let mut request = http
        .get(format!("{puller}{}", wire::OBJECTS))
        .bearer_auth(token)
        .query(&[("limit", limit.to_string())]);
    if let Some(q) = query {
        request = request.query(&[("q", q)]);
    }
    let response = request.send().await?;
    ensure_success(response, "listing objects")
        .await?
        .json()
        .await
        .context("parsing the object list")
}

/// Fetches an Object's dependency tree, verbatim — see [`list`] for why.
pub async fn tree(
    http: &reqwest::Client,
    puller: &str,
    token: &str,
    hash: &str,
) -> Result<serde_json::Value> {
    let response = http
        .get(format!(
            "{puller}{}",
            wire::with_hash(wire::OBJECT_TREE, hash)
        ))
        .bearer_auth(token)
        .send()
        .await?;
    ensure_success(response, "fetching the dependency tree")
        .await?
        .json()
        .await
        .context("parsing the dependency tree")
}

/// Fetches the pin listing, verbatim — see [`list`] for why.
pub async fn pins(http: &reqwest::Client, puller: &str, token: &str) -> Result<serde_json::Value> {
    let response = http
        .get(format!("{puller}{}", wire::PINS))
        .bearer_auth(token)
        .send()
        .await?;
    ensure_success(response, "listing pins")
        .await?
        .json()
        .await
        .context("parsing the pin list")
}

/// Renders the tree as indented lines, marking what the cache does not hold
/// and where a repeat was truncated.
pub fn render(node: &TreeNode, depth: usize, out: &mut String) {
    let mark = match (node.missing, node.truncated) {
        (true, _) => " (missing)",
        (_, true) => " (…)",
        _ => "",
    };
    out.push_str(&format!("{}{}{}\n", "  ".repeat(depth), node.name, mark));
    for child in &node.children {
        render(child, depth + 1, out);
    }
}

/// Renders a listing page, one Object per line, noting when more remain.
pub fn render_page(page: &wire::Page) -> String {
    let mut out = String::new();
    for object in &page.objects {
        out.push_str(&format!(
            "{}  {:<40} {:>12} on disk\n",
            object.hash,
            object.name,
            human_bytes(object.file_size),
        ));
    }
    if page.next_cursor.is_some() {
        out.push_str("… more results; narrow the query or raise --limit\n");
    }
    out
}

/// Renders the pins, one per line, each with its expiry as of `now` (Unix
/// seconds).
pub fn render_pins(pins: &[wire::Pin], now: i64) -> String {
    let mut out = String::new();
    for pin in pins {
        let expiry = match pin.expires_at {
            None => "permanent".to_owned(),
            Some(t) if t <= now => "expired".to_owned(),
            // Unix seconds, not a formatted date: no date dependency
            // for one column, and --json is the machine surface.
            Some(t) => format!("expires at {t}"),
        };
        out.push_str(&format!(
            "{:<20} {}  {} ({expiry})\n",
            pin.name,
            &pin.store_path_hash[..8.min(pin.store_path_hash.len())],
            pin.store_path,
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str, missing: bool, truncated: bool, children: Vec<TreeNode>) -> TreeNode {
        TreeNode {
            hash: String::new(),
            name: name.into(),
            missing,
            truncated,
            children,
        }
    }

    #[test]
    fn rendering_marks_missing_and_truncated_nodes() {
        let tree = node(
            "root",
            false,
            false,
            vec![
                node(
                    "dep",
                    false,
                    false,
                    vec![node("deep", false, false, vec![])],
                ),
                node("gone", true, false, vec![]),
                node("seen-before", false, true, vec![]),
            ],
        );
        let mut out = String::new();
        render(&tree, 0, &mut out);
        assert_eq!(
            out,
            "root\n  dep\n    deep\n  gone (missing)\n  seen-before (…)\n"
        );
    }

    #[test]
    fn a_page_lists_objects_and_notes_that_more_remain() {
        let object = wire::Summary {
            hash: "aaaa".into(),
            name: "hello-1.0".into(),
            store_path: "/nix/store/aaaa-hello-1.0".into(),
            nar_size: 4096,
            file_size: 2048,
            created_at: 0,
        };
        let mut page = wire::Page {
            objects: vec![object],
            next_cursor: None,
        };
        let out = render_page(&page);
        assert!(out.starts_with("aaaa  hello-1.0 "), "{out:?}");
        assert!(out.ends_with(" 2.0 KiB on disk\n"), "{out:?}");
        assert_eq!(out.lines().count(), 1);
        page.next_cursor = Some("c".into());
        assert!(
            render_page(&page).ends_with("on disk\n… more results; narrow the query or raise --limit\n")
        );
    }

    #[test]
    fn pins_show_permanent_expired_and_future_expiry() {
        let pin = |name: &str, expires_at| wire::Pin {
            name: name.into(),
            store_path_hash: "abcdefghijklmnop".into(),
            store_path: "/nix/store/abcdefghijklmnop-x".into(),
            expires_at,
            created_at: 0,
        };
        let pins = [
            pin("forever", None),
            pin("past", Some(100)),
            pin("edge", Some(1000)),
            pin("later", Some(1001)),
        ];
        let out = render_pins(&pins, 1000);
        let expiries: Vec<&str> = out
            .lines()
            .map(|l| &l[l.rfind('(').unwrap()..])
            .collect();
        assert_eq!(
            expiries,
            [
                "(permanent)",
                "(expired)",
                "(expired)",
                "(expires at 1001)"
            ]
        );
        assert!(out.starts_with("forever              abcdefgh  /nix/store/abcdefghijklmnop-x"));
    }
}
