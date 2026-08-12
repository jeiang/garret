//! `garret list` / `garret tree` — thin clients over the Puller's browse API
//! (spec 07). Rendering only; every query is the server's to answer.

use anyhow::{Context, Result};
use serde::Deserialize;

/// One Object in a `garret list` page, as the browse API summarizes it.
#[derive(Debug, Deserialize)]
pub struct Summary {
    /// The store path hash — the Object's key in the cache.
    pub hash: String,
    /// The store path's name part (everything after `<hash>-`).
    pub name: String,
    /// Uncompressed NAR size in bytes.
    pub nar_size: i64,
    /// Size of the stored Blob — the compressed NAR — in bytes.
    pub file_size: i64,
}

/// One page of `garret list` results.
#[derive(Debug, Deserialize)]
pub struct Page {
    /// The Objects on this page.
    pub objects: Vec<Summary>,
    /// Opaque cursor for the next page; `None` on the last one.
    pub next_cursor: Option<String>,
}

/// One node of a `garret tree` dependency tree.
#[derive(Debug, Deserialize)]
pub struct TreeNode {
    /// The store path's name part.
    pub name: String,
    /// The cache does not hold this dependency (referenced but never pushed,
    /// or evicted).
    pub missing: bool,
    /// The server pruned this subtree because the node already appeared
    /// elsewhere in the tree; [`render`] marks it `(…)`.
    pub truncated: bool,
    /// Direct dependencies; empty when missing or truncated.
    pub children: Vec<TreeNode>,
}

/// Both queries return the server's JSON verbatim rather than a re-shaped
/// struct. `--json` then passes it straight through — the browse API already
/// has a schema (spec 07), and mirroring it client-side would create a second
/// one to keep in sync. Human rendering deserializes it into the types below.
pub async fn list(
    http: &reqwest::Client,
    puller: &str,
    token: &str,
    query: Option<&str>,
    limit: usize,
) -> Result<serde_json::Value> {
    let mut request = http
        .get(format!("{puller}/api/v1/objects"))
        .bearer_auth(token)
        .query(&[("limit", limit.to_string())]);
    if let Some(q) = query {
        request = request.query(&[("q", q)]);
    }
    request
        .send()
        .await?
        .error_for_status()
        .context("listing objects")?
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
    http.get(format!("{puller}/api/v1/objects/{hash}/tree"))
        .bearer_auth(token)
        .send()
        .await?
        .error_for_status()
        .context("fetching the dependency tree")?
        .json()
        .await
        .context("parsing the dependency tree")
}

/// One pin in a `garret pins` listing (ticket 22).
#[derive(Debug, Deserialize)]
pub struct Pin {
    /// Operator-chosen pin name.
    pub name: String,
    /// Store path hash of the pinned root.
    pub store_path_hash: String,
    /// The store path's name part is not sent separately; render from the path.
    pub store_path: String,
    /// Unix time the pin stops protecting; `None` = permanent.
    pub expires_at: Option<i64>,
}

/// Fetches the pin listing, verbatim — see [`list`] for why.
pub async fn pins(http: &reqwest::Client, puller: &str, token: &str) -> Result<serde_json::Value> {
    http.get(format!("{puller}/api/v1/pins"))
        .bearer_auth(token)
        .send()
        .await?
        .error_for_status()
        .context("listing pins")?
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

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str, missing: bool, truncated: bool, children: Vec<TreeNode>) -> TreeNode {
        TreeNode {
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
}
