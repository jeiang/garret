//! `garret list` / `garret tree` — thin clients over the Puller's browse API
//! (spec 07). Rendering only; every query is the server's to answer.

use anyhow::{Context, Result};
use garret_common::wire::{self, TreeNode};

use crate::auth::ensure_success;

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
}
