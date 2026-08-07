//! `garret list` / `garret tree` — thin clients over the Puller's browse API
//! (spec 07). Rendering only; every query is the server's to answer.

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Summary {
    pub hash: String,
    pub name: String,
    pub nar_size: i64,
    pub file_size: i64,
}

#[derive(Debug, Deserialize)]
pub struct Page {
    pub objects: Vec<Summary>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TreeNode {
    pub name: String,
    pub missing: bool,
    pub truncated: bool,
    pub children: Vec<TreeNode>,
}

pub async fn list(
    http: &reqwest::Client,
    puller: &str,
    token: &str,
    query: Option<&str>,
    limit: usize,
) -> Result<Page> {
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

pub async fn tree(
    http: &reqwest::Client,
    puller: &str,
    token: &str,
    hash: &str,
) -> Result<TreeNode> {
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
