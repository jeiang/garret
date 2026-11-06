//! Turning a non-2xx answer into an error that says why. Servers and issuers
//! explain themselves in the body, which `error_for_status` discards.

use anyhow::{Result, bail};
use serde::Deserialize;

/// Passes a 2xx response through; turns anything else into an error naming
/// `what`, the status, and the body's [`error_detail`].
pub(crate) async fn ensure_success(
    response: reqwest::Response,
    what: &str,
) -> Result<reqwest::Response> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let detail = error_detail(response).await;
    match detail.is_empty() {
        true => bail!("{what}: {status}"),
        false => bail!("{what}: {status}: {detail}"),
    }
}

/// The part of an error response's body worth showing; see [`describe_body`].
pub(crate) async fn error_detail(response: reqwest::Response) -> String {
    describe_body(&response.text().await.unwrap_or_default())
}

/// An OAuth `error` (and its description, RFC 6749 §5.2) when the body is
/// that JSON, otherwise the body itself, cut short so a proxy's HTML error
/// page cannot flood the terminal.
fn describe_body(body: &str) -> String {
    #[derive(Deserialize)]
    struct OauthError {
        error: String,
        error_description: Option<String>,
    }
    if let Ok(e) = serde_json::from_str::<OauthError>(body) {
        return match e.error_description {
            Some(d) => format!("{}: {d}", e.error),
            None => e.error,
        };
    }
    let body = body.trim();
    match body.char_indices().nth(300) {
        Some((end, _)) => format!("{}…", &body[..end]),
        None => body.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_bodies_show_the_issuers_reason() {
        assert_eq!(
            describe_body(r#"{"error":"invalid_grant","error_description":"token revoked"}"#),
            "invalid_grant: token revoked"
        );
        assert_eq!(
            describe_body(r#"{"error":"invalid_client"}"#),
            "invalid_client"
        );
        assert_eq!(describe_body("  plain failure \n"), "plain failure");
        assert!(describe_body(&"x".repeat(1000)).chars().count() <= 301);
    }
}
