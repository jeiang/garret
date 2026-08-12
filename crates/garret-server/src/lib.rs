//! Shared server internals: config, DB, S3 storage, narinfo/signing.
#![warn(missing_docs)]

pub mod auth;
pub mod browse;
pub mod config;
pub mod db;
pub mod inflight;
pub mod metrics;
pub mod narinfo;
pub mod nix_base32;
pub mod storage;

use anyhow::Result;

/// Current unix time in seconds, as stored in `created_at`/`last_accessed_at`.
pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Reads and parses every configured signing key file, failing on the first
/// unreadable or malformed one — a partial key set would sign with fewer keys
/// than the rotation plan expects.
pub fn load_signing_keys(paths: &[String]) -> Result<Vec<narinfo::SigningKeyFile>> {
    paths
        .iter()
        .map(|p| {
            let text = std::fs::read_to_string(p)
                .map_err(|e| anyhow::anyhow!("reading signing key {p}: {e}"))?;
            narinfo::SigningKeyFile::parse(&text)
        })
        .collect()
}
