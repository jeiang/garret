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
pub mod runtime;
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
            #[cfg(unix)]
            if let Ok(meta) = std::fs::metadata(p) {
                use std::os::unix::fs::PermissionsExt;
                let mode = meta.permissions().mode() & 0o777;
                if mode & 0o077 != 0 {
                    tracing::warn!(
                        "signing key {p} is readable by group/other (mode {mode:o}); chmod 0400 (ADR-0009)"
                    );
                }
            }
            let text = std::fs::read_to_string(p)
                .map_err(|e| anyhow::anyhow!("reading signing key {p}: {e}"))?;
            narinfo::SigningKeyFile::parse(&text)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A group-readable key still loads: the mode check warns, never bails.
    #[cfg(unix)]
    #[test]
    fn group_readable_signing_key_loads() {
        use std::os::unix::fs::PermissionsExt;
        let path = std::env::temp_dir().join(format!("garret-key-{}", std::process::id()));
        std::fs::write(&path, format!("test-1:{}", "AQEB".repeat(21) + "AQ==")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        let keys = load_signing_keys(&[path.to_str().unwrap().to_owned()]);
        std::fs::remove_file(&path).unwrap();
        assert_eq!(keys.unwrap().len(), 1);
    }
}
