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
#[cfg(any(test, feature = "test-support"))]
pub mod testing;

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
/// than the rotation plan expects. On Unix a key readable by group or other is
/// refused too (ADR-0009): the Puller's user must not be able to read it.
pub fn load_signing_keys(paths: &[String]) -> Result<Vec<narinfo::SigningKeyFile>> {
    paths
        .iter()
        .map(|p| {
            #[cfg(unix)]
            if let Ok(meta) = std::fs::metadata(p) {
                use std::os::unix::fs::PermissionsExt;
                let mode = meta.permissions().mode() & 0o777;
                if mode & 0o077 != 0 {
                    anyhow::bail!(
                        "signing key {p} is readable by group/other (mode {mode:04o}); \
                         fix with `chmod 0400 {p}` and `chown garret {p}` (ADR-0009)"
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

    fn key_with_mode(mode: u32) -> (std::path::PathBuf, Result<Vec<narinfo::SigningKeyFile>>) {
        use std::os::unix::fs::PermissionsExt;
        let path = std::env::temp_dir().join(format!("garret-key-{mode:o}-{}", std::process::id()));
        std::fs::write(&path, format!("test-1:{}", "AQEB".repeat(21) + "AQ==")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        let keys = load_signing_keys(&[path.to_str().unwrap().to_owned()]);
        std::fs::remove_file(&path).unwrap();
        (path, keys)
    }

    /// A group- or other-readable key is refused, naming the file, mode and fix.
    #[cfg(unix)]
    #[test]
    fn group_readable_signing_key_is_refused() {
        let (path, keys) = key_with_mode(0o640);
        let err = keys.err().expect("refused").to_string();
        assert!(err.contains(path.to_str().unwrap()), "{err}");
        assert!(err.contains("0640") && err.contains("chmod 0400"), "{err}");
    }

    /// Owner-only keys load.
    #[cfg(unix)]
    #[test]
    fn owner_only_signing_key_loads() {
        for mode in [0o400, 0o600] {
            assert_eq!(key_with_mode(mode).1.unwrap().len(), 1);
        }
    }
}
