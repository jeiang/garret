//! Shared server internals: config, DB, S3 storage, narinfo/signing, auth,
//! browse queries, runtime, metrics, telemetry, in-flight claims.
#![warn(missing_docs)]

pub mod auth;
pub mod browse;
pub mod config;
pub mod db;
pub mod inflight;
pub mod metrics;
pub mod narinfo;
pub mod runtime;
pub mod storage;
pub mod telemetry;
#[cfg(feature = "test-support")]
pub mod testing;

use anyhow::Result;

/// Reads and parses every configured signing key file, failing on the first
/// unreadable or malformed one — a partial key set would sign with fewer keys
/// than the rotation plan expects. A key readable by group or other is
/// refused too (ADR-0009): the Puller's user must not be able to read it.
pub fn load_signing_keys(paths: &[String]) -> Result<Vec<narinfo::SigningKeyFile>> {
    paths
        .iter()
        .map(|p| {
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
        let dir = testing::scratch_dir();
        let path = dir.join("key");
        std::fs::write(&path, format!("test-1:{}", "AQEB".repeat(21) + "AQ==")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        let keys = load_signing_keys(&[path.to_str().unwrap().to_owned()]);
        (path, keys)
    }

    /// A group- or other-readable key is refused, naming the file, mode and fix.
    #[test]
    fn group_readable_signing_key_is_refused() {
        let (path, keys) = key_with_mode(0o640);
        let err = keys.err().expect("refused").to_string();
        assert!(err.contains(path.to_str().unwrap()), "{err}");
        assert!(err.contains("0640") && err.contains("chmod 0400"), "{err}");
    }

    /// Owner-only keys load.
    #[test]
    fn owner_only_signing_key_loads() {
        for mode in [0o400, 0o600] {
            assert_eq!(key_with_mode(mode).1.unwrap().len(), 1);
        }
    }
}
