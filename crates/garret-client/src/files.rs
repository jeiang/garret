//! The client's own files — token, config, Watcher Cursor, failed list,
//! nix.conf — read so that "absent" and "unreadable" never look alike, and
//! replaced whole so that a kill mid-write never leaves a truncated file.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// The file's contents, or `None` if it does not exist. Any other failure
/// (permissions, not UTF-8, a directory in the way) is an error naming the
/// path: treating it as absent would let the next write erase what is there.
pub fn read_optional(path: &Path) -> Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

/// Replaces `path` whole with `contents`, creating its directory if missing.
///
/// The bytes go to a fresh sibling created with `mode` (less the umask) in the
/// same open, `O_EXCL` so nothing already there is written through, which is
/// then renamed over `path`: a kill mid-write leaves the previous file, and a
/// symlink at `path` is replaced rather than followed. The sibling is named
/// per process, so two processes sharing a file (a drain overlapping the
/// daemon) cannot rename each other's half-written bytes into place.
pub fn write_atomic(path: &Path, contents: &[u8], mode: u32) -> Result<()> {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(format!(".{}.tmp", std::process::id()));
    let tmp = PathBuf::from(tmp);
    let written = (|| {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Left by a killed process that had this pid; `create_new` would
        // otherwise refuse every write from now on.
        match std::fs::remove_file(&tmp) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e),
            _ => {}
        }
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(mode)
            .open(&tmp)?;
        file.write_all(contents)?;
        file.sync_all()?;
        std::fs::rename(&tmp, path)
    })();
    if written.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    written.with_context(|| format!("writing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_missing_file_reads_as_absent() {
        let dir = crate::testing::scratch_dir();
        assert!(read_optional(&dir.join("missing")).unwrap().is_none());

        write_atomic(&dir.join("text"), b"hello", 0o666).unwrap();
        assert_eq!(
            read_optional(&dir.join("text")).unwrap().as_deref(),
            Some("hello")
        );

        // Present but unreadable as text: an error naming it, not `None`.
        write_atomic(&dir.join("binary"), &[0xff, 0xfe], 0o666).unwrap();
        let err = read_optional(&dir.join("binary")).unwrap_err();
        assert!(format!("{err:#}").contains("binary"), "{err:#}");
        assert!(read_optional(&dir).is_err(), "a directory read as absent");
    }
}
