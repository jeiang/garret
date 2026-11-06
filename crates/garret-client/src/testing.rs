//! Test-only fixtures.

use std::{
    ops::Deref,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

/// An empty directory, removed with its contents on drop, so a failing
/// test leaves nothing behind.
pub struct ScratchDir(PathBuf);

impl Deref for ScratchDir {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Creates a fresh [`ScratchDir`]: the pid tells concurrent test binaries
/// apart, the counter calls within one.
pub fn scratch_dir() -> ScratchDir {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let path = std::env::temp_dir().join(format!(
        "garret-client-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    // A dead process with the same pid may have left one behind.
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("creating scratch dir");
    ScratchDir(path)
}
