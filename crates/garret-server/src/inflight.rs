//! Which objects are being uploaded right now. This lives in memory and never
//! in the DB (spec 02-database): a crash leaves no half-written rows, and the
//! GC orphan sweep consults it to avoid aborting a live multipart.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

/// The shared set of store path hashes with an upload in progress. Clones
/// share the set.
#[derive(Default, Clone)]
pub struct InFlight(Arc<Mutex<HashSet<String>>>);

/// Releases the claim on drop, so a panicking or cancelled upload cannot
/// wedge a store path permanently.
pub struct Claim {
    set: InFlight,
    hash: String,
}

impl InFlight {
    /// An empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// `None` if another upload already holds this path — first writer wins.
    pub fn claim(&self, hash: &str) -> Option<Claim> {
        let mut set = self.0.lock().unwrap();
        set.insert(hash.to_owned()).then(|| Claim {
            set: self.clone(),
            hash: hash.to_owned(),
        })
    }

    /// Whether an upload of this path is live — the GC orphan sweep's check
    /// before aborting a multipart.
    pub fn contains(&self, hash: &str) -> bool {
        self.0.lock().unwrap().contains(hash)
    }

    /// How many uploads are in progress; exported as a gauge.
    pub fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }

    /// True when no upload is in progress.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        self.set.0.lock().unwrap().remove(&self.hash);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_first_claimer_wins_and_the_claim_is_released() {
        let set = InFlight::new();
        let first = set.claim("abc").expect("first claim should succeed");
        assert!(set.claim("abc").is_none());
        assert!(set.contains("abc"));
        // A different path is unaffected.
        assert!(set.claim("def").is_some());

        drop(first);
        assert!(!set.contains("abc"));
        assert!(set.claim("abc").is_some());
    }

    #[test]
    fn a_dropped_claim_cannot_wedge_the_path() {
        let set = InFlight::new();
        drop(set.claim("abc"));
        assert!(set.is_empty());
    }
}
