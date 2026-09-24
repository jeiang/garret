//! Which objects are being uploaded or deleted right now. This lives in memory
//! and never in the DB (spec 02-database): a crash leaves no half-written
//! rows, and the GC orphan sweep consults it to avoid aborting a live multipart.

use std::{
    collections::{HashMap, hash_map::Entry},
    sync::{Arc, Mutex},
};

/// What a claim on a store path is for. One claim per path at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// An upload is streaming the blob; its row lands once the blob does.
    Upload,
    /// GC, `garret-admin delete` or prune is removing the object: held from
    /// before the row delete until the blob delete returns, so no upload can
    /// write a new blob that the pending blob delete would then remove.
    Delete,
}

/// The shared map of store path hashes being uploaded or deleted. Clones
/// share the map.
#[derive(Default, Clone)]
pub struct InFlight(Arc<Mutex<HashMap<String, Kind>>>);

/// Releases the claim on drop, so a panicking or cancelled upload or delete
/// cannot wedge a store path permanently.
pub struct Claim {
    set: InFlight,
    hash: String,
}

impl InFlight {
    /// An empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// First claimer wins; `Err` names the kind of claim already held.
    pub fn claim(&self, hash: &str, kind: Kind) -> Result<Claim, Kind> {
        match self.0.lock().unwrap().entry(hash.to_owned()) {
            Entry::Occupied(held) => Err(*held.get()),
            Entry::Vacant(slot) => {
                slot.insert(kind);
                Ok(Claim {
                    set: self.clone(),
                    hash: hash.to_owned(),
                })
            }
        }
    }

    /// Whether an upload or delete of this path is live — the guard the GC
    /// orphan sweep and fsck check before treating the path as drift.
    pub fn contains(&self, hash: &str) -> bool {
        self.0.lock().unwrap().contains_key(hash)
    }

    /// How many uploads are in progress; exported as a gauge and drained by
    /// `fsck --quiesce`.
    pub fn uploads(&self) -> usize {
        self.0
            .lock()
            .unwrap()
            .values()
            .filter(|kind| **kind == Kind::Upload)
            .count()
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
        let first = set
            .claim("abc", Kind::Upload)
            .expect("first claim should succeed");
        assert_eq!(set.claim("abc", Kind::Upload).err(), Some(Kind::Upload));
        assert!(set.contains("abc"));
        // A different path is unaffected.
        assert!(set.claim("def", Kind::Upload).is_ok());

        drop(first);
        assert!(!set.contains("abc"));
        assert!(set.claim("abc", Kind::Upload).is_ok());
    }

    #[test]
    fn a_deletion_turns_an_upload_away_and_is_not_counted_as_one() {
        let set = InFlight::new();
        let deleting = set.claim("abc", Kind::Delete).unwrap();
        assert_eq!(set.claim("abc", Kind::Upload).err(), Some(Kind::Delete));
        assert_eq!(set.uploads(), 0, "a deletion counted as an upload");

        drop(deleting);
        let _uploading = set.claim("abc", Kind::Upload).unwrap();
        assert_eq!(set.claim("abc", Kind::Delete).err(), Some(Kind::Upload));
        assert_eq!(set.uploads(), 1);
    }

    #[test]
    fn a_dropped_claim_cannot_wedge_the_path() {
        let set = InFlight::new();
        drop(set.claim("abc", Kind::Delete));
        assert!(!set.contains("abc"));
    }
}
