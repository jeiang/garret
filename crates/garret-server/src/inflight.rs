//! Which objects are being uploaded or deleted right now. This lives in memory
//! and never in the DB (spec 02-database): a crash leaves no half-written
//! rows, and the GC orphan sweep consults it to avoid aborting a live multipart.

use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
    time::Duration,
};

use parking_lot::Mutex;
use tokio::sync::Notify;

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

/// Why [`InFlight::claim`] turned a claim away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    /// The path is already claimed by an upload.
    Uploading,
    /// The path is already claimed by a delete.
    Deleting,
    /// `fsck --repair --quiesce` holds the gate shut; only upload claims are
    /// refused, so GC, delete and prune keep working.
    Quiescing,
}

#[derive(Default)]
struct State {
    claims: HashMap<String, Kind>,
    /// Upload claims in `claims`, kept so admission and drain read one number.
    uploads: usize,
    /// Live [`QuiesceGuard`]s; the gate is shut while any is.
    quiescers: usize,
}

#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    /// Woken whenever an upload claim drops, for [`QuiesceGuard::drain`].
    drained: Notify,
}

/// The shared map of store path hashes being uploaded or deleted, and the
/// quiesce gate on new uploads. Clones share the state.
#[derive(Default, Clone)]
pub struct InFlight(Arc<Shared>);

/// Releases the claim on drop, so a panicking or cancelled upload or delete
/// cannot wedge a store path permanently.
pub struct Claim {
    set: InFlight,
    hash: String,
}

/// Refuses new upload claims while it lives (spec 05-gc). Guards nest: the
/// gate reopens only when the last one drops, however `fsck` returns.
pub struct QuiesceGuard(InFlight);

impl InFlight {
    /// An empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// First claimer wins; `Err` names why not. Upload claims are also
    /// refused while quiescing — checked under the same lock as the claim,
    /// so no upload is admitted that a drain cannot see.
    pub fn claim(&self, hash: &str, kind: Kind) -> Result<Claim, Refused> {
        let mut state = self.0.state.lock();
        let State {
            claims,
            uploads,
            quiescers,
        } = &mut *state;
        if kind == Kind::Upload && *quiescers > 0 {
            return Err(Refused::Quiescing);
        }
        match claims.entry(hash.to_owned()) {
            Entry::Occupied(held) => Err(match held.get() {
                Kind::Upload => Refused::Uploading,
                Kind::Delete => Refused::Deleting,
            }),
            Entry::Vacant(slot) => {
                slot.insert(kind);
                if kind == Kind::Upload {
                    *uploads += 1;
                }
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
        self.0.state.lock().claims.contains_key(hash)
    }

    /// How many uploads are in progress; exported as a gauge and drained by
    /// `fsck --quiesce`.
    pub fn uploads(&self) -> usize {
        self.0.state.lock().uploads
    }

    /// Whether a [`QuiesceGuard`] is live. Advisory, for rejecting a push
    /// early; [`InFlight::claim`] is the authoritative gate.
    pub fn is_quiescing(&self) -> bool {
        self.0.state.lock().quiescers > 0
    }

    /// Shuts the gate on new upload claims until the guard drops.
    pub fn quiesce(&self) -> QuiesceGuard {
        self.0.state.lock().quiescers += 1;
        QuiesceGuard(self.clone())
    }
}

impl QuiesceGuard {
    /// Waits until no upload claim is held; `false` if `timeout` elapses
    /// first. With the gate shut, none can be taken in the meantime.
    pub async fn drain(&self, timeout: Duration) -> bool {
        let shared = &self.0.0;
        tokio::time::timeout(timeout, async {
            loop {
                // Registered before the check, so a drop in between still
                // wakes this waiter.
                let notified = shared.drained.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if shared.state.lock().uploads == 0 {
                    return;
                }
                notified.await;
            }
        })
        .await
        .is_ok()
    }
}

impl Drop for QuiesceGuard {
    fn drop(&mut self) {
        self.0.0.state.lock().quiescers -= 1;
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        let shared = &self.set.0;
        let mut state = shared.state.lock();
        if state.claims.remove(&self.hash) == Some(Kind::Upload) {
            state.uploads -= 1;
            drop(state);
            shared.drained.notify_waiters();
        }
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
        assert_eq!(
            set.claim("abc", Kind::Upload).err(),
            Some(Refused::Uploading)
        );
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
        assert_eq!(
            set.claim("abc", Kind::Upload).err(),
            Some(Refused::Deleting)
        );
        assert_eq!(set.uploads(), 0, "a deletion counted as an upload");

        drop(deleting);
        let _uploading = set.claim("abc", Kind::Upload).unwrap();
        assert_eq!(
            set.claim("abc", Kind::Delete).err(),
            Some(Refused::Uploading)
        );
        assert_eq!(set.uploads(), 1);
    }

    #[test]
    fn a_dropped_claim_cannot_wedge_the_path() {
        let set = InFlight::new();
        drop(set.claim("abc", Kind::Delete));
        assert!(!set.contains("abc"));
    }

    #[test]
    fn quiescing_refuses_uploads_but_not_deletes() {
        let set = InFlight::new();
        let guard = set.quiesce();
        assert!(set.is_quiescing());
        assert_eq!(
            set.claim("abc", Kind::Upload).err(),
            Some(Refused::Quiescing)
        );
        assert_eq!(set.uploads(), 0);
        assert!(set.claim("abc", Kind::Delete).is_ok());

        drop(guard);
        assert!(!set.is_quiescing());
        assert!(set.claim("abc", Kind::Upload).is_ok());
    }

    #[test]
    fn nested_quiesces_keep_the_gate_shut_until_the_last_drops() {
        let set = InFlight::new();
        let first = set.quiesce();
        let second = set.quiesce();
        drop(first);
        assert_eq!(
            set.claim("abc", Kind::Upload).err(),
            Some(Refused::Quiescing)
        );
        drop(second);
        assert!(set.claim("abc", Kind::Upload).is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn drain_wakes_when_the_last_upload_drops_and_times_out_otherwise() {
        let set = InFlight::new();
        let first = set.claim("a", Kind::Upload).unwrap();
        let second = set.claim("b", Kind::Upload).unwrap();
        let _deleting = set.claim("c", Kind::Delete).unwrap();
        let guard = set.quiesce();

        assert!(!guard.drain(Duration::from_secs(1)).await);

        let waiter = tokio::spawn({
            let set = set.clone();
            async move {
                let guard = set.quiesce();
                let start = tokio::time::Instant::now();
                let drained = guard.drain(Duration::from_secs(300)).await;
                (drained, start.elapsed())
            }
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(first);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!waiter.is_finished(), "drained with an upload still held");
        drop(second);
        let (drained, waited) = waiter.await.unwrap();
        assert!(drained, "a held delete claim blocked the drain");
        assert!(waited < Duration::from_secs(3), "waited {waited:?}");
        drop(guard);
    }
}
