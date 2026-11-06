//! The one deletion sequence (spec 05), shared by GC, `garret-admin delete`
//! and prune: claim the path, delete its row, delete its blob, release the
//! claim. The claim spans the row and blob deletes: an upload of the path in
//! between would write a blob that the pending delete then removes, under the
//! row the upload inserts. Row first, blob second: a failed blob delete leaves
//! an orphan for the sweep, never a row without a blob.

use anyhow::Result;
use garret_server::{
    inflight::{Busy, Claim, InFlight},
    storage::{self, Storage},
};

/// Delete claims held and blobs queued; [`Deletion::finish`] deletes the
/// blobs, then releases the claims.
#[derive(Default)]
pub(crate) struct Deletion {
    claims: Vec<Claim>,
    keys: Vec<String>,
}

impl Deletion {
    /// Claims every path before the first row goes (admin commands). A path
    /// already being uploaded or deleted fails the whole command before
    /// anything is deleted.
    pub(crate) fn claim_all(in_flight: &InFlight, hashes: &[&str]) -> Result<Self> {
        let claims = hashes
            .iter()
            .map(|hash| {
                in_flight.claim_delete(hash).map_err(|busy| {
                    let doing = match busy {
                        Busy::Uploading => "uploaded",
                        Busy::Deleting => "deleted",
                    };
                    anyhow::anyhow!("{hash} is being {doing}; nothing was deleted, retry")
                })
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            claims,
            keys: Vec::new(),
        })
    }

    /// Claims the path unless an upload or deletion already holds it, in
    /// which case GC skips it. Dropping the claim instead of passing it to
    /// [`Deletion::hold`] releases the path.
    pub(crate) fn claim_available(in_flight: &InFlight, hash: &str) -> Option<Claim> {
        in_flight.claim_delete(hash).ok()
    }

    /// Queues the blob of a path from [`Deletion::claim_all`] whose row is gone.
    pub(crate) fn queue_blob(&mut self, hash: &str) {
        self.keys.push(storage::key_for(hash));
    }

    /// Keeps a claim from [`Deletion::claim_available`] whose row is gone and
    /// queues its blob.
    pub(crate) fn hold(&mut self, claim: Claim) {
        self.queue_blob(claim.hash());
        self.claims.push(claim);
    }

    /// How many blobs are queued.
    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }

    /// Deletes the queued blobs, then releases the claims, whether or not
    /// the blob delete succeeded.
    pub(crate) async fn finish(self, storage: &Storage) -> Result<()> {
        let Self { claims, keys } = self;
        let deleted = storage.delete_objects(&keys).await;
        drop(claims);
        deleted
    }
}
