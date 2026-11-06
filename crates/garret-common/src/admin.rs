//! The admin protocol: one JSON request per line, one JSON response back.
//!
//! Everything that touches the DB goes through the Pusher (spec 10-packaging):
//! it owns all writes, so `garret-admin` never opens the database itself.
//! Line-delimited JSON over an owner-only unix socket keeps both ends free of an
//! HTTP stack — the socket's file permissions are the whole access story.

use serde::{Deserialize, Serialize};

/// A command sent by `garret-admin` to the Pusher's admin socket.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "command", rename_all = "kebab-case")]
pub enum Request {
    /// Object count, usage against quota, uploads in flight.
    Status,
    /// Trigger an eviction pass now rather than waiting for the timer.
    GcRun,
    /// Re-sign every object with the currently configured keys — the backfill
    /// after adding a key during rotation.
    Resign,
    /// Remove objects by store-path hash, row and blob. The operator escape
    /// hatch for an object that must go before GC would reach it: a bad push,
    /// or one written by a server version whose metadata is now known wrong.
    Delete {
        /// 32-character store-path hashes of the objects to remove.
        hashes: Vec<String>,
        /// Report what would go without deleting anything.
        dry_run: bool,
    },
    /// Pin an object as a GC-exempt root (ticket 22): its whole closure is
    /// protected from eviction while the pin is live. Idempotent — pinning an
    /// existing name replaces it.
    Pin {
        /// Operator-chosen pin name.
        name: String,
        /// 32-character store-path hash of the root to protect. Must already
        /// be in the cache; pinning an unknown hash is a hard error.
        hash: String,
        /// Unix time the pin stops protecting; `None` = permanent.
        expires_at: Option<i64>,
    },
    /// Remove a pin by name.
    Unpin {
        /// The pin's name.
        name: String,
    },
    /// Audit row ⇔ blob consistency (spec 02/03), and optionally repair it.
    Fsck {
        /// Compare `file_size` against the S3 object's size for hashes with
        /// both a row and a blob; mismatches are a third finding category.
        verify_sizes: bool,
        /// Whether, and how, to delete the rows found.
        repair: Repair,
    },
    /// Delete every closure last pushed before a cutoff, keeping anything a
    /// newer push or a live pin still needs (spec 05).
    Prune {
        /// Unix time; objects last pushed before it are candidates. Must be
        /// at least a day ago, so a push in progress keeps its closure.
        before: i64,
        /// Report what would go without deleting anything.
        dry_run: bool,
    },
    /// Write a consistent copy of the database to `path` while both services
    /// keep running: the online backup (spec 10-packaging).
    Backup {
        /// Absolute destination path, as seen by the Pusher. Must not exist:
        /// a backup never overwrites anything.
        path: String,
    },
    /// Incident response: delete every object a given subject pushed, through
    /// the same path as [`Request::Delete`] (unconditional, closures and all).
    DeletePushedBy {
        /// The `pushed_by` value to match exactly: `<issuer>#<sub>`.
        subject: String,
        /// Unix time; only objects first pushed at or after it. `None` = all.
        since: Option<i64>,
        /// Report what would go without deleting anything.
        dry_run: bool,
    },
}

/// Whether [`Request::Fsck`] repairs what it finds.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Repair {
    /// Report only: the default.
    No,
    /// Delete dangling and size-mismatched rows, trusting the live
    /// in-flight signal.
    Live,
    /// Reject new pushes and wait for in-flight uploads to drain before
    /// repairing, for the degraded case where the live in-flight signal
    /// alone isn't trusted.
    Quiesced,
}

/// The Pusher's reply to a [`Request`]: the variant named after the command
/// ([`Response::Gc`] for [`Request::GcRun`]), or [`Response::Error`].
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum Response {
    /// Cache usage now.
    Status {
        /// Number of objects in the cache.
        objects: i64,
        /// Total compressed bytes stored, the figure quota is judged against.
        total_bytes: i64,
        /// Configured quota in bytes; `None` means unlimited.
        quota_bytes: Option<u64>,
        /// Uploads currently being received.
        uploads_in_flight: usize,
    },
    /// One eviction pass's outcome.
    Gc {
        /// Objects evicted by this pass.
        evicted: usize,
        /// Compressed bytes reclaimed.
        bytes_freed: i64,
        /// True when the pass stopped because no evictable object remained
        /// while usage was still above the low watermark — everything left
        /// is referenced.
        candidates_exhausted: bool,
    },
    /// Re-signing tally.
    Resign {
        /// Objects whose narinfo signatures were rewritten.
        resigned: usize,
        /// Objects left untouched because none of their existing signatures
        /// verified under a configured key (or they had none): re-signing
        /// would otherwise launder tampered metadata into a valid signature.
        refused: usize,
    },
    /// What was, or on a dry run would be, deleted.
    Delete {
        /// Objects removed (or, on a dry run, that would be removed).
        deleted: usize,
        /// Compressed bytes reclaimed (or reclaimable, on a dry run).
        bytes_freed: i64,
        /// Hashes that were not in the cache; deleting them is a no-op, but
        /// silently reporting success would hide a typo.
        missing: Vec<String>,
    },
    /// The pin is stored.
    Pin,
    /// Whether the named pin existed.
    Unpin {
        /// False when no pin had that name — reported, not swallowed, so a
        /// typo does not look like a successful unpin.
        removed: bool,
    },
    /// Audit findings and repairs.
    Fsck {
        /// Rows with no matching blob, past the in-flight and age guards.
        dangling: Vec<FsckRow>,
        /// Rows whose blob exists but disagrees on size (`--verify-sizes`
        /// only) — a third category, not double-reported as dangling.
        size_mismatches: Vec<FsckSizeMismatch>,
        /// Blob keys with no matching row, past the same guards. Informational
        /// only: the existing orphan sweep owns deleting these, on its own
        /// schedule.
        orphans: Vec<String>,
        /// Rows actually deleted this run (dangling + size-mismatch
        /// combined). Zero unless the request asked for a repair.
        repaired: usize,
        /// `None` unless the request was [`Repair::Quiesced`]; then whether
        /// in-flight uploads reached zero within the timeout. `Some(false)`
        /// means the drain timed out and no repair happened.
        quiesce_drained: Option<bool>,
    },
    /// What was, or on a dry run would be, pruned.
    Prune {
        /// Basenames (`<hash>-<name>`) removed, or that a dry run would remove.
        pruned: Vec<String>,
        /// Compressed bytes reclaimed (or reclaimable, on a dry run).
        bytes_freed: i64,
    },
    /// The copy is written.
    Backup {
        /// Size of the copy written, in bytes.
        bytes: u64,
    },
    /// What was, or on a dry run would be, deleted.
    DeletePushedBy {
        /// Basenames (`<hash>-<name>`) removed, or that a dry run would remove.
        objects: Vec<String>,
        /// Compressed bytes reclaimed (or reclaimable, on a dry run).
        bytes_freed: i64,
    },
    /// The command failed; `message` is operator-facing text.
    Error {
        /// Human-readable description of what went wrong.
        message: String,
    },
}

/// A dangling row: a store path with no matching blob (spec 02).
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct FsckRow {
    /// 32-character store-path hash.
    pub store_path_hash: String,
    /// Basename after the hash, for a human-readable report.
    pub name: String,
}

/// A row whose blob exists but whose size disagrees with `file_size`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct FsckSizeMismatch {
    /// 32-character store-path hash.
    pub store_path_hash: String,
    /// Basename after the hash, for a human-readable report.
    pub name: String,
    /// `file_size` as recorded in the database.
    pub db_size: i64,
    /// The S3 object's actual size.
    pub s3_size: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_have_a_fixed_wire_form() {
        let hash = "a".repeat(32);
        for (request, wire) in [
            (Request::Status, r#"{"command":"status"}"#.to_owned()),
            (Request::GcRun, r#"{"command":"gc-run"}"#.to_owned()),
            (Request::Resign, r#"{"command":"resign"}"#.to_owned()),
            (
                Request::Delete {
                    hashes: vec![hash.clone()],
                    dry_run: true,
                },
                format!(r#"{{"command":"delete","hashes":["{hash}"],"dry_run":true}}"#),
            ),
            (
                Request::Pin {
                    name: "release".into(),
                    hash: hash.clone(),
                    expires_at: Some(1234),
                },
                format!(r#"{{"command":"pin","name":"release","hash":"{hash}","expires_at":1234}}"#),
            ),
            (
                Request::Pin {
                    name: "release".into(),
                    hash: hash.clone(),
                    expires_at: None,
                },
                format!(r#"{{"command":"pin","name":"release","hash":"{hash}","expires_at":null}}"#),
            ),
            (
                Request::Unpin {
                    name: "release".into(),
                },
                r#"{"command":"unpin","name":"release"}"#.to_owned(),
            ),
            (
                Request::Fsck {
                    verify_sizes: false,
                    repair: Repair::Live,
                },
                r#"{"command":"fsck","verify_sizes":false,"repair":"live"}"#.to_owned(),
            ),
            (
                Request::Fsck {
                    verify_sizes: true,
                    repair: Repair::No,
                },
                r#"{"command":"fsck","verify_sizes":true,"repair":"no"}"#.to_owned(),
            ),
            (
                Request::Fsck {
                    verify_sizes: true,
                    repair: Repair::Quiesced,
                },
                r#"{"command":"fsck","verify_sizes":true,"repair":"quiesced"}"#.to_owned(),
            ),
            (
                Request::Prune {
                    before: 1234,
                    dry_run: false,
                },
                r#"{"command":"prune","before":1234,"dry_run":false}"#.to_owned(),
            ),
            (
                Request::Backup {
                    path: "/var/lib/garret/backup.db".into(),
                },
                r#"{"command":"backup","path":"/var/lib/garret/backup.db"}"#.to_owned(),
            ),
            (
                Request::DeletePushedBy {
                    subject: "https://issuer#repo:o/r".into(),
                    since: Some(1234),
                    dry_run: true,
                },
                r#"{"command":"delete-pushed-by","subject":"https://issuer#repo:o/r","since":1234,"dry_run":true}"#
                    .to_owned(),
            ),
            (
                Request::DeletePushedBy {
                    subject: "s".into(),
                    since: None,
                    dry_run: false,
                },
                r#"{"command":"delete-pushed-by","subject":"s","since":null,"dry_run":false}"#
                    .to_owned(),
            ),
        ] {
            assert_eq!(serde_json::to_string(&request).unwrap(), wire);
            assert_eq!(serde_json::from_str::<Request>(&wire).unwrap(), request);
        }
    }

    #[test]
    fn responses_have_a_fixed_wire_form() {
        let hash = "a".repeat(32);
        for (response, wire) in [
            (
                Response::Status {
                    objects: 3,
                    total_bytes: 100,
                    quota_bytes: Some(1000),
                    uploads_in_flight: 1,
                },
                r#"{"status":{"objects":3,"total_bytes":100,"quota_bytes":1000,"uploads_in_flight":1}}"#
                    .to_owned(),
            ),
            (
                Response::Status {
                    objects: 0,
                    total_bytes: 0,
                    quota_bytes: None,
                    uploads_in_flight: 0,
                },
                r#"{"status":{"objects":0,"total_bytes":0,"quota_bytes":null,"uploads_in_flight":0}}"#
                    .to_owned(),
            ),
            (
                Response::Gc {
                    evicted: 2,
                    bytes_freed: 50,
                    candidates_exhausted: true,
                },
                r#"{"gc":{"evicted":2,"bytes_freed":50,"candidates_exhausted":true}}"#.to_owned(),
            ),
            (
                Response::Resign {
                    resigned: 4,
                    refused: 1,
                },
                r#"{"resign":{"resigned":4,"refused":1}}"#.to_owned(),
            ),
            (
                Response::Delete {
                    deleted: 1,
                    bytes_freed: 10,
                    missing: vec![hash.clone()],
                },
                format!(r#"{{"delete":{{"deleted":1,"bytes_freed":10,"missing":["{hash}"]}}}}"#),
            ),
            (Response::Pin, r#""pin""#.to_owned()),
            (
                Response::Unpin { removed: false },
                r#"{"unpin":{"removed":false}}"#.to_owned(),
            ),
            (
                Response::Fsck {
                    dangling: vec![FsckRow {
                        store_path_hash: hash.clone(),
                        name: "x".into(),
                    }],
                    size_mismatches: vec![FsckSizeMismatch {
                        store_path_hash: hash.clone(),
                        name: "y".into(),
                        db_size: 5,
                        s3_size: 6,
                    }],
                    orphans: vec!["nar/k.nar.zst".into()],
                    repaired: 2,
                    quiesce_drained: Some(true),
                },
                format!(
                    r#"{{"fsck":{{"dangling":[{{"store_path_hash":"{hash}","name":"x"}}],"size_mismatches":[{{"store_path_hash":"{hash}","name":"y","db_size":5,"s3_size":6}}],"orphans":["nar/k.nar.zst"],"repaired":2,"quiesce_drained":true}}}}"#
                ),
            ),
            (
                Response::Fsck {
                    dangling: vec![],
                    size_mismatches: vec![],
                    orphans: vec![],
                    repaired: 0,
                    quiesce_drained: Some(false),
                },
                r#"{"fsck":{"dangling":[],"size_mismatches":[],"orphans":[],"repaired":0,"quiesce_drained":false}}"#
                    .to_owned(),
            ),
            (
                Response::Fsck {
                    dangling: vec![],
                    size_mismatches: vec![],
                    orphans: vec![],
                    repaired: 0,
                    quiesce_drained: None,
                },
                r#"{"fsck":{"dangling":[],"size_mismatches":[],"orphans":[],"repaired":0,"quiesce_drained":null}}"#
                    .to_owned(),
            ),
            (
                Response::Prune {
                    pruned: vec![format!("{hash}-x")],
                    bytes_freed: 7,
                },
                format!(r#"{{"prune":{{"pruned":["{hash}-x"],"bytes_freed":7}}}}"#),
            ),
            (
                Response::Backup { bytes: 4096 },
                r#"{"backup":{"bytes":4096}}"#.to_owned(),
            ),
            (
                Response::DeletePushedBy {
                    objects: vec![format!("{hash}-x")],
                    bytes_freed: 7,
                },
                format!(r#"{{"delete-pushed-by":{{"objects":["{hash}-x"],"bytes_freed":7}}}}"#),
            ),
            (
                Response::Error {
                    message: "no [gc] section".into(),
                },
                r#"{"error":{"message":"no [gc] section"}}"#.to_owned(),
            ),
        ] {
            assert_eq!(serde_json::to_string(&response).unwrap(), wire);
            assert_eq!(serde_json::from_str::<Response>(&wire).unwrap(), response);
        }
    }
}
