//! The admin protocol: one JSON request per line, one JSON response back.
//!
//! Everything that touches the DB goes through the Pusher (spec 10-packaging):
//! it owns all writes, so `garret-admin` never opens the database itself.
//! Line-delimited JSON over a root-only unix socket keeps both ends free of an
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
    },
    /// Audit row ⇔ blob consistency (spec 02/03), and optionally repair it.
    Fsck {
        /// Delete dangling rows and size-mismatched rows. Dry-run (report
        /// only) is the default.
        repair: bool,
        /// Compare `file_size` against the S3 object's size for hashes with
        /// both a row and a blob; mismatches are a third finding category.
        verify_sizes: bool,
        /// Reject new pushes and wait for in-flight uploads to drain before
        /// repairing, for the degraded case where the live in-flight signal
        /// alone isn't trusted. Only meaningful with `repair: true`.
        quiesce: bool,
    },
}

/// The Pusher's reply to a [`Request`]; variants mirror the request commands,
/// plus [`Response::Error`] for any failure.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum Response {
    /// Reply to [`Request::Status`].
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
    /// Reply to [`Request::GcRun`].
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
    /// Reply to [`Request::Resign`].
    Resign {
        /// Objects whose narinfo signatures were rewritten.
        resigned: usize,
    },
    /// Reply to [`Request::Delete`].
    Delete {
        /// Objects actually removed.
        deleted: usize,
        /// Compressed bytes reclaimed.
        bytes_freed: i64,
        /// Hashes that were not in the cache; deleting them is a no-op, but
        /// silently reporting success would hide a typo.
        missing: Vec<String>,
    },
    /// Reply to [`Request::Fsck`].
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
        /// combined). Zero unless `repair: true`.
        repaired: usize,
        /// Whether quiesce mode was engaged at all, mirroring the request.
        quiesced: bool,
        /// True if in-flight uploads reached zero within the timeout. False
        /// means the drain timed out and no repair happened, even if
        /// `repair` was requested.
        quiesce_drained: bool,
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
    fn requests_round_trip_as_one_line() {
        for request in [
            Request::Status,
            Request::GcRun,
            Request::Resign,
            Request::Delete {
                hashes: vec!["a".repeat(32)],
            },
            Request::Fsck {
                repair: true,
                verify_sizes: true,
                quiesce: true,
            },
        ] {
            let line = serde_json::to_string(&request).unwrap();
            assert!(!line.contains('\n'), "a request must fit on one line");
            assert_eq!(serde_json::from_str::<Request>(&line).unwrap(), request);
        }
        // The tag is what the wire carries, so it is part of the contract.
        assert_eq!(
            serde_json::to_string(&Request::GcRun).unwrap(),
            r#"{"command":"gc-run"}"#
        );
        assert_eq!(
            serde_json::to_string(&Request::Fsck {
                repair: false,
                verify_sizes: false,
                quiesce: false,
            })
            .unwrap(),
            r#"{"command":"fsck","repair":false,"verify_sizes":false,"quiesce":false}"#
        );
    }

    #[test]
    fn responses_round_trip() {
        let response = Response::Status {
            objects: 3,
            total_bytes: 100,
            quota_bytes: Some(1000),
            uploads_in_flight: 1,
        };
        let line = serde_json::to_string(&response).unwrap();
        assert_eq!(serde_json::from_str::<Response>(&line).unwrap(), response);
    }
}
