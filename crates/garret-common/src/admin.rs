//! The admin protocol: one JSON request per line, one JSON response back.
//!
//! Everything that touches the DB goes through the Pusher (spec 10-packaging):
//! it owns all writes, so `garret-admin` never opens the database itself.
//! Line-delimited JSON over a root-only unix socket keeps both ends free of an
//! HTTP stack — the socket's file permissions are the whole access story.

use serde::{Deserialize, Serialize};

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
    Delete { hashes: Vec<String> },
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum Response {
    Status {
        objects: i64,
        total_bytes: i64,
        quota_bytes: Option<u64>,
        uploads_in_flight: usize,
    },
    Gc {
        evicted: usize,
        bytes_freed: i64,
        candidates_exhausted: bool,
    },
    Resign {
        resigned: usize,
    },
    Delete {
        deleted: usize,
        bytes_freed: i64,
        /// Hashes that were not in the cache; deleting them is a no-op, but
        /// silently reporting success would hide a typo.
        missing: Vec<String>,
    },
    Error {
        message: String,
    },
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
