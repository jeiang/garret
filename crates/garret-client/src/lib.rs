//! Client library: token acquisition and the push path. `garret-bench` (M5)
//! reuses this so benchmarks exercise the real protocol.

#![warn(missing_docs)]

pub mod auth;
pub mod browse;
pub mod config;
pub mod discovery;
pub mod nixconf;
pub mod push;
pub mod watcher;
