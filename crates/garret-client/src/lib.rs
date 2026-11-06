//! Client library behind the `garret` CLI: token acquisition, the push path,
//! the store watcher and the browse and diagnosis commands.

#![warn(missing_docs)]

pub mod auth;
pub mod browse;
pub mod config;
pub mod discovery;
pub mod doctor;
pub mod files;
mod http;
pub mod nixconf;
pub mod push;
#[cfg(test)]
mod testing;
pub mod watcher;
