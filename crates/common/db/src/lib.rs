//! Scylla mappings, query caches, owned CQL schema and synchronization progress.
//! Application configuration and live orchestration belong to the caller.
pub mod db;
pub mod paginator;

pub mod entity;
mod metered_cache;
mod migrations;
pub mod scylla_serde;
pub mod util;

pub const DB_WRITE_CONCURRENCY: usize = 128;

#[cfg(feature = "test-support")]
pub mod test_support;

#[cfg(test)]
#[path = "tests/test_contract_traits.rs"]
mod test_contract_traits;

#[cfg(test)]
#[path = "tests/test_empty_message.rs"]
mod test_empty_message;

pub mod config;
