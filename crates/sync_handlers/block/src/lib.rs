//! Block synchronization and initial-transaction identity resolution.
//! Application configuration and live orchestration belong to the caller.
pub mod handler;

pub use handler::blocks_provider::BlocksProvider;

// pub for tests
pub use handler::initial_tx_resolver::is_initial_tx;
pub use handler::types::BlocksDBData;
pub use handler::BlockHandler;

pub mod config;
