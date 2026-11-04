//! Account-state reconstruction and synchronization with caller-supplied critical code identities.
//! Application configuration and live orchestration belong to the caller.
pub mod handler;

pub use handler::AccountHandler;

pub mod config;

#[cfg(test)]
#[path = "tests/test_config.rs"]
mod test_config;
