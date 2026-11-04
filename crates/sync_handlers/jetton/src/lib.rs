//! Jetton wallet and balance synchronization.
//! Application configuration and live orchestration belong to the caller.
pub mod handler;

pub mod config;

#[cfg(test)]
#[path = "tests/test_jetton_config.rs"]
mod test_jetton_config;
