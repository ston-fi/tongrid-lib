//! Shared Tongrid numeric, configuration and data helpers.
//!
//! Generic configuration utilities, time and sparse storage policies.
//! Process logging initialization and signal ownership stay with the application.

pub mod config;
pub mod constants;
pub mod math;
pub mod sparse_contracts;
pub mod time;

#[cfg(test)]
#[path = "tests/test_math.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/test_config.rs"]
mod test_config;

#[cfg(test)]
#[path = "tests/test_sparse_contracts.rs"]
mod test_sparse_contracts;

pub mod metrics;

#[cfg(test)]
#[path = "tests/test_sparse_config.rs"]
mod test_sparse_config;
