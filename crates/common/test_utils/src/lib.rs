//! Pure TON fixture builders and explicitly selected disposable loopback Scylla lifecycle.
//! Application configuration and live orchestration belong to the caller.
//! Leaf fixture helpers and opt-in disposable loopback Scylla lifecycle.
//! Domain setup belongs to the owning crate; private orchestration belongs to app tests.
#[cfg(feature = "scylla")]
pub mod db;
pub mod ton;
