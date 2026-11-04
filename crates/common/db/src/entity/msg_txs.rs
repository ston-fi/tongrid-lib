#[allow(
    clippy::module_inception,
    reason = "Keep the entity path alongside its mapper and Scylla modules"
)]
pub mod msg_txs;
pub mod msg_txs_mapper;
mod msg_txs_scylla;
