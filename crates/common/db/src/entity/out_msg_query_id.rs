#[allow(
    clippy::module_inception,
    reason = "Keep the entity path alongside its mapper and Scylla modules"
)]
pub mod out_msg_query_id;
pub mod out_msg_query_id_mapper;
mod out_msg_query_id_scylla;
