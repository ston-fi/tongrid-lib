pub mod mapper;
#[allow(
    clippy::module_inception,
    reason = "Keep the entity path alongside its mapper and Scylla modules"
)]
pub mod tvm_code_traits;
mod tvm_code_traits_scylla;
