//! V3 gRPC service algorithms, transaction-tree limits and protocol serialization.
//! Application configuration and live orchestration belong to the caller.
mod proto_serializer;

pub mod context;
pub mod error;
pub mod grpc_impl;
pub mod services;
pub mod tx_tree;
pub mod utils;

pub mod config;
