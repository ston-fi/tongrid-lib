#![doc(html_no_source)]
//! Generated Tongrid v3 gRPC services and messages.
//!
//! Library lookup is database content lookup by hash, including ordinary contract
//! and private library cells. It omits unknown hashes and preserves input order
//! and duplicates. It does not certify deployment or fetch missing bytecode from
//! the network. The deprecated request seqno is ignored. Emulation always resolves
//! libraries without historical deployment filtering.
//!
//! ```no_run
//! use tongrid_proto::v3::{GetLibrariesRequest, GetLibrariesResponse};
//! use tongrid_proto::v3::library_grpc_client::LibraryGrpcClient;
//! use tonic::transport::Channel;
//!
//! async fn lookup_code(
//!     channel: Channel,
//!     hashes: Vec<[u8; 32]>,
//! ) -> Result<GetLibrariesResponse, tonic::Status> {
//!     let mut client = LibraryGrpcClient::new(channel);
//!     let response = client.get_libraries(GetLibrariesRequest {
//!         lib_ids: hashes.into_iter().map(Vec::from).collect(),
//!         ..Default::default()
//!     }).await?;
//!     Ok(response.into_inner())
//! }
//! ```

pub mod v3 {
    tonic::include_proto!("tongrid.v3");
}

mod range;

pub const TONGRID_API_GRPC_DESCRIPTOR: &[u8] =
    tonic::include_file_descriptor_set!("tongrid_descriptor");

#[cfg(test)]
#[path = "tests/test_wire.rs"]
mod tests;
