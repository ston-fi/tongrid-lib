#![doc(html_no_source)]
//! Backup protobuf encoding/decoding, iteration and caller-owned local or S3 filesystems.
//! Application configuration and live orchestration belong to the caller.
pub mod proto {
    pub mod v1 {
        tonic::include_proto!("tongrid_backup.v1");
    }
}

pub mod backup_iterator;
pub mod backup_ser_de;
pub mod file_manager;
pub mod handler;
pub mod util;

const BLOCKS_IN_SUBFOLDER: usize = 1_000_000;

#[cfg(test)]
#[path = "tests/test_proto.rs"]
mod test_proto;

pub mod config;
