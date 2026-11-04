//! Component settings; application composition and private presets remain caller-owned.
use serde::Deserialize;

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct BlockSyncConfig {
    pub enabled: bool,
    pub resync_enabled: bool,
    pub sync_from: i32,
    pub sync_to: Option<u32>,
    pub block_stream_buffer_size: usize,
    pub blocks_max_parallel_processing: usize,
}

impl BlockSyncConfig {
    pub fn new(from: i32, to: Option<u32>) -> Self {
        Self {
            enabled: true,
            resync_enabled: false,
            sync_from: from,
            sync_to: to,
            block_stream_buffer_size: 1,
            blocks_max_parallel_processing: 1,
        }
    }
}
