use crate::types::sync_status::SyncStatus;
use std::sync::Arc;
use ton::block_tlb::{Block, ConfigParams};

pub struct BlockRecentData {
    pub last_mc_block: Block,
    pub last_bc_config: ConfigParams,
    pub last_key_block_seqno: u32,
}

pub struct RecentData {
    pub generation: i64, // How many times data were updated. Only for logs.
    pub sync_status: SyncStatus,
    pub block_data: Arc<BlockRecentData>,
}
