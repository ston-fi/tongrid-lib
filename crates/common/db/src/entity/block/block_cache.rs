use crate::entity::block::db_block::DBBlock;
use crate::metered_cache::MeteredCache;
use crate::util::init_db_cache;
use std::sync::Arc;

// cache by master_seqno
pub struct DBBlockCache {
    pub(super) mc_cache: MeteredCache<u32, Arc<DBBlock>>,
    pub(super) shards_cache: MeteredCache<i32, Arc<Vec<DBBlock>>>, // contains mc_block as well
}

#[derive(strum::IntoStaticStr, strum::EnumIter)]
enum CacheName {
    DBBlockMasterBlocks,
    DBBlockShardBlocks,
}

impl DBBlockCache {
    pub fn new(master_blocks_capacity: u64, shard_blocks_capacity: u64) -> Self {
        Self {
            mc_cache: init_db_cache(
                master_blocks_capacity,
                CacheName::DBBlockMasterBlocks.into(),
            ),
            shards_cache: init_db_cache(
                shard_blocks_capacity,
                CacheName::DBBlockShardBlocks.into(),
            ),
        }
    }
}
