//! Component settings; application composition and private presets remain caller-owned.
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct DBAccountCacheConfig {
    pub capacity: u64,
    pub ttl_seconds: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DBBlockCacheConfig {
    pub master_blocks_capacity: u64,
    pub shard_blocks_capacity: u64,
}
