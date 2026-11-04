//! Component settings; application composition and private presets remain caller-owned.
pub mod cache;
use self::cache::DBAccountCacheConfig;
use self::cache::DBBlockCacheConfig;
use serde::Deserialize;
use tongrid_core::sparse_contracts::config::SparseContractsConfig;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyspaceConfig {
    pub name: String,
    pub replication_factor: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ScyllaClientConfig {
    pub url: String,
    pub max_parallel_queries: usize,
    pub keyspace: KeyspaceConfig,
    pub request_timeout_ms: u64,
    pub retry_count: u32,
    pub initial_retry_delay_ms: u64,
    pub max_retry_delay_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DBConfig {
    pub scylla: ScyllaClientConfig,
    pub inmemory_state: bool,
    pub block_shard_cache: DBBlockCacheConfig,
    pub account_cache: DBAccountCacheConfig,
    pub tvm_code_cache_size: u64,
    pub tvm_code_traits_cache_size: u64,
    pub sparse_contracts: SparseContractsConfig,
}
