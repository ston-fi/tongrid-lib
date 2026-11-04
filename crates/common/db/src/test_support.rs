//! Disposable loopback-only DB construction; no application configuration.
use crate::config::{
    cache::{DBAccountCacheConfig, DBBlockCacheConfig},
    DBConfig, KeyspaceConfig, ScyllaClientConfig,
};
use crate::db::DB;
use std::{collections::HashSet, num::NonZeroU32, time::Duration};
use stonfi_scylla_client::client::ScyllaClient;
use tongrid_core::sparse_contracts::config::SparseContractsConfig;
use tongrid_test_utils::db::ScyllaTestEnv;
/// Creates the real schema in a unique local test keyspace. Caller owns cleanup.
pub async fn init_test_db() -> anyhow::Result<(ScyllaTestEnv, DB)> {
    let env = ScyllaTestEnv::new().await?;
    let scylla = ScyllaClient::builder(tongrid_test_utils::db::LOCAL_SCYLLA_URL, env.keyspace())
        .with_max_parallel_queries(1000)
        .with_replication_factor(1)
        .with_request_timeout(Duration::from_secs(30))
        .with_retry_count(5)
        .with_retry_min_delay(Duration::from_millis(100))
        .with_retry_max_delay(Duration::from_secs(1))
        .build()
        .await?;
    let db = {
        let _schema_lock = tongrid_test_utils::db::acquire_schema_lock().await?;
        tokio::time::timeout(
            Duration::from_secs(60),
            DB::new(
                scylla,
                &DBConfig {
                    scylla: ScyllaClientConfig {
                        url: tongrid_test_utils::db::LOCAL_SCYLLA_URL.into(),
                        max_parallel_queries: 1000,
                        keyspace: KeyspaceConfig {
                            name: env.keyspace().into(),
                            replication_factor: 1,
                        },
                        request_timeout_ms: 30_000,
                        retry_count: 5,
                        initial_retry_delay_ms: 100,
                        max_retry_delay_ms: 1000,
                    },
                    inmemory_state: false,
                    block_shard_cache: DBBlockCacheConfig {
                        master_blocks_capacity: 0,
                        shard_blocks_capacity: 0,
                    },
                    account_cache: DBAccountCacheConfig {
                        capacity: 0,
                        ttl_seconds: 300,
                    },
                    tvm_code_cache_size: 0,
                    tvm_code_traits_cache_size: 0,
                    sparse_contracts: SparseContractsConfig {
                        store_period: NonZeroU32::MIN,
                        addresses: HashSet::new(),
                    },
                },
            ),
        )
        .await??
    };
    Ok((env, db))
}
