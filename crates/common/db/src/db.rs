use crate::entity::account::account_cache::DBAccountCache;
use crate::entity::account::account_mapper::AccountStateMapper;
use crate::entity::account::db_account::DBAccount;
use crate::entity::account::db_account_lite::DBAccountLite;
use crate::entity::block::block_cache::DBBlockCache;
use crate::entity::jetton_balance::JettonBalanceMapper;
use crate::entity::jetton_wallet::JettonWalletMapper;
use crate::entity::msg_txs::msg_txs_mapper::MsgTxsMapper;
use crate::entity::ongoing_tx::OngoingTxMapper;
use crate::entity::out_msg_query_id::out_msg_query_id_mapper::OutMsgQueryIdMapper;
use crate::entity::tvm_code_traits::mapper::TvmCodeTraitsMapper;
use crate::entity::tvm_code_traits::tvm_code_traits::TvmCodeTraits;
use crate::entity::{block::block_mapper::BlockMapper, DBTx, DBTxLite};
use crate::entity::{TvmCode, TvmCodeMapper, TxMapper};
use crate::metered_cache::MeteredCache;
use crate::migrations::apply_migrations;
use crate::util::init_db_cache;
use anyhow::bail;
use std::sync::Arc;
use stonfi_scylla_client::client::ScyllaClient;
use stonfi_scylla_client::simple_migrator::SimpleMigrator;
use stonfi_sync_core::mem_progress_store::MemProgressStore;
use stonfi_sync_core::scylla_progress_store::ScyllaProgressStore;
use stonfi_sync_core::sync_engine::SyncProgressStore;
use ton::ton_core::cell::TonHash;
use tongrid_core::sparse_contracts::SparseContracts;

#[derive(Clone)]
pub struct DB {
    inner: Arc<Inner>,
}

impl DB {
    pub async fn new(scylla: ScyllaClient, config: &crate::config::DBConfig) -> anyhow::Result<DB> {
        let inner = Arc::new(
            Inner::new(
                scylla,
                config.inmemory_state,
                config.block_shard_cache.master_blocks_capacity,
                config.block_shard_cache.shard_blocks_capacity,
                config.account_cache.capacity,
                std::time::Duration::from_secs(config.account_cache.ttl_seconds),
                config.tvm_code_cache_size,
                config.tvm_code_traits_cache_size,
                SparseContracts::new(
                    config.sparse_contracts.store_period,
                    config.sparse_contracts.addresses.clone(),
                ),
            )
            .await?,
        );
        log::info!("[TONGRID_DB] initialized");
        Ok(DB { inner })
    }

    pub fn account(&self) -> AccountStateMapper<'_, DBAccount> {
        self.into()
    }

    pub fn account_lite(&self) -> AccountStateMapper<'_, DBAccountLite> {
        self.into()
    }

    pub fn block(&self) -> BlockMapper<'_> {
        self.into()
    }

    pub fn msg_txs(&self) -> MsgTxsMapper<'_> {
        self.into()
    }

    pub fn out_msg_query_id(&self) -> OutMsgQueryIdMapper<'_> {
        self.into()
    }

    pub fn tx(&self) -> TxMapper<'_, DBTx> {
        self.into()
    }

    pub fn tx_lite(&self) -> TxMapper<'_, DBTxLite> {
        self.into()
    }

    pub fn tvm_code(&self) -> TvmCodeMapper<'_> {
        self.into()
    }

    pub fn tvm_code_traits(&self) -> TvmCodeTraitsMapper<'_> {
        self.into()
    }

    pub fn jetton_wallet(&self) -> JettonWalletMapper<'_> {
        self.into()
    }

    pub fn jetton_balance(&self) -> JettonBalanceMapper<'_> {
        self.into()
    }

    pub fn progress(&self) -> &Arc<dyn SyncProgressStore> {
        &self.inner.progress
    }

    pub fn sparse_contracts(&self) -> &SparseContracts {
        &self.inner.sparse_contracts
    }

    pub fn ongoing_tx(&self) -> OngoingTxMapper<'_> {
        self.into()
    }

    pub(crate) fn inner(&self) -> &Inner {
        &self.inner
    }
}

pub(crate) struct Inner {
    pub(crate) scylla: ScyllaClient,
    pub(crate) progress: Arc<dyn SyncProgressStore>,
    pub(crate) block_shards_cache: DBBlockCache,
    pub(crate) account_cache: DBAccountCache<DBAccount>,
    pub(crate) account_lite_cache: DBAccountCache<DBAccountLite>,
    pub(crate) tvm_code_cache: MeteredCache<TonHash, TvmCode>,
    pub(crate) tvm_code_traits_cache: MeteredCache<TonHash, TvmCodeTraits>,
    pub(crate) sparse_contracts: SparseContracts,
}

impl Inner {
    #[allow(clippy::too_many_arguments)]
    async fn new(
        scylla: ScyllaClient,
        inmemory_state: bool,
        master_blocks_capacity: u64,
        shard_blocks_capacity: u64,
        account_capacity: u64,
        account_ttl: std::time::Duration,
        tvm_code_cache_size: u64,
        tvm_code_traits_cache_size: u64,
        sparse_contracts: SparseContracts,
    ) -> anyhow::Result<Self> {
        let migrator = SimpleMigrator::new(scylla.clone());

        apply_migrations(&migrator).await?;

        scylla.use_keyspace().await?;

        let progress = Box::pin(create_progress_store(&scylla, inmemory_state)).await?;

        let block_shards_cache = DBBlockCache::new(master_blocks_capacity, shard_blocks_capacity);
        let account_cache =
            DBAccountCache::new_with_name(account_capacity, account_ttl, "DBAccountCache");
        let account_lite_cache =
            DBAccountCache::new_with_name(account_capacity, account_ttl, "DBAccountLiteCache");
        let tvm_code_cache = init_db_cache(tvm_code_cache_size, TvmCode::CACHE_NAME);
        let tvm_code_traits_cache =
            init_db_cache(tvm_code_traits_cache_size, TvmCodeTraits::CACHE_NAME);

        let inner = Inner {
            scylla,
            progress,
            block_shards_cache,
            account_cache,
            account_lite_cache,
            tvm_code_cache,
            tvm_code_traits_cache,
            sparse_contracts,
        };
        Ok(inner)
    }
}

async fn create_progress_store(
    scylla: &ScyllaClient,
    in_memory: bool,
) -> anyhow::Result<Arc<dyn SyncProgressStore>> {
    let scylla_progress = ScyllaProgressStore::builder()
        .with_scylla_client(scylla.clone())
        .build()
        .await?;

    if !in_memory {
        return Ok(Arc::new(scylla_progress));
    }

    let Some(initial_height) = scylla_progress.load_initial_height().await? else {
        bail!("Initial seqno must be defined in db to use inmemory state");
    };
    let memory_progress = MemProgressStore::new(initial_height);
    Ok(Arc::new(memory_progress))
}
