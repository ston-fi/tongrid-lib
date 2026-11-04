//! Hardcoded component settings and API registration used by main.rs.
use anyhow::{ensure, Context};
use std::collections::HashSet;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use stonfi_scylla_client::client::ScyllaClient;
use stonfi_sync_core::{
    errors::SyncCoreResult,
    sync_engine::{SyncCallback, SyncHeight},
};
use ton::emulators::emulator_pool::EmulatorPool;
use ton::lite_client::LiteClient;
use tongrid_lib::tongrid_account_handler::{config::AccountHandlerConfig, handler::AccountHandler};
use tongrid_lib::tongrid_api_grpc_service_v3::{
    config::ServicesConfig,
    context::ServiceContext,
    grpc_impl::{
        block::BlockGrpcImpl, emulation::EmulationGrpcImpl, jetton::JettonGrpcImpl,
        library::LibraryGrpcImpl, send_msg::SendMsgGrpcImpl, sync_status::SyncStatusGrpcImpl,
        tx::TxGrpcImpl,
    },
};
use tongrid_lib::tongrid_classifier_handler::{config::ClassifierHandlerConfig, ClassifierHandler};
use tongrid_lib::tongrid_core::sparse_contracts::config::SparseContractsConfig;
use tongrid_lib::tongrid_db::{
    config::{
        cache::{DBAccountCacheConfig, DBBlockCacheConfig},
        DBConfig, KeyspaceConfig, ScyllaClientConfig,
    },
    db::DB,
    entity::StateID,
};
use tongrid_lib::tongrid_grpc_server::{
    config::{ForwardConfig, GrpcServerConfig},
    server::GrpcServer,
};
use tongrid_lib::tongrid_jetton_handler::{config::JettonHandlerConfig, handler::JettonHandler};
use tongrid_lib::tongrid_mem_store::store::MemStore;
use tongrid_lib::tongrid_proto::{
    v3::{
        block_grpc_server::BlockGrpcServer, emulation_grpc_server::EmulationGrpcServer,
        jetton_grpc_server::JettonGrpcServer, library_grpc_server::LibraryGrpcServer,
        send_msg_grpc_server::SendMsgGrpcServer, sync_status_grpc_server::SyncStatusGrpcServer,
        tx_grpc_server::TxGrpcServer,
    },
    TONGRID_API_GRPC_DESCRIPTOR,
};
use tongrid_lib::tongrid_tda::{
    ser_de::block::block_serdb,
    ton_db_adapter::{config::genesis_config, TonDBAdapter},
};
use tongrid_lib::tongrid_ton::tg_ton::{block_stream::TongridBlock, TGTon};

pub const SCYLLA_ADDRESS: &str = "127.0.0.1:19042";
pub const KEYSPACE: &str = "tongrid_sync_demo_mainnet";
pub const API_ADDRESS: &str = "127.0.0.1:50051";
pub const START_SCYLLA: &str = "Scylla is not ready at 127.0.0.1:19042. From the repository root, run `docker compose up -d --wait scylla` (or `docker-compose up -d scylla`), then run tongrid-sync-demo again.";

pub async fn connect_scylla(address: &str, deadline: Duration) -> anyhow::Result<ScyllaClient> {
    tokio::time::timeout(deadline, async {
        let config = database_config().scylla;
        let client = ScyllaClient::builder(address, &config.keyspace.name)
            .with_max_parallel_queries(config.max_parallel_queries)
            .with_replication_factor(config.keyspace.replication_factor)
            .with_request_timeout(Duration::from_millis(config.request_timeout_ms))
            .with_retry_count(config.retry_count)
            .with_retry_min_delay(Duration::from_millis(config.initial_retry_delay_ms))
            .with_retry_max_delay(Duration::from_millis(config.max_retry_delay_ms))
            .build()
            .await?;
        let version = client
            .select_one::<(String,)>(
                "SELECT release_version FROM system.local",
                (),
                "demo_preflight",
            )
            .await?;
        ensure!(version.is_some(), "Scylla readiness query returned no row");
        anyhow::Ok(client)
    })
    .await
    .map_err(|_| anyhow::anyhow!(START_SCYLLA))?
    .map_err(|_| anyhow::anyhow!(START_SCYLLA))
}

pub async fn initialize_ton(scylla: ScyllaClient) -> anyhow::Result<TonDBAdapter> {
    let db = DB::new(scylla, &database_config())
        .await
        .context("Initialize the demo schema")?;
    let client = LiteClient::builder()?
        .with_net_config_json(ton::net_config::TON_NET_CONF_MAINNET_PUBLIC)?
        .build()?;
    let latest = client
        .get_mc_info()
        .await
        .context("Read mainnet head from ton-rs public nodes")?;
    let block = client.get_block(latest.last, None).await?.data;
    let ton = TGTon::new(
        client,
        EmulatorPool::builder()?.with_threads_count(2).build()?,
        Arc::new(block.info.clone().into_inner()),
    );
    // Initialize exactly once. A restart must never reset handler checkpoints.
    if db.progress().load_initial_height().await?.is_none() {
        let initial = block.info.seqno;
        seed_block(&db, &ton.lite_client, initial).await?;
        seed_block(&db, &ton.lite_client, block.info.prev_key_block_seqno).await?;
        db.progress()
            .save_initial_height(u64::from(initial))
            .await?;
        println!("Starting mainnet sync after masterchain block {initial}");
    }
    let tda = TonDBAdapter::new(db, ton, genesis_config()?, 32).await?;
    // API startup also works after interruption before the first fully synced range.
    let full = u32::try_from(
        tda.db()
            .progress()
            .load_synced_or_initial(StateID::JettonSync.as_ref())
            .await?,
    )?;
    seed_block(tda.db(), &tda.ton().lite_client, full).await?;
    let key = tda
        .db()
        .block()
        .get_master_by_mc_seqno(full)
        .await?
        .key_block_seqno;
    seed_block(tda.db(), &tda.ton().lite_client, key).await?;
    Ok(tda)
}

async fn seed_block(db: &DB, client: &LiteClient, seqno: u32) -> anyhow::Result<()> {
    if db
        .block()
        .get_master_by_mc_seqno_opt(seqno)
        .await?
        .is_some()
    {
        return Ok(());
    }
    let id = client.lookup_mc_block(seqno).await?;
    let block = client.get_block(id.clone(), None).await?.data;
    let block = TongridBlock::new(block, id)?;
    db.block().insert(&block_serdb(seqno, &block)?).await
}

// The coordinator and worker each own a handler instance; both use the same DB.
pub async fn handlers(
    tda: &TonDBAdapter,
) -> anyhow::Result<(AccountHandler, ClassifierHandler, JettonHandler)> {
    let account_config = AccountHandlerConfig {
        max_batch_size: 1,
        latest_state_cache_size: 1000,
        ..Default::default()
    };
    let account = AccountHandler::new(
        tda.ton().clone(),
        tda.clone(),
        &account_config,
        HashSet::new(),
        StateID::AccountSync,
    )
    .await?;
    let classifier = ClassifierHandler::new(
        tda.clone(),
        &ClassifierHandlerConfig::default(),
        StateID::ClassifierSync,
        Arc::new(|_| Vec::new()),
    )
    .await?;
    let jetton = JettonHandler::new(
        tda.clone(),
        &JettonHandlerConfig::default(),
        StateID::JettonSync,
    )
    .await?;
    Ok((account, classifier, jetton))
}

pub async fn api(tda: TonDBAdapter) -> anyhow::Result<GrpcServer> {
    let senders = vec![Arc::new(tda.ton().lite_client.clone())
        as Arc<dyn tongrid_lib::tongrid_ton::ton_msg_sender::TonMsgSender>];
    let store = MemStore::new(tda.clone()).await?;
    let context = Arc::new(ServiceContext::new(
        tda,
        store,
        senders,
        ServicesConfig::default(),
    )?);
    let mut server = GrpcServer::new(
        &GrpcServerConfig {
            listen_address: API_ADDRESS.into(),
            max_concurrent_streams: 32,
            forward: ForwardConfig {
                address: String::new(),
                percentage: 0.0,
                enabled: false,
            },
        },
        None,
        Some(TONGRID_API_GRPC_DESCRIPTOR),
    )?;
    server
        .add_service(SyncStatusGrpcServer::new(SyncStatusGrpcImpl::new(
            context.clone(),
        )))
        .await?;
    server
        .add_service(BlockGrpcServer::new(BlockGrpcImpl::new(context.clone())))
        .await?;
    server
        .add_service(TxGrpcServer::new(TxGrpcImpl::new(context.clone())))
        .await?;
    server
        .add_service(JettonGrpcServer::new(JettonGrpcImpl::new(context.clone())?))
        .await?;
    server
        .add_service(LibraryGrpcServer::new(LibraryGrpcImpl::new(
            context.clone(),
        )))
        .await?;
    server
        .add_service(EmulationGrpcServer::new(EmulationGrpcImpl::new(
            context.clone(),
        )?))
        .await?;
    server
        .add_service(SendMsgGrpcServer::new(SendMsgGrpcImpl::new(context)))
        .await?;
    Ok(server)
}

fn database_config() -> DBConfig {
    DBConfig {
        scylla: ScyllaClientConfig {
            url: SCYLLA_ADDRESS.into(),
            max_parallel_queries: 32,
            keyspace: KeyspaceConfig {
                name: KEYSPACE.into(),
                replication_factor: 1,
            },
            request_timeout_ms: 5000,
            retry_count: 3,
            initial_retry_delay_ms: 100,
            max_retry_delay_ms: 1000,
        },
        inmemory_state: false,
        block_shard_cache: DBBlockCacheConfig {
            master_blocks_capacity: 128,
            shard_blocks_capacity: 512,
        },
        account_cache: DBAccountCacheConfig {
            capacity: 1000,
            ttl_seconds: 60,
        },
        tvm_code_cache_size: 1000,
        tvm_code_traits_cache_size: 1000,
        sparse_contracts: SparseContractsConfig {
            store_period: NonZeroU32::MIN,
            addresses: HashSet::new(),
        },
    }
}

pub struct PrintProgress;
#[async_trait::async_trait]
impl SyncCallback for PrintProgress {
    async fn on_sync_error(
        &self,
        handler: &str,
        from: SyncHeight,
        to: SyncHeight,
    ) -> SyncCoreResult<()> {
        eprintln!("{handler}: range {from}..={to} failed; the sync engine will retry");
        Ok(())
    }
    async fn on_sync_complete(
        &self,
        handler: &str,
        _from: SyncHeight,
        _to: SyncHeight,
        completed: SyncHeight,
    ) -> SyncCoreResult<()> {
        println!("{handler}: synced through {completed}");
        Ok(())
    }
}
