//! One process: Scylla -> TON -> block -> account -> classifier -> jetton -> API.
mod setup;

use anyhow::Context;
use std::fs::OpenOptions;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use stonfi_distributed_sync::{
    coordinator::Coordinator, task_server::TaskServer, traits::DistributedHandler, worker::Worker,
};
use stonfi_sync_core::sync_engine::{HeightProvider, SyncEngine, Synchronizer};
use tongrid_lib::tongrid_block_handler::{config::BlockSyncConfig, BlockHandler};
use tongrid_lib::tongrid_db::entity::StateID;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    stonfi_metrics::init_metrics!()?;
    ton::sys_utils::sys_tonlib_set_verbosity_level(0);

    // 1. Check real CQL readiness before any schema changes or mainnet access.
    let scylla = setup::connect_scylla(setup::SCYLLA_ADDRESS, Duration::from_secs(5)).await?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(std::env::temp_dir().join("tongrid-sync-demo.lock"))?;
    fs2::FileExt::try_lock_exclusive(&lock)
        .context("Another tongrid-sync-demo is already running")?;

    let startup = async {
        // 2. Initialize storage and public TON clients, preserving saved checkpoints.
        let tda = setup::initialize_ton(scylla).await?;
        let heights = HeightProvider::new(tda.ton().clone());
        let initial = i32::try_from(
            tda.db()
                .progress()
                .load_synced_or_initial(StateID::BlockSync.as_ref())
                .await?,
        )?;
        let block: Synchronizer = BlockHandler::new(
            tda.clone(),
            &BlockSyncConfig::new(initial, None),
            StateID::BlockSync,
        )
        .await?
        .into();

        // 3. Keep the coordinator and its worker inside this process on loopback.
        let coordinator = Coordinator::new();
        let task_server = TaskServer::builder(coordinator.clone())
            .with_listen_address("127.0.0.1:0".parse()?)
            .build()
            .await?;
        let address = task_server.local_address();
        let (account, classifier, jetton) = setup::handlers(&tda).await?;
        let worker = Worker::builder(format!("http://{address}"))
            .with_parallelism(NonZeroUsize::MIN.saturating_add(1))
            .with_shutdown_timeout(Duration::from_secs(5))
            .add_handler(account)?
            .add_handler(classifier)?
            .add_handler(jetton)?
            .build()?;

        // 4. Wire progress dependencies. Each stage waits for its upstream stage.
        let (account, classifier, jetton) = setup::handlers(&tda).await?;
        let account: Synchronizer = account.into_sync(coordinator.clone()).into();
        let classifier: Synchronizer = classifier.into_sync(coordinator.clone()).into();
        let jetton = jetton.into_sync(coordinator);
        let engine = SyncEngine::builder(tda.db().progress().clone())
            .add_synchronizer(jetton, &[&classifier])?
            .add_synchronizer(classifier, &[&account])?
            .add_synchronizer(account, &[&block])?
            .add_synchronizer(block, &[&heights])?
            .add_height_provider(heights)?
            .add_callback(Arc::new(setup::PrintProgress))
            .with_shutdown_timeout(Duration::from_secs(5))
            .build();
        let api = setup::api(tda).await?;
        anyhow::Ok((task_server, worker, engine, api))
    };
    let (task_server, worker, engine, api) = tokio::select! {
        signal = tokio::signal::ctrl_c() => { signal?; return Ok(()); }
        result = tokio::time::timeout(Duration::from_secs(120), startup) => result.context("Demo startup timed out")??,
    };

    // 5. Start sync and the API together; Ctrl-C or an API failure stops the demo.
    let task_server = task_server.run();
    let worker = worker.run();
    let engine = engine.run();
    println!(
        "Sync started; starting API on {}. Press Ctrl-C to stop.",
        setup::API_ADDRESS
    );
    let result = tokio::select! {
        signal = tokio::signal::ctrl_c() => signal.map_err(anyhow::Error::from),
        result = api.serve() => result,
    };

    // Stop every component even if an earlier shutdown reports an error.
    let engine_result = engine.shutdown().await;
    let worker_result = worker.shutdown().await;
    let server_result = task_server.shutdown().await;
    result?;
    engine_result?;
    worker_result?;
    server_result?;
    Ok(())
}

#[cfg(test)]
#[path = "../../crates/public/tongrid_lib/src/tests/test_sync_demo.rs"]
mod tests;
