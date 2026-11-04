use log::LevelFilter;
use std::sync::Arc;
use std::time::Duration;
use stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};
use stonfi_sync_core::sync_engine::{HeightLoader, SyncHeight};
use tokio::sync::watch::{Receiver, Sender};
use tokio::{spawn, time::sleep};
use ton::block_tlb::BlockInfo;
use ton::emulators::emulator_pool::EmulatorPool;
use ton::lite_client::LiteClient;

pub mod block_stream;
mod metrics;

#[derive(Clone)]
pub struct TGTon {
    pub lite_client: LiteClient,
    pub emul_pool: EmulatorPool,
    last_block_info: Receiver<Arc<BlockInfo>>,
    _workers: Arc<Workers>,
}

impl TGTon {
    /// Starts monitoring an already initialized client. The last clone owns worker shutdown.
    pub fn new(
        lite_client: LiteClient,
        emul_pool: EmulatorPool,
        last_info: Arc<BlockInfo>,
    ) -> Self {
        metrics::set_last_seen_info(&last_info);
        let (tx, rx) = tokio::sync::watch::channel(last_info);
        let workers = Workers {
            last_block: spawn(last_block_loop(lite_client.clone(), tx.clone())),
            metrics: spawn(metric_loop(lite_client.clone(), tx)),
        };
        Self {
            lite_client,
            emul_pool,
            last_block_info: rx,
            _workers: Arc::new(workers),
        }
    }

    pub fn get_last_seen_info(&self) -> Arc<BlockInfo> {
        self.last_block_info.borrow().clone()
    }
}

/// Uses the monitored height when ahead, otherwise looks up the next masterchain block.
/// Lookup cancellation and retry scheduling belong to the sync engine.
#[async_trait::async_trait]
impl HeightLoader for TGTon {
    fn id(&self) -> &str {
        // Preserve the existing sync-engine identity across removal of the app wrapper.
        "TONInitiator"
    }

    async fn latest_height(&mut self, after: SyncHeight) -> SyncCoreResult<SyncHeight> {
        let last_height = SyncHeight::from(self.get_last_seen_info().seqno);
        if last_height > after {
            return Ok(last_height);
        }

        let next_seqno = u32::try_from(after)
            .map_err(SyncCoreError::invalid_args)?
            .checked_add(1)
            .ok_or_else(|| SyncCoreError::invalid_args("TON height exceeds u32"))?;
        let block_id = self
            .lite_client
            .lookup_mc_block(next_seqno)
            .await
            .map_err(SyncCoreError::net)?;
        Ok(SyncHeight::from(block_id.seqno))
    }
}

async fn last_block_loop(client: LiteClient, tx: Sender<Arc<BlockInfo>>) {
    log::info!("[last_block_loop] loop started");
    let mut next_seqno = tx.subscribe().borrow().seqno + 1;

    loop {
        let info = match fetch_block(&client, next_seqno).await {
            Ok(info) => Arc::new(info),
            Err(err) => {
                log::warn!("[last_block_loop] fail to fetch block with seqno {next_seqno}: {err}");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        if tx.send(info.clone()).is_err() {
            log::error!(
                "[last_block_loop] can't to send block_info for seqno {next_seqno} to channel: receiver is closed"
            );
            break;
        };
        metrics::set_last_seen_info(&info);
        next_seqno += 1;
    }
    log::info!("[last_block_loop] loop completed");
}

async fn metric_loop(client: LiteClient, shutdown: Sender<Arc<BlockInfo>>) {
    log::info!("[metric_loop] loop started");
    let mut prev_snapshot = None;

    loop {
        let snapshot = client.metrics();
        metrics::set_lite_client_metrics(&snapshot, prev_snapshot.as_ref());
        prev_snapshot = Some(snapshot);
        tokio::select! {
            _ = shutdown.closed() => break,
            _ = sleep(Duration::from_secs(1)) => {},
        }
    }
    log::info!("[metric_loop] loop completed");
}

async fn fetch_block(client: &LiteClient, seqno: u32) -> anyhow::Result<BlockInfo> {
    let block_id = client.lookup_mc_block(seqno).await?;
    let block = client.get_block(block_id, None).await?;
    Ok(block.data.info.into_inner())
}

pub fn detect_verbosity_level(max_level: LevelFilter) -> u32 {
    match max_level {
        LevelFilter::Off | LevelFilter::Error | LevelFilter::Warn => 0,
        LevelFilter::Info => 1,
        LevelFilter::Debug => 2,
        LevelFilter::Trace => 4,
    }
}

struct Workers {
    last_block: tokio::task::JoinHandle<()>,
    metrics: tokio::task::JoinHandle<()>,
}
impl Drop for Workers {
    fn drop(&mut self) {
        self.last_block.abort();
        self.metrics.abort();
    }
}

pub mod config;

#[cfg(test)]
#[path = "tests/test_height_loader.rs"]
mod tests;
