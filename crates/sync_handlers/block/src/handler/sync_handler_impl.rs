use crate::handler::metrics;
use crate::BlockHandler;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use stonfi_distributed_sync::stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};
use stonfi_distributed_sync::stonfi_sync_core::sync_engine::{SyncHandler, SyncHeight};
use tongrid_core::time::cur_ts_ms;

#[async_trait]
impl SyncHandler for BlockHandler {
    fn id(&self) -> &str {
        &self.sync_id
    }

    async fn sync_range(
        &mut self,
        from: SyncHeight,
        to: SyncHeight,
    ) -> SyncCoreResult<Option<SyncHeight>> {
        let from_seqno = u32::try_from(from).map_err(SyncCoreError::invalid_args)?;
        let to_seqno = u32::try_from(to).map_err(SyncCoreError::invalid_args)?;
        let Some(provider) = &self.provider else {
            return Err(SyncCoreError::logic("BlocksProvider is not initialized"));
        };

        if self.last_fetched_blocks.lock().is_empty() {
            let Some(items) = provider.next().await else {
                log::warn!("BlockStream for {} is done, going to sleep", self.id());
                tokio::time::sleep(Duration::from_secs(u64::MAX)).await;
                return Ok(None);
            };
            if items.is_empty() {
                log::warn!("BlockStream returned empty batch");
                return Ok(Some(from)); // nothing to process, but stream is not closed, so we can continue
            }
            *self.last_fetched_blocks.lock() = Arc::new(items);
        }

        let items = self.last_fetched_blocks.lock().clone();

        let (first_seqno, last_seqno, last_utime) = {
            // safe - data is not empty
            (
                items
                    .first()
                    .ok_or_else(|| SyncCoreError::logic("Missing first block in batch"))?
                    .mc_block
                    .info
                    .seqno,
                items
                    .last()
                    .ok_or_else(|| SyncCoreError::logic("Missing last block in batch"))?
                    .mc_block
                    .info
                    .seqno,
                items
                    .last()
                    .ok_or_else(|| SyncCoreError::logic("Missing last block in batch"))?
                    .mc_block
                    .info
                    .gen_utime,
            )
        };

        if first_seqno > from_seqno {
            Err(SyncCoreError::system(format!(
                "Got unexpected seqno range from BlockStream: [{}, {}], called with [{}, {}]",
                first_seqno, last_seqno, from_seqno, to_seqno
            )))?;
        }

        let delay_ms = cur_ts_ms()
            .map_err(SyncCoreError::system)?
            .saturating_sub(last_utime as u128 * 1000);
        metrics::update_block_delay(delay_ms, self.state_id);

        let max_seqno = loop {
            match self.process_many(&items).await {
                Ok(x) => break x,
                Err(e) => {
                    log::warn!("Error processing block: {}", e);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        };

        if max_seqno <= to_seqno {
            *self.last_fetched_blocks.lock() = Arc::new(vec![]);
        }

        Ok(Some(SyncHeight::from(max_seqno.min(to_seqno))))
    }

    fn is_enabled(&self) -> bool {
        self.provider.is_some()
    }

    fn max_batch_size(&self) -> usize {
        self.max_next_items
    }

    fn sync_timeout(&self) -> Duration {
        Duration::from_secs(10)
    }
}
