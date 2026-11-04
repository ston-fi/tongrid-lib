use crate::store::metrics::{self, UpdateStage};
use crate::store::updater::getters::{collect_recent_data, get_sync_status};
use crate::store::Inner;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;
use tongrid_core::time::cur_ts_ms;
use tongrid_tda::ton_db_adapter::TonDBAdapter;

pub mod getters;

pub struct StoreUpdater {
    tda: TonDBAdapter,
    store: Arc<Inner>,
}

impl StoreUpdater {
    pub(super) fn new(tda: TonDBAdapter, store: Arc<Inner>) -> Self {
        Self { tda, store }
    }

    pub(super) fn full_synced_seqno(&self) -> u32 {
        self.store.get_recent_data().sync_status.full_synced_seqno
    }

    pub(super) async fn update_store(&self) -> anyhow::Result<()> {
        let start = Instant::now();
        let cur_data = self.store.get_recent_data();
        // Primary metric for api alerts: uses actual data
        metrics::update_full_synced_seqno_delay(
            cur_data.sync_status.full_synced_seqno_utime as u64,
        )?;

        let new_sync_status = get_sync_status(&self.tda).await?;
        metrics::update_duration(UpdateStage::SyncStatus, start.elapsed());

        if cur_data.sync_status == new_sync_status {
            return Ok(());
        }

        let new_data =
            collect_recent_data(Some(cur_data.deref()), new_sync_status, &self.tda).await?;
        let new_generation = cur_data.generation + 1;
        self.store.set_recent_data(new_data);

        metrics::update_sync_status(&new_sync_status);
        if cur_data.sync_status.full_synced_seqno != new_sync_status.full_synced_seqno {
            let block_delay_ms =
                cur_ts_ms()?.saturating_sub(new_sync_status.full_synced_seqno_utime as u128 * 1000);
            metrics::update_block_sync_delay_ms(block_delay_ms as u64);
        }
        log::info!("Updated recent_data with {new_sync_status} (gen: {new_generation})");
        metrics::update_duration(UpdateStage::Total, start.elapsed());
        Ok(())
    }
}
