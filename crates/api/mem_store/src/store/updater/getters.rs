use crate::types::recent_data::{BlockRecentData, RecentData};
use crate::types::sync_status::SyncStatus;
use anyhow::{anyhow, bail};
use std::sync::Arc;
use tokio::try_join;
use ton::block_tlb::{Block, ConfigParams};
use ton::ton_core::traits::tlb::TLB;
use tongrid_core::time::cur_ts_sec;
use tongrid_db::entity::StateID::JettonSync;
use tongrid_tda::ton_db_adapter::TonDBAdapter;

pub(crate) async fn get_sync_status(tda: &TonDBAdapter) -> anyhow::Result<SyncStatus> {
    let (maybe_full_synced_height, maybe_initial_height) = try_join!(
        tda.db().progress().load_synced_height(JettonSync.as_ref()),
        tda.db().progress().load_initial_height(),
    )?;
    let Some(initial_height) = maybe_initial_height else {
        bail!("Initial seqno is not defined")
    };
    let full_synced_height = maybe_full_synced_height.unwrap_or(initial_height);
    let full_synced_seqno = u32::try_from(full_synced_height)?;
    let initial_seqno = u32::try_from(initial_height)?;

    let full_synced_seqno_utime = tda
        .db()
        .block()
        .get_master_by_mc_seqno(full_synced_seqno)
        .await?
        .utime;

    let block_info = tda.ton().get_last_seen_info();

    let result = SyncStatus {
        initial_seqno,
        full_synced_seqno,
        full_synced_seqno_utime,
        last_seen_seqno: block_info.seqno,
        last_seen_seqno_utime: block_info.gen_utime,
        sync_utime: cur_ts_sec()? as u32,
    };
    Ok(result)
}

pub(crate) async fn get_ton_config(
    tda: &TonDBAdapter,
    key_seqno: u32,
) -> anyhow::Result<ConfigParams> {
    let key_block = tda.db().block().get_master_by_mc_seqno(key_seqno).await?;
    let ton_block = Block::from_boc(key_block.boc_stripped.clone())?;
    let config = ton_block
        .extra
        .mc_block_extra
        .as_ref()
        .ok_or_else(|| anyhow!("No extra found in key_block for mc_seqno={key_seqno}"))?
        .config
        .as_ref()
        .ok_or_else(|| anyhow!("No config found in mc_extra for mc_seqno={key_seqno}"))?;
    Ok(config.clone())
}

pub(crate) async fn collect_recent_data(
    maybe_cur_data: Option<&RecentData>,
    new_sync_status: SyncStatus,
    tda: &TonDBAdapter,
) -> anyhow::Result<RecentData> {
    let cur_data = match maybe_cur_data {
        Some(data) => data,
        None => {
            let block_data = collect_block_recent_data(tda, new_sync_status).await?;
            return Ok(RecentData {
                generation: 0,
                sync_status: new_sync_status,
                block_data: Arc::new(block_data),
            });
        }
    };

    let old_sync_status = &cur_data.sync_status;
    let update_block = old_sync_status.full_synced_seqno != new_sync_status.full_synced_seqno;

    let block_data = if update_block {
        Arc::new(collect_block_recent_data(tda, new_sync_status).await?)
    } else {
        cur_data.block_data.clone()
    };

    let recent_data = RecentData {
        generation: cur_data.generation + 1,
        sync_status: new_sync_status,
        block_data,
    };

    Ok(recent_data)
}

pub(crate) async fn collect_block_recent_data(
    tda: &TonDBAdapter,
    new_sync_status: SyncStatus,
) -> anyhow::Result<BlockRecentData> {
    let mc_seqno = new_sync_status.full_synced_seqno;
    let mc_block = tda.db().block().get_master_by_mc_seqno(mc_seqno).await?;
    let ton_block = Block::from_boc(mc_block.boc_stripped.clone())?;
    let key_seqno = ton_block.info.prev_key_block_seqno;

    let block_data = BlockRecentData {
        last_mc_block: ton_block,
        last_bc_config: get_ton_config(tda, key_seqno).await?,
        last_key_block_seqno: key_seqno,
    };
    Ok(block_data)
}
