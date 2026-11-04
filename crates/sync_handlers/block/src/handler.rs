use crate::handler::blocks_provider::BlocksProvider;
use crate::handler::metered_cache::MeteredCache;
use crate::handler::scylla_block_stream::ScyllaBlockStream;
use crate::is_initial_tx;
use anyhow::bail;
use futures::future::{join_all, try_join_all};
use initial_tx_resolver::InititalTxResolver;
use moka::future::Cache;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::iter::once;
use std::sync::Arc;
use tokio::try_join;
use tongrid_db::entity::ongoing_tx::OngoingTx;
use tongrid_db::entity::tx::tx_key::DBTxKey;
use tongrid_db::entity::StateID::{BlockResync, BlockSync};
use tongrid_db::entity::{DBTx, StateID};
use tongrid_tda::ton_db_adapter::TonDBAdapter;
use tongrid_ton::tg_ton::block_stream::{BlockStreamItem, TONBlockStream};
use types::BlocksDBData;

pub mod blocks_provider;
pub mod initial_tx_resolver;
mod metered_cache;
mod metrics;
pub mod scylla_block_stream;
mod sync_handler_impl;
pub mod types;

const ONGOING_TX_CACHE_SIZE: u64 = 10_000;

#[derive(Debug, Clone)]
struct OngoingTxUpdate {
    current_mc_seqno: u32,
    count_change: i32,
}

/// fills block_boc, block, transaction, message_transactions tables
pub struct BlockHandler {
    sync_id: String,
    max_next_items: usize,
    state_id: StateID,
    tda: TonDBAdapter,
    provider: Option<BlocksProvider>,
    // In case of processing error, we can't extract block from provider - so we keep it here
    initial_tx_resolver: InititalTxResolver,
    last_fetched_blocks: Mutex<Arc<Vec<BlockStreamItem>>>,
    ongoing_txs_cache: MeteredCache<DBTxKey, OngoingTx>,
}

impl BlockHandler {
    pub async fn new(
        tda: TonDBAdapter,
        config: &crate::config::BlockSyncConfig,
        state_id: StateID,
    ) -> anyhow::Result<Self> {
        let enabled = match state_id {
            BlockSync => config.enabled,
            BlockResync => config.resync_enabled,
            _ => bail!("Unsupported StateID for BlockHandler: {state_id}"),
        };
        let buffer_size = config.block_stream_buffer_size;
        let max_items = config.blocks_max_parallel_processing;
        let synced_seqno = u32::try_from(
            tda.db()
                .progress()
                .load_synced_or_initial(BlockSync.as_ref())
                .await?,
        )?;

        let provider = match state_id {
            BlockSync => {
                if enabled {
                    let ton_stream =
                        TONBlockStream::new(tda.ton().lite_client.clone(), synced_seqno + 1, None)
                            .await?;
                    Some(BlocksProvider::new(ton_stream, buffer_size, max_items).await?)
                } else {
                    None
                }
            }
            BlockResync => {
                if enabled {
                    let synced = u32::try_from(
                        tda.db()
                            .progress()
                            .load_synced_or_initial(BlockResync.as_ref())
                            .await?,
                    )?;
                    let scylla_stream =
                        ScyllaBlockStream::new(tda.clone(), synced + 1, synced_seqno)?;
                    Some(BlocksProvider::new(scylla_stream, buffer_size, max_items).await?)
                } else {
                    None
                }
            }
            _ => bail!("Unsupported StateID for BlockHandler: {state_id}"),
        };

        let initial_tx_resolver = InititalTxResolver::new();

        let ongoing_txs_cache = MeteredCache::new(
            "ongoing_tx",
            Cache::builder().max_capacity(ONGOING_TX_CACHE_SIZE).build(),
        );

        Ok(Self {
            sync_id: state_id.to_string(),
            state_id,
            max_next_items: max_items,
            tda,
            initial_tx_resolver,
            last_fetched_blocks: Default::default(),
            ongoing_txs_cache,
            provider,
        })
    }

    // We can reduce function to process_one_block as we must process transactions consequently thus have syncronous part of code and can't do it asynchronously.
    pub async fn process_many(&self, bs_items: &[BlockStreamItem]) -> anyhow::Result<u32> {
        if bs_items.is_empty() {
            bail!("Got empty block stream items");
        }
        let max_mc_seqno = bs_items
            .iter()
            .map(|bs_item| bs_item.mc_block.info.seqno)
            .max()
            .ok_or_else(|| anyhow::anyhow!("Cannot process an empty block batch"))?; // unwrap is safe if bs_items is not empty

        let mut all_blocks = vec![];
        let mut all_txs = vec![];
        let mut all_tvm_codes = vec![];

        for bs_item in bs_items {
            let db_data = self.extract_data(bs_item)?;
            let BlocksDBData {
                blocks,
                txs,
                tvm_codes,
            } = db_data;

            all_blocks.extend(blocks);
            all_txs.extend(txs);
            all_tvm_codes.extend(tvm_codes);
        }
        self.initial_tx_resolver
            .fill_initial_txs(&mut all_txs, &self.tda)
            .await?;

        let tvm_codes_len = all_tvm_codes.len();

        let db = self.tda.db();

        let insert_messages_fut = self.tda.msg().save_message_data(&all_txs);
        let insert_block_shard_fut = db.block().insert_many(&all_blocks);
        let insert_tvm_codes_fut = db.tvm_code().insert(all_tvm_codes);
        let insert_txs_fut = db.tx().insert_many(&all_txs);

        // Ongoing_tx for resync doesn't make sense, so we disable processing it
        let insert_ongoing_txs_fut = async {
            if self.state_id != BlockResync {
                self.save_ongoing_txs(&all_txs).await
            } else {
                Ok(())
            }
        };

        let res = try_join!(
            insert_messages_fut,
            insert_block_shard_fut,
            insert_tvm_codes_fut,
            insert_txs_fut,
            insert_ongoing_txs_fut,
        )?;

        metrics::update_processed_objects(
            all_blocks.len(),
            all_txs.len(),
            res.0,
            tvm_codes_len,
            self.state_id,
        );
        Ok(max_mc_seqno)
    }

    pub fn extract_data(&self, bs_item: &BlockStreamItem) -> anyhow::Result<BlocksDBData> {
        let all_shards = bs_item.shard_blocks.iter().chain(once(&bs_item.mc_block));
        let master_seqno = bs_item.mc_block.info.seqno;

        let mut results = vec![];
        for shard in all_shards {
            results.push(BlocksDBData::from_block(shard, master_seqno)?);
        }

        let mut all_blocks_db_data = BlocksDBData::default();
        for res in results {
            all_blocks_db_data.blocks.extend(res.blocks);
            all_blocks_db_data.txs.extend(res.txs);
            all_blocks_db_data.tvm_codes.extend(res.tvm_codes);
        }

        all_blocks_db_data.txs.sort_by_key(|x| x.tx_lt);
        Ok(all_blocks_db_data)
    }

    /// Expects txs sorted by lt
    pub async fn save_ongoing_txs(&self, txs: &[DBTx]) -> anyhow::Result<()> {
        let mut ongoing_txs_updates = HashMap::with_capacity(txs.len());
        for tx in txs {
            if tx.initial_tx.is_zero() {
                // Partial history can leave the chain root unknown; do not merge those chains under the zero key.
                // TODO: backfill roots and ongoing counters together before counting these transactions.
                log::debug!("initial_tx is zero for tx: {:?}", tx.tx_key());
                continue;
            }
            let ongoing_txs_delta = calc_ongoing_txs_delta(tx)?;
            ongoing_txs_updates
                .entry(&tx.initial_tx)
                .and_modify(|x: &mut OngoingTxUpdate| {
                    x.count_change += ongoing_txs_delta;
                    x.current_mc_seqno = tx.mc_seqno;
                })
                .or_insert(OngoingTxUpdate {
                    current_mc_seqno: tx.mc_seqno,
                    count_change: ongoing_txs_delta,
                });
        }

        self.update_ongoing_txs(ongoing_txs_updates).await?;
        Ok(())
    }

    async fn update_ongoing_txs(
        &self,
        ongoing_txs_updates: HashMap<&DBTxKey, OngoingTxUpdate>,
    ) -> anyhow::Result<()> {
        let futures = ongoing_txs_updates
            .iter()
            .map(|(init_tx_key, update)| async move {
                if update.count_change == 0 {
                    return Ok(None);
                }
                self.get_updated_ongoing_tx(
                    init_tx_key,
                    update.count_change,
                    update.current_mc_seqno,
                )
                .await
            });
        let new_ongoing_txs: Vec<OngoingTx> =
            try_join_all(futures).await?.into_iter().flatten().collect();

        let mut ongoing_txs_to_update = Vec::with_capacity(new_ongoing_txs.len());
        let mut delete_futures = vec![];
        for ong_tx in new_ongoing_txs {
            match ong_tx.count {
                0 => delete_futures.push(async move {
                    self.ongoing_txs_cache
                        .insert(ong_tx.tx_key(), ong_tx.clone())
                        .await;
                    self.tda
                        .db()
                        .ongoing_tx()
                        .delete(&ong_tx.address, ong_tx.tx_lt as i64)
                        .await
                }),
                1..=i32::MAX => ongoing_txs_to_update.push(ong_tx),
                i32::MIN..=-1 => {
                    log::warn!("OngoingTx.count < 0 for: {ong_tx:?}");
                    continue;
                }
            }
        }

        try_join_all(delete_futures).await?;
        self.tda
            .db()
            .ongoing_tx()
            .insert_many(&ongoing_txs_to_update)
            .await?;
        let cache_update_futs = ongoing_txs_to_update
            .into_iter()
            .map(|tx| self.ongoing_txs_cache.insert(tx.tx_key(), tx));
        join_all(cache_update_futs).await;

        Ok(())
    }

    pub async fn get_updated_ongoing_tx(
        &self,
        init_tx_key: &DBTxKey,
        count_change: i32,
        mc_seqno: u32,
    ) -> anyhow::Result<Option<OngoingTx>> {
        let init_fut = self
            .tda
            .db()
            .ongoing_tx()
            .get_opt(&init_tx_key.address, init_tx_key.tx_lt as i64);
        let ongoing_tx = self
            .ongoing_txs_cache
            .get_or_init_option(init_tx_key, init_fut)
            .await?;
        match ongoing_tx {
            Some(mut tx) => {
                if tx.last_processed_mc_seqno >= mc_seqno {
                    return Ok(None);
                }
                tx.count += count_change;
                tx.last_processed_mc_seqno = mc_seqno;
                Ok(Some(tx))
            }
            None => Ok(Some(OngoingTx {
                address: init_tx_key.address.clone(),
                tx_lt: init_tx_key.tx_lt,
                mc_seqno: init_tx_key.mc_seqno,
                count: count_change,
                last_processed_mc_seqno: mc_seqno,
            })),
        }
    }
}

fn calc_ongoing_txs_delta(tx: &DBTx) -> anyhow::Result<i32> {
    let txs_produced = tx.out_msgs.iter().filter(|msg| !msg.dst.is_empty()).count() as i32;
    let txs_completed = match is_initial_tx(tx)? {
        true => 0,
        false => tx.in_msg.is_some() as i32,
    };
    Ok(txs_produced - txs_completed)
}

#[cfg(test)]
mod tests {
    use super::*;

    use tongrid_db::entity::scylla_types::DBMsg;
    use tongrid_db::entity::DBTx;

    #[test]
    fn test_calc_ongoing_txs_delta() -> anyhow::Result<()> {
        let tx = DBTx::default();
        assert_eq!(calc_ongoing_txs_delta(&tx)?, 0);

        let tx = DBTx {
            in_msg: Some(DBMsg::default()),
            out_msgs: vec![],
            ..Default::default()
        };
        assert_eq!(calc_ongoing_txs_delta(&tx)?, -1);

        let tx = DBTx {
            in_msg: None,
            out_msgs: vec![DBMsg::default()],
            ..Default::default()
        };
        assert_eq!(calc_ongoing_txs_delta(&tx)?, 1);

        let tx = DBTx {
            in_msg: Some(DBMsg::default()),
            out_msgs: vec![DBMsg::default(), DBMsg::default()],
            ..Default::default()
        };
        assert_eq!(calc_ongoing_txs_delta(&tx)?, 1);

        let tx = DBTx {
            in_msg: Some(DBMsg::default()),
            out_msgs: vec![DBMsg::default(), DBMsg::default(), DBMsg::default()],
            ..Default::default()
        };
        assert_eq!(calc_ongoing_txs_delta(&tx)?, 2);

        let tx = DBTx {
            in_msg: None,
            out_msgs: vec![DBMsg::default(), DBMsg::default(), DBMsg::default()],
            ..Default::default()
        };
        assert_eq!(calc_ongoing_txs_delta(&tx)?, 3);

        Ok(())
    }
}
