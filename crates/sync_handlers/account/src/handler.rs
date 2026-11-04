use anyhow::{anyhow, bail};
use futures::future::try_join_all;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use tongrid_db::entity::{DBTx, DBTxKey, StateID};

use crate::handler::types::*;
use anyhow::Result;
use log::log_enabled;
use metrics::ProcessingStage::*;
use moka::future::Cache;
use state_keeper::StateKeeper;
use std::time::Duration;
use stonfi_distributed_sync::coordinator::TaskPriority;
use stonfi_distributed_sync::task::EmptyTaskResult;
use task::AccountSyncTask;
use tokio::join;
use ton::block_tlb::{Block, BlockIdExt, ShardAccount};
use ton::emulators::tx_emulator::C7PrevBlocksInfo;
use ton::ton_core::traits::tlb::TLB;
use ton::ton_core::types::TonAddress;
use tongrid_db::entity::block::block_shard_key::DBBlockKey;
use tongrid_tda::ton_db_adapter::TonDBAdapter;
use tongrid_ton::tg_ton::TGTon;

pub mod distributed_handler_impl;
mod metrics;
mod state_keeper;
pub mod task;
mod ton_libs_keeper;
pub mod types;
mod verify_state;

// we usually need only 16, or 32 if resync enabled. So it's huge capacity
const EMUL_BLOCKS_INFO_CACHE_SIZE: u64 = 256;

type EmulBlocksInfoMap = HashMap<DBBlockKey, Arc<EmulBlockInfo>>;

pub struct AccountHandler {
    sequential_emulation: bool,
    pub(super) ton: TGTon,
    pub(super) state_keeper: StateKeeper,
    pub(super) tda: TonDBAdapter,
    emul_blocks_info_cache: Cache<u32, Arc<EmulBlocksInfoMap>>,
    pub(super) state_id: StateID,
    pub(super) sync_id: String,
    pub(super) enabled: bool,
    pub(super) task_timeout: Duration,
    pub(super) task_priority: TaskPriority,
    pub(super) max_batch_size: u32,
    pub(super) fix_batch_size: u32,
    pub(super) initial_synced_height: u32,
}

impl AccountHandler {
    pub async fn new(
        ton: TGTon,
        tda: TonDBAdapter,
        config: &crate::config::AccountHandlerConfig,
        critical_contracts: HashSet<ton::ton_core::cell::TonHash>,
        state_id: StateID,
    ) -> Result<Self> {
        let (enabled, task_priority) = config.state_settings(state_id)?;
        let task_timeout = config.task_timeout;
        let max_batch_size = config.max_batch_size;
        let fix_batch_size = config.fix_batch_size;
        let sequential_emulation = config.sequential_emulation;
        let state_keeper =
            StateKeeper::new(config, ton.clone(), tda.clone(), critical_contracts).await?;
        let progress = tda.db().progress();
        let initial_synced_height =
            u32::try_from(progress.load_initial_height().await?.unwrap_or_default())?;

        let handler = Self {
            sequential_emulation,
            ton,
            state_keeper,
            tda,
            emul_blocks_info_cache: Cache::new(EMUL_BLOCKS_INFO_CACHE_SIZE),
            state_id,
            sync_id: state_id.to_string(),
            enabled,
            task_timeout,
            task_priority,
            max_batch_size,
            fix_batch_size,
            initial_synced_height,
        };
        log::info!("[ACCOUNT_PROCESSOR][{state_id}] created (enabled: {enabled})");
        Ok(handler)
    }

    pub async fn process(&self, task: AccountSyncTask) -> Result<EmptyTaskResult> {
        log::debug!("[ACCOUNT_PROCESSOR] task={task:?}: start processing");
        let sync_type = StateID::from_str(&task.sync_type)?;
        let _tracker = metrics::track_stage_duration(ProcessAll, sync_type);

        // we fetch config from ConfKeeper cache inside emul_blocks_info_cache load function
        // it doesn't work well with stack
        let emul_data = Box::pin(self.collect_emulation_data(task.clone())).await?;

        let process_txs_res = self.process_txs(emul_data.txs_sorted, sync_type).await?;
        self.state_keeper
            .save_states(process_txs_res, sync_type)
            .await?;

        log::debug!("[ACCOUNT_PROCESSOR] task={task:?}: completed");
        metrics::update_objects_processed(
            1,
            emul_data.blocks.len(),
            emul_data.accs_cnt,
            emul_data.txs_cnt,
            sync_type,
        );
        Ok(EmptyTaskResult)
    }

    pub(super) async fn get_invalid_account_keys(
        &self,
        from_seqno: u32,
        to_seqno: u32,
    ) -> Result<HashMap<String, Vec<DBTxKey>>> {
        let account_mapper = self.tda.db().account();
        let tx_mapper = self.tda.db().tx();

        let mc_seqno_vec = Vec::from_iter(from_seqno..=to_seqno);
        let mc_seqno_range = mc_seqno_vec.as_slice();
        let broken_acc_tx_keys: Vec<DBTxKey> = account_mapper
            .get_invalid_keys_by_mc_seqnos(mc_seqno_range)
            .await?;

        if broken_acc_tx_keys.is_empty() {
            return Ok(HashMap::new());
        }

        let unique_addresses: HashSet<String> = broken_acc_tx_keys
            .into_iter()
            .map(|acc| acc.address)
            .collect();

        let address_futs = unique_addresses.into_iter().map(|address| async move {
            let mut tx_keys: Vec<DBTxKey> = tx_mapper
                .get_keys_by_address_mc_seqnos(mc_seqno_range, &address)
                .await?;
            tx_keys.sort_by_key(|x| x.tx_lt);
            Ok::<(String, Vec<DBTxKey>), anyhow::Error>((address, tx_keys))
        });

        let txs_by_address: Vec<(String, Vec<DBTxKey>)> =
            futures::future::try_join_all(address_futs).await?;
        Ok(txs_by_address.into_iter().collect())
    }

    pub(super) async fn create_regular_tasks(
        &self,
        from_seqno: u32,
        to_seqno: u32,
    ) -> Result<Vec<AccountSyncTask>> {
        let mc_seqnos = Vec::from_iter(from_seqno..=to_seqno);
        let block_txs = self.tda.db().tx_lite().get_by_mc_seqnos(&mc_seqnos).await?;
        let mut txs_by_addr = HashMap::new();
        for tx in block_txs {
            let tx_key = tx.key();
            let task_key = tx.address;
            txs_by_addr
                .entry(task_key)
                .or_insert_with(Vec::new)
                .push(tx_key);
        }

        let mut txs_by_addr = txs_by_addr.into_iter().collect::<Vec<_>>();
        txs_by_addr.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));

        txs_by_addr
            .into_iter()
            .map(|(_, mut tx_keys)| {
                tx_keys.sort_unstable_by_key(|tx_key| (tx_key.mc_seqno, tx_key.tx_lt));
                let mc_seqno = tx_keys
                    .first()
                    .map(|tx_key| tx_key.mc_seqno)
                    .ok_or_else(|| anyhow!("account sync task has no transactions"))?;
                Ok(AccountSyncTask {
                    mc_seqno,
                    tx_keys: tx_keys.into_iter().map(Into::into).collect(),
                    sync_all: false,
                    sync_type: self.sync_id.clone(),
                })
            })
            .collect()
    }

    pub(super) fn build_account_tasks(
        &self,
        txs_by_addr: HashMap<String, Vec<DBTxKey>>,
    ) -> Result<Vec<AccountSyncTask>> {
        let mut tasks = vec![];
        for tx_keys in txs_by_addr.into_values() {
            let mut grouped_by_seqno: HashMap<u32, Vec<DBTxKey>> = HashMap::new();
            for tx_key in tx_keys {
                grouped_by_seqno
                    .entry(tx_key.mc_seqno)
                    .or_default()
                    .push(tx_key);
            }

            for (mc_seqno, seq_tx_keys) in grouped_by_seqno {
                if self.state_id == StateID::AccountFixSync
                    && self
                        .tda
                        .db()
                        .sparse_contracts()
                        .is_sparse_at_height(&seq_tx_keys[0].address, mc_seqno)
                {
                    continue;
                }

                tasks.push(AccountSyncTask {
                    mc_seqno,
                    tx_keys: seq_tx_keys.into_iter().map(Into::into).collect(),
                    sync_all: false,
                    sync_type: self.sync_id.clone(),
                });
            }
        }
        Ok(tasks)
    }

    pub(super) async fn current_fix_sync_upper_bound(&self) -> Result<Option<u32>> {
        self.tda
            .db()
            .progress()
            .load_synced_height(StateID::AccountSync.as_ref())
            .await?
            .map(u32::try_from)
            .transpose()
            .map_err(Into::into)
    }
}

impl AccountHandler {
    async fn process_txs(
        &self,
        mut txs_info: HashMap<TonAddress, Vec<EmulTxInfo>>,
        sync_type: StateID,
    ) -> Result<Vec<EmulTxInfo>> {
        let _tracker = metrics::track_stage_duration(EmulateAll, sync_type);
        if self.sequential_emulation {
            for txs in txs_info.values_mut() {
                self.state_keeper
                    .gen_states_with_timeout(txs, sync_type)
                    .await?;
            }
        } else {
            let futs = txs_info
                .values_mut()
                .map(|txs| self.state_keeper.gen_states_with_timeout(txs, sync_type));
            try_join_all(futs).await?;
        };
        let states = txs_info.into_values().flatten().collect();

        Ok(states)
    }

    async fn collect_emulation_data(&self, task: AccountSyncTask) -> Result<TxEmulData> {
        let sync_type = StateID::from_str(&task.sync_type)?;
        let mc_seqnos = if task.sync_all {
            vec![task.mc_seqno]
        } else {
            let mut mc_seqnos = task
                .tx_keys
                .iter()
                .map(|tx_key| tx_key.mc_seqno)
                .collect::<Vec<_>>();
            mc_seqnos.sort_unstable();
            mc_seqnos.dedup();
            mc_seqnos
        };

        // collect blocks & txs data
        let (blocks_map, txs) = {
            let _tracker = metrics::track_stage_duration(CollectData, sync_type);
            let blocks_fut = self.collect_blocks_info(&mc_seqnos, sync_type);

            let txs_fut = async {
                let tx_mapper = self.tda.db().tx();
                match task.sync_all {
                    true => tx_mapper.get_by_mc_seqno(task.mc_seqno).await,
                    false => {
                        let keys: Vec<_> = task.tx_keys.into_iter().map(Into::into).collect();
                        tx_mapper.get_many(&keys).await
                    }
                }
            };
            let (blocks, txs) = join!(blocks_fut, txs_fut);
            (blocks?, txs?)
        };

        // link & sort
        let (txs_info_sorted, accs_cnt, txs_cnt) = {
            let _tracker = metrics::track_stage_duration(BuildTxsInfo, sync_type);
            let txs_info = self.collect_txs_info(txs, &blocks_map, sync_type).await?;
            let txs_cnt = txs_info.len();
            // Group by address and explicitly sort each account's transactions by (mc_seqno, LT).
            let txs_info_sorted = split_by_addr(txs_info);
            let accs_cnt = txs_info_sorted.len();
            log::debug!(
                "[ACCOUNT_PROCESSOR] got {} blocks, {accs_cnt} accs, {txs_cnt} txs",
                blocks_map.len()
            );
            if log_enabled!(log::Level::Trace) {
                trace_txs_info(&txs_info_sorted);
            }
            (txs_info_sorted, accs_cnt, txs_cnt)
        };

        let emulation_data = TxEmulData {
            blocks: blocks_map,
            txs_sorted: txs_info_sorted,
            accs_cnt,
            txs_cnt,
        };
        Ok(emulation_data)
    }

    async fn collect_blocks_info(
        &self,
        mc_seqnos: &[u32],
        sync_type: StateID,
    ) -> Result<Arc<EmulBlocksInfoMap>> {
        let _tracker = metrics::track_stage_duration(CollectBlockInfo, sync_type);
        let block_info_futs = mc_seqnos.iter().copied().map(|mc_seqno| async move {
            self.emul_blocks_info_cache
                .try_get_with(mc_seqno, self.load_blocks_info(mc_seqno))
                .await
                .map_err(|e| anyhow!("Error getting block info for mc_seqno={mc_seqno}: {e}"))
        });
        let mut block_info = HashMap::new();
        for seqno_block_info in try_join_all(block_info_futs).await? {
            block_info.extend(
                seqno_block_info
                    .iter()
                    .map(|(key, value)| (*key, value.clone())),
            );
        }
        Ok(Arc::new(block_info))
    }

    async fn load_blocks_info(&self, mc_seqno: u32) -> Result<Arc<EmulBlocksInfoMap>> {
        let blocks = self.tda.db().block().get_by_mc_seqno(mc_seqno).await?;

        if blocks.is_empty() {
            log::warn!("No blocks found for mc_seqno={mc_seqno}");
            return Ok(Arc::new(HashMap::new()));
        };

        let (prev_blocks_ids_res, prev_block_ids_div_100_res) = join!(
            self.get_prev_mc_block_ids(mc_seqno as i32),
            self.get_prev_mc_block_ids_div_100(mc_seqno as i32),
        );

        let prev_blocks_ids = Arc::new(prev_blocks_ids_res?);
        let last_mc_block_divided_100 = Arc::new(prev_block_ids_div_100_res?);

        let emul_blocks_info_futs = blocks.iter().map(|bs| {
            let prev_blocks_ids = prev_blocks_ids.clone();
            let last_mc_block_divided_100 = last_mc_block_divided_100.clone();
            async move {
                let key = bs.get_key();
                let ton_block = Block::from_boc(bs.boc_stripped.clone())?;
                let block_extra = &ton_block.extra;
                let prev_key_block_seqno = ton_block.info.prev_key_block_seqno;
                let config = self.tda.conf().get_config(prev_key_block_seqno).await?;

                let key_block_id = self.load_mc_block_id(prev_key_block_seqno).await?;

                let prev_blocks_info = C7PrevBlocksInfo {
                    last_mc_blocks: prev_blocks_ids.clone(),
                    prev_key_block: key_block_id,
                    last_mc_blocks_div_100: last_mc_block_divided_100.clone(),
                };

                let block = EmulBlockInfo::new(key, block_extra, config, prev_blocks_info).await?;
                Ok::<_, anyhow::Error>((key, Arc::new(block)))
            }
        });
        let blocks_map = try_join_all(emul_blocks_info_futs)
            .await?
            .into_iter()
            .collect();
        Ok(Arc::new(blocks_map))
    }

    async fn collect_txs_info(
        &self,
        block_db_txs: Vec<DBTx>,
        blocks_map: &HashMap<DBBlockKey, Arc<EmulBlockInfo>>,
        sync_type: StateID,
    ) -> Result<Vec<EmulTxInfo>> {
        if block_db_txs.is_empty() {
            return Ok(vec![]);
        }
        let _tracker = metrics::track_stage_duration(CollectTxsInfo, sync_type);
        let mut mc_seqnos = block_db_txs
            .iter()
            .map(|tx| tx.mc_seqno)
            .collect::<Vec<_>>();
        mc_seqnos.sort_unstable();
        mc_seqnos.dedup();
        let fill_known_states = try_join_all(mc_seqnos.into_iter().map(|mc_seqno| async move {
            Ok::<_, anyhow::Error>((mc_seqno, need_fill_known_states(&self.tda, mc_seqno).await?))
        }))
        .await?
        .into_iter()
        .collect::<HashMap<_, _>>();

        let fill_known_states = &fill_known_states;
        let emul_txs_futs = block_db_txs.into_iter().map(|db_tx| async move {
            let _tracker = metrics::track_stage_duration(CollectOneTxInfo, sync_type);

            let addr = TonAddress::from_str(&db_tx.address)?;

            let (ton_tx, lib_ids) = {
                let _tracker = metrics::track_stage_duration(BuildTonTx, sync_type);
                self.tda.tx().build_ton_tx(&db_tx).await?
            };

            let shard_key = db_tx.block_key();
            let block_info = match blocks_map.get(&shard_key) {
                Some(block_info) => block_info,
                None => bail!(
                    "block not found for tx: {}:{:X} in blocks_map",
                    ton_tx.lt,
                    ton_tx.cell_hash()?
                ),
            };

            let should_try_known_state = fill_known_states
                .get(&db_tx.mc_seqno)
                .copied()
                .unwrap_or(false);
            let emulated_state = self
                .try_get_known_state(&db_tx, should_try_known_state, sync_type)
                .await?;
            let tx_info = EmulTxInfo::new(
                addr,
                db_tx,
                ton_tx,
                lib_ids,
                block_info.clone(),
                emulated_state,
                None,
            )?;
            Ok(tx_info)
        });

        let emulation_txs = try_join_all(emul_txs_futs).await?;
        Ok(emulation_txs)
    }

    async fn try_get_known_state(
        &self,
        db_tx: &DBTx,
        should_try: bool,
        sync_type: StateID,
    ) -> Result<Option<ShardAccount>> {
        let _tracker = metrics::track_stage_duration(TryGetKnownState, sync_type);
        if !should_try {
            return Ok(None);
        }
        let tx_key = db_tx.tx_key();
        let db_state = match self.tda.db().account().get_opt(&tx_key).await? {
            Some(db_state) => db_state,
            None => return Ok(None),
        };

        if db_state.boc_stripped.is_empty() {
            return Ok(None);
        }

        let (shard_acc, _) = self.tda.account().build_shard_account(&db_state).await?;
        Ok(Some(shard_acc))
    }

    pub async fn get_prev_mc_block_ids(&self, mc_seqno: i32) -> Result<Vec<BlockIdExt>> {
        let mut block_ids_futs = vec![];

        // we always need 16 blocks here ( https://docs.ton.org/v3/documentation/tvm/changelog/tvm-upgrade-2023-07#opcodes-to-work-with-new-c7-values )
        let from_seqno = std::cmp::max(mc_seqno - 16, 0);
        for seqno in from_seqno..mc_seqno {
            block_ids_futs.push(self.load_mc_block_id(seqno as u32));
        }
        try_join_all(block_ids_futs).await
    }

    // we always need 16 blocks here ( https://docs.ton.org/v3/documentation/tvm/changelog/tvm-upgrade-2023-07#opcodes-to-work-with-new-c7-values )
    pub async fn get_prev_mc_block_ids_div_100(&self, mc_seqno: i32) -> Result<Vec<BlockIdExt>> {
        let mut block_ids_futs = vec![];
        let mut cur_seqno = mc_seqno / 100 * 100;
        for _ in 0..16 {
            if cur_seqno < 0 {
                break;
            }

            block_ids_futs.push(self.load_mc_block_id(cur_seqno as u32));
            cur_seqno -= 100;
        }
        try_join_all(block_ids_futs).await
    }

    pub async fn load_mc_block_id(&self, mc_seqno: u32) -> Result<BlockIdExt> {
        if mc_seqno == 0 {
            return Ok(BlockIdExt::ZERO_BLOCK_ID.clone());
        }

        let block_id = {
            log::debug!("No block_id_ext with mc_seqno={mc_seqno} in cache, trying db..");
            match self
                .tda
                .db()
                .block()
                .get_master_by_mc_seqno_opt(mc_seqno)
                .await?
            {
                Some(block_shard) => block_shard.block_id(),
                None => {
                    // workaroun for stage
                    log::debug!("Fail to get mc_block_shard with seqno={mc_seqno} from db, trying blockchain..");
                    self.ton.lite_client.lookup_mc_block(mc_seqno).await?
                }
            }
        };
        Ok(block_id)
    }
}

async fn need_fill_known_states(tda: &TonDBAdapter, mc_seqno: u32) -> Result<bool> {
    tda.db().account().any_for_mc_seqno(mc_seqno).await
}

fn split_by_addr(txs_info: Vec<EmulTxInfo>) -> HashMap<TonAddress, Vec<EmulTxInfo>> {
    let mut txs_info_split = HashMap::new();
    for tx in txs_info {
        let entry = txs_info_split.entry(tx.addr).or_insert(vec![]);
        entry.push(tx);
    }
    for txs in txs_info_split.values_mut() {
        txs.sort_unstable_by_key(|tx| (tx.db_tx.mc_seqno, tx.db_tx.tx_lt));
    }
    txs_info_split
}

fn trace_txs_info(txs_per_addr: &HashMap<TonAddress, Vec<EmulTxInfo>>) {
    let mut total_txs_cnt = 0;
    let mut text = String::new();
    for (addr, txs) in txs_per_addr {
        text += &format!("\nAddress: {addr}\n");
        let txs_cnt = txs.len();
        total_txs_cnt += txs_cnt;
        for (pos, tx) in txs.iter().enumerate() {
            text += &format!("TxInfo({}/{}): {}\n", pos + 1, txs_cnt, tx);
        }
    }
    let txs_info = format!(
        "===txs_info ({} addrs, {} txs)===\n{}",
        txs_per_addr.len(),
        total_txs_cnt,
        text
    );
    log::trace!("[ACCOUNT_PROCESSOR] {txs_info}");
}
