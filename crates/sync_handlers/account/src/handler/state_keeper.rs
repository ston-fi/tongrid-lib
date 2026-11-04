use crate::handler::metrics;
use crate::handler::metrics::EmulationCase;
use crate::handler::metrics::EmulationCase::{LostState, SparseContract};
use crate::handler::metrics::ProcessingStage::{
    EmulateAddress, EmulateTx, GetFirstState, GetFirstStateFromBC, GetLastState,
    GetLastStateFromBC, SaveAddressStates, TryGenState,
};
use crate::handler::ton_libs_keeper::TonLibsKeeper;
use crate::handler::types::EmulTxInfo;
use crate::handler::verify_state::{verify_state_after_emulation, verify_state_before_emulation};
use anyhow::bail;
use anyhow::Result;
use futures::future::try_join_all;
use futures::try_join;
use log::log_enabled;
use moka::future::Cache;
use std::collections::HashSet;
use std::error::Error;
use std::ops::Deref;
use std::sync::Arc;
use ton::block_tlb::{AccountNone, AccountState, MaybeAccount, ShardAccount, Tx, TxDescr};
use ton::emulators::tx_emulator::{TXEmulArgs, TXEmulOrdArgs, TXEmulTickTockArgs};
use ton::ton_core::cell::TonHash;
use ton::ton_core::traits::tlb::TLB;
use ton::ton_core::types::TonAddress;
use tongrid_db::entity::{StateID, TxKeyLookup};
use tongrid_tda::ser_de::account::shard_account_serdb;
use tongrid_tda::ton_db_adapter::TonDBAdapter;
use tongrid_ton::tg_ton::TGTon;

pub struct StateKeeper {
    gen_address_states_timeout: std::time::Duration,
    get_state_from_bc_timeout: std::time::Duration,
    ton: TGTon,
    tda: TonDBAdapter,
    libs_keeper: TonLibsKeeper,
    latest_state_cache: Cache<TonAddress, Arc<ShardAccount>>,
    critical_contracts: HashSet<TonHash>,
}

impl StateKeeper {
    pub(super) async fn new(
        config: &crate::config::AccountHandlerConfig,
        ton: TGTon,
        tda: TonDBAdapter,
        critical_contracts: HashSet<TonHash>,
    ) -> Result<Self> {
        let libs_keeper = TonLibsKeeper::new(tda.clone())?;
        let latest_state_cache = Cache::new(config.latest_state_cache_size);
        let state_keeper = Self {
            gen_address_states_timeout: config.gen_address_states_timeout,
            get_state_from_bc_timeout: config.get_state_from_bc_timeout,
            ton,
            tda,
            libs_keeper,
            latest_state_cache,
            critical_contracts,
        };
        Ok(state_keeper)
    }

    pub(crate) async fn gen_states_with_timeout(
        &self,
        account_txs: &mut Vec<EmulTxInfo>,
        sync_type: StateID,
    ) -> Result<()> {
        let addr = account_txs[0].addr;
        let mut from = 0;
        while from < account_txs.len() {
            let mc_seqno = account_txs[from].block_info.block_key.mc_seqno;
            let len = account_txs[from..]
                .iter()
                .take_while(|tx| tx.block_info.block_key.mc_seqno == mc_seqno)
                .count();
            let to = from + len;
            self.gen_height_states_with_timeout(&mut account_txs[from..to], sync_type)
                .await?;
            from = to;
        }

        log::debug!("[gen_account_states] emulation completed for addr: {addr}");
        if log_enabled!(log::Level::Trace) {
            for state in account_txs {
                log::trace!("[gen_account_states] new_state: {state:?}");
            }
        }
        Ok(())
    }

    async fn gen_height_states_with_timeout(
        &self,
        account_txs: &mut [EmulTxInfo],
        sync_type: StateID,
    ) -> Result<()> {
        let addr = account_txs[0].addr;
        let mc_seqno = account_txs[0].block_info.block_key.mc_seqno;

        if self
            .tda
            .db()
            .sparse_contracts()
            .is_sparse_at_height(&addr.to_string(), mc_seqno)
        {
            log::debug!("Skipping gen_states for addr: {addr} because of sparse_contracts");
            return Ok(());
        }

        log::debug!(
            "[gen_account_states] started for addr: {addr}, mc_seqno: {mc_seqno} ({} txs)",
            account_txs.len()
        );

        let emul_timeout = self.gen_address_states_timeout;

        match tokio::time::timeout(emul_timeout, self.gen_states(account_txs, sync_type)).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                bail!("[gen_account_states] failed to gen states for addr: {addr}, err: {err:?}")
            }
            Err(_) => {
                metrics::tick_emulation_case(EmulationCase::Timeout, 1, sync_type);
                log::error!("[gen_account_states] timeout for addr: {addr}"); // ignore error - just log it
            }
        };
        Ok(())
    }

    // function expect all blocks txs as an input (we can't start from the middle of the block)
    async fn gen_states(&self, acc_txs_info: &mut [EmulTxInfo], sync_type: StateID) -> Result<()> {
        let _tracker = metrics::track_stage_duration(EmulateAddress, sync_type);

        let mut last_state = {
            let _tracker = metrics::track_stage_duration(GetFirstState, sync_type);
            match acc_txs_info[0].emulated_state.as_ref() {
                Some(emul) if emul.account.as_active().is_some() => None,
                _ => {
                    self.get_state_before_tx(&acc_txs_info[0], sync_type)
                        .await?
                }
            }
        };

        for tx_info in acc_txs_info.iter_mut() {
            log::debug!(
                "gen state for tx: {tx_info}, already_saved: {}",
                tx_info.state_already_saved
            );
            if tx_info.state_already_saved {
                metrics::tick_emulation_case(EmulationCase::Known, 1, sync_type);
                tx_info.prev_emulated_state = last_state;
                last_state = tx_info.emulated_state.clone();
                continue;
            }

            let new_state = match last_state {
                Some(ref state) => self
                    .try_gen_state(state.clone(), tx_info, sync_type)
                    .await?
                    .map(Arc::new),
                None => {
                    metrics::tick_emulation_case(EmulationCase::UnknownPrevState, 1, sync_type);
                    continue;
                }
            };
            log::debug!(
                "got new state for tx: {tx_info}, state_presented: {}",
                new_state.is_some()
            );

            tx_info.emulated_state = new_state.clone();
            tx_info.prev_emulated_state = last_state;
            last_state = new_state;
        }

        let _tracker = metrics::track_stage_duration(GetLastState, sync_type);
        let last_tx_info = acc_txs_info
            .last_mut()
            .ok_or_else(|| anyhow::anyhow!("Cannot process an empty account transaction batch"))?;
        if last_tx_info.emulated_state.is_none() {
            // try get from blockchain
            last_tx_info.emulated_state = self
                .get_state_after_tx_from_bchain(last_tx_info, sync_type)
                .await?
                .map(Arc::new);
            match &last_tx_info.emulated_state {
                Some(_) => log::info!("last_state was restored from bc for tx: {last_tx_info}"),
                None => {
                    log::warn!("Failed to restore last_state from bc for tx: {last_tx_info}")
                }
            }
        }

        if let Some(state) = &last_tx_info.emulated_state {
            let key = last_tx_info.addr;
            let val = state.clone();
            self.latest_state_cache.insert(key, val).await;
        }

        Ok(())
    }

    async fn try_gen_state(
        &self,
        prev_state: Arc<ShardAccount>,
        tx_info: &EmulTxInfo,
        sync_type: StateID,
    ) -> Result<Option<ShardAccount>> {
        let _tracker = metrics::track_stage_duration(TryGenState, sync_type);
        // state before emulation MUST match with txs info by design
        verify_state_before_emulation(tx_info, &prev_state)?;

        let emul_args = TXEmulArgs {
            shard_account_boc: Arc::new(prev_state.to_boc()?),
            bc_config: tx_info.block_info.config.clone(),
            rand_seed: tx_info.block_info.random_seed,
            utime: tx_info.ton_tx.now,
            lt: tx_info.ton_tx.lt,
            ignore_chksig: false,
            c7_prev_blocks_info_boc: Some(tx_info.block_info.c7_prev_blocks_info_boc.clone()),
            libs_boc: self
                .libs_keeper
                .get_libs(&prev_state.account, tx_info, sync_type)
                .await?
                .map(Arc::new),
        };

        let emul_result = {
            let _tracker = metrics::track_stage_duration(EmulateTx, sync_type);

            match tx_info.ton_tx.descr.deref() {
                TxDescr::TickTock(descr) => {
                    let task = TXEmulTickTockArgs {
                        is_tock: descr.is_tock,
                        emul_args,
                    };
                    self.ton.emul_pool.emul_ticktock_tx(task, None).await?
                }
                _ => {
                    let Some(in_msg) = &tx_info.ton_tx.msgs.in_msg else {
                        bail!("No in_msg for non-ticktock tx: {tx_info}");
                    };
                    let task = TXEmulOrdArgs {
                        in_msg_boc: Arc::new(in_msg.to_boc()?),
                        emul_args,
                    };
                    self.ton.emul_pool.emul_ord_tx(task, None).await?
                }
            }
        };

        let new_state = match emul_result.into_success() {
            Ok(success) => {
                let mut new_state = success.shard_account_parsed()?;
                new_state.last_tx_lt = tx_info.ton_tx.lt;
                new_state.last_tx_hash = tx_info.tx_hash;
                new_state
            }
            Err(err) => {
                metrics::tick_emulation_case(EmulationCase::EmulationFailed, 1, sync_type);
                if let Some(code) = prev_state.account.get_code() {
                    if self.critical_contracts.contains(code.hash()?) {
                        bail!("Fail to emulate critical contract for tx: {tx_info}\nerr: {err:?}");
                    }
                }
                log::error!("emulator failed for tx: {tx_info}, err:\n{err:?}");
                return Ok(None);
            }
        };

        match verify_state_after_emulation(tx_info, &prev_state, &new_state) {
            Ok(_) => {
                metrics::tick_emulation_case(EmulationCase::Emulated, 1, sync_type);
                Ok(Some(new_state))
            }
            Err(err) => {
                metrics::tick_emulation_case(EmulationCase::VerifyAfterFailed, 1, sync_type);
                if let MaybeAccount::Account(acc) = prev_state.account.deref() {
                    if let AccountState::Active(active) = &acc.storage.state {
                        if let Some(code) = &active.state_init.code {
                            if self.critical_contracts.contains(&code.hash()?.clone()) {
                                bail!("Fail to emulate critical contract for tx: {tx_info}\nerr: {err:?}");
                            }
                        }
                    }
                }

                match sync_type {
                    StateID::AccountFixSync => log::debug!(
                        "[{sync_type}] state verification failed for tx: {tx_info}\nerr: {err:?}"
                    ),
                    _ => log::warn!(
                        "[{sync_type}] state verification failed for tx: {tx_info}\nerr: {err:?}"
                    ),
                }
                Ok(None)
            }
        }
    }

    pub(crate) async fn save_states(
        &self,
        generated_states: Vec<EmulTxInfo>,
        sync_type: StateID,
    ) -> Result<()> {
        let _tracker = metrics::track_stage_duration(SaveAddressStates, sync_type);

        let mut save_futs = vec![];
        for tx_info in generated_states {
            let save_fut = async move {
                let (db_shard_acc, tvm_codes) = shard_account_serdb(
                    &tx_info.db_tx,
                    tx_info.emulated_state.as_deref(),
                    tx_info.prev_emulated_state.as_deref(),
                )?;

                try_join!(
                    self.tda.db().account().insert(&db_shard_acc),
                    self.tda.db().tvm_code().insert(tvm_codes),
                    self.track_failed_state(tx_info, sync_type),
                )
            };
            save_futs.push(save_fut);
        }
        try_join_all(save_futs).await?;
        Ok(())
    }

    async fn track_failed_state(&self, tx_info: EmulTxInfo, sync_type: StateID) -> Result<()> {
        if tx_info.emulated_state.is_none() {
            if self
                .tda
                .db()
                .sparse_contracts()
                .is_sparse_at_height(&tx_info.addr.to_string(), tx_info.db_tx.mc_seqno)
            {
                metrics::tick_emulation_case(SparseContract, 1, sync_type);
            } else {
                metrics::tick_emulation_case(LostState, 1, sync_type);
            }
        }
        Ok(())
    }

    async fn get_state_before_tx(
        &self,
        tx_info: &EmulTxInfo,
        sync_type: StateID,
    ) -> Result<Option<Arc<ShardAccount>>> {
        if tx_info.ton_tx.orig_status.as_non_exist().is_some() {
            let state = build_first_state(&tx_info.ton_tx)?;
            return Ok(Some(Arc::new(state)));
        }

        // Keep in mind tx_info.tx.orig_status != AccountStatus::AccStateNonexist here
        // It's usually the case for mc_seqno == 1:
        // states were generated by magic in zero block
        if tx_info.ton_tx.prev_tx_lt == 0 {
            let state = self.get_zero_state(&tx_info.addr).await?;
            return Ok(Some(Arc::new(state)));
        }

        let prev_tx_lt = tx_info.ton_tx.prev_tx_lt;
        let prev_tx_hash = &tx_info.ton_tx.prev_tx_hash;

        // we likely won't lookup for state after that tx again,
        // but will populate cache by new state after block emulation
        if let Some(state) = self.latest_state_cache.remove(&tx_info.addr).await {
            if &state.last_tx_hash == prev_tx_hash {
                metrics::tick_emulation_case(EmulationCase::GotFromCache, 1, sync_type);
                return Ok(Some(state));
            }
        }

        log::debug!("Getting prev_state from scylla for tx: {tx_info}");
        if let Some(state) = self
            .get_state_from_scylla(prev_tx_lt as i64, prev_tx_hash)
            .await?
        {
            metrics::tick_emulation_case(EmulationCase::GotFromScylla, 1, sync_type);
            return Ok(Some(Arc::new(state)));
        }
        log::debug!("Getting prev_state from blockchain for tx: {tx_info}");
        let bc_state = self
            .get_state_before_tx_from_bchain(tx_info, sync_type)
            .await?;
        Ok(bc_state.map(Arc::new))
    }

    async fn get_zero_state(&self, addr: &TonAddress) -> Result<ShardAccount> {
        let account = self
            .ton
            .lite_client
            .get_account_state(addr, 0, None)
            .await?;
        match account {
            MaybeAccount::Account(_) => Ok(ShardAccount {
                account: account.into(),
                last_tx_hash: TonHash::ZERO,
                last_tx_lt: 0,
            }),
            MaybeAccount::None(_) => bail!("State not found in zero block for addr: {addr}"),
            _ => bail!("Unsupported zero-state account variant for addr: {addr}"),
        }
    }

    async fn get_state_from_scylla(
        &self,
        tx_lt: i64,
        tx_hash: &TonHash,
    ) -> Result<Option<ShardAccount>> {
        log::trace!("Getting state from scylla for tx: {tx_lt}:{tx_hash}");

        let tx_key = match self
            .tda
            .db()
            .tx()
            .lookup_key(TxKeyLookup::tx_hash(tx_hash))
            .await?
        {
            Some(tx_key) => tx_key,
            None => return Ok(None),
        };

        let db_acc_state = match self.tda.db().account().get_opt(&tx_key).await? {
            Some(db_state) => db_state,
            None => return Ok(None),
        };
        if db_acc_state.boc_stripped.is_empty() {
            return Ok(None);
        }
        let (shard_acc, _) = self
            .tda
            .account()
            .build_shard_account(&db_acc_state)
            .await?;
        Ok(Some(shard_acc))
    }

    async fn get_state_before_tx_from_bchain(
        &self,
        tx_info: &EmulTxInfo,
        sync_type: StateID,
    ) -> Result<Option<ShardAccount>> {
        let _tracker = metrics::track_stage_duration(GetFirstStateFromBC, sync_type);
        let mc_seqno = tx_info.block_info.block_key.mc_seqno - 1;
        let state = self
            .get_state_from_bchain(mc_seqno, &tx_info.addr, sync_type)
            .await?;
        Ok(state.map(|x| ShardAccount {
            account: x.into(),
            last_tx_hash: tx_info.ton_tx.prev_tx_hash,
            last_tx_lt: tx_info.ton_tx.prev_tx_lt,
        }))
    }

    async fn get_state_after_tx_from_bchain(
        &self,
        tx_info: &EmulTxInfo,
        sync_type: StateID,
    ) -> Result<Option<ShardAccount>> {
        let _tracker = metrics::track_stage_duration(GetLastStateFromBC, sync_type);
        let mc_seqno = tx_info.block_info.block_key.mc_seqno;
        let state = self
            .get_state_from_bchain(mc_seqno, &tx_info.addr, sync_type)
            .await?;
        Ok(state.map(|x| ShardAccount {
            account: x.into(),
            last_tx_hash: tx_info.db_tx.tx_hash,
            last_tx_lt: tx_info.db_tx.tx_lt,
        }))
    }

    async fn get_state_from_bchain(
        &self,
        mc_seqno: u32,
        address: &TonAddress,
        sync_type: StateID,
    ) -> Result<Option<MaybeAccount>> {
        let get_future = self
            .ton
            .lite_client
            .get_account_state(address, mc_seqno, None);
        let exec_timeout = self.get_state_from_bc_timeout;

        let get_account_state_result = match tokio::time::timeout(exec_timeout, get_future).await {
            Ok(res) => res,
            Err(_) => {
                log::warn!("Timeout ({exec_timeout:?}) while getting state from bc for address: {address}, mc_seqno: {mc_seqno}");
                return Ok(None);
            }
        };

        let account = match get_account_state_result {
            Ok(acc_boc) => acc_boc,
            Err(err) => {
                // Some archived account states cannot be reconstructed by the provider.
                if is_recoverable_account_state_error(&err) {
                    log::warn!("Fail to get blockchain account state for addr: {address}, mc_seqno: {mc_seqno}, error: {err:?}",);
                    return Ok(None);
                }
                return Err(err.into());
            }
        };

        metrics::tick_emulation_case(EmulationCase::GotFromBC, 1, sync_type);
        Ok(Some(account))
    }
}

fn is_recoverable_account_state_error(err: &(dyn Error + 'static)) -> bool {
    let mut cur_err = Some(err);
    while let Some(err) = cur_err {
        let err_msg = err.to_string();
        if err_msg.contains("Client was dropped")
            || err_msg.contains("Can't read BOC from empty slice")
        {
            return true;
        }
        cur_err = err.source();
    }
    false
}

fn build_first_state(first_tx: &Tx) -> Result<ShardAccount> {
    let state = ShardAccount {
        account: MaybeAccount::None(AccountNone).into(),
        last_tx_lt: first_tx.prev_tx_lt,
        last_tx_hash: first_tx.prev_tx_hash,
    };
    Ok(state)
}
