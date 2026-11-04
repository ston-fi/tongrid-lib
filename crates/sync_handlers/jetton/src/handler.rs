use anyhow::bail;
use tongrid_db::util::bigint_from_i512;

use anyhow::Result;
use futures::future::try_join_all;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use stonfi_distributed_sync::coordinator::TaskPriority;
use stonfi_distributed_sync::task::EmptyTaskResult;

use crate::handler::task::{JettonSyncTask, JettonSyncTxInfo};
use num_bigint::BigInt;
use ton::block_tlb::{FromTVMStack, TVMStack};
use ton::contracts::tep::jetton::jetton_wallet_contract::GetWalletDataResult;
use ton::ton_core::traits::tlb::TLB;
use ton::ton_core::types::TonAddress;
use tongrid_db::entity::tx::db_tx_lite::DBTxLite;
use tongrid_db::entity::{
    account::db_account::DBAccount, jetton_balance::DBJettonBalance, jetton_wallet::DBJettonWallet,
    DBTxKey, StateID, TxKeyLookup,
};
use tongrid_proto::v3::ContractTrait;
use tongrid_tda::ton_db_adapter::TonDBAdapter;

pub mod distributed_handler_impl;
pub mod task;

pub struct JettonHandler {
    tda: TonDBAdapter,
    pub(crate) state_id: StateID,
    pub(crate) sync_id: String,
    pub(crate) enabled: bool,
    pub(crate) task_timeout: Duration,
    pub(crate) task_priority: TaskPriority,
    pub(crate) max_batch_size: u32,
}

#[derive(Debug)]
struct JettonWalletData {
    wallet_address: String,
    mc_seqno: u32,
    key_block_seqno: u32,
    txs_utime: Vec<u32>,
    account_states: Vec<DBAccount>,
    prev_balance: Option<BigInt>,
}

impl JettonHandler {
    pub async fn new(
        tda: TonDBAdapter,
        config: &crate::config::JettonHandlerConfig,
        state_id: StateID,
    ) -> Result<JettonHandler> {
        let (enabled, task_priority) = config.state_settings(state_id)?;
        let task_timeout = config.task_timeout;
        let max_batch_size = config.max_batch_size;
        let handler = JettonHandler {
            tda,
            state_id,
            sync_id: state_id.to_string(),
            enabled,
            task_timeout,
            task_priority,
            max_batch_size,
        };
        log::info!("[JETTON_SYNC_PROCESSOR] created");
        Ok(handler)
    }

    pub async fn process(&self, task: JettonSyncTask) -> Result<EmptyTaskResult> {
        if matches!(task.tx_info.first(), Some(tx) if tx.tx_lt == 0) {
            bail!("Invalid jetton sync task: first_lt must be greater than zero");
        }

        log::debug!(
            "[JETTON_SYNC_PROCESSOR] called with seqno={}",
            task.mc_seqno
        );

        let key_block_seqno = self
            .tda
            .db()
            .block()
            .get_master_by_mc_seqno(task.mc_seqno)
            .await?
            .key_block_seqno;

        let jetton_wallet_data = self
            .process_jetton_wallet(task.mc_seqno, key_block_seqno, task.address, task.tx_info)
            .await?;

        let jetton_wallet_data = match jetton_wallet_data {
            Some(data) => data,
            None => return Ok(EmptyTaskResult),
        };

        let jetton_balances = self.process_jetton_balance(&jetton_wallet_data).await?;

        self.tda
            .db()
            .jetton_balance()
            .insert_many(&jetton_balances)
            .await?;

        Ok(EmptyTaskResult)
    }

    async fn process_jetton_wallet(
        &self,
        mc_seqno: u32,
        key_block_seqno: u32,
        address: String,
        txs_sorted: Vec<JettonSyncTxInfo>,
    ) -> Result<Option<JettonWalletData>> {
        if txs_sorted.is_empty() {
            bail!("Internal error: txs_sorted should not not be empty");
        }

        let first_lt = txs_sorted[0].tx_lt;

        //try to get jetton_balance and jetton wallet from db
        if let Some(jb) = self
            .tda
            .db()
            .jetton_balance()
            .get_latest(&address, first_lt - 1)
            .await?
        {
            let account_state_futs = txs_sorted
                .iter()
                .map(|tx| {
                    let address_clone = address.clone();
                    async move {
                        self.tda
                            .db()
                            .account()
                            .get(&DBTxKey {
                                mc_seqno,
                                address: address_clone,
                                tx_lt: tx.tx_lt,
                            })
                            .await
                    }
                })
                .collect::<Vec<_>>();

            let account_states = try_join_all(account_state_futs).await?;

            return Ok(Some(JettonWalletData {
                wallet_address: address,
                txs_utime: txs_sorted.iter().map(|tx| tx.tx_utime).collect::<Vec<_>>(),
                account_states,
                prev_balance: jb.balance,
                key_block_seqno,
                mc_seqno,
            }));
        }

        if let Some(wallet) = self.tda.db().jetton_wallet().get_opt(&address).await? {
            if !wallet.is_legit {
                return Ok(None);
            }
        };

        // checking new contracts
        let jetton_wallets_states_futs = txs_sorted.iter().map(|tx| {
            let address_clone = address.clone();
            async move {
                self.tda
                    .db()
                    .account()
                    .get(&DBTxKey {
                        mc_seqno,
                        address: address_clone,
                        tx_lt: tx.tx_lt,
                    })
                    .await
            }
        });
        let account_states = try_join_all(jetton_wallets_states_futs).await?;

        // check if its jetton wallet
        let mut maybe_account_with_code = None;
        for account_state in &account_states {
            if let Some(code_hash) = &account_state.code_hash {
                let maybe_code_traits = self
                    .tda
                    .db()
                    .tvm_code_traits()
                    .get_by_code_hash(code_hash)
                    .await?;
                if let Some(code_traits) = maybe_code_traits {
                    if code_traits.traits.contains(&ContractTrait::JettonWallet) {
                        maybe_account_with_code = Some(account_state);
                        break;
                    }
                }
            }
        }

        let account_with_code = match maybe_account_with_code {
            Some(account_with_code) => account_with_code,
            None => return Ok(None),
        };

        // check wallet data
        let wallet_data = match self
            .emulate_get_wallet_data(account_with_code, key_block_seqno, mc_seqno)
            .await?
        {
            Some(result) => result,
            None => return Ok(None),
        };
        let wallet_data_master_address = &wallet_data.master.to_string();
        let master_tx_lookup =
            TxKeyLookup::address_latest(wallet_data_master_address, account_with_code.mc_seqno);
        let master_state = match self.tda.db().account().lookup_opt(master_tx_lookup).await? {
            Some(state) => state,
            None => return Ok(None),
        };

        if !master_state.is_valid {
            return Ok(None);
        }

        let is_legit = if master_state.code_hash.is_none() {
            false
        } else {
            matches!(
                self.emulate_get_wallet_address(&master_state, &wallet_data.owner.to_string(), key_block_seqno, mc_seqno).await?,
                Some(addr) if addr.to_string().as_str() == account_with_code.address
            )
        };

        let db_jetton_wallet = DBJettonWallet {
            wallet_address: account_with_code.address.clone(),
            is_legit,
            master_address: wallet_data.master.to_string(),
            owner_address: wallet_data.owner.to_string(),
            mc_seqno,
        };
        self.tda
            .db()
            .jetton_wallet()
            .insert(&db_jetton_wallet)
            .await?;

        if !is_legit {
            return Ok(None);
        }

        Ok(Some(JettonWalletData {
            wallet_address: address,
            txs_utime: txs_sorted.iter().map(|tx| tx.tx_utime).collect(),
            account_states,
            prev_balance: None,
            key_block_seqno,
            mc_seqno,
        }))
    }

    async fn process_jetton_balance(&self, ctx: &JettonWalletData) -> Result<Vec<DBJettonBalance>> {
        let mut jetton_balances = vec![];

        let mut prev_balance = ctx.prev_balance.clone();

        for i in 0..ctx.account_states.len() {
            let jetton_wallet = &ctx.account_states[i];

            if jetton_wallet.code_hash.is_none() {
                jetton_balances.push(DBJettonBalance {
                    wallet_address: ctx.wallet_address.clone(),
                    tx_lt: jetton_wallet.tx_lt,
                    is_valid: false,
                    prev_balance,
                    balance: None,
                    tx_utime: ctx.txs_utime[i],
                    mc_seqno: ctx.mc_seqno,
                });
                prev_balance = None;
                continue;
            }

            let maybe_wallet_data = self
                .emulate_get_wallet_data(jetton_wallet, ctx.key_block_seqno, ctx.mc_seqno)
                .await?;
            let res = match &maybe_wallet_data {
                Some(wallet_data) => {
                    let r = DBJettonBalance {
                        wallet_address: ctx.wallet_address.clone(),
                        tx_lt: jetton_wallet.tx_lt,
                        is_valid: true,
                        prev_balance: prev_balance.clone(),
                        balance: Some(bigint_from_i512(wallet_data.balance)?),
                        tx_utime: ctx.txs_utime[i],
                        mc_seqno: ctx.mc_seqno,
                    };
                    prev_balance = Some(bigint_from_i512(wallet_data.balance)?);
                    r
                }
                None => DBJettonBalance {
                    wallet_address: ctx.wallet_address.clone(),
                    tx_lt: jetton_wallet.tx_lt,
                    is_valid: false,
                    prev_balance: prev_balance.clone(),
                    balance: None,
                    tx_utime: ctx.txs_utime[i],
                    mc_seqno: ctx.mc_seqno,
                },
            };
            jetton_balances.push(res);
        }
        Ok(jetton_balances)
    }

    async fn emulate_get_wallet_data(
        &self,
        wallet_state: &DBAccount,
        key_seqno: u32,
        mc_seqno: u32,
    ) -> Result<Option<GetWalletDataResult>> {
        let emulation_data = self
            .tda
            .emulation()
            .collect_tvm_states(wallet_state, mc_seqno, key_seqno)
            .await?;

        let wallet_address = &wallet_state.address;
        let emulation_result = match self
            .tda
            .emulation()
            .emulate_get_method(
                emulation_data,
                "get_wallet_data",
                TVMStack::EMPTY_BOC.to_owned(),
            )
            .await
        {
            Ok(result) if result.exit_success() => result,
            rest => {
                log::warn!("failed to emulate get_wallet_data for {wallet_address}: {rest:?}");
                return Ok(None);
            }
        };

        let mut stack = match emulation_result.stack_parsed() {
            Ok(stack) => stack,
            Err(err) => {
                log::warn!(
                    "failed to parse get_wallet_data return stack for {wallet_address}: {err}"
                );
                return Ok(None);
            }
        };

        match GetWalletDataResult::from_stack(&mut stack) {
            Ok(res) => Ok(Some(res)),
            Err(err) => {
                log::warn!("incorrect get_wallet_data return stack for {wallet_address}: {err}");
                Ok(None)
            }
        }
    }

    async fn emulate_get_wallet_address(
        &self,
        master_state: &DBAccount,
        owner_addr: &str,
        key_seqno: u32,
        mc_seqno: u32,
    ) -> Result<Option<TonAddress>> {
        let emul_data = self
            .tda
            .emulation()
            .collect_tvm_states(master_state, mc_seqno, key_seqno)
            .await?;

        let mut stack = TVMStack::default();
        stack.push_cell_slice(TonAddress::from_str(owner_addr)?.to_cell()?);

        let master_address = &master_state.address;
        let emulation_result = match self
            .tda
            .emulation()
            .emulate_get_method(emul_data, "get_wallet_address", stack.to_boc()?)
            .await
        {
            Ok(result) if result.exit_success() => result,
            rest => {
                log::warn!("failed to emulate get_wallet_address for {master_address}: {rest:?}");
                return Ok(None);
            }
        };

        let mut stack = match emulation_result.stack_parsed() {
            Ok(stack) => stack,
            Err(err) => {
                log::warn!(
                    "failed to parse get_wallet_address return stack for {master_address}: {err}"
                );
                return Ok(None);
            }
        };

        let wallet_address = match &stack.pop_cell() {
            Ok(cell) => TonAddress::from_cell(cell)?,
            Err(err) => {
                log::warn!("failed to pop cell from get_wallet_address return stack for {master_address}: {err}");
                return Ok(None);
            }
        };
        Ok(Some(wallet_address))
    }

    pub(crate) async fn collect_tasks(
        &self,
        seqno: u32,
    ) -> Result<HashMap<String, JettonSyncTask>> {
        let txs = self.tda.db().tx_lite().get_by_mc_seqno(seqno).await?;
        Self::group_tasks(txs)
    }

    pub fn group_tasks(txs: Vec<DBTxLite>) -> Result<HashMap<String, JettonSyncTask>> {
        let mut grouped: HashMap<String, JettonSyncTask> = HashMap::new();

        for tx in txs {
            let DBTxLite {
                mc_seqno,
                address,
                tx_lt,
                tx_utime,
                ..
            } = tx;
            grouped
                .entry(address.clone())
                .or_insert(JettonSyncTask {
                    mc_seqno,
                    address,
                    tx_info: vec![],
                })
                .tx_info
                .push(JettonSyncTxInfo { tx_lt, tx_utime });
        }

        Ok(grouped)
    }
}
