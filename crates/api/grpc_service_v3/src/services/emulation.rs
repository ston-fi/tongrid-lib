use crate::context::ServiceContext;
use crate::error::GrpcServiceError;
use crate::tx_tree::{RequestedTxInfo, TxTree, TxTreeEntry};
use crate::utils::{get_tx_by_query, normalize_address, ResponseWithWaitTime};
use anyhow::anyhow;
use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use ton::block_tlb::{Msg, ShardAccount, TVMStack, Tx};
use ton::emulators::emulator_pool::EmulatorPool;
use ton::emulators::tvm_emulator::TVMState;
use ton::emulators::tvm_emulator::{TVMEmulatorC7, TVMGetMethodID};
use ton::errors::TonError;
use ton::ton_core::cell::{TonCell, TonCellUtils};
use ton::ton_core::traits::tlb::TLB;
use ton::ton_core::types::TonAddress;
use tongrid_core::time::cur_ts_sec;
use tongrid_db::entity::block::block_shard_key::DBBlockKey;
use tongrid_db::entity::{DBTx, TxKeyLookup};
use tongrid_proto::v3::emulate_get_method_request::{ContractState, MethodId};
use tongrid_proto::v3::emulate_get_method_response::EmulateGetMethodResult;
use tongrid_proto::v3::*;
use tongrid_tda::ser_de::address::tlb_address_to_str;
use tongrid_tda::ser_de::msg::msg_serdb;
use tongrid_tda::ser_de::tx::tx_serdb;
use tongrid_tda::ton_db_adapter::emulation::build_tvm_state;
use tree_emul_args_provider::TreeEmulArgsProviderScylla;
use tx_tree_emulator::TxTreeEmulator;

mod metrics;
pub mod tree_emul_args_provider;
pub mod tx_tree_emulator;

pub struct EmulationService {
    ctx: Arc<ServiceContext>,
    emulators: EmulatorPool,
}

impl EmulationService {
    pub fn new(ctx: Arc<ServiceContext>) -> Self {
        let emulators = ctx.emulator_pool.clone();
        Self { ctx, emulators }
    }

    pub async fn emulate_get_method(
        &self,
        req: EmulateGetMethodRequest,
    ) -> Result<ResponseWithWaitTime<EmulateGetMethodResponse>, GrpcServiceError> {
        let req_state = match req.contract_state {
            Some(state) => state,
            None => {
                return Err(GrpcServiceError::InvalidArgument(
                    "contract_state is required".to_string(),
                ))
            }
        };
        let method_id = match req.method_id {
            Some(MethodId::MethodIdInt(id)) => TVMGetMethodID::from(id),
            Some(MethodId::MethodIdStr(id)) => TVMGetMethodID::from(id),
            None => {
                return Err(GrpcServiceError::InvalidArgument(
                    "method_id is required".to_string(),
                ))
            }
        };

        if let Some(stack_boc) = &req.stack_boc {
            TVMStack::from_boc(stack_boc.clone()).map_err(|err| {
                GrpcServiceError::InvalidArgument(format!("Invalid stack BOC: {err}"))
            })?;
        }
        let stack_boc = req
            .stack_boc
            .unwrap_or_else(|| TVMStack::EMPTY_BOC.to_owned());

        let emul_data = self
            .collect_emul_run_method_data(req_state, req.mc_seqno)
            .await?;

        let emul_result = match self
            .ctx
            .tda
            .emulation()
            .emulate_get_method(emul_data, method_id, stack_boc)
            .await
        {
            Ok(res) => {
                let stack_boc = res.stack_boc()?;
                EmulateGetMethodResult::Success(EmulateGetMethodSuccess {
                    exit_code: res.vm_exit_code,
                    stack_boc,
                })
            }
            Err(TonError::EmulatorEmulationError {
                vm_exit_code,
                response_raw,
            }) => EmulateGetMethodResult::Error(EmulateGetMethodError {
                exit_code: vm_exit_code,
                emulator_response_json: response_raw,
            }),
            Err(err) => return Err(err.into()),
        };

        Ok(ResponseWithWaitTime {
            response: EmulateGetMethodResponse {
                emulate_get_method_result: Some(emul_result),
            },
            time_waited: Default::default(),
        })
    }

    async fn collect_emul_run_method_data(
        &self,
        state: ContractState,
        mc_seqno: u32,
    ) -> Result<TVMState, GrpcServiceError> {
        let tda = &self.ctx.tda;

        let db_account_state = match state {
            ContractState::Custom(mut state) => {
                state.address = normalize_address(&state.address)?;
                let code = TonCell::from_boc(state.code_boc.clone()).map_err(|err| {
                    GrpcServiceError::InvalidArgument(format!("Invalid code BOC: {err}"))
                })?;
                let data = TonCell::from_boc(state.data_boc.clone()).map_err(|err| {
                    GrpcServiceError::InvalidArgument(format!("Invalid data BOC: {err}"))
                })?;
                let lib_ids = TonCellUtils::extract_lib_ids([&code, &data])?;
                let libs_boc = tda.libs().get_libs_dict(&lib_ids).await?;
                let bc_config = tda.conf().get_config_for_mc_block(mc_seqno).await?;
                let mut c7 = TVMEmulatorC7::new(TonAddress::from_str(&state.address)?, bc_config)?;
                c7.balance = state.balance;
                return Ok(build_tvm_state(
                    state.code_boc,
                    state.data_boc,
                    c7,
                    libs_boc,
                ));
            }
            ContractState::Address(address) => {
                let address = normalize_address(&address)?;
                let key_lookup = TxKeyLookup::address_latest(&address, mc_seqno);
                match tda.db().account().lookup_opt(key_lookup).await? {
                    Some(account_state) => account_state,
                    None => {
                        let err_str =
                            format!("Account state not found for mc_seqno: {mc_seqno}, tx_key_lookup: {key_lookup:?}");
                        return Err(GrpcServiceError::NotFound(err_str));
                    }
                }
            }
            ContractState::AddressLt(address_lt) => {
                let address = normalize_address(&address_lt.address)?;
                let key_lookup = TxKeyLookup::address_lt(&address, address_lt.lt);
                match tda.db().account().lookup_opt(key_lookup).await? {
                    Some(account_state) => account_state,
                    None => {
                        let err_str =
                            format!("Account state not found for mc_seqno: {mc_seqno}, tx_key_lookup: {key_lookup:?}");
                        return Err(GrpcServiceError::NotFound(err_str));
                    }
                }
            }
            ContractState::TxQuery(tx_query) => {
                let tx_query_value = tx_query.value.as_ref().ok_or_else(|| {
                    GrpcServiceError::InvalidArgument("tx_query_value is required".to_string())
                })?;
                let db_tx = get_tx_by_query(
                    tda.db(),
                    tx_query_value,
                    tx_query.address.as_deref(),
                    tx_query.from_utime.unwrap_or_default(),
                    mc_seqno,
                )
                .await?;
                match tda.db().account().get_opt(&db_tx.tx_key()).await? {
                    Some(account_state) => account_state,
                    None => {
                        let err_str = format!(
                            "Account state not found for mc_seqno: {mc_seqno}, tx_key: {:?}",
                            db_tx.tx_key()
                        );
                        return Err(GrpcServiceError::NotFound(err_str));
                    }
                }
            }
        };
        let key_block_seqno = tda
            .db()
            .block()
            .get_master_by_mc_seqno(mc_seqno)
            .await?
            .key_block_seqno;

        let emul_data = tda
            .emulation()
            .collect_tvm_states(
                &db_account_state,
                db_account_state.mc_seqno,
                key_block_seqno,
            )
            .await?;
        Ok(emul_data)
    }

    pub async fn emulate_tx_tree(
        &self,
        req: EmulateTxTreeRequest,
    ) -> Result<ResponseWithWaitTime<EmulateTxTreeResponse>, GrpcServiceError> {
        self.ctx
            .config
            .emulation_tree_limits
            .check_width(req.in_msgs_boc.len())?;
        let in_msgs = req
            .in_msgs_boc
            .into_iter()
            .map(|boc| {
                Msg::from_boc(boc).map_err(|err| {
                    GrpcServiceError::InvalidArgument(format!("Invalid TON message BOC: {err}"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let utime = req.emulation_utime.unwrap_or(cur_ts_sec()? as u32);
        let ignore_chksig = req.ignore_checksig.unwrap_or(false);

        let arg_provider = Arc::new(TreeEmulArgsProviderScylla::new(
            self.ctx.tda.clone(),
            ignore_chksig,
            utime,
        ));
        let tree_emulator = TxTreeEmulator::new(
            arg_provider,
            self.emulators.clone(),
            req.mc_seqno,
            self.ctx.config.emulation_tree_limits,
        )
        .await?;

        let initial_msgs_len = in_msgs.len();

        let tree_emul_result = tree_emulator
            .emulate_tree(in_msgs.clone(), req.adjust_creation_info)
            .await?;

        // Emulated transactions have no containing block: mc_seqno records the requested snapshot,
        // while shard and block seqno are synthetic masterchain-key placeholders in the response.
        // TODO: define explicit emulated-block metadata before treating these fields as chain provenance.
        let mut block_shard_key = DBBlockKey::new_mc(req.mc_seqno);

        let mut db_msgs = Vec::with_capacity(initial_msgs_len);
        for (_addr, tx) in tree_emul_result.txs.iter().take(initial_msgs_len) {
            match &tx.msgs.in_msg {
                Some(msg) => db_msgs.push(msg_serdb(msg)?.0),
                None => {
                    return Err(GrpcServiceError::InternalError(
                        "in_msg should always be set for emulated transactions".to_string(),
                    ))
                }
            }
        }

        let mut db_txs = Vec::with_capacity(tree_emul_result.txs.len() + 1);
        let zero_tx = DBTx {
            out_msgs: db_msgs,
            tx_hash: TonCell::EMPTY_CELL_HASH,
            boc_stripped: Tx::default().to_boc()?.into(),
            ..Default::default()
        };
        db_txs.push(zero_tx.clone());

        for (addr, tx) in tree_emul_result.txs.iter() {
            block_shard_key.workchain = addr.workchain;
            let (db_tx, _) = tx_serdb(&block_shard_key, tx)?;
            db_txs.push(db_tx);
        }

        let tx_info = RequestedTxInfo {
            ton_hash: db_txs[0].tx_hash,
            lt: db_txs[0].tx_lt,
        };
        let mut tx_tree = TxTree::from_txs(db_txs, &tx_info)?;
        for entry in &mut tx_tree.txs {
            let tx_hash = entry.tx.tx_hash;
            entry.emulated = tree_emul_result.state_pos_by_tx.contains_key(&tx_hash);
        }

        let mut accounts = HashMap::with_capacity(tx_tree.txs.len());
        let mut final_states_positions = vec![];

        let mut first = true;
        for (tx_pos, entry) in tx_tree.txs.iter().enumerate() {
            let tx_hash = entry.tx.tx_hash;
            if first {
                first = false;
                accounts.insert(tx_hash, ShardAccount::default());
                continue;
            }
            let db_tx = &entry.tx;
            let index = match tree_emul_result.state_pos_by_tx.get(&db_tx.tx_hash) {
                None => {
                    return Err(GrpcServiceError::InternalError(format!(
                        "Index not found by tx {db_tx:?}"
                    )))
                }
                Some(i) => *i,
            };
            let states = match tree_emul_result
                .states
                .get(&TonAddress::from_str(&db_tx.address)?)
            {
                None => {
                    return Err(GrpcServiceError::InternalError(format!(
                        "State not found by tx {db_tx:?}"
                    )))
                }
                Some(s) => s,
            };

            if index == states.len() - 1 {
                final_states_positions.push(tx_pos as u32);
            }
            match states.get(index).cloned() {
                None => {
                    return Err(GrpcServiceError::InternalError(format!(
                        "ShardAccount not found for tx {db_tx:?}"
                    )))
                }
                Some(account) => accounts.insert(tx_hash, account),
            };
        }

        let mut ton_txs = HashMap::with_capacity(tree_emul_result.txs.len());
        ton_txs.insert(zero_tx.tx_hash, Tx::default());
        for (_, tx) in tree_emul_result.txs {
            ton_txs.insert(tx.cell_hash()?, tx);
        }

        let tx_rsp_params = req.tx_response_params.unwrap_or_default();
        let recent_data = self.ctx.store.get_recent_data();
        let tree_proto = self
            .ctx
            .serial
            .tx()
            .tree_to_proto(
                &tx_tree,
                &tx_rsp_params,
                &recent_data.block_data.last_bc_config,
                Some(&accounts),
                Some(&ton_txs),
            )
            .await?;

        let initial_tx_positions = Self::get_initinal_tx_positions(&tx_tree.txs, in_msgs)?;

        Ok(ResponseWithWaitTime {
            response: EmulateTxTreeResponse {
                txs: tree_proto.txs,
                initial_tx_positions,
                final_states_positions,
            },
            time_waited: Default::default(),
        })
    }

    fn get_initinal_tx_positions(
        txs: &[TxTreeEntry],
        in_msgs: Vec<Msg>,
    ) -> anyhow::Result<Vec<u32>> {
        let mut tx_msgs = HashMap::with_capacity(txs[0].out_txs_pos.len());
        for pos in &txs[0].out_txs_pos {
            let tx = txs
                .get(*pos)
                .map(|e| &e.tx)
                .ok_or_else(|| anyhow!("No tx for initial tx position {pos}"))?;
            let msg = tx.in_msg.as_ref().ok_or_else(|| {
                anyhow!(
                    "No in_msg set for initial tx: address {}, lt: {}",
                    tx.address,
                    tx.tx_lt
                )
            })?;
            tx_msgs
                .entry((msg.dst.clone(), msg.created_lt))
                .or_insert_with(VecDeque::new)
                .push_back(*pos);
        }

        let mut res = Vec::with_capacity(in_msgs.len());
        for msg in in_msgs.into_iter() {
            let dest = tlb_address_to_str(msg.dst())?;
            let key = (dest, msg.created_lt().unwrap_or(0));
            let pos = tx_msgs
                .get_mut(&key)
                .and_then(VecDeque::pop_front)
                .ok_or_else(|| {
                    anyhow!(
                        "Initial tx position not found by in message: dst {}, created_lt {}",
                        key.0,
                        key.1,
                    )
                })?;
            res.push(pos as u32);
        }

        Ok(res)
    }
}
