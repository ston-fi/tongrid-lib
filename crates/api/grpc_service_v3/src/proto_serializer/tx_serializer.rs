use crate::proto_serializer::ProtoSerializer;
use crate::tx_tree::{TxTree, TxTreeEntry};
use crate::utils::{load_trait_ids_from_code_hashes, load_traits_for_account};
use anyhow::Result;
use futures::future::try_join_all;
use std::collections::{HashMap, HashSet};
use ton::block_tlb::{CommonMsgInfo, ConfigParams, Msg, ShardAccount, Tx};
use ton::ton_core::cell::TonHash;
use ton::ton_core::traits::tlb::TLB;
use tongrid_db::entity::DBTx;
use tongrid_proto::v3::Msg as ProtoMsg;
use tongrid_proto::v3::TxTreeEntry as ProtoTxTreeEntry;
use tongrid_proto::v3::{MsgResponseParams, Tx as ProtoTx, TxResponseParams};
use tongrid_tda::ser_de::address::tlb_address_to_str;

pub struct TxTreeProto {
    pub txs: Vec<ProtoTxTreeEntry>,
}

pub(crate) struct TxSerializer<'a> {
    serial: &'a ProtoSerializer,
}

impl<'a> TxSerializer<'a> {
    pub fn new(serial: &'a ProtoSerializer) -> Self {
        Self { serial }
    }

    pub fn ton_to_proto(
        &self,
        ton_tx: &Tx,
        db_tx: &DBTx,
        rsp_params: &TxResponseParams,
        bc_config: &ConfigParams,
        // The caller supplies state so emulated transactions need no database account row.
        shard_account: Option<&ShardAccount>,
        traits: Vec<i32>,
    ) -> Result<ProtoTx> {
        let msg_rsp_params = rsp_params.msg_response_params.unwrap_or_default();

        let in_msg = match (rsp_params.in_msg_required, &ton_tx.msgs.in_msg) {
            (true, Some(msg)) => Some(self.msg_to_proto(msg, &msg_rsp_params)?),
            _ => None,
        };

        let out_msgs = match rsp_params.out_msgs_required {
            true => ton_tx
                .msgs
                .out_msgs
                .iter()
                .map(|msg| self.msg_to_proto(msg, &msg_rsp_params))
                .collect::<Result<_>>()?,
            false => vec![],
        };

        let tx_data = match rsp_params.data_required {
            true => Some(ton_tx.to_boc()?),
            false => None,
        };

        let exit_code = match rsp_params.exit_code_required {
            true => ton_tx.descr.exit_code(),
            false => None,
        };

        let (storage_fee, other_fee, total_fee) = match rsp_params.fees_required {
            true => {
                // handle out_msgs
                let mut all_fee = ton_tx.total_fees.coins.to_u128();
                for msg in &ton_tx.msgs.out_msgs {
                    match &msg.info {
                        CommonMsgInfo::Int(info) => {
                            all_fee += info.fwd_fee.to_u128();
                            all_fee += info.ihr_fee.to_u128();
                        }
                        CommonMsgInfo::ExtIn(info) => all_fee += info.import_fee.to_u128(),
                        _ => {}
                    }
                }
                let storage_fee = match ton_tx.descr.storage_phase() {
                    Some(phase) => phase.storage_fees_collected.to_u64()? as i64,
                    None => 0,
                };
                let all_fee_int = all_fee as i64;

                let other_fee = all_fee_int - storage_fee;
                (Some(storage_fee), Some(other_fee), Some(all_fee_int))
            }
            false => (None, None, None),
        };

        let account_required = rsp_params.account_required || rsp_params.account_parsed_required;
        let (account_state, account_state_parsed) = match (account_required, shard_account) {
            (true, Some(state)) => {
                let account_state = if rsp_params.account_required {
                    Some(self.serial.account().ton_to_proto(
                        state,
                        db_tx.mc_seqno,
                        traits.clone(),
                    )?)
                } else {
                    None
                };
                let account_state_parsed = if rsp_params.account_parsed_required {
                    Some(
                        self.serial
                            .account()
                            .ton_to_parsed(state, bc_config, traits)?,
                    )
                } else {
                    None
                };
                (account_state, account_state_parsed)
            }
            (true, None) => {
                log::warn!(
                    "[ton_to_proto] account state is required, but not provided for tx ({:?})",
                    db_tx.tx_key()
                );
                (None, None)
            }
            (false, _) => (None, None),
        };

        let (in_msg_hash, in_msg_hash_normalized) = match &ton_tx.msgs.in_msg {
            Some(msg) => (
                Some(msg.cell_hash()?.into_vec()),
                Some(msg.cell_hash_normalized()?.into_vec()),
            ),
            None => (None, None),
        };

        Ok(tongrid_proto::v3::Tx {
            mc_seqno: db_tx.mc_seqno,
            block_workchain: db_tx.block_workchain,
            block_shard: db_tx.block_shard as u64,
            block_seqno: db_tx.block_seqno,
            address: db_tx.address.clone(),
            tx_hash: db_tx.tx_hash.to_vec(),
            tx_lt: ton_tx.lt,
            utime: ton_tx.now,
            prev_tx_hash: ton_tx.prev_tx_hash.to_vec(),
            prev_tx_lt: ton_tx.prev_tx_lt,
            tx_boc: tx_data,
            in_msg,
            out_msgs,
            storage_fee,
            other_fee,
            total_fee,
            in_msg_hash,
            in_msg_hash_normalized,
            exit_code,
            account_state,
            account_state_parsed,
        })
    }

    pub async fn db_to_proto(
        &self,
        db_tx: &DBTx,
        rsp_params: &TxResponseParams,
        bc_config: &ConfigParams,
    ) -> Result<ProtoTx> {
        let ton_tx = self.serial.tda.tx().build_ton_tx(db_tx).await?.0;

        if !rsp_params.account_required && !rsp_params.account_parsed_required {
            return self.ton_to_proto(&ton_tx, db_tx, rsp_params, bc_config, None, vec![]);
        }

        let db_state = self.serial.tda.db().account().get(&db_tx.tx_key()).await?;
        if db_state.boc_stripped.is_empty() {
            self.ton_to_proto(&ton_tx, db_tx, rsp_params, bc_config, None, vec![])
        } else {
            let shard_account = self
                .serial
                .tda
                .account()
                .build_shard_account(&db_state)
                .await?
                .0;
            let traits =
                load_traits_for_account(&shard_account, self.serial.tda.db(), true).await?;
            self.ton_to_proto(
                &ton_tx,
                db_tx,
                rsp_params,
                bc_config,
                Some(&shard_account),
                traits,
            )
        }
    }

    pub async fn tree_to_proto(
        &self,
        tree: &TxTree,
        rsp_params: &TxResponseParams,
        bc_config: &ConfigParams,
        states: Option<&HashMap<TonHash, ShardAccount>>,
        ton_txs: Option<&HashMap<TonHash, Tx>>,
    ) -> Result<TxTreeProto> {
        let account_required = rsp_params.account_required || rsp_params.account_parsed_required;
        let traits_map = if account_required {
            self.batch_load_traits_for_tree(states).await?
        } else {
            HashMap::new()
        };

        let mut tree_entries_futs = Vec::with_capacity(tree.txs.len());
        for entry in &tree.txs {
            let tx_hash = &entry.tx.tx_hash;
            let ton_tx = match ton_txs {
                Some(ton_txs) => Some(ton_txs.get(tx_hash).ok_or_else(|| {
                    anyhow::anyhow!(
                        "[tree_to_proto] ton_txs provided, but not found for tx_hash: {tx_hash}"
                    )
                })?),
                None => None,
            };
            let state = match states {
                Some(states) => Some(states.get(tx_hash).ok_or_else(|| {
                    anyhow::anyhow!(
                        "[tree_to_proto] states provided, but not found for tx_hash: {tx_hash}"
                    )
                })?),
                None => None,
            };
            let fut = self.tree_entry_to_proto(
                entry,
                rsp_params,
                bc_config,
                ton_tx,
                state,
                traits_map.clone(),
            );
            tree_entries_futs.push(fut);
        }
        let tree_entries = try_join_all(tree_entries_futs).await?;

        Ok(TxTreeProto { txs: tree_entries })
    }

    async fn tree_entry_to_proto(
        &self,
        entry: &TxTreeEntry,
        rsp_params: &TxResponseParams,
        bc_config: &ConfigParams,
        ton_tx: Option<&Tx>,
        account: Option<&ShardAccount>,
        traits_by_code_hash: HashMap<TonHash, Vec<i32>>,
    ) -> Result<ProtoTxTreeEntry> {
        let ton_tx = match ton_tx {
            Some(tx) => tx,
            None => &self.serial.tda.tx().build_ton_tx(&entry.tx).await?.0,
        };

        // Helper function to extract traits from code hash
        let get_traits = |account: Option<&ShardAccount>| -> Vec<i32> {
            account
                .and_then(|sa| {
                    sa.account
                        .get_code()
                        .map(|x| x.hash())
                        .transpose()
                        .ok()
                        .flatten()
                })
                .and_then(|code_hash| traits_by_code_hash.get(code_hash).cloned())
                .unwrap_or_default()
        };

        let proto_tx = match rsp_params.account_required || rsp_params.account_parsed_required {
            true => match account {
                Some(shard_account) => {
                    let traits = get_traits(Some(shard_account));
                    self.ton_to_proto(
                        ton_tx,
                        &entry.tx,
                        rsp_params,
                        bc_config,
                        Some(shard_account),
                        traits,
                    )?
                }
                None => {
                    let db_state = self
                        .serial
                        .tda
                        .db()
                        .account()
                        .get(&entry.tx.tx_key())
                        .await?;
                    if db_state.boc_stripped.is_empty() {
                        log::warn!(
                            "[tree_entry_to_proto] account is required, but boc_stripped is empty for tx: ({}:{})",
                            entry.tx.tx_lt,
                            entry.tx.tx_hash
                        );
                        self.ton_to_proto(ton_tx, &entry.tx, rsp_params, bc_config, None, vec![])?
                    } else {
                        let shard_account = self
                            .serial
                            .tda
                            .account()
                            .build_shard_account(&db_state)
                            .await?
                            .0;
                        let traits = get_traits(Some(&shard_account));
                        self.ton_to_proto(
                            ton_tx,
                            &entry.tx,
                            rsp_params,
                            bc_config,
                            Some(&shard_account),
                            traits,
                        )?
                    }
                }
            },
            false => self.ton_to_proto(ton_tx, &entry.tx, rsp_params, bc_config, None, vec![])?,
        };

        Ok(ProtoTxTreeEntry {
            tx: Some(proto_tx),
            in_tx_pos: entry.in_tx_pos.map(|pos| pos as u32),
            out_txs_pos: entry.out_txs_pos.iter().map(|pos| *pos as u32).collect(),
            emulated: entry.emulated,
        })
    }

    pub async fn batch_load_traits_for_tree(
        &self,
        states: Option<&HashMap<TonHash, ShardAccount>>,
    ) -> Result<HashMap<TonHash, Vec<i32>>> {
        let mut code_hashes = HashSet::new();

        if let Some(states) = states {
            for shard_account in states.values() {
                if let Some(code_hash) = shard_account
                    .account
                    .get_code()
                    .map(|x| x.hash())
                    .transpose()
                    .ok()
                    .flatten()
                {
                    code_hashes.insert(*code_hash);
                }
            }
        }

        let code_hash_vec: Vec<TonHash> = code_hashes.into_iter().collect();
        load_trait_ids_from_code_hashes(self.serial.tda.db(), &code_hash_vec).await
    }

    fn msg_to_proto(&self, msg: &Msg, rsp_opts: &MsgResponseParams) -> Result<ProtoMsg> {
        let msg_boc =
            if rsp_opts.value_required || rsp_opts.body_required || rsp_opts.init_state_required {
                Some(msg.to_boc()?)
            } else {
                None
            };

        Ok(tongrid_proto::v3::Msg {
            src: tlb_address_to_str(msg.src())?,
            dst: tlb_address_to_str(msg.dst())?,
            created_lt: msg.created_lt().unwrap_or(0),
            msg_boc,
        })
    }
}
