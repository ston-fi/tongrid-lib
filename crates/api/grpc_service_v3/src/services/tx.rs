use crate::services::emulation::tree_emul_args_provider::TreeEmulArgsProviderScylla;
use crate::services::emulation::tx_tree_emulator::{TxTreeEmulResult, TxTreeEmulator};
use crate::tx_tree::{get_next_txs, get_prev_txs, RequestedTxInfo, TxTree};
use crate::utils::{get_tx_by_query, make_tx_key_lookup, normalize_address, ResponseWithWaitTime};
use crate::{context::ServiceContext, error::GrpcServiceError};
use anyhow::Result;
use futures::future::try_join_all;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use ton::block_tlb::{Msg, ShardAccount, Tx as TonTx};
use ton::ton_core::cell::TonHash;
use ton::ton_core::traits::tlb::TLB;
use tongrid_core::time::cur_ts_sec;
use tongrid_db::db::DB;
use tongrid_db::entity::block::block_shard_key::DBBlockKey;
use tongrid_db::entity::scylla_types::DBMsg;
use tongrid_db::entity::{DBTx, DBTxKey, TxKeyLookup};
use tongrid_db::paginator::{LTPage, LTPageLoader, LTPaginator};
use tongrid_mem_store::types::recent_data::RecentData;
use tongrid_proto::v3::tx_query::Value as TxQueryValue;
use tongrid_proto::v3::*;
use tongrid_tda::ser_de::msg::msg_deserdb;
use tongrid_tda::ser_de::tx::tx_serdb;
use tonic::async_trait;

const GET_BLOCKS_TXS_MAX_RANGE: usize = 100;
const GET_BLOCKS_TXS_CONCURRENCY: usize = 128;

fn get_blocks_seqno_range(
    from_mc_seqno: u32,
    to_mc_seqno: u32,
) -> Result<Vec<u32>, GrpcServiceError> {
    let range_len = u64::from(to_mc_seqno)
        .checked_sub(u64::from(from_mc_seqno))
        .ok_or_else(|| {
            GrpcServiceError::InvalidArgument(format!(
                "Invalid mc seqno range: from={from_mc_seqno} is greater than to={to_mc_seqno}"
            ))
        })?
        + 1;
    if range_len > GET_BLOCKS_TXS_MAX_RANGE as u64 {
        return Err(GrpcServiceError::InvalidArgument(format!(
            "Requested range is too big: {range_len} (max: {GET_BLOCKS_TXS_MAX_RANGE})"
        )));
    }

    Ok((from_mc_seqno..=to_mc_seqno).collect())
}

pub struct TxService {
    ctx: Arc<ServiceContext>,
}

impl TxService {
    pub fn new(ctx: Arc<ServiceContext>) -> Self {
        Self { ctx }
    }

    pub async fn get_blocks_txs(
        &self,
        req: GetBlocksTxsRequest,
    ) -> Result<ResponseWithWaitTime<TxsResponse>, GrpcServiceError> {
        let tx_rsp_params = req.tx_response_params.unwrap_or_default();
        let (latest_per_address, filter_by_code_hashes) = match req.filter {
            Some(filter) => {
                let latest = filter.latest_per_address.unwrap_or(false);
                if filter.account_code_hashes.is_empty() {
                    (latest, None)
                } else {
                    (latest, Some(filter))
                }
            }
            None => (false, None),
        };

        let Some(range) = req.seqno_range else {
            return Err(GrpcServiceError::InvalidArgument(
                "seqno_range is required".to_string(),
            ));
        };

        let seqno_range = get_blocks_seqno_range(range.from_mc_seqno, range.to_mc_seqno)?;

        let mut txs: Vec<DBTx> = match filter_by_code_hashes {
            Some(filter) => {
                let tx_keys =
                    get_tx_keys_by_filter(self.ctx.tda.db(), &seqno_range, filter).await?;
                stream::iter(tx_keys)
                    .map(|tx_key| async move { self.ctx.tda.db().tx().get(&tx_key).await })
                    .buffered(GET_BLOCKS_TXS_CONCURRENCY)
                    .try_collect()
                    .await?
            }
            None => stream::iter(seqno_range.iter().copied())
                .map(|seqno| self.ctx.tda.db().tx().get_by_mc_seqno(seqno))
                .buffered(GET_BLOCKS_TXS_CONCURRENCY)
                .try_collect::<Vec<Vec<DBTx>>>()
                .await?
                .into_iter()
                .flatten()
                .collect(),
        };

        if latest_per_address {
            let mut uniq: HashMap<String, DBTx> = HashMap::with_capacity(txs.len());
            for tx in txs {
                match uniq.get_mut(&tx.address) {
                    Some(cur) if cur.tx_lt > tx.tx_lt => continue,
                    _ => {
                        uniq.insert(tx.address.clone(), tx);
                    }
                }
            }
            txs = uniq.into_values().collect::<Vec<_>>();
        }

        let txs_proto = self.serialize_blocks_txs(txs, &tx_rsp_params).await?;

        Ok(ResponseWithWaitTime {
            response: TxsResponse { txs: txs_proto },
            time_waited: Default::default(),
        })
    }

    pub async fn get_tx(
        &self,
        req: GetTxRequest,
    ) -> Result<ResponseWithWaitTime<TxResponse>, GrpcServiceError> {
        let tx_rsp_params = req.tx_response_params.unwrap_or_default();
        let address_lt = req.address_lt.ok_or_else(|| {
            GrpcServiceError::InvalidArgument("address_lt is required".to_string())
        })?;

        let db_tx = self
            .ctx
            .tda
            .db()
            .tx()
            .lookup(TxKeyLookup::address_lt(
                &normalize_address(&address_lt.address)?,
                address_lt.lt,
            ))
            .await
            .map_err(|e| GrpcServiceError::NotFound(e.to_string()))?;

        let tx_proto = self.serialize_tx(&db_tx, &tx_rsp_params).await?;

        Ok(ResponseWithWaitTime {
            response: TxResponse { tx: Some(tx_proto) },
            time_waited: Duration::ZERO,
        })
    }

    pub async fn query_txs(
        &self,
        req: QueryTxsRequest,
    ) -> Result<ResponseWithWaitTime<TxsResponse>, GrpcServiceError> {
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(u64::from(req.wait_timeout_ms));
        let tx_rsp_params = req.tx_response_params.unwrap_or_default();
        let tx_query = req
            .tx_query
            .ok_or_else(|| GrpcServiceError::InvalidArgument("tx_query is required".to_string()))?;
        let mut recent_data_rx = self.ctx.store.subscribe();
        let recent_data = recent_data_rx.borrow().clone();

        let mut txs = self.query_txs_impl(&tx_query, recent_data.deref()).await?;

        if txs.is_empty() && req.wait_timeout_ms > 0 {
            let txs_res = tokio::time::timeout_at(deadline, async move {
                let mut full_synced_seqno = recent_data.sync_status.full_synced_seqno;
                loop {
                    if recent_data_rx.changed().await.is_err() {
                        return Err(GrpcServiceError::InternalError(
                            "recent_data_rx is closed".to_string(),
                        ));
                    };
                    let recent_data = recent_data_rx.borrow().clone();
                    if recent_data.sync_status.full_synced_seqno == full_synced_seqno {
                        continue;
                    }
                    full_synced_seqno = recent_data.sync_status.full_synced_seqno;
                    let txs_local = self.query_txs_impl(&tx_query, recent_data.deref()).await?;
                    if !txs_local.is_empty() {
                        return Ok(txs_local);
                    }
                }
            })
            .await;

            match txs_res {
                Ok(res) => txs = res?,
                Err(_) => {
                    return Err(GrpcServiceError::TimedOut(format!(
                        "No fully synced transactions found for the query within {} ms",
                        req.wait_timeout_ms
                    )))
                }
            }
        }

        let txs_proto = self.serialize_txs(&txs, &tx_rsp_params).await?;

        Ok(ResponseWithWaitTime {
            response: TxsResponse { txs: txs_proto },
            time_waited: Duration::ZERO,
        })
    }

    pub async fn get_tx_tree(
        &self,
        req: GetTxTreeRequest,
    ) -> Result<ResponseWithWaitTime<GetTxTreeResponse>, GrpcServiceError> {
        let limits = self.ctx.config.tx_service.tree_limits;
        let tx_rsp_opts = req.tx_response_params.unwrap_or_default();
        let recent_data = self.ctx.store.get_recent_data();
        let tx_id = req
            .tx_id
            .ok_or_else(|| GrpcServiceError::InvalidArgument("tx_id is required".to_string()))?;

        let tx = self
            .ctx
            .tda
            .db()
            .tx()
            .lookup(TxKeyLookup::address_lt(
                &normalize_address(&tx_id.address)?,
                tx_id.lt,
            ))
            .await
            .map_err(|e| GrpcServiceError::NotFound(e.to_string()))?;
        let tx_hash = tx.tx_hash;
        let tx_lt = tx.tx_lt;

        let prev_txs = get_prev_txs(
            &tx,
            self.ctx.tda.db(),
            req.in_depth.min(limits.max_depth.saturating_add(1)),
        )
        .await?;
        limits.check_depth(prev_txs.len() as u32)?;
        let next_txs = get_next_txs(
            &tx,
            self.ctx.tda.db(),
            req.out_depth,
            recent_data.sync_status.full_synced_seqno,
            limits,
        )
        .await?;

        let mut all_txs = Vec::with_capacity(prev_txs.len() + next_txs.len() + 1);
        all_txs.extend(prev_txs);
        all_txs.push(tx);
        all_txs.extend(next_txs);

        let tx_info = RequestedTxInfo {
            ton_hash: tx_hash,
            lt: tx_lt,
        };

        let mut emulated = None;

        if req.emulate_ongoing_end.unwrap_or(false) {
            let unresolved = self
                .collect_unresolved_leaf_msgs(&all_txs, tx_info.lt)
                .await?;
            if !unresolved.is_empty() {
                let mut emulated_ends = self
                    .emulate_tree_leafs(unresolved, recent_data.sync_status.full_synced_seqno)
                    .await?;
                all_txs.append(&mut emulated_ends.db_txs);
                emulated = Some(emulated_ends);
            }
        }

        let mut tx_tree = TxTree::from_txs(all_txs, &tx_info)?;

        if let Some(res) = emulated.as_ref() {
            for entry in &mut tx_tree.txs {
                let tx_hash = entry.tx.tx_hash;
                if res.ton_txs.contains_key(&tx_hash) {
                    entry.emulated = true;
                }
            }
        }

        let tree_txs: Vec<DBTx> = tx_tree.txs.iter().map(|entry| entry.tx.clone()).collect();
        let ton_txs = match emulated.as_ref() {
            Some(res) => Some(self.build_ton_txs_map(&tx_tree, &res.ton_txs).await?),
            None => None,
        };

        let states = if tx_rsp_opts.account_required || tx_rsp_opts.account_parsed_required {
            let mut states_map = HashMap::new();
            let mut missing_state_txs = Vec::new();
            for tx in &tree_txs {
                let tx_hash = tx.tx_hash;
                if let Some(shard_account) = emulated.as_ref().and_then(|x| x.states.get(&tx_hash))
                {
                    states_map.insert(tx_hash, shard_account.clone());
                    continue;
                }
                missing_state_txs.push((tx_hash, tx.tx_key()));
            }

            let account_states_futs = missing_state_txs
                .iter()
                .map(|(_, tx_key)| self.ctx.tda.db().account().get(tx_key));
            let account_states = try_join_all(account_states_futs).await?;

            for ((tx_hash, _), db_state) in missing_state_txs.into_iter().zip(account_states) {
                if !db_state.boc_stripped.is_empty() {
                    if let Ok(shard_account) =
                        self.ctx.tda.account().build_shard_account(&db_state).await
                    {
                        states_map.insert(tx_hash, shard_account.0);
                    }
                }
            }

            Some(states_map)
        } else {
            None
        };

        let tree_proto = self
            .ctx
            .serial
            .tx()
            .tree_to_proto(
                &tx_tree,
                &tx_rsp_opts,
                &recent_data.block_data.last_bc_config,
                states.as_ref(),
                ton_txs.as_ref(),
            )
            .await?;

        Ok(ResponseWithWaitTime {
            response: GetTxTreeResponse {
                txs: tree_proto.txs,
                requested_tx_pos: tx_tree.requested_tx_pos as u32,
                root_tx_pos: tx_tree.root_tx_pos as u32,
                initial_tx_pos: tx_tree.initial_tx_pos.map(|pos| pos as u32),
                completed: tx_tree.completed,
            },
            time_waited: Duration::ZERO,
        })
    }

    pub async fn get_initial_tx(
        &self,
        req: GetInitialTxRequest,
    ) -> Result<ResponseWithWaitTime<TxResponse>, GrpcServiceError> {
        let tx_rsp_params = req.tx_response_params.unwrap_or_default();
        let tx_id = req
            .tx_id
            .ok_or_else(|| GrpcServiceError::InvalidArgument("tx_id is required".to_string()))?;

        let requested_tx = self
            .ctx
            .tda
            .db()
            .tx()
            .lookup(TxKeyLookup::address_lt(
                &normalize_address(&tx_id.address)?,
                tx_id.lt,
            ))
            .await
            .map_err(|e| GrpcServiceError::NotFound(e.to_string()))?;

        let initial_tx = if self.ctx.config.tx_service.get_initial_tx_from_tx_chain {
            self.get_initial_tx_from_prev_txs(requested_tx).await?
        } else {
            self.get_initial_tx_from_initial_key(requested_tx).await?
        };

        let tx_proto = self.serialize_tx(&initial_tx, &tx_rsp_params).await?;

        Ok(ResponseWithWaitTime {
            response: TxResponse { tx: Some(tx_proto) },
            time_waited: Duration::ZERO,
        })
    }

    pub async fn get_txs_by_address(
        &self,
        mut req: GetTxsByAddressRequest,
    ) -> Result<ResponseWithWaitTime<GetTxsByAddressResponse>, GrpcServiceError> {
        req.address = normalize_address(&req.address)?;
        let tx_rsp_params = req.tx_response_params.unwrap_or_default();
        let recent_data = self.ctx.store.get_recent_data();

        let mut paginator = LTPaginator::<TxsLoader>::new(
            self.ctx.tda.db().clone(),
            req.address,
            recent_data.sync_status.full_synced_seqno,
            req.pagination_params.as_ref(),
        )
        .await?;

        let acc_txs = paginator.load_page().await?;
        let txs_proto = self.serialize_txs(&acc_txs, &tx_rsp_params).await?;

        Ok(ResponseWithWaitTime {
            response: GetTxsByAddressResponse {
                txs: txs_proto,
                next_pagination_params: paginator.next_page_proto(),
            },
            time_waited: Duration::ZERO,
        })
    }

    pub async fn get_ongoing_txs(
        &self,
        mut req: GetOngoingTxsRequest,
    ) -> Result<ResponseWithWaitTime<TxsResponse>, GrpcServiceError> {
        req.address = normalize_address(&req.address)?;
        let tx_rsp_params = req.tx_response_params.unwrap_or_default();
        let ongoing_txs = self
            .ctx
            .tda
            .db()
            .ongoing_tx()
            .get_ongoing_txs(&req.address)
            .await?;

        let futures = ongoing_txs
            .into_iter()
            .map(|ongoing_tx| async move {
                self.ctx
                    .tda
                    .db()
                    .tx()
                    .lookup(TxKeyLookup::address_lt(
                        &ongoing_tx.address,
                        ongoing_tx.tx_lt,
                    ))
                    .await
                    .map_err(|e| GrpcServiceError::NotFound(e.to_string()))
            })
            .collect::<Vec<_>>();
        let db_txs = try_join_all(futures).await?;

        let txs_proto = self.serialize_txs(&db_txs, &tx_rsp_params).await?;

        Ok(ResponseWithWaitTime {
            response: TxsResponse { txs: txs_proto },
            time_waited: Duration::ZERO,
        })
    }

    pub async fn get_last_tx_at_mc_seqno(
        &self,
        mut req: GetLastTxAtMcSeqnoRequest,
    ) -> Result<ResponseWithWaitTime<TxResponse>, GrpcServiceError> {
        req.address = normalize_address(&req.address)?;
        let tx_rsp_params = req.tx_response_params.unwrap_or_default();

        let db_tx = self
            .ctx
            .tda
            .db()
            .tx()
            .lookup(TxKeyLookup::address_latest(&req.address, req.mc_seqno))
            .await
            .map_err(|e| GrpcServiceError::NotFound(e.to_string()))?;
        let tx_proto = self.serialize_tx(&db_tx, &tx_rsp_params).await?;

        Ok(ResponseWithWaitTime {
            response: TxResponse { tx: Some(tx_proto) },
            time_waited: Default::default(),
        })
    }

    async fn query_txs_impl(
        &self,
        tx_query: &TxQuery,
        recent_data: &RecentData,
    ) -> Result<Vec<DBTx>, GrpcServiceError> {
        let tx_query_value = tx_query.value.as_ref().ok_or_else(|| {
            GrpcServiceError::InvalidArgument("tx_query.value is required".to_string())
        })?;
        let from_utime = tx_query.from_utime.unwrap_or_default();
        let query_address = tx_query
            .address
            .as_deref()
            .filter(|addr| !addr.is_empty())
            .map(normalize_address)
            .transpose()?;
        let query_address = query_address.as_deref();

        let db = self.ctx.tda.db();

        let mut txs = match tx_query_value {
            TxQueryValue::QueryId(_) => {
                let lookup_key = make_tx_key_lookup(tx_query_value, query_address, from_utime)?;
                let mut txs = db.tx().lookup_many(lookup_key).await?;
                txs.retain(|tx| {
                    tx.tx_utime >= from_utime && query_address.is_none_or(|addr| tx.address == addr)
                });
                txs
            }
            _ => match get_tx_by_query(
                db,
                tx_query_value,
                query_address,
                from_utime,
                recent_data.sync_status.full_synced_seqno,
            )
            .await
            {
                Ok(tx) => vec![tx],
                Err(GrpcServiceError::NotFound(_)) => vec![],
                Err(err) => return Err(err),
            },
        };
        // Transaction rows can be visible before account and other dependent syncs finish.
        txs.retain(|tx| tx.mc_seqno <= recent_data.sync_status.full_synced_seqno);
        Ok(txs)
    }

    async fn serialize_blocks_txs(
        &self,
        txs: Vec<DBTx>,
        tx_rsp_params: &TxResponseParams,
    ) -> Result<Vec<Tx>, GrpcServiceError> {
        let recent_data = self.ctx.store.get_recent_data();
        let bc_config = &recent_data.block_data.last_bc_config;
        let tx_serial = self.ctx.serial.tx();
        let futures = txs.into_iter().map(|tx| {
            let tx_serial = &tx_serial;
            async move { tx_serial.db_to_proto(&tx, tx_rsp_params, bc_config).await }
        });
        Ok(stream::iter(futures)
            .buffered(GET_BLOCKS_TXS_CONCURRENCY)
            .try_collect()
            .await?)
    }

    async fn serialize_txs(
        &self,
        txs: &[DBTx],
        tx_rsp_params: &TxResponseParams,
    ) -> Result<Vec<Tx>, GrpcServiceError> {
        let recent_data = self.ctx.store.get_recent_data();
        let bc_config = &recent_data.block_data.last_bc_config;
        let tx_serial = self.ctx.serial.tx();
        let futures = txs
            .iter()
            .map(|tx| tx_serial.db_to_proto(tx, tx_rsp_params, bc_config));
        Ok(try_join_all(futures).await?)
    }

    async fn serialize_tx(
        &self,
        tx: &DBTx,
        tx_rsp_params: &TxResponseParams,
    ) -> Result<Tx, GrpcServiceError> {
        let recent_data = self.ctx.store.get_recent_data();
        Ok(self
            .ctx
            .serial
            .tx()
            .db_to_proto(tx, tx_rsp_params, &recent_data.block_data.last_bc_config)
            .await?)
    }

    async fn get_initial_tx_from_initial_key(
        &self,
        requested_tx: DBTx,
    ) -> Result<DBTx, GrpcServiceError> {
        if requested_tx.initial_tx.is_zero() {
            return Err(GrpcServiceError::InternalError(format!(
                "Initial tx is not set for requested tx_hash='{:?}'",
                requested_tx.tx_hash,
            )));
        }
        if requested_tx.initial_tx == requested_tx.tx_key() {
            return Ok(requested_tx);
        }

        self.ctx
            .tda
            .db()
            .tx()
            .get_opt(&requested_tx.initial_tx)
            .await?
            .ok_or_else(|| {
                GrpcServiceError::NotFound(format!(
                    "Failed to find initial_tx for tx_hash='{:?}'",
                    requested_tx.tx_hash
                ))
            })
    }

    async fn get_initial_tx_from_prev_txs(
        &self,
        requested_tx: DBTx,
    ) -> Result<DBTx, GrpcServiceError> {
        let initial_tx = if requested_tx.in_msg.is_none()
            || requested_tx
                .in_msg
                .as_ref()
                .is_some_and(|msg| msg.src.is_empty())
        {
            requested_tx.clone()
        } else {
            get_prev_txs(&requested_tx, self.ctx.tda.db(), u32::MAX)
                .await?
                .into_iter()
                .find(|tx| {
                    tx.in_msg.is_none() || tx.in_msg.as_ref().is_some_and(|msg| msg.src.is_empty())
                })
                .ok_or_else(|| {
                    GrpcServiceError::NotFound(format!(
                        "Failed to find initial_tx for tx_hash='{:?}'",
                        requested_tx.tx_hash
                    ))
                })?
        };
        Ok(initial_tx)
    }

    async fn collect_unresolved_leaf_msgs(
        &self,
        txs: &[DBTx],
        min_lt: u64,
    ) -> Result<Vec<DBMsg>, GrpcServiceError> {
        let in_msgs: HashSet<(&str, &str, u64)> = txs
            .iter()
            .filter_map(|tx| tx.in_msg.as_ref())
            .map(|msg| (msg.src.as_str(), msg.dst.as_str(), msg.created_lt))
            .collect();

        let mut unresolved = Vec::new();
        for tx in txs {
            if tx.tx_lt < min_lt {
                continue;
            }
            for msg in &tx.out_msgs {
                if msg.dst.is_empty() {
                    continue;
                }
                let key = (msg.src.as_str(), msg.dst.as_str(), msg.created_lt);
                if in_msgs.contains(&key) {
                    continue;
                }
                self.ctx
                    .config
                    .emulation_tree_limits
                    .check_width(unresolved.len() + 1)?;
                unresolved.push(msg.clone());
            }
        }
        Ok(unresolved)
    }

    async fn emulate_tree_leafs(
        &self,
        leaf_msgs: Vec<DBMsg>,
        mc_seqno: u32,
    ) -> Result<EmulatedEndResult, GrpcServiceError> {
        let in_msgs = self.deserialize_leaf_msgs(leaf_msgs).await?;
        let utime = cur_ts_sec()? as u32;
        let args_provider = Arc::new(TreeEmulArgsProviderScylla::new(
            self.ctx.tda.clone(),
            false,
            utime,
        ));

        let tree_emulator = TxTreeEmulator::new(
            args_provider,
            self.ctx.emulator_pool.clone(),
            mc_seqno,
            self.ctx.config.emulation_tree_limits,
        )
        .await?;
        let emulated_tree = tree_emulator.emulate_tree(in_msgs, false).await?;
        self.emulated_tree_to_db(emulated_tree, mc_seqno)
    }

    fn emulated_tree_to_db(
        &self,
        emulated_tree: TxTreeEmulResult,
        mc_seqno: u32,
    ) -> Result<EmulatedEndResult, GrpcServiceError> {
        let TxTreeEmulResult {
            txs,
            state_pos_by_tx,
            states,
            ..
        } = emulated_tree;

        let mut db_txs = Vec::with_capacity(txs.len());
        let mut ton_txs = HashMap::with_capacity(txs.len());
        let mut emul_states = HashMap::with_capacity(txs.len());
        let mut block_shard_key = DBBlockKey::new_mc(mc_seqno);

        for (addr, tx) in txs {
            block_shard_key.workchain = addr.workchain;
            let tx_hash = tx.cell_hash()?;
            let state_pos = *state_pos_by_tx.get(&tx_hash).ok_or_else(|| {
                GrpcServiceError::InternalError(format!(
                    "Missing emulated state position for tx hash {tx_hash}"
                ))
            })?;
            let state = states
                .get(&addr)
                .and_then(|state_chain| state_chain.get(state_pos))
                .cloned()
                .ok_or_else(|| {
                    GrpcServiceError::InternalError(format!(
                        "Missing emulated state for tx hash {tx_hash}"
                    ))
                })?;

            let (db_tx, _) = tx_serdb(&block_shard_key, &tx)?;
            ton_txs.insert(tx_hash, tx);
            emul_states.insert(tx_hash, state);
            db_txs.push(db_tx);
        }

        Ok(EmulatedEndResult {
            db_txs,
            ton_txs,
            states: emul_states,
        })
    }

    async fn build_ton_txs_map(
        &self,
        tx_tree: &TxTree,
        emulated_ton_txs: &HashMap<TonHash, TonTx>,
    ) -> Result<HashMap<TonHash, TonTx>, GrpcServiceError> {
        let build_futs = tx_tree.txs.iter().map(|entry| async {
            let tx_hash = entry.tx.tx_hash;
            if let Some(emulated_tx) = emulated_ton_txs.get(&tx_hash) {
                return Ok::<(TonHash, TonTx), GrpcServiceError>((tx_hash, emulated_tx.clone()));
            }
            let ton_tx = self.ctx.tda.tx().build_ton_tx(&entry.tx).await?.0;
            Ok::<(TonHash, TonTx), GrpcServiceError>((tx_hash, ton_tx))
        });
        let ton_txs: HashMap<TonHash, TonTx> =
            try_join_all(build_futs).await?.into_iter().collect();

        Ok(ton_txs)
    }

    async fn deserialize_leaf_msgs(
        &self,
        leaf_msgs: Vec<DBMsg>,
    ) -> Result<Vec<Msg>, GrpcServiceError> {
        let mut code_hashes = HashSet::new();
        for msg in &leaf_msgs {
            if let Some(code_hash) = msg.code_hash.as_ref() {
                code_hashes.insert(*code_hash);
            }
            code_hashes.extend(msg.simple_libs.iter().map(|lib| lib.code_hash));
        }
        let tvm_codes = self
            .ctx
            .tda
            .db()
            .tvm_code()
            .get_by_hashes(code_hashes.iter())
            .await?;

        leaf_msgs
            .into_iter()
            .map(|msg| {
                msg_deserdb(&msg, &tvm_codes)
                    .map(|(ton_msg, _)| ton_msg)
                    .map_err(|err| {
                        GrpcServiceError::InternalError(format!(
                            "Failed to restore leaf message for emulation: {err}"
                        ))
                    })
            })
            .collect()
    }
}

struct EmulatedEndResult {
    db_txs: Vec<DBTx>,
    ton_txs: HashMap<TonHash, TonTx>,
    states: HashMap<TonHash, ShardAccount>,
}

async fn get_tx_keys_by_filter(
    db: &DB,
    seqno_range: &[u32],
    filter: TxFilter,
) -> std::result::Result<Vec<DBTxKey>, GrpcServiceError> {
    let code_hashes = filter
        .account_code_hashes
        .iter()
        .map(|h| h.as_slice())
        .collect::<Vec<_>>();

    let mut by_code_hash = db
        .tx()
        .lookup_keys(TxKeyLookup::SeqnosCodeHashes {
            mc_seqnos: seqno_range,
            code_hashes: &code_hashes,
        })
        .await?;

    let by_last_code_hash = db
        .tx()
        .lookup_keys(TxKeyLookup::SeqnosLastCodeHashes {
            mc_seqnos: seqno_range,
            last_code_hashes: &code_hashes,
        })
        .await?;

    by_code_hash.extend(by_last_code_hash);
    by_code_hash.sort();
    by_code_hash.dedup();
    Ok(by_code_hash)
}

struct TxsLoader;

#[async_trait]
impl LTPageLoader for TxsLoader {
    type EntityWithLT = DBTx;

    async fn load_page_sorted(
        db: &DB,
        address: &str,
        page: &LTPage,
    ) -> Result<Vec<Self::EntityWithLT>> {
        let key_lookup = TxKeyLookup::address_lt_range(address, page);
        db.tx().lookup_many(key_lookup).await
    }

    fn read_lt(entity: &DBTx) -> u64 {
        entity.tx_lt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_blocks_seqno_range_rejects_oversized_range_before_allocation() -> anyhow::Result<()>
    {
        let largest_allowed = get_blocks_seqno_range(u32::MAX - 99, u32::MAX)?;
        assert_eq!(largest_allowed.len(), GET_BLOCKS_TXS_MAX_RANGE);
        assert!(matches!(
            get_blocks_seqno_range(0, u32::MAX),
            Err(GrpcServiceError::InvalidArgument(_))
        ));
        assert!(matches!(
            get_blocks_seqno_range(1, 0),
            Err(GrpcServiceError::InvalidArgument(_))
        ));
        Ok(())
    }
}
