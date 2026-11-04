use anyhow::bail;
use anyhow::Result;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use ton::block_tlb::{Msg, ShardAccount, Tx};

use super::metrics;
use crate::error::GrpcServiceError;
use crate::services::emulation::tree_emul_args_provider::TreeEmulArgsProvider;
use crate::tx_tree::TreeLimits;
use ton::emulators::emulator_pool::EmulatorPool;
use ton::ton_core::cell::{TonCell, TonHash};
use ton::ton_core::traits::tlb::TLB;
use ton::ton_core::types::TonAddress;

pub struct TxTreeEmulator {
    mc_seqno: u32,
    args_provider: Arc<dyn TreeEmulArgsProvider>,
    emulator_pool: EmulatorPool,
    limits: TreeLimits,
}

#[derive(Debug, Clone)]
pub struct TxTreeEmulResult {
    pub mc_seqno: u32,
    pub txs: Vec<(TonAddress, Tx)>,
    pub state_pos_by_tx: HashMap<TonHash, usize>,
    pub states: HashMap<TonAddress, Vec<ShardAccount>>, // AddressHash
}

impl TxTreeEmulator {
    pub async fn new(
        args_provider: Arc<dyn TreeEmulArgsProvider>,
        emulator_pool: EmulatorPool,
        mc_seqno: u32,
        limits: TreeLimits,
    ) -> Result<TxTreeEmulator> {
        Ok(TxTreeEmulator {
            mc_seqno,
            args_provider,
            emulator_pool,
            limits,
        })
    }

    pub async fn emulate_tree(
        &self,
        in_msgs: Vec<Msg>,
        adjust_creation_info: bool,
    ) -> Result<TxTreeEmulResult, GrpcServiceError> {
        self.limits.check_width(in_msgs.len())?;
        let mut res = TxTreeEmulResult {
            mc_seqno: self.mc_seqno,
            txs: vec![],
            state_pos_by_tx: Default::default(),
            states: Default::default(),
        };

        let mut current_level = in_msgs
            .into_iter()
            .map(|msg| (TonAddress::ZERO, msg))
            .collect::<Vec<_>>();
        let mut depth = 0;

        while !current_level.is_empty() {
            if depth >= self.limits.max_depth {
                return Err(GrpcServiceError::ResourceExhausted(format!(
                    "Reached maximum emulation depth {}",
                    self.limits.max_depth
                )));
            }

            let adjust = depth == 0 && adjust_creation_info;
            let level_results = self
                .process_level(current_level, adjust, &res.states)
                .await?;
            let mut next_level = vec![];
            for (addr, tx, shard_account) in level_results.into_iter().flatten() {
                for msg in &tx.msgs.out_msgs {
                    if msg.info.as_ext_out().is_none() {
                        self.limits.check_width(next_level.len() + 1)?;
                        next_level.push((addr, msg.clone()));
                    }
                }
                res.txs.push((addr, tx));

                let states = res.states.entry(addr).or_default();
                res.state_pos_by_tx
                    .insert(shard_account.last_tx_hash, states.len());
                states.push(shard_account);
            }

            current_level = next_level;
            depth += 1;
        }

        Ok(res)
    }

    async fn process_level(
        &self,
        level: Vec<(TonAddress, Msg)>,
        adjust: bool,
        states: &HashMap<TonAddress, Vec<ShardAccount>>,
    ) -> Result<Vec<Vec<(TonAddress, Tx, ShardAccount)>>> {
        let mut addr_groups: HashMap<TonAddress, Vec<(TonAddress, Msg)>> = HashMap::new();
        let mut order: Vec<TonAddress> = Vec::with_capacity(level.len());
        for (src_addr, msg) in level {
            let dst_addr = TonAddress::from_msg_address(msg.dst())?;
            if !addr_groups.contains_key(&dst_addr) {
                order.push(dst_addr);
            }
            addr_groups
                .entry(dst_addr)
                .or_default()
                .push((src_addr, msg));
        }

        let futures = order.into_iter().map(|dst_addr| {
            let group = addr_groups.remove(&dst_addr).unwrap_or_default();
            let prev_state = states.get(&dst_addr).and_then(|x| x.last());
            self.emulate_txs_with_same_address(prev_state, dst_addr, group, adjust)
        });

        stream::iter(futures)
            .buffered(self.limits.concurrency.get())
            .try_collect()
            .await
    }

    async fn emulate_txs_with_same_address(
        &self,
        last_state: Option<&ShardAccount>,
        dst_addr: TonAddress,
        group: Vec<(TonAddress, Msg)>,
        adjust: bool,
    ) -> Result<Vec<(TonAddress, Tx, ShardAccount)>> {
        let mut result = Vec::with_capacity(group.len());

        for (addr, msg) in group {
            let maybe_state = result.last().map(|(_, _, state)| state).or(last_state);

            let emul_args = self
                .args_provider
                .clone()
                .build_args(self.mc_seqno, addr, dst_addr, msg, maybe_state, adjust)
                .await?;

            let start = Instant::now();
            let emul_result = self
                .emulator_pool
                .emul_ord_tx(emul_args.clone(), None)
                .await;
            metrics::observe_emulation_duration(start.elapsed());

            let response = match emul_result {
                Ok(resp) => resp,
                Err(err) => {
                    bail!(
                        "Emulation fail: {err:?}, emul_args: {emul_args}, address: {dst_addr}, in_msg: {:?}",
                        Msg::<TonCell>::from_boc(emul_args.in_msg_boc.to_vec())?,
                    )
                }
            };

            let success = response.into_success()?;

            let tx = success.tx_parsed()?;
            let mut shard_account = success.shard_account_parsed()?;
            shard_account.last_tx_hash = tx.cell_hash()?;
            shard_account.last_tx_lt = tx.lt;

            result.push((dst_addr, tx, shard_account));
        }

        Ok(result)
    }
}
