use crate::error::GrpcServiceError;
use anyhow::Result;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use ton::ton_core::cell::TonHash;
use tongrid_db::db::DB;
use tongrid_db::entity::DBTx;

#[derive(Debug, Copy, Clone, serde::Deserialize)]
#[serde(default)]
pub struct TreeLimits {
    pub max_depth: u32,
    pub max_width: usize,
    pub concurrency: NonZeroUsize,
}

impl Default for TreeLimits {
    fn default() -> Self {
        Self {
            max_depth: 30,
            max_width: 30,
            concurrency: NonZeroUsize::MIN.saturating_add(127),
        }
    }
}

impl TreeLimits {
    pub(crate) fn check_depth(&self, depth: u32) -> Result<(), GrpcServiceError> {
        if depth > self.max_depth {
            return Err(GrpcServiceError::ResourceExhausted(format!(
                "Tree depth {depth} exceeds maximum {}",
                self.max_depth
            )));
        }
        Ok(())
    }

    pub(crate) fn check_width(&self, width: usize) -> Result<(), GrpcServiceError> {
        if width > self.max_width {
            return Err(GrpcServiceError::ResourceExhausted(format!(
                "Tree level width {width} exceeds maximum {}",
                self.max_width
            )));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct TxTreeEntry {
    pub tx: DBTx,
    pub in_tx_pos: Option<usize>,
    pub out_txs_pos: Vec<usize>,
    pub emulated: bool,
}

#[derive(Default, Debug)]
pub struct TxTree {
    pub txs: Vec<TxTreeEntry>,
    pub requested_tx_pos: usize,
    pub root_tx_pos: usize,
    pub initial_tx_pos: Option<usize>,
    pub completed: bool,
}
// Identify the requested transaction without cloning its payload.
pub struct RequestedTxInfo {
    pub ton_hash: TonHash,
    pub lt: u64,
}

impl TxTree {
    pub(crate) fn from_txs(
        mut txs: Vec<DBTx>,
        requested_tx_info: &RequestedTxInfo,
    ) -> Result<Self, GrpcServiceError> {
        txs.sort_by_key(|x| x.tx_lt);

        // index
        let mut txs_by_out_msg = HashMap::with_capacity(txs.len());
        let mut txs_by_in_msg = HashMap::with_capacity(txs.len());
        for (pos, tx) in txs.iter().rev().enumerate() {
            let pos = txs.len() - 1 - pos;

            for msg in tx.out_msgs.iter() {
                let msg_key = (msg.src.clone(), msg.dst.clone(), msg.created_lt);

                // Here, when going before our requested transaction, we want collect txs only on path to our requested tx
                if tx.tx_lt < requested_tx_info.lt {
                    if txs_by_in_msg.contains_key(&msg_key) {
                        txs_by_out_msg.insert(msg_key, pos);
                    }
                } else {
                    txs_by_out_msg.insert(msg_key, pos);
                }
            }
            if let Some(msg) = &tx.in_msg {
                let msg_key = (msg.src.clone(), msg.dst.clone(), msg.created_lt);
                // External messages to the same address share a key; keep their execution order.
                txs_by_in_msg
                    .entry(msg_key)
                    .or_insert_with(VecDeque::new)
                    .push_front(pos);
            }
        }
        let mut result = Self {
            txs: Vec::with_capacity(txs.len()),
            requested_tx_pos: 0,
            root_tx_pos: 0, // Transactions are sorted by LT; clients must use the returned position.
            initial_tx_pos: None,
            completed: true,
        };

        for (pos, tx) in txs.into_iter().enumerate() {
            if tx.tx_hash == requested_tx_info.ton_hash {
                result.requested_tx_pos = pos;
            }

            if tx.in_msg.as_ref().is_none_or(|msg| msg.src.is_empty()) {
                result.initial_tx_pos = Some(pos);
            }

            let in_tx_pos = match &tx.in_msg {
                Some(_) if pos == 0 => None,
                Some(msg) => {
                    let msg_key = (msg.src.clone(), msg.dst.clone(), msg.created_lt);
                    let maybe_in_msg_pos = txs_by_out_msg.get(&msg_key);
                    match maybe_in_msg_pos {
                        Some(in_msg_pos) => Some(*in_msg_pos),
                        None => {
                            return Err(GrpcServiceError::NotFound(format!(
                                "in_tx not found for tx: {tx:?}, pos: {pos}"
                            )))
                        }
                    }
                }
                None => None,
            };

            let mut out_txs_pos = Vec::with_capacity(tx.out_msgs.len());
            for msg in tx.out_msgs.iter() {
                let msg_key = (msg.src.clone(), msg.dst.clone(), msg.created_lt);
                let maybe_out_msg_pos = txs_by_in_msg
                    .get_mut(&msg_key)
                    .and_then(VecDeque::pop_front);
                match maybe_out_msg_pos {
                    Some(out_msg_pos) => out_txs_pos.push(out_msg_pos),
                    None if !msg.dst.is_empty() && txs_by_out_msg.contains_key(&msg_key) => {
                        result.completed = false
                    } // At least one transaction not completed
                    None => {} // complete = true by default
                }
            }

            result.txs.push(TxTreeEntry {
                tx,
                in_tx_pos,
                out_txs_pos,
                emulated: false,
            });
        }
        Ok(result)
    }
}

pub async fn get_prev_txs(tx: &DBTx, db: &DB, depth: u32) -> Result<Vec<DBTx>, GrpcServiceError> {
    let mut prev_txs = Vec::new();
    let mut current_tx = tx;
    for _ in 0..depth {
        let Some(in_msg) = current_tx.in_msg.as_ref().filter(|msg| !msg.src.is_empty()) else {
            break;
        };
        let maybe_prev_tx_key = db
            .msg_txs()
            .get(in_msg.src.as_str(), in_msg.dst.as_str(), in_msg.created_lt)
            .await?
            .and_then(|msg_tx| msg_tx.src_tx_key());

        let prev_tx_key = match maybe_prev_tx_key {
            Some(key) => key,
            None => {
                return Err(GrpcServiceError::NotFound(format!(
                    "prev_tx_key not found for tx: {current_tx:?}"
                )))
            }
        };
        let prev_tx = db.tx().get_opt(&prev_tx_key).await?.ok_or_else(|| {
            GrpcServiceError::NotFound(format!("prev_tx not found for tx: {current_tx:?}"))
        })?;
        prev_txs.push(prev_tx);
        current_tx = prev_txs.last().ok_or_else(|| {
            GrpcServiceError::InternalError("Missing previous transaction after append".into())
        })?;
    }
    prev_txs.reverse();
    Ok(prev_txs)
}

pub async fn get_next_txs(
    tx: &DBTx,
    db: &DB,
    depth: u32,
    last_mc_seqno: u32,
    limits: TreeLimits,
) -> Result<Vec<DBTx>, GrpcServiceError> {
    let mut next_txs = Vec::new();
    let mut current_level = std::slice::from_ref(tx);
    for level in 0..depth {
        // Owned indices let async-trait prove that the buffered futures are Send.
        let messages = (0..current_level.len()).flat_map(|tx_pos| {
            (0..current_level[tx_pos].out_msgs.len()).map(move |msg_pos| (tx_pos, msg_pos))
        });
        let futs = messages.map(|(tx_pos, msg_pos)| async move {
            let tx = &current_level[tx_pos];
            let msg = &tx.out_msgs[msg_pos];
            let maybe_next_tx_key = db
                .msg_txs()
                .get(&msg.src, &msg.dst, msg.created_lt)
                .await?
                .and_then(|msg_tx| msg_tx.dst_tx_key());
            let Some(next_tx_key) = maybe_next_tx_key else {
                return Ok::<Option<DBTx>, anyhow::Error>(None);
            };
            let next_tx = db.tx().get_opt(&next_tx_key).await?.ok_or_else(|| {
                GrpcServiceError::NotFound(format!("next_tx not found for tx: {tx:?}"))
            })?;
            Ok((next_tx.mc_seqno <= last_mc_seqno).then_some(next_tx))
        });
        // Read one extra transaction to detect overflow without collecting the whole level.
        let next_level = stream::iter(futs)
            .buffered(limits.concurrency.get())
            .try_filter_map(|tx| futures::future::ready(Ok(tx)))
            .take(limits.max_width.saturating_add(1))
            .try_collect::<Vec<_>>()
            .await?;
        if next_level.is_empty() {
            break;
        }
        limits.check_depth(level + 1)?;
        limits.check_width(next_level.len())?;
        let level_start = next_txs.len();
        next_txs.extend(next_level);
        current_level = &next_txs[level_start..];
    }
    Ok(next_txs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_test::assert_ok;
    use tongrid_db::entity::scylla_types::DBMsg;
    use tongrid_db::entity::DBTx;
    use tongrid_test_utils::ton::make_db_ton_hash;

    fn make_message(src: &str, dst: &str, lt: i64) -> DBMsg {
        DBMsg {
            src: src.to_string(),
            dst: dst.to_string(),
            created_lt: lt as u64,
            ..DBMsg::default()
        }
    }

    fn make_tx(id: u8, in_msg: Option<DBMsg>, out_msgs: &[DBMsg]) -> DBTx {
        DBTx {
            tx_hash: make_db_ton_hash(&[id; 32]),
            in_msg,
            out_msgs: out_msgs.to_vec(),
            tx_utime: id as u32,
            ..Default::default()
        }
    }

    #[test]
    fn test_tx_tree() -> Result<()> {
        // txs are linked by msgs
        // broadly speaking, tx1 -> tx2 <-> tx1.out_msg == tx2.in_msg
        // let's build simple tree with 5 txs
        // 0->1, 1->2
        //       1->3, 3->4

        let txs = vec![
            make_tx(0, None, &[make_message("0", "1", 0)]),
            make_tx(
                1,
                Some(make_message("0", "1", 0)),
                &[make_message("1", "2", 1), make_message("1", "3", 1)],
            ),
            make_tx(2, Some(make_message("1", "2", 1)), &[]),
            make_tx(
                3,
                Some(make_message("1", "3", 1)),
                &[make_message("3", "4", 3)],
            ),
            make_tx(4, Some(make_message("3", "4", 3)), &[]),
        ];

        // full tree
        for req_tx in 0..5 {
            let req_tx_info = RequestedTxInfo {
                ton_hash: txs[req_tx].tx_hash,
                lt: txs[req_tx].tx_lt,
            };
            let tree = assert_ok!(TxTree::from_txs(txs.clone(), &req_tx_info));
            assert_eq!(tree.requested_tx_pos, req_tx);
            assert_eq!(tree.root_tx_pos, 0);
            assert_eq!(tree.initial_tx_pos, Some(0));
            assert_eq!(tree.txs.len(), 5);
            assert!(tree.completed);
            assert_eq!(tree.txs[0].in_tx_pos, None);
            assert_eq!(tree.txs[0].out_txs_pos, vec![1]);
            assert_eq!(tree.txs[1].in_tx_pos, Some(0));
            assert_eq!(tree.txs[1].out_txs_pos, vec![2, 3]);
            assert_eq!(tree.txs[2].in_tx_pos, Some(1));
            assert_eq!(tree.txs[2].out_txs_pos, Vec::<usize>::new());
            assert_eq!(tree.txs[3].in_tx_pos, Some(1));
            assert_eq!(tree.txs[3].out_txs_pos, vec![4]);
            assert_eq!(tree.txs[4].in_tx_pos, Some(3));
            assert_eq!(tree.txs[4].out_txs_pos, Vec::<usize>::new());
        }

        // no initial_tx
        for req_tx in 1..5 {
            let req_tx_info = RequestedTxInfo {
                ton_hash: txs[req_tx].tx_hash,
                lt: txs[req_tx].tx_lt,
            };
            let tree = assert_ok!(TxTree::from_txs(Vec::from(&txs[1..5]), &req_tx_info));
            assert_eq!(tree.requested_tx_pos, req_tx - 1);
            assert_eq!(tree.root_tx_pos, 0);
            assert_eq!(tree.initial_tx_pos, None);
            assert_eq!(tree.txs.len(), 4);
            assert!(tree.completed);
        }

        // no last tx
        let req_tx_info = RequestedTxInfo {
            ton_hash: txs[3].tx_hash,
            lt: txs[3].tx_lt,
        };
        let tree = assert_ok!(TxTree::from_txs(Vec::from(&txs[0..4]), &req_tx_info));
        assert_eq!(tree.requested_tx_pos, 3);
        assert_eq!(tree.root_tx_pos, 0);
        assert_eq!(tree.initial_tx_pos, Some(0));
        assert_eq!(tree.txs.len(), 4);
        assert!(!tree.completed);

        Ok(())
    }

    #[test]
    fn set_completion_if_last_msg_is_ext_out() -> Result<()> {
        // Our tree:
        // 0 -> 1 -> 2 -> External
        // Must be completed

        let txs = vec![
            make_tx(0, None, &[make_message("0", "1", 0)]),
            make_tx(
                1,
                Some(make_message("0", "1", 0)),
                &[make_message("1", "2", 1)],
            ),
            make_tx(
                2,
                Some(make_message("1", "2", 1)),
                &[make_message("2", "", 2)],
            ),
        ];

        // full tree
        for req_tx in 0..3 {
            let req_tx_info = RequestedTxInfo {
                ton_hash: txs[req_tx].tx_hash,
                lt: txs[req_tx].tx_lt,
            };
            let tree = assert_ok!(TxTree::from_txs(txs.clone(), &req_tx_info));
            assert_eq!(tree.requested_tx_pos, req_tx);
            assert_eq!(tree.root_tx_pos, 0);
            assert_eq!(tree.initial_tx_pos, Some(0));
            assert_eq!(tree.txs.len(), 3);
            assert!(tree.completed);
        }

        // no last tx -> Must be incompleted
        let req_tx_info = RequestedTxInfo {
            ton_hash: txs[1].tx_hash,
            lt: txs[1].tx_lt,
        };
        let tree = assert_ok!(TxTree::from_txs(Vec::from(&txs[0..2]), &req_tx_info));
        assert_eq!(tree.requested_tx_pos, 1);
        assert_eq!(tree.root_tx_pos, 0);
        assert_eq!(tree.initial_tx_pos, Some(0));
        assert_eq!(tree.txs.len(), 2);
        assert!(!tree.completed);

        Ok(())
    }

    #[test]
    fn set_completion_if_last_msg_is_ext_out_hard() -> Result<()> {
        // Our tree:
        // 0 -> 1 -> 2 -> External
        //      1 -> 3 -> ?4
        // Must be incompleted

        let txs = vec![
            make_tx(0, None, &[make_message("0", "1", 0)]),
            make_tx(
                1,
                Some(make_message("0", "1", 0)),
                &[make_message("1", "2", 1), make_message("1", "3", 1)],
            ),
            make_tx(
                2,
                Some(make_message("1", "2", 1)),
                &[make_message("2", "", 2)],
            ),
            make_tx(
                3,
                Some(make_message("1", "3", 1)),
                &[make_message("3", "4", 2)],
            ),
        ];

        // full tree Must be incompleted
        for req_tx in 0..4 {
            let req_tx_info = RequestedTxInfo {
                ton_hash: txs[req_tx].tx_hash,
                lt: txs[req_tx].tx_lt,
            };
            let tree = assert_ok!(TxTree::from_txs(txs.clone(), &req_tx_info));
            assert_eq!(tree.requested_tx_pos, req_tx);
            assert_eq!(tree.root_tx_pos, 0);
            assert_eq!(tree.initial_tx_pos, Some(0));
            assert_eq!(tree.txs.len(), 4);
            assert!(!tree.completed);
        }

        Ok(())
    }
}
