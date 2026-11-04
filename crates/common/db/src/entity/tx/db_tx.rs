use crate::entity::block::block_shard_key::DBBlockKey;
use crate::entity::scylla_types::DBMsg;
use crate::entity::tx::traits::TxEntity;
use crate::entity::tx::tx_key::DBTxKey;
use std::collections::HashSet;
use std::sync::Arc;
use ton::ton_core::cell::TonHash;

/// Stored transaction with messages separated from its stripped transaction BOC.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct DBTx {
    pub mc_seqno: u32,
    pub address: String,
    pub tx_lt: u64,
    pub tx_utime: u32,
    pub tx_hash: TonHash,
    pub initial_tx: DBTxKey,
    pub block_workchain: i32,
    pub block_shard: i64,
    pub block_seqno: u32,
    pub boc_stripped: Arc<Vec<u8>>, // shared with TonCell when parsing
    pub in_msg: Option<DBMsg>,
    pub out_msgs: Vec<DBMsg>,
    pub in_msg_hash: Option<TonHash>,
    pub in_msg_hash_normalized: Option<TonHash>,
}

impl TxEntity for DBTx {
    const FIELDS: &'static str = "mc_seqno, address, tx_lt, tx_utime, tx_hash, initial_tx, block_workchain, block_shard, block_seqno, boc_stripped, in_msg, out_msgs, in_msg_hash, in_msg_hash_normalized";
}

impl DBTx {
    pub const FIELDS_CNT: usize = 14;

    pub fn tx_key(&self) -> DBTxKey {
        DBTxKey {
            mc_seqno: self.mc_seqno,
            address: self.address.clone(),
            tx_lt: self.tx_lt,
        }
    }

    pub fn block_key(&self) -> DBBlockKey {
        DBBlockKey {
            mc_seqno: self.mc_seqno,
            workchain: self.block_workchain,
            shard: self.block_shard,
            seqno: self.block_seqno,
        }
    }

    pub fn tvm_code_hashes(&self) -> HashSet<TonHash> {
        let mut code_hashes = HashSet::new();
        if let Some(in_msg) = &self.in_msg {
            if let Some(code_hash) = in_msg.code_hash.as_ref() {
                code_hashes.insert(*code_hash);
            }
            for lib in &in_msg.simple_libs {
                code_hashes.insert(lib.code_hash);
            }
        }
        for out_msg in &self.out_msgs {
            if let Some(code_hash) = out_msg.code_hash.as_ref() {
                code_hashes.insert(*code_hash);
            }
            for lib in &out_msg.simple_libs {
                code_hashes.insert(lib.code_hash);
            }
        }
        code_hashes
    }
}
