use crate::entity::block::block_shard_key::DBBlockKey;
use crate::entity::tx::{traits::TxEntity, tx_key::DBTxKey};
use ton::ton_core::cell::TonHash;

/// Transaction metadata without message payloads or the transaction BOC.
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct DBTxLite {
    pub mc_seqno: u32,
    pub address: String,
    pub tx_lt: u64,
    pub tx_utime: u32,
    pub tx_hash: TonHash,
    pub initial_tx: Option<DBTxKey>,
    pub block_workchain: i32,
    pub block_shard: i64,
    pub block_seqno: u32,
}

impl TxEntity for DBTxLite {
    const FIELDS: &'static str =
        "mc_seqno, address, tx_lt, tx_utime, tx_hash, initial_tx, block_workchain, block_shard, block_seqno";
}

impl DBTxLite {
    pub const FIELDS_CNT: usize = 9;

    pub fn key(&self) -> DBTxKey {
        DBTxKey {
            mc_seqno: self.mc_seqno,
            address: self.address.clone(),
            tx_lt: self.tx_lt,
        }
    }

    pub fn get_shard_key(&self) -> DBBlockKey {
        DBBlockKey {
            mc_seqno: self.mc_seqno,
            workchain: self.block_workchain,
            shard: self.block_shard,
            seqno: self.block_seqno,
        }
    }
}
