use crate::entity::DBTxKey;

/// Index entry linking an outgoing message's query ID to its transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutMsgQueryId {
    pub address: String,
    pub query_id: i64, // query_id is u64 in TON, but scylla does not support unsigned types
    pub tx_utime: u32,
    pub tx_lt: u64,
    pub mc_seqno: u32,
}

impl OutMsgQueryId {
    pub const FIELDS: &'static str = "address, query_id, tx_utime, tx_lt, mc_seqno";
    pub fn tx_key(&self) -> DBTxKey {
        DBTxKey::new(self.mc_seqno, self.address.clone(), self.tx_lt)
    }
}
