use crate::entity::scylla_types::DBMsg;
use crate::entity::DBTxKey;
/// Links a message to its source and destination transactions as each side is synchronized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MsgTxs {
    pub src: String,
    pub dst: String,
    pub created_lt: u64,
    pub src_mc_seqno: Option<u32>,
    pub src_tx_lt: Option<u64>,
    pub dst_mc_seqno: Option<u32>,
    pub dst_tx_lt: Option<u64>,
}

impl MsgTxs {
    pub(super) const FIELDS: &'static str =
        "src, dst, created_lt, src_mc_seqno, src_tx_lt, dst_mc_seqno, dst_tx_lt";

    pub fn new_in_msg(msg: &DBMsg, tx_key: DBTxKey) -> Self {
        Self {
            src: msg.src.clone(),
            dst: msg.dst.clone(),
            created_lt: msg.created_lt,
            src_mc_seqno: None,
            src_tx_lt: None,
            dst_mc_seqno: Some(tx_key.mc_seqno),
            dst_tx_lt: Some(tx_key.tx_lt),
        }
    }

    pub fn new_out_msg(msg: &DBMsg, tx_key: DBTxKey) -> Self {
        Self {
            src: msg.src.clone(),
            dst: msg.dst.clone(),
            created_lt: msg.created_lt,
            src_mc_seqno: Some(tx_key.mc_seqno),
            src_tx_lt: Some(tx_key.tx_lt),
            dst_mc_seqno: None,
            dst_tx_lt: None,
        }
    }

    pub fn src_tx_key(&self) -> Option<DBTxKey> {
        if let (Some(mc_seqno), Some(tx_lt)) = (self.src_mc_seqno, self.src_tx_lt) {
            Some(DBTxKey::new(mc_seqno, &self.src, tx_lt))
        } else {
            None
        }
    }

    pub fn dst_tx_key(&self) -> Option<DBTxKey> {
        if let (Some(mc_seqno), Some(tx_lt)) = (self.dst_mc_seqno, self.dst_tx_lt) {
            Some(DBTxKey::new(mc_seqno, &self.dst, tx_lt))
        } else {
            None
        }
    }
}
