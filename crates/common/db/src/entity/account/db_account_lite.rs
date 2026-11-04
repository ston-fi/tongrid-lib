use crate::entity::account::traits::AccountEntity;
use crate::entity::scylla_types::DBSimpleLib;
use crate::entity::tx::tx_key::DBTxKey;
use std::hash::Hash;
use ton::ton_core::cell::TonHash;

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub struct DBAccountLite {
    pub mc_seqno: u32,
    pub address: String,
    pub tx_lt: u64,
    pub tx_hash: TonHash,
    pub is_valid: bool,
    pub code_hash: Option<TonHash>,
    pub last_code_hash: Option<TonHash>,
    pub simple_libs: Vec<DBSimpleLib>,
}

impl DBAccountLite {
    pub fn tx_key(&self) -> DBTxKey {
        DBTxKey {
            mc_seqno: self.mc_seqno,
            address: self.address.clone(),
            tx_lt: self.tx_lt,
        }
    }
}

impl AccountEntity for DBAccountLite {
    const FIELDS: &'static str =
        "mc_seqno, address, tx_lt, tx_hash, is_valid, code_hash, last_code_hash, simple_libs";
    const FIELDS_CNT: usize = 8;
}
