use crate::entity::account::db_account_lite::DBAccountLite;
use crate::entity::account::traits::AccountEntity;
use crate::entity::scylla_types::DBSimpleLib;
use crate::entity::tx::tx_key::DBTxKey;
use const_format::formatcp;
use std::collections::HashSet;
use std::sync::Arc;
use ton::ton_core::cell::TonHash;

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub struct DBAccount {
    pub mc_seqno: u32,
    pub address: String,
    pub tx_lt: u64,
    pub tx_hash: TonHash,
    pub is_valid: bool,
    pub code_hash: Option<TonHash>,
    pub last_code_hash: Option<TonHash>,
    pub simple_libs: Vec<DBSimpleLib>,
    pub data_lib_ids: Vec<TonHash>,
    pub boc_stripped: Arc<Vec<u8>>, // shared with TonCell when parsing
}

impl AccountEntity for DBAccount {
    const FIELDS: &'static str =
        formatcp!("{f}, data_lib_ids, boc_stripped", f = DBAccountLite::FIELDS);
    const FIELDS_CNT: usize = DBAccountLite::FIELDS_CNT + 2;
}

impl DBAccount {
    pub fn tx_key(&self) -> DBTxKey {
        DBTxKey {
            mc_seqno: self.mc_seqno,
            address: self.address.clone(),
            tx_lt: self.tx_lt,
        }
    }

    pub fn tvm_code_hashes(&self) -> HashSet<TonHash> {
        let mut tvm_code_hashes = HashSet::new();
        if let Some(code_hash) = &self.code_hash {
            tvm_code_hashes.insert(*code_hash);
        }
        for simple_lib in &self.simple_libs {
            tvm_code_hashes.insert(simple_lib.code_hash);
        }
        for lib_id in &self.data_lib_ids {
            tvm_code_hashes.insert(*lib_id);
        }
        tvm_code_hashes
    }
}
