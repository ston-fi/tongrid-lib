use bincode::{Decode, Encode};
use stonfi_distributed_sync::stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};
use stonfi_distributed_sync::traits::TaskPayload;
use tongrid_db::entity::DBTxKey;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct AccountSyncTask {
    pub mc_seqno: u32,
    pub tx_keys: Vec<AccountTxKey>,
    pub sync_all: bool,
    pub sync_type: String,
}

impl AccountSyncTask {
    pub fn new(mc_seqno: u32, tx_keys: Vec<AccountTxKey>, sync_type: String) -> Self {
        Self {
            mc_seqno,
            tx_keys,
            sync_all: false,
            sync_type,
        }
    }

    pub fn all_shards(mc_seqno: u32, sync_type: String) -> Self {
        Self {
            mc_seqno,
            tx_keys: vec![],
            sync_all: true,
            sync_type,
        }
    }
}

impl TaskPayload for AccountSyncTask {
    fn encode(&self) -> SyncCoreResult<Vec<u8>> {
        bincode::encode_to_vec(self, bincode::config::standard()).map_err(SyncCoreError::custom)
    }

    fn decode(data: &[u8]) -> SyncCoreResult<Self> {
        let (task, consumed) = bincode::decode_from_slice(data, bincode::config::standard())
            .map_err(SyncCoreError::invalid_args)?;
        if consumed != data.len() {
            return Err(SyncCoreError::invalid_args(
                "account task payload contains trailing bytes",
            ));
        }
        Ok(task)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct AccountTxKey {
    pub mc_seqno: u32,
    pub address: String,
    pub tx_lt: u64,
}

impl From<DBTxKey> for AccountTxKey {
    fn from(value: DBTxKey) -> Self {
        Self {
            mc_seqno: value.mc_seqno,
            address: value.address,
            tx_lt: value.tx_lt,
        }
    }
}

impl From<AccountTxKey> for DBTxKey {
    fn from(value: AccountTxKey) -> Self {
        Self {
            mc_seqno: value.mc_seqno,
            address: value.address,
            tx_lt: value.tx_lt,
        }
    }
}
