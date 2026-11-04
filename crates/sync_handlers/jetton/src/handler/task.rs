use bincode::{Decode, Encode};
use stonfi_distributed_sync::stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};
use stonfi_distributed_sync::traits::TaskPayload;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct JettonSyncTask {
    pub mc_seqno: u32,
    pub address: String,
    pub tx_info: Vec<JettonSyncTxInfo>,
}

impl TaskPayload for JettonSyncTask {
    fn encode(&self) -> SyncCoreResult<Vec<u8>> {
        bincode::encode_to_vec(self, bincode::config::standard()).map_err(SyncCoreError::custom)
    }

    fn decode(data: &[u8]) -> SyncCoreResult<Self> {
        let (task, consumed) = bincode::decode_from_slice(data, bincode::config::standard())
            .map_err(SyncCoreError::invalid_args)?;
        if consumed != data.len() {
            return Err(SyncCoreError::invalid_args(
                "jetton task payload contains trailing bytes",
            ));
        }
        Ok(task)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct JettonSyncTxInfo {
    pub tx_lt: u64,
    pub tx_utime: u32,
}
