use bincode::{Decode, Encode};
use stonfi_distributed_sync::stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};
use stonfi_distributed_sync::traits::TaskPayload;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct BackupSyncTask {
    pub mc_seqno_from: u32,
    pub mc_seqno_to: u32,
    pub state_id: String,
}

impl TaskPayload for BackupSyncTask {
    fn encode(&self) -> SyncCoreResult<Vec<u8>> {
        bincode::encode_to_vec(self, bincode::config::standard()).map_err(SyncCoreError::custom)
    }

    fn decode(data: &[u8]) -> SyncCoreResult<Self> {
        let (task, consumed) = bincode::decode_from_slice(data, bincode::config::standard())
            .map_err(SyncCoreError::invalid_args)?;
        if consumed != data.len() {
            return Err(SyncCoreError::invalid_args(
                "backup task payload contains trailing bytes",
            ));
        }
        Ok(task)
    }
}
