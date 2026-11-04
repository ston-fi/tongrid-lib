use std::sync::Arc;

use crate::proto_serializer::{
    account_serializer::AccountSerial, jetton_serializer::JettonBalanceSerializer,
};
use tongrid_db::entity::block::db_block::DBBlock;
use tongrid_mem_store::types::sync_status::SyncStatus;
use tongrid_tda::ton_db_adapter::TonDBAdapter;

use crate::proto_serializer::tx_serializer::TxSerializer;

mod account_serializer;
mod jetton_serializer;
mod tx_serializer;

pub struct ProtoSerializer {
    tda: TonDBAdapter,
}

impl ProtoSerializer {
    pub fn new(tda: TonDBAdapter) -> Self {
        Self { tda }
    }
    pub fn account(&self) -> AccountSerial {
        AccountSerial
    }
    pub fn tx(&self) -> TxSerializer<'_> {
        TxSerializer::new(self)
    }

    pub fn jetton_balance(&self) -> JettonBalanceSerializer {
        JettonBalanceSerializer::new(self)
    }

    pub fn block_to_proto(
        &self,
        block_shard: DBBlock,
        boc_required: bool,
    ) -> tongrid_proto::v3::BlockShard {
        tongrid_proto::v3::BlockShard {
            master_seqno: block_shard.mc_seqno,
            workchain: block_shard.workchain,
            shard: block_shard.shard as u64,
            seqno: block_shard.seqno,
            root_hash: block_shard.root_hash.into_vec(),
            file_hash: block_shard.file_hash.into_vec(),
            end_lt: block_shard.end_lt,
            key_block_seqno: block_shard.key_block_seqno,
            utime: block_shard.utime,
            boc: boc_required.then(|| Arc::unwrap_or_clone(block_shard.boc_stripped)),
        }
    }

    pub fn sync_status_to_proto(
        &self,
        sync_status: &SyncStatus,
    ) -> Option<tongrid_proto::v3::SyncStatus> {
        Some(tongrid_proto::v3::SyncStatus {
            initial_seqno: sync_status.initial_seqno as i32,
            full_synced_seqno: sync_status.full_synced_seqno,
            full_synced_seqno_utime: sync_status.full_synced_seqno_utime,
            last_seen_seqno: sync_status.last_seen_seqno,
            last_seen_seqno_utime: sync_status.last_seen_seqno_utime,
            sync_utime: sync_status.sync_utime,
        })
    }
}
