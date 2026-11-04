use crate::backup_ser_de::ProtoSerial;
use crate::proto::v1::DbBlockProto;
use std::sync::Arc;
use ton::ton_core::cell::TonHash;
use tongrid_db::entity::block::db_block::DBBlock;

impl ProtoSerial for DBBlock {
    type ProtoType = DbBlockProto;

    fn into_proto(self) -> Self::ProtoType {
        Self::ProtoType {
            master_seqno: self.mc_seqno as i32,
            workchain: self.workchain,
            shard: self.shard,
            seqno: self.seqno as i32,
            root_hash: self.root_hash.into_vec(),
            file_hash: self.file_hash.into_vec(),
            boc_stripped: self.boc_stripped.to_vec(),
            end_lt: self.end_lt as i64,
            key_block_seqno: self.key_block_seqno as i32,
            utime: self.utime as i64,
            rand_seed: self.rand_seed.into_vec(),
        }
    }

    fn from_proto(proto: Self::ProtoType) -> anyhow::Result<Self> {
        Ok(Self {
            mc_seqno: proto.master_seqno as u32,
            workchain: proto.workchain,
            shard: proto.shard,
            seqno: proto.seqno as u32,
            root_hash: TonHash::from_vec(proto.root_hash)?,
            file_hash: TonHash::from_vec(proto.file_hash)?,
            end_lt: proto.end_lt as u64,
            key_block_seqno: proto.key_block_seqno as u32,
            utime: proto.utime as u32,
            rand_seed: TonHash::from_vec(proto.rand_seed)?,
            boc_stripped: Arc::new(proto.boc_stripped),
        })
    }
}
