use crate::entity::block::block_shard_key::DBBlockKey;
use std::sync::Arc;
use ton::block_tlb::{BlockIdExt, ShardIdent};
use ton::ton_core::cell::TonHash;

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub struct DBBlock {
    pub mc_seqno: u32,
    pub workchain: i32,
    pub shard: i64,
    pub seqno: u32,
    pub root_hash: TonHash,
    pub file_hash: TonHash,
    pub boc_stripped: Arc<Vec<u8>>, // shared with TonCell when parsing
    pub end_lt: u64, // can't be used to determinate last address tx by lt at specified mc_seqno
    pub key_block_seqno: u32,
    pub utime: u32,
    pub rand_seed: TonHash,
}

impl DBBlock {
    pub const FIELDS: &'static str =
        "mc_seqno, workchain, shard, seqno, root_hash, file_hash, boc_stripped, end_lt, key_block_seqno, utime, rand_seed";

    pub fn get_key(&self) -> DBBlockKey {
        DBBlockKey {
            mc_seqno: self.mc_seqno,
            workchain: self.workchain,
            shard: self.shard,
            seqno: self.seqno,
        }
    }

    pub fn block_id(&self) -> BlockIdExt {
        BlockIdExt {
            shard_ident: ShardIdent::new(self.workchain, self.shard as u64),
            seqno: self.seqno,
            root_hash: self.root_hash,
            file_hash: self.file_hash,
        }
    }
}
