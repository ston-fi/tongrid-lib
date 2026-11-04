use std::sync::Arc;
use ton::ton_core::traits::tlb::TLB;
use tongrid_db::entity::block::db_block::DBBlock;
use tongrid_ton::tg_ton::block_stream::TongridBlock;

pub fn block_serdb(master_seqno: u32, tongrid_block: &TongridBlock) -> anyhow::Result<DBBlock> {
    let block_shard = DBBlock {
        mc_seqno: master_seqno,
        workchain: tongrid_block.info.shard.workchain,
        shard: tongrid_block.info.shard.shard as i64,
        seqno: tongrid_block.info.seqno,
        root_hash: tongrid_block.block_id.root_hash,
        file_hash: tongrid_block.block_id.file_hash,
        end_lt: tongrid_block.info.end_lt,
        key_block_seqno: tongrid_block.info.prev_key_block_seqno,
        utime: tongrid_block.info.gen_utime,
        rand_seed: tongrid_block.block_stripped.extra.rand_seed,
        boc_stripped: Arc::new(tongrid_block.block_stripped.to_boc()?),
    };
    Ok(block_shard)
}
