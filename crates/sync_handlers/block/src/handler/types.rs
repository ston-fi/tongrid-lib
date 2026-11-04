use std::collections::HashSet;
use tongrid_db::entity::block::db_block::DBBlock as DBBlockShard;
use tongrid_db::entity::DBTx as DBTransaction;
use tongrid_db::entity::TvmCode;
use tongrid_tda::ser_de::block::block_serdb;
use tongrid_tda::ser_de::tx::tx_serdb;
use tongrid_ton::tg_ton::block_stream::TongridBlock;

#[derive(Default)]
pub struct BlocksDBData {
    pub blocks: Vec<DBBlockShard>,
    pub txs: Vec<DBTransaction>,
    pub tvm_codes: HashSet<TvmCode>,
}

impl BlocksDBData {
    pub fn from_block(tongrid_block: &TongridBlock, master_seqno: u32) -> anyhow::Result<Self> {
        let mut blocks_db_data = BlocksDBData {
            blocks: Vec::from([block_serdb(master_seqno, tongrid_block)?]),
            ..Default::default()
        };

        let shard_key = blocks_db_data.blocks[0].get_key();
        for tx in &tongrid_block.ton_txs {
            let (db_tx, db_tvm_codes) = tx_serdb(&shard_key, tx)?;
            blocks_db_data.txs.push(db_tx);
            blocks_db_data.tvm_codes.extend(db_tvm_codes);
        }

        blocks_db_data.txs.sort_by_key(|tx| tx.tx_lt);
        Ok(blocks_db_data)
    }
}
