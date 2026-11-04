use anyhow::{anyhow, bail};
use async_trait::async_trait;
use futures_util::future::try_join_all;
use std::collections::HashMap;
use std::ops::Deref;
use ton::block_tlb::Block;
use ton::ton_core::traits::tlb::TLB;
use tongrid_db::entity::block::block_shard_key::DBBlockKey;
use tongrid_tda::ton_db_adapter::TonDBAdapter;
use tongrid_ton::tg_ton::block_stream::{BlockStream, BlockStreamItem, TongridBlock};

pub struct ScyllaBlockStream {
    tda: TonDBAdapter,
    prev_mc_seqno: u32,
    to_mc_seqno: u32,
}
impl ScyllaBlockStream {
    pub fn new(tda: TonDBAdapter, from_mc_seqno: u32, to_mc_seqno: u32) -> anyhow::Result<Self> {
        if from_mc_seqno == 0 {
            bail!("from_mc_seqno is 0");
        }
        log::debug!("Creating ScyllaBlockStream on [{from_mc_seqno}, {to_mc_seqno}]");
        Ok(Self {
            tda,
            to_mc_seqno,
            prev_mc_seqno: from_mc_seqno - 1,
        })
    }
}

#[async_trait]
impl BlockStream for ScyllaBlockStream {
    async fn next(&mut self) -> anyhow::Result<Option<BlockStreamItem>> {
        if self.prev_mc_seqno >= self.to_mc_seqno {
            return Ok(None);
        }

        let cur_seqno = self.prev_mc_seqno + 1;

        let blocks = self
            .tda
            .db()
            .block()
            .get_by_mc_seqno(cur_seqno)
            .await?
            .deref()
            .clone();
        if blocks.is_empty() {
            bail!("[ScyllaBlockStream] No blocks found for mc_seqno={cur_seqno}");
        };

        let tongrid_blocks: Vec<TongridBlock> = blocks
            .into_iter()
            .map(|x| {
                let id = x.block_id();
                let block = Block::from_boc(x.boc_stripped)?;
                TongridBlock::new(block, id)
            })
            .collect::<anyhow::Result<_>>()?;

        let mut block_by_block_key = HashMap::new();
        for block in tongrid_blocks {
            let key = DBBlockKey {
                mc_seqno: cur_seqno,
                workchain: block.info.shard.workchain,
                shard: block.info.shard.shard as i64,
                seqno: block.info.seqno,
            };
            block_by_block_key.insert(key, block);
        }

        let txs = self.tda.db().tx().get_by_mc_seqno(cur_seqno).await?;
        let mut ton_txs_with_block_key = vec![];
        for tx in txs {
            let tda_clone = self.tda.clone();
            let result = async move {
                let ton_tx = tda_clone.tx().build_ton_tx(&tx).await?;
                Ok::<_, anyhow::Error>((tx.block_key(), ton_tx.0))
            };
            ton_txs_with_block_key.push(result);
        }
        let ton_txs_with_block_key = try_join_all(ton_txs_with_block_key).await?;

        for key_and_tx in ton_txs_with_block_key {
            block_by_block_key
                .get_mut(&key_and_tx.0)
                .ok_or_else(|| anyhow!("No block for a transaction, something went wrong..."))?
                .ton_txs
                .push(key_and_tx.1);
        }

        let mut tongrid_blocks: Vec<TongridBlock> = block_by_block_key.into_values().collect();

        let mc_block_index = tongrid_blocks
            .iter()
            .position(|block| block.block_id.shard_ident.workchain == -1)
            .ok_or_else(|| anyhow!("No mc block found for range [{cur_seqno}:{cur_seqno}]"))?;
        let mc_block = tongrid_blocks.swap_remove(mc_block_index);

        self.prev_mc_seqno = cur_seqno;

        Ok(Some(BlockStreamItem {
            mc_block,
            shard_blocks: tongrid_blocks,
        }))
    }
}
