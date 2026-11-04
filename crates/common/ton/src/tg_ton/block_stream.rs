use anyhow::anyhow;
use anyhow::Error;
use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::future::try_join_all;
use itertools::Itertools;
use std::collections::HashSet;
use std::ops::Deref;
use ton::block_tlb::Block;
use ton::block_tlb::Tx;
use ton::block_tlb::{BlockIdExt, BlockInfo};
use ton::lite_client::LiteClient;
use ton::ton_core::traits::tlb::TLB;
use ton_block::Block as LegacyBlock;
use ton_block::{
    Deserializable, HashmapAugType, InMsgDescr, OutMsgDescr, Serializable, ShardAccountBlocks,
};

#[derive(Clone)]
pub struct TONBlockStream {
    lite_client: LiteClient,
    to_mc_seqno: u32,
    prev_mc_seqno: u32,
    prev_last_shards: HashSet<BlockIdExt>, // the latest workchain shards for prev_mc_seqno
}

#[async_trait]
impl BlockStream for TONBlockStream {
    async fn next(&mut self) -> anyhow::Result<Option<BlockStreamItem>> {
        if self.prev_mc_seqno >= self.to_mc_seqno {
            return Ok(None);
        }
        let seeking_mc_seqno = self.prev_mc_seqno + 1;

        log::debug!("Getting block with mc_seqno: {seeking_mc_seqno}");
        let mc_id_ext = self.lite_client.lookup_mc_block(seeking_mc_seqno).await?;
        let mc_block = self
            .lite_client
            .get_block(mc_id_ext.clone(), None)
            .await?
            .data;
        let mc_block_unpacked = TongridBlock::new(mc_block, mc_id_ext.clone())?;

        let shard_ids = extract_shards_from_master_block(&mc_block_unpacked.block_stripped).await?;
        log::debug!(
            "Got masterchain block: {:?} with shards: {:?}",
            mc_block_unpacked.block_stripped,
            shard_ids
        );

        let unseen_shards = self
            .get_unseen_prev_blocks(&shard_ids)
            .await?
            .into_iter()
            .unique_by(|(id, _)| id.clone())
            .map(|(_, block)| block)
            .collect();

        let result = Some(BlockStreamItem {
            mc_block: mc_block_unpacked,
            shard_blocks: unseen_shards,
        });

        // update it at the end to maintain exception-safety
        self.prev_mc_seqno = seeking_mc_seqno;
        self.prev_last_shards = shard_ids.into_iter().collect();

        Ok(result)
    }
}

impl TONBlockStream {
    /// [from_mc_seqno, to_mc_seqno] - including borders
    /// if "to_mc_seqno" is None, then stream will be infinite
    /// Returns an error if "from_mc_seqno" is zero.
    pub async fn new(
        lite_client: LiteClient,
        from_mc_seqno: u32,
        to_mc_seqno: Option<u32>,
    ) -> anyhow::Result<Self> {
        log::info!(
            "Creating BlockStream for range [{}, {}]",
            from_mc_seqno,
            to_mc_seqno
                .as_ref()
                .map(|x| x.to_string())
                .unwrap_or("+inf".to_string())
        );
        let prev_mc_seqno = from_mc_seqno
            .checked_sub(1)
            .ok_or_else(|| anyhow!("from_mc_seqno must be greater than or equal to 1"))?;
        let prev_last_shards = match prev_mc_seqno > 0 {
            true => {
                let block_id = lite_client.lookup_mc_block(prev_mc_seqno).await?;
                let mc_block = lite_client.get_block(block_id, None).await?.data;
                extract_shards_from_master_block(&mc_block).await?
            }
            false => vec![],
        };

        Ok(Self {
            lite_client,
            to_mc_seqno: to_mc_seqno.unwrap_or(i32::MAX as u32),
            prev_mc_seqno,
            prev_last_shards: prev_last_shards.into_iter().collect(),
        })
    }

    /// in case of error, can be called again safely
    /// return Ok(None) if stream is over by "to_mc_seqno"
    /// Blocks in stream are sequential by mc_seqno
    #[async_recursion]
    async fn get_unseen_prev_blocks(
        &self,
        shard_block_ids: &[BlockIdExt],
    ) -> anyhow::Result<Vec<(BlockIdExt, TongridBlock)>> {
        let blocks_fut = shard_block_ids.iter().map(|block_id| async {
            let block_id = block_id.clone(); // copy to scope

            // blockchain feels bad about seq_no == 0:
            // got unexpected response Error(Error { code: 651, message: String(not in db) })
            // So technically block exists, but we can't get it
            if self.prev_last_shards.contains(&block_id) || block_id.seqno == 0 {
                return Ok::<Vec<(BlockIdExt, TongridBlock)>, Error>(vec![]);
            }
            // get cur block
            let cur_block = self
                .lite_client
                .get_block(block_id.clone(), None)
                .await?
                .data;
            let tongrid_block = TongridBlock::new(cur_block, block_id.clone())?;

            // recursively get previous block
            let prev_ids = tongrid_block.info.prev_block_ids()?;
            let mut prev_blocks = self.get_unseen_prev_blocks(&prev_ids).await?;
            prev_blocks.push((block_id, tongrid_block));

            Ok(prev_blocks)
        });

        // maybe worth check size before/after unique to confirm we don't make unnecessary lite calls
        let blocks = try_join_all(blocks_fut)
            .await?
            .into_iter()
            .flatten()
            .collect();
        Ok(blocks)
    }
}

async fn extract_shards_from_master_block(mc_block: &Block) -> anyhow::Result<Vec<BlockIdExt>> {
    let mc_extra = mc_block
        .extra
        .mc_block_extra
        .as_ref()
        .ok_or_else(|| anyhow!("Masterchain block has no mc_block_extra"))?;
    Ok(mc_extra.shard_ids())
}

#[derive(Clone, Debug, PartialEq)]
pub struct TongridBlock {
    pub block_stripped: Block, // Already stripped_block, because all other necessary information in other fields
    pub info: BlockInfo, // It is here, because it will be used frequently and parsing it can fall, so we want it to fall in the constructor
    pub block_id: BlockIdExt,
    pub ton_txs: Vec<Tx>,
}

//From Boc_Stripped  from scylla test
impl TongridBlock {
    pub fn new(mut block: Block, block_id: BlockIdExt) -> anyhow::Result<Self> {
        let mut legacy_block = LegacyBlock::construct_from_bytes(&block.to_boc()?)?;
        let mut legacy_extra = legacy_block.read_extra()?;
        let shard_accounts = legacy_extra.read_account_blocks()?;
        let info = block.info.clone().into_inner();

        // overwrite extra content with empty values as all needed content in tonTx
        legacy_extra.write_account_blocks(&ShardAccountBlocks::default())?;
        legacy_extra.write_in_msg_descr(&InMsgDescr::default())?;
        legacy_extra.write_out_msg_descr(&OutMsgDescr::default())?;
        legacy_block.write_extra(&legacy_extra)?;
        block = Block::from_boc(legacy_block.write_to_bytes()?)?;

        let mut ton_txs = vec![];
        shard_accounts.iterate_objects(|acc| {
            acc.transactions().iterate_objects(|tx| {
                let tx = Tx::from_boc(tx.deref().write_to_bytes()?)?;
                ton_txs.push(tx);
                Ok(true)
            })?;
            Ok(true)
        })?;

        Ok(Self {
            info,
            block_stripped: block,
            block_id,
            ton_txs,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BlockStreamItem {
    pub mc_block: TongridBlock,
    pub shard_blocks: Vec<TongridBlock>,
}

#[async_trait]
pub trait BlockStream: Send + Sync + 'static {
    async fn next(&mut self) -> anyhow::Result<Option<BlockStreamItem>>;
}
