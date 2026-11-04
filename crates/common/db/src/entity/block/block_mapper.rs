use crate::db::{Inner, DB};
use crate::entity::block::block_cache::DBBlockCache;
use crate::entity::block::db_block::DBBlock;
use crate::DB_WRITE_CONCURRENCY;
use anyhow::bail;
use const_format::formatcp;
use function_name::named;
use futures::future::try_join_all;
use futures::stream::{self, TryStreamExt};
use std::ops::Deref;
use std::sync::Arc;
use stonfi_scylla_client::client::ScyllaClient;

#[derive(Clone, Copy)]
pub struct BlockMapper<'a> {
    db_inner: &'a Inner,
}

impl<'a> BlockMapper<'a> {
    const TABLE_NAME: &'static str = "block";

    fn scylla(&self) -> &ScyllaClient {
        &self.db_inner.scylla
    }

    fn cache(&self) -> &DBBlockCache {
        &self.db_inner.block_shards_cache
    }

    #[named]
    pub async fn insert(self, block_shard: &DBBlock) -> anyhow::Result<()> {
        const QUERY: &str = formatcp!(
            "INSERT INTO {t} ({f}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            t = BlockMapper::TABLE_NAME,
            f = DBBlock::FIELDS
        );
        self.scylla()
            .insert(QUERY, block_shard, function_name!())
            .await?;
        Ok(())
    }

    pub async fn insert_many(self, block_shards: &[DBBlock]) -> anyhow::Result<()> {
        stream::iter(block_shards.iter().map(Ok::<_, anyhow::Error>))
            .try_for_each_concurrent(DB_WRITE_CONCURRENCY, |block| self.insert(block))
            .await
    }

    #[named]
    pub async fn get_by_mc_seqno(self, mc_seqno: u32) -> anyhow::Result<Arc<Vec<DBBlock>>> {
        const QUERY: &str = formatcp!(
            "SELECT {f} FROM {t} WHERE mc_seqno = ?",
            f = DBBlock::FIELDS,
            t = BlockMapper::TABLE_NAME
        );
        let mc_seqno = mc_seqno as i32;
        let init = async {
            let shards: Vec<DBBlock> = self
                .scylla()
                .select(QUERY, (mc_seqno,), function_name!())
                .await?;
            if shards.is_empty() {
                return Ok(None);
            }
            Ok(Some(Arc::new(shards)))
        };
        match self
            .cache()
            .shards_cache
            .get_or_init_option(&mc_seqno, init)
            .await?
        {
            Some(shards) => Ok(shards),
            None => Ok(Arc::new(vec![])),
        }
    }

    pub async fn get_by_mc_seqnos(self, mc_seqnos: &[u32]) -> anyhow::Result<Vec<DBBlock>> {
        let futures = mc_seqnos
            .iter()
            .cloned()
            .map(|seqno| self.get_by_mc_seqno(seqno));

        // Return owned records while the cached shard lists remain shared through Arc.
        let shards = try_join_all(futures).await?;
        let results = shards
            .into_iter()
            .flat_map(|x| x.deref().clone().into_iter())
            .collect::<Vec<DBBlock>>();
        Ok(results)
    }

    #[named]
    pub async fn get_shard(
        self,
        mc_seqno: u32,
        workchain: i32,
        shard: i64,
        seqno: u32,
    ) -> anyhow::Result<Arc<DBBlock>> {
        const QUERY: &str = formatcp!(
            "SELECT {f} FROM {t} WHERE mc_seqno = ? AND workchain = ? AND shard = ? AND seqno = ?",
            f = DBBlock::FIELDS,
            t = BlockMapper::TABLE_NAME
        );
        let block = self
            .scylla()
            .select_one(
                QUERY,
                (mc_seqno as i32, workchain, shard, seqno as i32),
                function_name!(),
            )
            .await?;
        match block {
            Some(shard) => Ok(Arc::new(shard)),
            None => {
                bail!("block not found for mc_seqno={mc_seqno}, workchain={workchain}, shard={shard}, seqno={seqno}")
            }
        }
    }

    #[named]
    pub async fn get_master_by_mc_seqno_opt(
        self,
        mc_seqno: u32,
    ) -> anyhow::Result<Option<Arc<DBBlock>>> {
        const QUERY: &str = formatcp!(
            "SELECT {f} FROM {t} WHERE mc_seqno = ? AND workchain = -1",
            f = DBBlock::FIELDS,
            t = BlockMapper::TABLE_NAME
        );
        let init = async {
            let block_id = self
                .scylla()
                .select_one(QUERY, (mc_seqno as i32,), function_name!())
                .await?;
            Ok::<_, anyhow::Error>(block_id.map(Arc::new))
        };
        self.cache()
            .mc_cache
            .get_or_init_option(&mc_seqno, init)
            .await
    }

    pub async fn get_master_by_mc_seqno(self, mc_seqno: u32) -> anyhow::Result<Arc<DBBlock>> {
        match self.get_master_by_mc_seqno_opt(mc_seqno).await? {
            Some(block_shard) => Ok(block_shard),
            None => bail!("mc_block not found for mc_seqno={mc_seqno}"),
        }
    }
}

impl<'a> From<&'a DB> for BlockMapper<'a> {
    fn from(db: &'a DB) -> Self {
        Self {
            db_inner: db.inner(),
        }
    }
}
