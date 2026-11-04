use crate::db::{Inner, DB};
use crate::entity::tvm_code_traits::tvm_code_traits::TvmCodeTraits;
use crate::metered_cache::MeteredCache;
use function_name::named;
use futures::future::try_join_all;
use scylla::DeserializeRow;
use std::collections::HashMap;
use stonfi_scylla_client::client::ScyllaClient;
use ton::ton_core::cell::TonHash;

static TABLE_NAME: &str = "tvm_code_traits";
static TABLE_NAME_BY_MC_SEQNO: &str = "tvm_code_traits_by_mc_seqno";

#[derive(Clone, Copy)]
pub struct TvmCodeTraitsMapper<'a> {
    db_inner: &'a Inner,
}

impl<'a> TvmCodeTraitsMapper<'a> {
    fn scylla(&self) -> &ScyllaClient {
        &self.db_inner.scylla
    }

    fn cache(&self) -> &MeteredCache<TonHash, TvmCodeTraits> {
        &self.db_inner.tvm_code_traits_cache
    }

    #[named]
    pub async fn get_by_code_hash(self, hash: &TonHash) -> anyhow::Result<Option<TvmCodeTraits>> {
        let init_func = async {
            let query_str = format!(
                "SELECT {} FROM {TABLE_NAME} WHERE code_hash = ? ORDER BY version DESC LIMIT 1",
                TvmCodeTraits::FIELDS
            );
            self.scylla()
                .select_one(query_str, (hash.as_slice(),), function_name!())
                .await
                .map_err(anyhow::Error::from)
        };
        self.cache().get_or_init_option(hash, init_func).await
    }

    #[named]
    pub async fn get_by_seqno(self, seqno: u32) -> anyhow::Result<HashMap<TonHash, TvmCodeTraits>> {
        #[derive(DeserializeRow)]
        struct Key {
            code_hash: Vec<u8>,
        }
        let query_str =
            format!("SELECT code_hash FROM {TABLE_NAME_BY_MC_SEQNO} WHERE mc_seqno = ?");
        let seqno = seqno as i32;
        let keys: Vec<Key> = self
            .scylla()
            .select(query_str, (seqno,), function_name!())
            .await?;
        if keys.is_empty() {
            return Ok(Default::default());
        }
        let hashes = keys
            .into_iter()
            .map(|x| TonHash::from_slice(&x.code_hash))
            .collect::<Result<Vec<_>, _>>()?;
        self.get_by_code_hashes(&hashes).await
    }

    pub async fn get_by_mc_seqnos(self, seqnos: &[u32]) -> anyhow::Result<Vec<TvmCodeTraits>> {
        let futs = seqnos.iter().map(|&seqno| self.get_by_seqno(seqno));
        Ok(try_join_all(futs)
            .await?
            .into_iter()
            .flatten()
            .map(|x| x.1)
            .collect())
    }

    #[named]
    async fn insert(self, traits: &TvmCodeTraits) -> anyhow::Result<()> {
        let query_str = format!(
            "INSERT INTO {TABLE_NAME} (code_hash, version, traits, mc_seqno) VALUES (?, ?, ?, ?)"
        );
        self.scylla()
            .insert(query_str, traits, function_name!())
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn insert_many(
        self,
        traits: impl IntoIterator<Item = TvmCodeTraits>,
    ) -> anyhow::Result<()> {
        let futs = traits.into_iter().map(|x| async {
            if !self.cache().contains(&x.code_hash) {
                // insert to db first
                self.insert(&x).await?;
                self.cache().insert(x.code_hash, x).await;
            }
            Ok::<(), anyhow::Error>(())
        });
        try_join_all(futs).await?;
        Ok(())
    }

    pub async fn get_by_code_hashes<'b, I>(
        self,
        hashes: I,
    ) -> anyhow::Result<HashMap<TonHash, TvmCodeTraits>>
    where
        I: IntoIterator<Item = &'b TonHash>,
    {
        let futures = hashes.into_iter().map(|x| self.get_by_code_hash(x));
        let tvm_codes = try_join_all(futures)
            .await?
            .into_iter()
            .flatten()
            .map(|x| (x.code_hash, x))
            .collect();

        Ok(tvm_codes)
    }

    #[named]
    pub async fn delete_old_versions(self, hash: TonHash, last_version: i32) -> anyhow::Result<()> {
        let query_str = format!("DELETE FROM {TABLE_NAME} WHERE code_hash = ? AND version < ?");
        self.scylla()
            .delete(
                query_str.clone(),
                (hash.as_slice(), last_version),
                function_name!(),
            )
            .await?;
        Ok(())
    }
}

impl<'a> From<&'a DB> for TvmCodeTraitsMapper<'a> {
    fn from(db: &'a DB) -> Self {
        Self {
            db_inner: db.inner(),
        }
    }
}
