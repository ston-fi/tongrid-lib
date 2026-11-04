use crate::db::{Inner, DB};
use crate::entity::tx::db_tx::DBTx;
use crate::entity::tx::db_tx_lite::DBTxLite;
use crate::entity::tx::traits::TxEntity;
use crate::entity::tx::tx_key_lookup::TxKeyLookup;
use crate::entity::DBTxKey;
use crate::DB_WRITE_CONCURRENCY;
use anyhow::{anyhow, bail};
use function_name::named;
use futures::future::try_join_all;
use futures::stream::{self, TryStreamExt};
use std::any::type_name;
use stonfi_scylla_client::client::ScyllaClient;

static TX_TABLE_NAME: &str = "tx";
pub(crate) static TX_TABLE_NAME_BY_ADDRESS_MC_SEQNO: &str = "tx_by_address_mc_seqno_order_desc";

pub struct TxMapper<'a, T: TxEntity> {
    db_inner: &'a Inner,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T: TxEntity> TxMapper<'a, T> {
    fn scylla(&self) -> &ScyllaClient {
        &self.db_inner.scylla
    }

    #[named]
    pub async fn insert(self, tx: &DBTx) -> anyhow::Result<()> {
        let values_tpl = "?,".repeat(DBTx::FIELDS_CNT - 1) + "?";
        let query_str = format!(
            "INSERT INTO {TX_TABLE_NAME} ({}) VALUES ({values_tpl})",
            DBTx::FIELDS
        );
        self.scylla()
            .insert(query_str, tx, function_name!())
            .await?;
        Ok(())
    }

    pub async fn insert_many(self, txs: &[DBTx]) -> anyhow::Result<()> {
        stream::iter(txs.iter().map(Ok::<_, anyhow::Error>))
            .try_for_each_concurrent(DB_WRITE_CONCURRENCY, |tx| self.insert(tx))
            .await
    }

    #[named]
    pub async fn get_opt(self, tx_key: &DBTxKey) -> anyhow::Result<Option<T>> {
        let fields = T::FIELDS;
        let where_clause = DBTxKey::CQL_WHERE;
        let query = format!("SELECT {fields} FROM {TX_TABLE_NAME} WHERE {where_clause}");
        self.scylla()
            .select_one(query, tx_key, function_name!())
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get(self, key: &DBTxKey) -> anyhow::Result<T> {
        self.get_opt(key)
            .await?
            .ok_or_else(|| anyhow!("{} not found for tx_key: {key:?}", type_name::<T>()))
    }

    pub async fn get_many<'b, I>(&'b self, tx_keys: I) -> anyhow::Result<Vec<T>>
    where
        I: IntoIterator<Item = &'b DBTxKey>,
    {
        try_join_all(tx_keys.into_iter().map(|key| self.get(key))).await
    }

    #[named]
    pub async fn get_by_mc_seqno(self, mc_seqno: u32) -> anyhow::Result<Vec<T>> {
        let fields = T::FIELDS;
        let query = format!("SELECT {fields} FROM {TX_TABLE_NAME} WHERE mc_seqno = ?");
        let mc_seqno = mc_seqno as i32;
        self.scylla()
            .select(query, (mc_seqno,), function_name!())
            .await
            .map_err(anyhow::Error::from)
    }

    #[named]
    pub async fn get_keys_by_address_mc_seqnos(
        self,
        mc_seqnos: &[u32],
        address: &str,
    ) -> anyhow::Result<Vec<DBTxKey>> {
        let fields = DBTxKey::FIELDS;
        let query =
            format!("SELECT {fields} FROM {TX_TABLE_NAME_BY_ADDRESS_MC_SEQNO} WHERE address = ? AND mc_seqno = ?");

        let futs = mc_seqnos.iter().map(|mc_seqno| async {
            self.scylla()
                .select(query.clone(), (address, *mc_seqno as i32), function_name!())
                .await
                .map_err(anyhow::Error::from)
        });
        Ok(try_join_all(futs).await?.into_iter().flatten().collect())
    }

    pub async fn get_by_mc_seqnos(self, mc_seqnos: &[u32]) -> anyhow::Result<Vec<T>> {
        let futs = mc_seqnos.iter().map(|seqno| self.get_by_mc_seqno(*seqno));
        let txs = try_join_all(futs).await?.into_iter().flatten().collect();
        Ok(txs)
    }

    pub async fn lookup_key(self, filter: TxKeyLookup<'_>) -> anyhow::Result<Option<DBTxKey>> {
        filter.find_key(self.scylla()).await
    }

    pub async fn lookup_latest_key(
        self,
        address: &str,
        mc_seqno: u32,
    ) -> anyhow::Result<Option<T>> {
        let filter = TxKeyLookup::address_latest(address, mc_seqno);
        self.lookup_opt(filter).await
    }

    pub async fn lookup_opt(self, filter: TxKeyLookup<'_>) -> anyhow::Result<Option<T>> {
        match self.lookup_key(filter).await? {
            Some(key) => self.get_opt(&key).await,
            None => Ok(None),
        }
    }

    pub async fn lookup(self, filter: TxKeyLookup<'_>) -> anyhow::Result<T> {
        match self.lookup_opt(filter).await? {
            Some(tx) => Ok(tx),
            None => bail!("Tx not found for key: {filter:?}"),
        }
    }

    pub async fn lookup_keys(self, filter: TxKeyLookup<'_>) -> anyhow::Result<Vec<DBTxKey>> {
        filter.find_keys(self.scylla()).await
    }

    pub async fn lookup_many(self, filter: TxKeyLookup<'_>) -> anyhow::Result<Vec<T>> {
        self.get_many(&self.lookup_keys(filter).await?).await
    }

    #[named]
    pub async fn lookup_master_by_mc_seqno(self, mc_seqno: u32) -> anyhow::Result<Vec<DBTxKey>> {
        let fields = DBTxKey::FIELDS;
        let mc_seqno = mc_seqno as i32;
        let query =
            format!("SELECT {fields} FROM {TX_TABLE_NAME} WHERE mc_seqno = ? and block_workchain = -1 ALLOW FILTERING");
        self.scylla()
            .select(query, (mc_seqno,), function_name!())
            .await
            .map_err(anyhow::Error::from)
    }

    // for tests
    pub async fn delete(self, mc_seqno: u32) -> anyhow::Result<()> {
        let mc_seqno = mc_seqno as i32;
        self.scylla()
            .execute_unprepared(
                &format!("DELETE FROM {TX_TABLE_NAME} WHERE mc_seqno = {mc_seqno}"),
                "delete_by_mc_seqno",
            )
            .await?;
        Ok(())
    }
}

// must be implemented manually to work with generic
impl<T: TxEntity> Clone for TxMapper<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: TxEntity> Copy for TxMapper<'_, T> {}

impl<'a> From<&'a DB> for TxMapper<'a, DBTx> {
    fn from(db: &'a DB) -> Self {
        Self {
            db_inner: db.inner(),
            _phantom: Default::default(),
        }
    }
}

impl<'a> From<&'a DB> for TxMapper<'a, DBTxLite> {
    fn from(db: &'a DB) -> Self {
        Self {
            db_inner: db.inner(),
            _phantom: Default::default(),
        }
    }
}
