use crate::db::{Inner, DB};
use crate::entity::account::account_cache::{DBAccountCache, DBAccountCacheKey};
use crate::entity::account::db_account_lite::DBAccountLite;
use crate::entity::account::traits::AccountEntity;
use crate::entity::tx::tx_key::DBTxKey;
use crate::entity::{account::db_account::DBAccount, TxKeyLookup};
use anyhow::{anyhow, bail};
use function_name::named;
use futures::future::try_join_all;
use std::marker::PhantomData;
use std::sync::Arc;
use stonfi_scylla_client::client::ScyllaClient;

static ACCOUNT_TABLE_NAME: &str = "account";
static ACCOUNT_TABLE_NAME_INVALID_STATE: &str = "account_invalid";

pub struct AccountStateMapper<'a, T: AccountEntity + Clone + Send + Sync + 'static> {
    db_inner: &'a Inner,
    cache_getter: fn(&Inner) -> &DBAccountCache<T>,
    _phantom: PhantomData<T>,
}

impl<'a, T: AccountEntity + Clone + Send + Sync + 'static> AccountStateMapper<'a, T> {
    fn scylla(&self) -> &ScyllaClient {
        &self.db_inner.scylla
    }

    fn cache(&self) -> &DBAccountCache<T> {
        (self.cache_getter)(self.db_inner)
    }

    #[named]
    pub async fn insert(self, state: &DBAccount) -> anyhow::Result<()> {
        let query_str = format!(
            "INSERT INTO {ACCOUNT_TABLE_NAME} ({}) VALUES ({})",
            DBAccount::FIELDS,
            "?, ".repeat(DBAccount::FIELDS_CNT - 1) + "?"
        );
        self.scylla()
            .insert(query_str, state, function_name!())
            .await?;
        Ok(())
    }

    pub async fn insert_many(self, states: &[DBAccount]) -> anyhow::Result<()> {
        try_join_all(states.iter().map(|x| self.insert(x))).await?;
        Ok(())
    }
    #[named]
    pub async fn get_by_mc_seqno(self, mc_seqno: u32) -> anyhow::Result<Vec<T>> {
        let mc_seqno = mc_seqno as i32;
        let query_str = format!(
            "SELECT {} FROM {ACCOUNT_TABLE_NAME} WHERE mc_seqno = ?",
            T::FIELDS
        );
        self.scylla()
            .select(query_str, (mc_seqno,), function_name!())
            .await
            .map_err(anyhow::Error::from)
    }

    #[named]
    pub async fn get_invalid_keys_by_mc_seqnos(
        self,
        mc_seqnos: &[u32],
    ) -> anyhow::Result<Vec<DBTxKey>> {
        let query_str = format!(
            "SELECT {} FROM {ACCOUNT_TABLE_NAME_INVALID_STATE} WHERE mc_seqno = ?",
            DBTxKey::FIELDS,
        );

        let futs = mc_seqnos.iter().map(|x| async {
            self.scylla()
                .select(query_str.clone(), (*x as i32,), function_name!())
                .await
                .map_err(anyhow::Error::from)
        });
        Ok(try_join_all(futs).await?.into_iter().flatten().collect())
    }

    pub async fn get_by_mc_seqnos(self, mc_seqnos: &[u32]) -> anyhow::Result<Vec<T>> {
        let futs = mc_seqnos.iter().map(|x| self.get_by_mc_seqno(*x));
        Ok(try_join_all(futs).await?.into_iter().flatten().collect())
    }

    #[named]
    pub async fn any_for_mc_seqno(self, mc_seqno: u32) -> anyhow::Result<bool> {
        let query_str =
            format!("SELECT mc_seqno FROM {ACCOUNT_TABLE_NAME} WHERE mc_seqno = ? LIMIT 1");
        let res = self
            .scylla()
            .select_row(query_str, (mc_seqno as i32,), function_name!())
            .await?;
        Ok(!res.is_empty())
    }

    #[named]
    pub async fn get_opt(self, key: &DBTxKey) -> anyhow::Result<Option<T>> {
        let query_str = format!(
            "SELECT {} FROM {ACCOUNT_TABLE_NAME} WHERE {}",
            T::FIELDS,
            DBTxKey::CQL_WHERE
        );

        self.scylla()
            .select_one(query_str, key, function_name!())
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get(self, key: &DBTxKey) -> anyhow::Result<T> {
        self.get_opt(key)
            .await?
            .ok_or_else(|| anyhow!("DBAccount not found for tx_key: {key:?}"))
    }

    pub async fn get_many<'b, I>(&'b self, tx_keys: I) -> anyhow::Result<Vec<T>>
    where
        I: IntoIterator<Item = &'b DBTxKey>,
    {
        try_join_all(tx_keys.into_iter().map(|k| self.get(k))).await
    }

    pub async fn lookup_opt(self, filter: TxKeyLookup<'_>) -> anyhow::Result<Option<T>> {
        match filter {
            TxKeyLookup::AddressLatest {
                address,
                to_mc_seqno,
            } => {
                let cache_key = DBAccountCacheKey::new(to_mc_seqno, address);
                let latest_filter = TxKeyLookup::AddressLatest {
                    address,
                    to_mc_seqno,
                };
                let init_filter = latest_filter;
                let init = async move {
                    match init_filter.find_key(self.scylla()).await? {
                        Some(key) => Ok(Some(Arc::new(self.get(&key).await?))),
                        None => Ok(None),
                    }
                };

                Ok(self
                    .cache()
                    .cache
                    .get_or_init_option(&cache_key, init)
                    .await?
                    .map(|value| value.as_ref().clone()))
            }
            other => match other.find_key(self.scylla()).await? {
                Some(key) => self.get_opt(&key).await,
                None => Ok(None),
            },
        }
    }

    pub async fn lookup(self, filter: TxKeyLookup<'_>) -> anyhow::Result<T> {
        match self.lookup_opt(filter).await? {
            Some(key) => Ok(key),
            None => bail!("tx_key not found for filter: {:?}", filter),
        }
    }

    pub async fn lookup_many(self, filter: TxKeyLookup<'_>) -> anyhow::Result<Vec<T>> {
        self.get_many(&filter.find_keys(self.scylla()).await?).await
    }
}

// must be implemented manually to work with generic
impl<T: AccountEntity + Clone + Send + Sync + 'static> Clone for AccountStateMapper<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T: AccountEntity + Clone + Send + Sync + 'static> Copy for AccountStateMapper<'_, T> {}

impl<'a> From<&'a DB> for AccountStateMapper<'a, DBAccount> {
    fn from(db: &'a DB) -> Self {
        Self {
            db_inner: db.inner(),
            cache_getter: |inner| &inner.account_cache,
            _phantom: Default::default(),
        }
    }
}

impl<'a> From<&'a DB> for AccountStateMapper<'a, DBAccountLite> {
    fn from(db: &'a DB) -> Self {
        Self {
            db_inner: db.inner(),
            cache_getter: |inner| &inner.account_lite_cache,
            _phantom: Default::default(),
        }
    }
}
