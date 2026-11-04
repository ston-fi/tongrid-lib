use crate::db::{Inner, DB};
use function_name::named;
use futures::future::try_join_all;
use num_bigint::BigInt;
use std::cmp::min;
use stonfi_scylla_client::client::ScyllaClient;

use crate::{entity::tx::tx_key_lookup::MAX_LIMIT, paginator::LTPage};

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct DBJettonBalance {
    pub wallet_address: String,
    pub tx_lt: u64,
    pub is_valid: bool,
    pub prev_balance: Option<BigInt>,
    pub balance: Option<BigInt>,
    pub tx_utime: u32,
    pub mc_seqno: u32,
}

impl DBJettonBalance {
    const FIELDS: &'static str =
        "wallet_address, tx_lt, is_valid, prev_balance, balance, tx_utime, mc_seqno";
    pub(crate) const FIELDS_CNT: usize = 7;
}

#[derive(Clone, Copy)]
pub struct JettonBalanceMapper<'a> {
    db_inner: &'a Inner,
}

impl<'a> JettonBalanceMapper<'a> {
    const TABLE_NAME: &'static str = "jetton_balance_order_desc";
    const TABLE_NAME_BY_MC_SEQNO: &'static str = "jetton_balance_by_mc_seqno";

    fn scylla(&self) -> &ScyllaClient {
        &self.db_inner.scylla
    }

    #[named]
    pub async fn insert(self, balance: &DBJettonBalance) -> anyhow::Result<()> {
        let query_str = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            Self::TABLE_NAME,
            DBJettonBalance::FIELDS,
            "?, ".repeat(DBJettonBalance::FIELDS_CNT - 1) + "?"
        );
        self.scylla()
            .insert(query_str, balance, function_name!())
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn insert_many(self, balances: &[DBJettonBalance]) -> anyhow::Result<()> {
        try_join_all(balances.iter().map(|x| self.insert(x))).await?;
        Ok(())
    }

    #[named]
    pub async fn get_latest(
        self,
        wallet_address: &str,
        max_lt: u64,
    ) -> anyhow::Result<Option<DBJettonBalance>> {
        let page = LTPage {
            from_lt: 0,
            to_lt: max_lt,
            limit: 1,
            order_desc: true,
        };
        let page = self
            .get_page_impl(wallet_address, &page, function_name!())
            .await?;
        Ok(page.into_iter().next())
    }

    #[named]
    pub async fn get_latest_valid(
        self,
        wallet_address: &str,
        max_lt: u64,
    ) -> anyhow::Result<Option<DBJettonBalance>> {
        let page = LTPage {
            from_lt: 0,
            to_lt: max_lt,
            limit: 1,
            order_desc: true,
        };
        let page = self
            .get_page_valid_impl(wallet_address, &page, function_name!())
            .await?;
        Ok(page.into_iter().next())
    }

    #[named]
    pub async fn get_page(
        self,
        wallet_address: &str,
        page: &LTPage,
    ) -> anyhow::Result<Vec<DBJettonBalance>> {
        self.get_page_impl(wallet_address, page, function_name!())
            .await
    }

    #[named]
    pub async fn get_page_valid(
        self,
        wallet_address: &str,
        page: &LTPage,
    ) -> anyhow::Result<Vec<DBJettonBalance>> {
        self.get_page_valid_impl(wallet_address, page, function_name!())
            .await
    }

    async fn get_page_impl(
        self,
        wallet_address: &str,
        page: &LTPage,
        caller: &'static str,
    ) -> anyhow::Result<Vec<DBJettonBalance>> {
        let query_str = format!(
            "SELECT {} FROM {} WHERE wallet_address = ? and tx_lt >= ? and tx_lt <= ? order by tx_lt {} limit ?",
            DBJettonBalance::FIELDS,
            Self::TABLE_NAME,
            if page.order_desc { "DESC" } else { "ASC" }
        );
        self.scylla()
            .select(
                query_str,
                (
                    wallet_address,
                    page.from_lt as i64,
                    page.to_lt as i64,
                    min(MAX_LIMIT, page.limit) as i32,
                ),
                caller,
            )
            .await
            .map_err(anyhow::Error::from)
    }

    async fn get_page_valid_impl(
        self,
        wallet_address: &str,
        page: &LTPage,
        caller: &'static str,
    ) -> anyhow::Result<Vec<DBJettonBalance>> {
        let query_str = format!(
            "SELECT {} FROM {} WHERE wallet_address = ? and tx_lt >= ? and tx_lt <= ? and is_valid = true order by tx_lt {} limit ? ALLOW FILTERING",
            DBJettonBalance::FIELDS,
            Self::TABLE_NAME,
            if page.order_desc { "DESC" } else { "ASC" }
        );
        self.scylla()
            .select(
                query_str,
                (
                    wallet_address,
                    page.from_lt as i64,
                    page.to_lt as i64,
                    min(MAX_LIMIT, page.limit) as i32,
                ),
                caller,
            )
            .await
            .map_err(anyhow::Error::from)
    }

    #[named]
    async fn get_by_mc_seqno(self, mc_seqno: u32) -> anyhow::Result<Vec<DBJettonBalance>> {
        let query_str = format!(
            "SELECT {} FROM {} WHERE mc_seqno = ?",
            DBJettonBalance::FIELDS,
            Self::TABLE_NAME_BY_MC_SEQNO
        );
        self.scylla()
            .select(query_str, (mc_seqno as i32,), function_name!())
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get_by_mc_seqnos(self, mc_seqnos: &[u32]) -> anyhow::Result<Vec<DBJettonBalance>> {
        let futures = mc_seqnos
            .iter()
            .cloned()
            .map(|seqno| self.get_by_mc_seqno(seqno));
        let results = try_join_all(futures).await?;
        Ok(results.into_iter().flatten().collect())
    }
}

impl<'a> From<&'a DB> for JettonBalanceMapper<'a> {
    fn from(db: &'a DB) -> Self {
        Self {
            db_inner: db.inner(),
        }
    }
}
