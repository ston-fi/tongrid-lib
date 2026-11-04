use crate::db::{Inner, DB};
use crate::entity::tx::tx_key_lookup::MAX_LIMIT;
use anyhow::bail;
use function_name::named;
use futures::future::try_join_all;
use std::cmp::min;
use stonfi_scylla_client::client::ScyllaClient;

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct DBJettonWallet {
    pub wallet_address: String,
    pub is_legit: bool,
    pub master_address: String,
    pub owner_address: String,
    pub mc_seqno: u32,
}

impl DBJettonWallet {
    const FIELDS: &'static str =
        "wallet_address, is_legit, master_address, owner_address, mc_seqno";
    pub(crate) const FIELDS_CNT: usize = 5;
}

#[derive(Clone, Copy)]
pub struct JettonWalletMapper<'a> {
    db_inner: &'a Inner,
}

impl<'a> JettonWalletMapper<'a> {
    const TABLE_NAME: &'static str = "jetton_wallet";
    const TABLE_NAME_BY_MASTER: &'static str = "jetton_wallet_by_master";
    const TABLE_NAME_BY_OWNER: &'static str = "jetton_wallet_by_owner";
    const TABLE_NAME_BY_MC_SEQNO: &'static str = "jetton_wallet_by_mc_seqno";

    fn scylla(&self) -> &ScyllaClient {
        &self.db_inner.scylla
    }

    #[named]
    pub async fn insert(self, wallet: &DBJettonWallet) -> anyhow::Result<()> {
        let query_str = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            Self::TABLE_NAME,
            DBJettonWallet::FIELDS,
            "?, ".repeat(DBJettonWallet::FIELDS_CNT - 1) + "?"
        );
        self.scylla()
            .insert(query_str, wallet, function_name!())
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn insert_many(self, wallets: &[DBJettonWallet]) -> anyhow::Result<()> {
        try_join_all(wallets.iter().map(|x| self.insert(x))).await?;
        Ok(())
    }

    #[named]
    pub async fn get_opt(self, wallet_address: &str) -> anyhow::Result<Option<DBJettonWallet>> {
        let query_str = format!(
            "SELECT {} FROM {} WHERE wallet_address = ?",
            DBJettonWallet::FIELDS,
            Self::TABLE_NAME
        );
        self.scylla()
            .select_one(query_str, (wallet_address,), function_name!())
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get(self, wallet_address: &str) -> anyhow::Result<DBJettonWallet> {
        match self.get_opt(wallet_address).await? {
            Some(wallet) => Ok(wallet),
            None => bail!("Jetton wallet not found for wallet: {wallet_address}"),
        }
    }

    pub async fn get_legit_by_master(
        self,
        master: &str,
        after_owner: Option<&str>,
        limit: Option<i32>,
    ) -> anyhow::Result<Vec<DBJettonWallet>> {
        self.get_legit(Self::TABLE_NAME_BY_MASTER, master, after_owner, limit)
            .await
    }

    #[named]
    pub async fn get_legit_by_owner_and_master(
        self,
        owner: &str,
        master: &str,
    ) -> anyhow::Result<Option<DBJettonWallet>> {
        // all records in jetton_wallet_by_owner/master are legit by scylla view definition
        let query_str = format!(
            "SELECT {} FROM {} WHERE owner_address = ? AND master_address = ? limit 1",
            DBJettonWallet::FIELDS,
            Self::TABLE_NAME_BY_OWNER
        );

        self.scylla()
            .select_one(query_str, (owner, master), function_name!())
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get_legit_by_owner(
        self,
        owner: &str,
        after_master: Option<&str>,
        limit: Option<i32>,
    ) -> anyhow::Result<Vec<DBJettonWallet>> {
        self.get_legit(Self::TABLE_NAME_BY_OWNER, owner, after_master, limit)
            .await
    }

    #[named]
    async fn get_legit(
        self,
        table_name: &str,
        key_address: &str,
        other_address: Option<&str>,
        limit: Option<i32>,
    ) -> anyhow::Result<Vec<DBJettonWallet>> {
        let real_limit = min(MAX_LIMIT as i32, limit.unwrap_or(MAX_LIMIT as i32));
        let (key_field, other_field) = match table_name {
            Self::TABLE_NAME_BY_MASTER => ("master_address", "owner_address"),
            Self::TABLE_NAME_BY_OWNER => ("owner_address", "master_address"),
            _ => bail!("Unexpected table name: {}", table_name),
        };
        let wallet_condition = match &other_address {
            Some(_) => format!("and {other_field} > ?"),
            None => String::new(),
        };
        let query_str = format!(
            "SELECT {} FROM {} WHERE {} = ? {} limit ?",
            DBJettonWallet::FIELDS,
            table_name,
            key_field,
            wallet_condition
        );

        match other_address {
            Some(other) => self
                .scylla()
                .select(
                    query_str,
                    (key_address, other, real_limit),
                    concat!(function_name!(), "_exist_case"),
                )
                .await
                .map_err(anyhow::Error::from),
            None => self
                .scylla()
                .select(
                    query_str,
                    (key_address, real_limit),
                    concat!(function_name!(), "_none_case"),
                )
                .await
                .map_err(anyhow::Error::from),
        }
    }

    #[named] // for backups
    pub async fn get_by_mc_seqno(self, mc_seqno: u32) -> anyhow::Result<Vec<DBJettonWallet>> {
        let query_str = format!(
            "SELECT {} FROM {} WHERE mc_seqno = ?",
            DBJettonWallet::FIELDS,
            Self::TABLE_NAME_BY_MC_SEQNO
        );
        self.scylla()
            .select(query_str, (mc_seqno as i32,), function_name!())
            .await
            .map_err(anyhow::Error::from)
    }

    // for backups
    pub async fn get_by_mc_seqnos(self, mc_seqnos: &[u32]) -> anyhow::Result<Vec<DBJettonWallet>> {
        let futs = mc_seqnos.iter().cloned().map(|s| self.get_by_mc_seqno(s));
        let results = try_join_all(futs).await?;
        Ok(results.into_iter().flatten().collect())
    }
}

impl<'a> From<&'a DB> for JettonWalletMapper<'a> {
    fn from(db: &'a DB) -> Self {
        Self {
            db_inner: db.inner(),
        }
    }
}
