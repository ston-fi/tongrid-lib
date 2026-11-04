use crate::ser_de::account::account_deserdb;
use crate::ton_db_adapter::TonDBAdapter;
use std::collections::HashSet;
use ton::block_tlb::{MaybeAccount, ShardAccount};
use ton::ton_core::cell::TonHash;
use tongrid_db::entity::account::db_account::DBAccount;

pub type TvmCodeHashes = HashSet<TonHash>;

pub struct AccountAdapter<'a> {
    tda: &'a TonDBAdapter,
}

impl<'a> From<&'a TonDBAdapter> for AccountAdapter<'a> {
    fn from(tda: &'a TonDBAdapter) -> Self {
        Self { tda }
    }
}

impl<'a> AccountAdapter<'a> {
    pub async fn build_shard_account(
        self,
        db_account: &DBAccount,
    ) -> anyhow::Result<(ShardAccount, TvmCodeHashes)> {
        let (account, lib_ids) = self.build_account(db_account).await?;
        let shard_account = ShardAccount {
            account: account.into(),
            last_tx_lt: db_account.tx_lt,
            last_tx_hash: db_account.tx_hash,
        };
        Ok((shard_account, lib_ids))
    }

    pub async fn build_account(
        self,
        db_account: &DBAccount,
    ) -> anyhow::Result<(MaybeAccount, TvmCodeHashes)> {
        let code_hashes = db_account.tvm_code_hashes();
        let tvm_codes = self.tda.db().tvm_code().get_by_hashes(&code_hashes).await?;
        account_deserdb(db_account, &tvm_codes)
    }
}
