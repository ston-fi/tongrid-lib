use anyhow::bail;
use num_traits::ToPrimitive;
use std::ops::Deref;
use ton::block_tlb::{ConfigParams, MaybeAccount, ShardAccount};
use ton::ton_core::constants::TON_MASTERCHAIN;
use ton::ton_core::traits::tlb::TLB;
use tongrid_proto::v3::{AccountParsed, AccountState, StorageInfo};
use tongrid_ton::extractor::TonExtractor;

pub(crate) struct AccountSerial;

impl AccountSerial {
    pub fn ton_to_proto(
        &self,
        shard_account: &ShardAccount,
        mc_seqno: u32,
        traits: Vec<i32>,
    ) -> anyhow::Result<AccountState> {
        Ok(AccountState {
            mc_seqno,
            shard_account_boc: shard_account.to_boc()?,
            traits,
        })
    }

    pub fn ton_to_parsed(
        &self,
        shard_account: &ShardAccount,
        bc_config: &ConfigParams,
        traits: Vec<i32>,
    ) -> anyhow::Result<AccountParsed> {
        let account = &shard_account.account;
        let code = account.get_code().map(|cell| cell.to_boc()).transpose()?;
        let data = account.get_data().map(|cell| cell.to_boc()).transpose()?;
        let code_hash = account
            .get_code()
            .map(|cell| cell.hash())
            .transpose()?
            .map(|hash| hash.to_vec());
        let balance = match account.get_balance() {
            Some(balance) => balance.to_u64()?,
            None => 0,
        };

        Ok(AccountParsed {
            code,
            data,
            last_tx_lt: shard_account.last_tx_lt,
            last_tx_hash: shard_account.last_tx_hash.to_vec(),
            balance,
            storage_info: build_storage_info(account, bc_config)?,
            frozen_hash: account.as_frozen().map(|frozen| frozen.state_hash.to_vec()),
            code_hash,
            traits,
        })
    }
}

fn build_storage_info(
    account: &MaybeAccount,
    bc_config: &ConfigParams,
) -> anyhow::Result<Option<StorageInfo>> {
    let (storage_info, is_masterchain) = match account.as_account() {
        Some(account) => (&account.storage_stat, account.addr.wc() == TON_MASTERCHAIN),
        None => return Ok(None),
    };
    let price_ps_biguint = TonExtractor::storage_price_ps(
        storage_info,
        is_masterchain,
        bc_config.storage_prices()?.deref(),
    )?;
    let price_ps = match price_ps_biguint.to_u64() {
        Some(price) => price,
        None => bail!("storage_price_per_sec ({price_ps_biguint}) doesn't fit in u64"),
    };
    let due_payment = match &storage_info.due_payment {
        Some(payment) => payment.to_u64()?,
        None => 0,
    };

    Ok(Some(StorageInfo {
        last_paid_ts: storage_info.last_paid,
        price_ps,
        due_payment,
    }))
}
