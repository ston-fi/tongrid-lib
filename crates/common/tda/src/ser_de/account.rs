use crate::ser_de::state_init::{
    extract_state_init_data, fill_state_init_code, fill_state_init_simple_libs,
};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use ton::block_tlb::{MaybeAccount, ShardAccount};
use ton::ton_core::cell::TonHash;
use ton::ton_core::traits::tlb::TLB;
use tongrid_db::entity::{account::db_account::DBAccount, DBTx, TvmCode};

pub fn shard_account_deserdb(
    db_state: &DBAccount,
    tvm_codes: &HashMap<TonHash, TvmCode>,
) -> Result<(ShardAccount, HashSet<TonHash>)> {
    let (ton_acc, lib_ids) = account_deserdb(db_state, tvm_codes)?;
    let shard_account = ShardAccount {
        account: ton_acc.into(),
        last_tx_hash: db_state.tx_hash,
        last_tx_lt: db_state.tx_lt,
    };
    Ok((shard_account, lib_ids))
}

pub fn shard_account_serdb(
    tx: &DBTx,
    account: Option<&ShardAccount>,
    prev_account: Option<&ShardAccount>,
) -> Result<(DBAccount, HashSet<TvmCode>)> {
    account_serdb(
        tx,
        account.map(|x| x.account.deref()),
        prev_account.map(|x| x.account.deref()),
    )
}

pub fn account_deserdb(
    db_state: &DBAccount,
    tvm_codes: &HashMap<TonHash, TvmCode>,
) -> Result<(MaybeAccount, HashSet<TonHash>)> {
    let mut account_lib_ids = HashSet::new();
    account_lib_ids.extend(db_state.data_lib_ids.iter().cloned());

    let mut account = MaybeAccount::from_boc(db_state.boc_stripped.clone())?;

    if let Some(account_active) = account.as_active_mut() {
        let state_init = &mut account_active.state_init;
        let lib_ids =
            fill_state_init_code(Some(state_init), db_state.code_hash.as_ref(), tvm_codes)?;
        account_lib_ids.extend(lib_ids);
        let lib_ids =
            fill_state_init_simple_libs(Some(state_init), &db_state.simple_libs, tvm_codes)?;
        account_lib_ids.extend(lib_ids);
    }
    Ok((account, account_lib_ids))
}

// extract state_init.code and state_init.library from MaybeAccount to tvm_codes
pub fn account_serdb(
    tx: &DBTx,
    maybe_account: Option<&MaybeAccount>,
    prev_account: Option<&MaybeAccount>,
) -> Result<(DBAccount, HashSet<TvmCode>)> {
    let mut account = match maybe_account {
        Some(acc) => acc.clone(),
        None => {
            let db_shard_account = DBAccount {
                mc_seqno: tx.mc_seqno,
                tx_hash: tx.tx_hash,
                tx_lt: tx.tx_lt,
                address: tx.address.clone(),
                is_valid: false,
                ..Default::default()
            };
            return Ok((db_shard_account, HashSet::new()));
        }
    };

    let state_init_data =
        extract_state_init_data(account.as_active_mut().map(|x| &mut x.state_init))?;
    let db_shard_acc = match account.as_active() {
        Some(_) => DBAccount {
            mc_seqno: tx.mc_seqno,
            address: tx.address.clone(),
            tx_lt: tx.tx_lt,
            tx_hash: tx.tx_hash,
            is_valid: true,
            boc_stripped: account.to_boc()?.into(),
            code_hash: state_init_data.code_hash,
            data_lib_ids: state_init_data.data_lib_ids,
            simple_libs: state_init_data.simple_libs,
            last_code_hash: None,
        },
        None => {
            let last_code_hash = match prev_account.and_then(|acc| acc.get_code()) {
                Some(code) => Some(*code.hash()?),
                None => None,
            };
            DBAccount {
                mc_seqno: tx.mc_seqno,
                address: tx.address.clone(),
                tx_lt: tx.tx_lt,
                tx_hash: tx.tx_hash,
                is_valid: last_code_hash.is_some(),
                boc_stripped: account.to_boc()?.into(),
                code_hash: state_init_data.code_hash,
                data_lib_ids: state_init_data.data_lib_ids,
                simple_libs: state_init_data.simple_libs,
                last_code_hash,
            }
        }
    };

    Ok((db_shard_acc, state_init_data.tvm_codes))
}
