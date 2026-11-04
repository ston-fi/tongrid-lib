use crate::handler::types::EmulTxInfo;
use anyhow::bail;
use ton::block_tlb::{AccountStatusNotExist, ShardAccount};
use ton::ton_core::traits::tlb::TLB;

fn format_error(
    msg: &str,
    old_shard_acc: &ShardAccount,
    new_shard_acc: Option<&ShardAccount>,
) -> anyhow::Result<String> {
    let main_part = format!(
        "{msg},\n\
        OldShardAcc: last_tx:{}:{:X}, acc_hash: {:X}",
        old_shard_acc.last_tx_lt,
        old_shard_acc.last_tx_hash,
        old_shard_acc.account.cell_hash()?,
    );
    let res = match new_shard_acc {
        None => format!("{main_part}\nNewShardAcc: None"),
        Some(new_shard_acc) => {
            format!(
                "{}\n\
                NewShardAcc: last_tx:{}:{:X}, acc_hash: {:X}",
                main_part,
                new_shard_acc.last_tx_lt,
                new_shard_acc.last_tx_hash,
                new_shard_acc.account.cell_hash()?,
            )
        }
    };
    Ok(res)
}

pub fn verify_state_before_emulation(
    tx_info: &EmulTxInfo,
    old_shard_acc: &ShardAccount,
) -> anyhow::Result<()> {
    let throw_error = |reason: &str| -> anyhow::Result<()> {
        let msg = format!("[before_emulation] {reason}");
        bail!(format_error(&msg, old_shard_acc, None)?)
    };
    let acc_hash = old_shard_acc.account.cell_hash()?;
    if acc_hash != tx_info.old_acc_hash {
        throw_error("account_hashes mismatch")?;
    }

    // txs sometimes forget about previous transactions on account
    // (seems it happens if tx failed AND it happened in another block)
    // so check lt data only for accounts with existing status
    if tx_info.ton_tx.orig_status != AccountStatusNotExist.into() {
        if old_shard_acc.last_tx_lt != tx_info.ton_tx.prev_tx_lt {
            throw_error("last_tx_lt mismatch")?;
        }
        if old_shard_acc.last_tx_hash != tx_info.ton_tx.prev_tx_hash {
            throw_error("last_tx_hash mismatch")?;
        }
    }

    Ok(())
}

pub fn verify_state_after_emulation(
    tx_info: &EmulTxInfo,
    old_shard_acc: &ShardAccount,
    new_shard_acc: &ShardAccount,
) -> anyhow::Result<()> {
    let throw_error = |reason: &str| -> anyhow::Result<()> {
        let msg = format!("[after_emulation] {reason}");
        bail!(format_error(&msg, old_shard_acc, Some(new_shard_acc))?)
    };

    let acc_hash = new_shard_acc.account.cell_hash()?;
    if acc_hash != tx_info.new_acc_hash {
        throw_error("account_hashes mismatch")?;
    }

    if new_shard_acc.last_tx_lt != tx_info.ton_tx.lt {
        throw_error("last_tx_lt mismatch")?;
    }
    if new_shard_acc.last_tx_hash != tx_info.tx_hash {
        throw_error("last_tx_hash mismatch")?;
    }

    Ok(())
}
