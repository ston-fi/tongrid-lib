use super::msg::{msg_deserdb, msg_serdb};
use anyhow::bail;
use std::collections::{HashMap, HashSet};
use ton::block_tlb::{CommonMsgInfo, Tx};
use ton::ton_core::cell::TonHash;
use ton::ton_core::traits::tlb::TLB;
use ton::ton_core::types::TonAddress;
use tongrid_db::entity::block::block_shard_key::DBBlockKey;
use tongrid_db::entity::tx::tx_key::DB_TX_KEY_ZERO;
use tongrid_db::entity::DBTx;
use tongrid_db::entity::TvmCode as DBTvmCode;

pub fn tx_serdb(shard_key: &DBBlockKey, ton_tx: &Tx) -> anyhow::Result<(DBTx, HashSet<DBTvmCode>)> {
    let mut tx_tvm_codes = HashSet::new();

    let (in_msg, in_msg_hash, in_msg_hash_normalized) = match &ton_tx.msgs.in_msg {
        Some(ton_in_msg) => {
            let (db_msg, msg_tvm_codes) = msg_serdb(ton_in_msg)?;
            tx_tvm_codes.extend(msg_tvm_codes);

            let in_msg = Some(db_msg);

            let (in_msg_hash, in_msg_hash_normalized) = match ton_in_msg.info {
                CommonMsgInfo::ExtIn(_) => (
                    Some(ton_in_msg.cell_hash()?),
                    Some(ton_in_msg.cell_hash_normalized()?),
                ),
                _ => (None, None),
            };

            (in_msg, in_msg_hash, in_msg_hash_normalized)
        }
        None => (None, None, None),
    };

    // handle out_msgs
    let mut out_msgs = Vec::with_capacity(ton_tx.msgs.out_msgs.len());
    for ton_msg in &ton_tx.msgs.out_msgs {
        let (db_msg, msg_tvm_codes) = msg_serdb(ton_msg)?;
        out_msgs.push(db_msg);
        tx_tvm_codes.extend(msg_tvm_codes);
    }

    let mut ton_tx_stripped = ton_tx.clone();
    ton_tx_stripped.msgs.in_msg = None;
    ton_tx_stripped.msgs.out_msgs.clear();

    let db_tx = DBTx {
        mc_seqno: shard_key.mc_seqno,
        address: TonAddress::new(shard_key.workchain, ton_tx.account_addr).to_string(),
        tx_lt: ton_tx.lt,
        tx_utime: ton_tx.now,
        block_workchain: shard_key.workchain,
        block_shard: shard_key.shard,
        block_seqno: shard_key.seqno,
        tx_hash: ton_tx.cell_hash()?,
        boc_stripped: ton_tx_stripped.to_boc()?.into(),
        in_msg,
        out_msgs,
        in_msg_hash,
        in_msg_hash_normalized,
        initial_tx: DB_TX_KEY_ZERO.clone(),
    };
    Ok((db_tx, tx_tvm_codes))
}

pub fn tx_deserdb(
    db_tx: &DBTx,
    tvm_codes: &HashMap<TonHash, DBTvmCode>,
) -> anyhow::Result<(Tx, HashSet<TonHash>)> {
    let mut tx_lib_ids = HashSet::new();
    let mut ton_tx = Tx::from_boc(db_tx.boc_stripped.clone())?;

    if let Some((in_msg, in_msg_lib_ids)) = db_tx
        .in_msg
        .as_ref()
        .map(|x| msg_deserdb(x, tvm_codes))
        .transpose()?
    {
        ton_tx.msgs.in_msg = Some(in_msg);
        tx_lib_ids.extend(in_msg_lib_ids);
    }

    for db_out_msg in &db_tx.out_msgs {
        let (out_msg, out_msg_lib_ids) = msg_deserdb(db_out_msg, tvm_codes)?;
        ton_tx.msgs.out_msgs.push(out_msg);
        tx_lib_ids.extend(out_msg_lib_ids);
    }

    if ton_tx.cell_hash()?.as_slice() != db_tx.tx_hash.as_slice() {
        bail!(
            "[tx_deserdb] tx_hash mismatch: db: {}, ton: {}",
            db_tx.tx_hash,
            ton_tx.cell_hash()?
        )
    }
    Ok((ton_tx, tx_lib_ids))
}
