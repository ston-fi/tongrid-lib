use crate::ser_de::tx::{tx_deserdb, tx_serdb};
use crate::ton_db_adapter::TonDBAdapter;
use anyhow::bail;
use std::collections::HashSet;
use tokio::try_join;
use ton::block_tlb::Tx;
use ton::ton_core::cell::TonHash;
use ton::ton_core::traits::tlb::TLB;
use tongrid_db::entity::block::block_shard_key::DBBlockKey;
use tongrid_db::entity::DBTx;

pub struct TxAdapter<'a> {
    tda: &'a TonDBAdapter,
}

impl<'a> From<&'a TonDBAdapter> for TxAdapter<'a> {
    fn from(tda: &'a TonDBAdapter) -> Self {
        Self { tda }
    }
}

impl<'a> TxAdapter<'a> {
    pub async fn save_ton_txs(self, txs: &[Tx], shard_keys: &[DBBlockKey]) -> anyhow::Result<()> {
        let (mut db_txs, mut tvm_codes) = (Vec::new(), HashSet::new());
        if txs.len() != shard_keys.len() {
            bail!(
                "[save_ton_txs] length mismatch: txs={}, shard_keys={}",
                txs.len(),
                shard_keys.len(),
            );
        }
        for (index, tx) in txs.iter().enumerate() {
            let shard_key = &shard_keys[index];
            let (db_tx, tvm_code) = tx_serdb(shard_key, tx)?;
            db_txs.push(db_tx);
            tvm_codes.extend(tvm_code);
        }
        try_join!(
            self.tda.db().tx().insert_many(&db_txs),
            self.tda.msg().save_message_data(&db_txs),
            self.tda.db().tvm_code().insert(tvm_codes)
        )?;
        Ok(())
    }

    pub async fn build_ton_tx(self, db_tx: &DBTx) -> anyhow::Result<(Tx, HashSet<TonHash>)> {
        let tvm_code_hashes = db_tx.tvm_code_hashes();
        let tvm_codes = self
            .tda
            .db()
            .tvm_code()
            .get_by_hashes(tvm_code_hashes.iter())
            .await?;
        let (ton_tx, lib_ids) = tx_deserdb(db_tx, &tvm_codes)?;
        if ton_tx.cell_hash()? != TonHash::from(db_tx.tx_hash.as_slice_sized()) {
            bail!(
                "[build_ton_tx]: tx hash mismatch: db_tx.hash={:?}, ton_tx.hash={:?}",
                db_tx.tx_hash,
                ton_tx.cell_hash()?
            );
        };
        Ok((ton_tx, lib_ids))
    }
}
