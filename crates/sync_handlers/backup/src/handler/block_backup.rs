use crate::backup_ser_de::ProtoSerial;
use crate::proto::v1::BackupUnitBlock;
use anyhow::bail;
use prost::Message;
use std::collections::HashSet;
use std::future::Future;
use ton::ton_core::cell::TonHash;
use tongrid_db::entity::block::db_block::DBBlock;
use tongrid_db::entity::DBTx;

pub(super) async fn encode_block_range<F>(
    from_seqno: u32,
    to_seqno: u32,
    load: impl Fn(Vec<u32>) -> F,
) -> anyhow::Result<(Vec<u8>, HashSet<TonHash>)>
where
    F: Future<Output = anyhow::Result<(Vec<DBBlock>, Vec<DBTx>)>> + Send,
{
    let mut seqnos = from_seqno..=to_seqno;
    let mut bytes = Vec::new();
    let mut hashes = HashSet::new();
    let mut block_count = 0;
    loop {
        // Each mapper starts one query per seqno. Keep backup reads below the
        // shared Scylla query limit so live sync can continue making progress.
        let batch: Vec<_> = seqnos.by_ref().take(10).collect();
        if batch.is_empty() {
            break;
        }
        let last_seqno = batch[batch.len() - 1];
        let (blocks, txs) = load(batch).await?;
        block_count += blocks.len();
        hashes.extend(txs.iter().flat_map(DBTx::tvm_code_hashes));
        let backup = BackupUnitBlock {
            blocks: blocks.into_iter().map(ProtoSerial::into_proto).collect(),
            txs: txs.into_iter().map(ProtoSerial::into_proto).collect(),
            tvm_codes: vec![],
        };
        // Concatenated protobuf messages merge repeated fields. Encode each
        // batch immediately to release its database objects before reading more.
        backup.encode(&mut bytes)?;
        log::debug!(
            "[BACKUP_BLOCK] collected through {}: {} blocks, {} encoded bytes",
            last_seqno,
            block_count,
            bytes.len()
        );
    }
    if block_count == 0 {
        bail!("no shards found for range [{from_seqno}:{to_seqno}]");
    }
    Ok((bytes, hashes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::ensure;
    use tongrid_db::entity::scylla_types::DBMsg;

    #[tokio::test]
    async fn test_large_range_bounds_reads_and_preserves_backup_data() -> anyhow::Result<()> {
        let hash = TonHash::from([7; 32]);
        let (bytes, hashes) = encode_block_range(90538000, 90538999, |seqnos| async move {
            ensure!(
                seqnos.len() <= 10,
                "backup overloaded database with {} concurrent partition reads",
                seqnos.len()
            );
            let blocks = seqnos
                .iter()
                .map(|seqno| DBBlock {
                    mc_seqno: *seqno,
                    boc_stripped: vec![1, 2, 3].into(),
                    ..Default::default()
                })
                .collect();
            let txs = seqnos
                .iter()
                .map(|seqno| DBTx {
                    mc_seqno: *seqno,
                    boc_stripped: vec![4, 5, 6].into(),
                    in_msg: Some(DBMsg {
                        code_hash: Some(hash),
                        ..DBMsg::default()
                    }),
                    ..Default::default()
                })
                .collect();
            Ok((blocks, txs))
        })
        .await?;
        let restored = BackupUnitBlock::decode(bytes.as_slice())?;
        assert_eq!(restored.blocks.len(), 1000);
        assert_eq!(restored.txs.len(), 1000);
        for (offset, (block, tx)) in restored.blocks.iter().zip(&restored.txs).enumerate() {
            assert_eq!(block.master_seqno, 90538000 + offset as i32);
            assert_eq!(tx.mc_seqno, 90538000 + offset as i32);
            assert_eq!(block.boc_stripped, [1, 2, 3]);
            assert_eq!(tx.boc_stripped, [4, 5, 6]);
        }
        assert_eq!(hashes, HashSet::from([hash]));
        Ok(())
    }

    #[tokio::test]
    async fn test_empty_block_range_is_rejected() {
        let result = encode_block_range(1, 25, |_| async { Ok((vec![], vec![])) }).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_read_failure_does_not_produce_partial_backup() {
        let result = encode_block_range(1, 25, |seqnos| async move {
            if seqnos.contains(&15) {
                bail!("database read failed");
            }
            Ok((vec![DBBlock::default()], vec![]))
        })
        .await;
        assert!(result.is_err());
    }
}
