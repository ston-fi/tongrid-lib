use crate::backup_ser_de::ProtoSerial;
use crate::proto::v1::BackupUnitAccount;
use anyhow::ensure;
use flate2::write::GzEncoder;
use flate2::Compression;
use prost::Message;
use std::collections::HashSet;
use std::future::Future;
use std::io::Write;
use tokio::task::spawn_blocking;
use ton::ton_core::cell::TonHash;
use tongrid_db::entity::account::db_account::DBAccount;
use tongrid_db::entity::TvmCode;

/// Keep only one database batch and the compressed output, and load shared code once per file.
pub(super) async fn encode_account_range<A, C>(
    from_seqno: u32,
    to_seqno: u32,
    load_accounts: impl Fn(Vec<u32>) -> A,
    load_codes: impl FnOnce(HashSet<TonHash>) -> C,
) -> anyhow::Result<Vec<u8>>
where
    A: Future<Output = anyhow::Result<Vec<DBAccount>>>,
    C: Future<Output = anyhow::Result<HashSet<TvmCode>>>,
{
    let mut seqnos = from_seqno..=to_seqno;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut hashes = HashSet::new();
    loop {
        let batch: Vec<_> = seqnos.by_ref().take(10).collect();
        let Some(&first) = batch.first() else { break };
        let last = batch.last().copied().unwrap_or(first);
        let accounts = load_accounts(batch).await?;
        ensure!(
            !accounts.is_empty(),
            "no accounts found for range [{first}:{last}]"
        );
        let count = accounts.len();
        for account in &accounts {
            if !account.boc_stripped.is_empty() {
                hashes.extend(account.tvm_code_hashes());
            }
        }
        encoder = spawn_blocking(move || {
            let mut bytes = Vec::new();
            for account in accounts {
                bytes.clear();
                BackupUnitAccount {
                    accounts: vec![account.into_proto()],
                    tvm_codes: vec![],
                }
                .encode(&mut bytes)?;
                encoder.write_all(&bytes)?;
            }
            Ok::<_, anyhow::Error>(encoder)
        })
        .await??;
        log::debug!("[BACKUP_ACCOUNT] encoded {count} accounts for [{first}, {last}]");
    }
    log::info!(
        "[BACKUP_ACCOUNT] fetching {} distinct code hashes for [{from_seqno}, {to_seqno}]",
        hashes.len()
    );
    let codes = load_codes(hashes).await?;
    spawn_blocking(move || {
        let mut bytes = Vec::new();
        for code in codes {
            bytes.clear();
            BackupUnitAccount {
                accounts: vec![],
                tvm_codes: vec![code.into_proto()],
            }
            .encode(&mut bytes)?;
            encoder.write_all(&bytes)?;
        }
        // Repeated protobuf fields merge across records, inside one gzip stream.
        // Finish explicitly so gzip trailer/write errors propagate before publication.
        Ok(encoder.finish()?)
    })
    .await?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_manager::decompress_sync;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_dense_accounts_release_batches_and_fetch_shared_code_once() -> anyhow::Result<()>
    {
        let hash = TonHash::from([7; 32]);
        let payload = Arc::new(vec![42; 64 * 1024]);
        let compressed = encode_account_range(
            1,
            20,
            |seqnos| {
                // The previous batch must have released its account BOCs before another read.
                assert_eq!(Arc::strong_count(&payload), 1);
                assert!(seqnos.len() <= 10);
                let accounts = seqnos
                    .into_iter()
                    .flat_map(|seqno| (0..50).map(move |index| (seqno, index)))
                    .map(|(seqno, index)| DBAccount {
                        mc_seqno: seqno,
                        address: index.to_string(),
                        code_hash: Some(hash),
                        boc_stripped: payload.clone(),
                        ..Default::default()
                    })
                    .collect();
                async { Ok(accounts) }
            },
            |hashes| async move {
                assert_eq!(hashes, HashSet::from([hash]));
                Ok(HashSet::from([TvmCode {
                    code_hash: hash,
                    boc: vec![3, 4, 5],
                    lib_ids: vec![],
                }]))
            },
        )
        .await?;
        assert_eq!(Arc::strong_count(&payload), 1);
        assert!(compressed.len() < payload.len() * 10);
        let restored = BackupUnitAccount::decode(decompress_sync(compressed)?.as_slice())?;
        assert_eq!(restored.accounts.len(), 1000);
        for (index, account) in restored.accounts.iter().enumerate() {
            assert_eq!(account.mc_seqno, (index / 50 + 1) as i32);
            assert_eq!(account.address, (index % 50).to_string());
            assert_eq!(account.boc_stripped, *payload);
            assert_eq!(account.code_hash.as_deref(), Some(hash.as_slice()));
        }
        assert_eq!(restored.tvm_codes.len(), 1);
        assert_eq!(restored.tvm_codes[0].boc, [3, 4, 5]);
        Ok(())
    }

    #[tokio::test]
    async fn test_failed_account_read_does_not_finish_a_partial_backup() {
        let result = encode_account_range(
            1,
            20,
            |seqnos| async move {
                ensure!(seqnos[0] == 1, "read failed");
                Ok(vec![DBAccount::default()])
            },
            |_| async { panic!("code lookup must not run after a failed account read") },
        )
        .await;
        assert!(result.is_err());
    }
}
