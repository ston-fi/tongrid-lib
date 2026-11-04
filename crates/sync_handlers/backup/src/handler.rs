use crate::backup_ser_de::ProtoSerial;
use crate::file_manager::FileManager;
use crate::proto::v1::{BackupUnitBlock, BackupUnitClassifier, BackupUnitJetton};
use crate::util::{make_backup_path_from_parts, make_state_backup_dir};
use anyhow::bail;
use prost::Message;
use std::collections::HashSet;
use std::str::FromStr;
use std::time::Duration;
use stonfi_distributed_sync::task::EmptyTaskResult;
use task::BackupSyncTask;
use ton::ton_core::cell::TonHash;
use tongrid_db::entity::{StateID, TvmCode};
use tongrid_tda::ton_db_adapter::TonDBAdapter;

mod account_backup;
mod block_backup;
mod distributed_handler_impl;
pub mod task;

pub struct BackupHandler {
    tda: TonDBAdapter,
    file_manager: FileManager,
    all_backups_dst_path: String,
    pub(crate) state_id: StateID,
    pub(crate) sync_id: String,
    pub(crate) enabled: bool,
    pub(crate) task_timeout: Duration,
    pub(crate) batch_size: u32,
}

impl BackupHandler {
    pub async fn new(
        tda: TonDBAdapter,
        file_manager: FileManager,
        config: &crate::config::BackupConfig,
        handler_config: &crate::config::BackupHandlerConfig,
        state_id: StateID,
    ) -> anyhow::Result<BackupHandler> {
        let (enabled, task_timeout) = handler_config.state_settings(state_id)?;
        let batch_size = handler_config.batch_size;
        let all_backups_dst_path = config.all_backups_dst_path.clone();
        if !batch_size.is_multiple_of(10) {
            bail!("Batch size must be a multiple of 10, got {}", batch_size);
        }

        let handler = BackupHandler {
            tda,
            file_manager,
            all_backups_dst_path,
            state_id,
            sync_id: state_id.to_string(),
            enabled,
            task_timeout,
            batch_size,
        };

        log::info!(
            "[BACKUP_PROCESSOR] created:  all_backups_dst_path={}",
            handler.all_backups_dst_path
        );
        Ok(handler)
    }

    #[rustfmt::skip]
    pub async fn process(&self, task: BackupSyncTask) -> anyhow::Result<EmptyTaskResult> {
        let from_seqno = task.mc_seqno_from;
        let to_seqno = task.mc_seqno_to;
        if from_seqno > to_seqno {
            bail!("from_seqno must be less than to_seqno, got [{from_seqno}, {to_seqno}]");
        }
        let state_id = StateID::from_str(&task.state_id)?;
        let state_backup_dir = make_state_backup_dir(&self.all_backups_dst_path, state_id);
        log::debug!("[BACKUP] call for [{from_seqno}, {to_seqno}] (state_id={state_id})");

        self.file_manager.create_dir_all(&state_backup_dir).await?;
        let backup_full_path = make_backup_path_from_parts(&state_backup_dir, from_seqno, to_seqno)?;

        match state_id {
            StateID::BackupBlockSync => self.backup_blocks(&backup_full_path, from_seqno, to_seqno).await?,
            StateID::BackupAccountSync => self.backup_accounts(&backup_full_path, from_seqno, to_seqno).await?,
            StateID::BackupClassifierSync => self.backup_classifier(&backup_full_path, from_seqno, to_seqno).await?,
            StateID::BackupJettonSync => self.backup_jetton(&backup_full_path, from_seqno, to_seqno).await?,
            _ => bail!("[BACKUP]: Unsupported state_id: {state_id}"),
        }

        Ok(EmptyTaskResult)
    }

    async fn backup_blocks(
        &self,
        dst_path: &str,
        from_seqno: u32,
        to_seqno: u32,
    ) -> anyhow::Result<()> {
        let started = std::time::Instant::now();
        log::info!("[BACKUP_BLOCK] collecting range [{from_seqno}, {to_seqno}]");
        let (mut backup_bin, code_hashes) =
            block_backup::encode_block_range(from_seqno, to_seqno, |seqnos| async move {
                let blocks = self.tda.db().block().get_by_mc_seqnos(&seqnos).await?;
                let txs = self.tda.db().tx().get_by_mc_seqnos(&seqnos).await?;
                Ok((blocks, txs))
            })
            .await?;
        log::info!(
            "[BACKUP_BLOCK] collected range [{from_seqno}, {to_seqno}]: {} bytes, {} distinct code hashes in {:?}",
            backup_bin.len(),
            code_hashes.len(),
            started.elapsed()
        );

        // Fetch shared code once for the whole file, including one library level.
        let tvm_codes = self.fetch_tvm_codes(&code_hashes, 1).await?;
        BackupUnitBlock {
            blocks: vec![],
            txs: vec![],
            tvm_codes: tvm_codes.into_iter().map(ProtoSerial::into_proto).collect(),
        }
        .encode(&mut backup_bin)?;

        log::info!(
            "[BACKUP_BLOCK] writing {} bytes to {dst_path} after {:?}",
            backup_bin.len(),
            started.elapsed()
        );
        self.file_manager
            .write_compressed(dst_path, backup_bin)
            .await?;
        log::info!(
            "[BACKUP_BLOCK] completed range [{from_seqno}, {to_seqno}] in {:?}",
            started.elapsed()
        );
        Ok(())
    }

    async fn backup_accounts(
        &self,
        dst_path: &str,
        from_seqno: u32,
        to_seqno: u32,
    ) -> anyhow::Result<()> {
        let started = std::time::Instant::now();
        log::info!("[BACKUP_ACCOUNT] collecting range [{from_seqno}, {to_seqno}]");
        let compressed = account_backup::encode_account_range(
            from_seqno,
            to_seqno,
            |seqnos| async move { self.tda.db().account().get_by_mc_seqnos(&seqnos).await },
            |hashes| async move { self.fetch_tvm_codes(&hashes, 1).await },
        )
        .await?;
        log::info!(
            "[BACKUP_ACCOUNT] writing {} compressed bytes to {dst_path}",
            compressed.len()
        );
        self.file_manager
            .write_verified(dst_path, compressed)
            .await?;
        log::info!(
            "[BACKUP_ACCOUNT] completed range [{from_seqno}, {to_seqno}] in {:?}",
            started.elapsed()
        );
        Ok(())
    }

    async fn backup_classifier(
        &self,
        dst_path: &str,
        from_seqno: u32,
        to_seqno: u32,
    ) -> anyhow::Result<()> {
        let seqno_range = Vec::from_iter(from_seqno..=to_seqno);

        let traits = self
            .tda
            .db()
            .tvm_code_traits()
            .get_by_mc_seqnos(&seqno_range)
            .await?;
        let backup_bin = {
            let b_unit = BackupUnitClassifier {
                tvm_code_traits: traits.into_iter().map(ProtoSerial::into_proto).collect(),
            };
            b_unit.encode_to_vec()
        };
        log::info!(
            "[BACKUP] writing {} bytes to path: {dst_path}",
            backup_bin.len()
        );
        self.file_manager
            .write_compressed(dst_path, backup_bin)
            .await?;
        Ok(())
    }

    async fn backup_jetton(
        &self,
        dst_path: &str,
        from_seqno: u32,
        to_seqno: u32,
    ) -> anyhow::Result<()> {
        let seqno_range = Vec::from_iter(from_seqno..=to_seqno);

        let wallets = self
            .tda
            .db()
            .jetton_wallet()
            .get_by_mc_seqnos(&seqno_range)
            .await?;
        let balances = self
            .tda
            .db()
            .jetton_balance()
            .get_by_mc_seqnos(&seqno_range)
            .await?;

        let wallets_len = wallets.len();
        let balances_len = balances.len();
        let backup_bin = {
            let b_unit = BackupUnitJetton {
                jetton_wallets: wallets.into_iter().map(ProtoSerial::into_proto).collect(),
                jetton_balances: balances.into_iter().map(ProtoSerial::into_proto).collect(),
            };
            b_unit.encode_to_vec()
        };

        log::info!(
            "[BACKUP] writing {} bytes to path: {dst_path} (wallets: {}, balances: {})",
            backup_bin.len(),
            wallets_len,
            balances_len
        );
        self.file_manager
            .write_compressed(dst_path, backup_bin)
            .await?;
        Ok(())
    }

    // fetch direct tvm_codes by specified hashes and recursively fetch TvmCode for it's lib_ids up to `deep` levels
    async fn fetch_tvm_codes<'a, I>(
        &self,
        hashes: I,
        mut deep: usize,
    ) -> anyhow::Result<HashSet<TvmCode>>
    where
        I: IntoIterator<Item = &'a TonHash>,
    {
        let mut all_tvm_codes = HashSet::new();
        let mut fetched_hashes = HashSet::new();

        let mut cur_hashes: HashSet<_> = hashes.into_iter().cloned().collect();
        loop {
            let hashes: Vec<_> = cur_hashes.drain().collect();
            fetched_hashes.extend(hashes.iter().copied());
            for batch in hashes.chunks(100) {
                let tvm_codes = self.tda.db().tvm_code().get_by_hashes(batch).await?;
                for (_, code) in tvm_codes {
                    let lib_ids = code.lib_ids.clone();
                    if all_tvm_codes.insert(code) {
                        cur_hashes.extend(lib_ids);
                    }
                }
            }
            cur_hashes.retain(|hash| !fetched_hashes.contains(hash));

            if deep == 0 || cur_hashes.is_empty() {
                break;
            }
            deep -= 1;
        }
        Ok(all_tvm_codes)
    }
}
