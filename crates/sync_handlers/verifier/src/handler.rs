use anyhow::bail;
use anyhow::Result;
use futures::future::try_join_all;
use std::time::Duration;
use stonfi_distributed_sync::coordinator::TaskPriority;
use stonfi_distributed_sync::task::{EmptyTaskResult, RangeTask};
use ton::ton_core::traits::tlb::TLB;
use tongrid_db::entity::block::db_block::DBBlock;
use tongrid_db::entity::StateID;
use tongrid_tda::ton_db_adapter::TonDBAdapter;

mod distributed_handler_impl;

pub struct VerifierHandler {
    tda: TonDBAdapter,
    pub(crate) sync_id: String,
    pub(crate) enabled: bool,
    pub(crate) task_timeout: Duration,
    pub(crate) task_priority: TaskPriority,
    pub(crate) max_batch_size: u32,
}

impl VerifierHandler {
    pub async fn new(
        tda: TonDBAdapter,
        config: &crate::config::VerifierHandlerConfig,
    ) -> Result<Self> {
        let enabled = config.enabled;
        let task_timeout = config.task_timeout;
        let max_batch_size = config.max_batch_size;
        let handler = Self {
            tda,
            sync_id: StateID::VerifierSync.to_string(),
            enabled,
            task_timeout,
            task_priority: TaskPriority::Normal,
            max_batch_size,
        };
        log::info!("[VERIFIER_PROCESSOR] created");
        Ok(handler)
    }

    pub async fn process(&self, task: RangeTask) -> Result<EmptyTaskResult> {
        log::debug!("[VERIFIER_PROCESSOR] task={task:?}: start processing");

        let from = u32::try_from(task.from)?;
        let to = u32::try_from(task.to)?;

        for mc_seqno in from..=to {
            self.verify_blocks(mc_seqno).await?;
            self.verify_txs_and_accounts(mc_seqno).await?;
        }
        log::debug!("[VERIFIER_PROCESSOR] task={task:?}: completed");
        Ok(EmptyTaskResult)
    }

    async fn verify_blocks(&self, mc_seqno: u32) -> Result<()> {
        let blocks = self.tda.db().block().get_by_mc_seqno(mc_seqno).await?;
        Self::verify_single_master(mc_seqno, &blocks)?;
        self.verify_prev_keyblock(mc_seqno, &blocks).await?;
        Ok(())
    }

    fn verify_single_master(mc_seqno: u32, blocks: &[DBBlock]) -> Result<()> {
        let mut mc_blocks_count = 0;
        for block in blocks {
            if block.workchain == -1 {
                mc_blocks_count += 1;
            }
        }
        if mc_blocks_count != 1 {
            bail!(
                "[verify_single_master] expected 1 masterchain block for mc_seqno {mc_seqno}, found {mc_blocks_count}"
            );
        }
        Ok(())
    }

    async fn verify_prev_keyblock(&self, mc_seqno: u32, blocks: &[DBBlock]) -> Result<()> {
        let initial_seqno = match self.tda.db().progress().load_initial_height().await? {
            Some(height) => u32::try_from(height)?,
            None => bail!("[verify_prev_keyblock] No initial seqno found for mc_seqno {mc_seqno}"),
        };

        let mut check_keyblock_futs = vec![];
        for block in blocks {
            if block.key_block_seqno > initial_seqno {
                check_keyblock_futs.push(
                    self.tda
                        .db()
                        .block()
                        .get_master_by_mc_seqno(block.key_block_seqno),
                );
            }
        }

        if let Err(err) = try_join_all(check_keyblock_futs).await {
            bail!("[verify_prev_keyblock]: fail to check key_block for mc_seqno: {mc_seqno}, err: {err}");
        }
        Ok(())
    }

    async fn verify_txs_and_accounts(&self, mc_seqno: u32) -> Result<()> {
        let shard_accs = self.tda.db().account().get_by_mc_seqno(mc_seqno).await?;
        let db_txs = self.tda.db().tx().get_by_mc_seqno(mc_seqno).await?;

        for (shard_acc, db_tx) in shard_accs.into_iter().zip(db_txs) {
            if shard_acc.boc_stripped.is_empty() {
                continue;
            }

            let (tx, _) = self.tda.tx().build_ton_tx(&db_tx).await?;

            if tx.cell_hash()? != db_tx.tx_hash {
                bail!("[verify_txs_and_accounts] tx_hash mismatch for mc_seqno: {mc_seqno}, addr: {}, lt: {}: expected {}, got {}", db_tx.address, tx.lt, db_tx.tx_hash, tx.cell_hash()?);
            }

            let (account, _) = self.tda.account().build_account(&shard_acc).await?;

            if account.cell_hash()? != tx.state_update.new {
                bail!("[verify_txs_and_accounts] account_hash mismatch for mc_seqno: {mc_seqno}, addr: {}, lt: {}: expected {}, got {}", db_tx.address, tx.lt, tx.state_update.new, account.cell_hash()?);
            }
        }
        Ok(())
    }
}
