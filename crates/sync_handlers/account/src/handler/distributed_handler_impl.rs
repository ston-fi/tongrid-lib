use crate::handler::task::AccountSyncTask;
use crate::handler::AccountHandler;
use stonfi_distributed_sync::coordinator::TaskPriority;
use stonfi_distributed_sync::stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};
use stonfi_distributed_sync::stonfi_sync_core::sync_engine::SyncHeight;
use stonfi_distributed_sync::task::EmptyTaskResult;
use stonfi_distributed_sync::traits::{DistributedHandler, TaskBatch};
use tongrid_db::entity::StateID;

#[async_trait::async_trait]
impl DistributedHandler for AccountHandler {
    type Task = AccountSyncTask;
    type TaskResult = EmptyTaskResult;

    fn id(&self) -> &str {
        &self.sync_id
    }

    async fn create_tasks(
        &self,
        from: SyncHeight,
        to: SyncHeight,
    ) -> SyncCoreResult<Option<TaskBatch<Self::Task>>> {
        let from_seqno = u32::try_from(from).map_err(SyncCoreError::invalid_args)?;
        let to_seqno = u32::try_from(to).map_err(SyncCoreError::invalid_args)?;
        let regular_synced_height = from_seqno
            .saturating_add(self.max_batch_size - 1)
            .min(to_seqno);
        let tasks = match self.state_id {
            StateID::AccountSync | StateID::AccountResync => {
                self.create_regular_tasks(from_seqno, regular_synced_height)
                    .await
            }
            StateID::AccountFixSync => self
                .get_invalid_account_keys(from_seqno, to_seqno)
                .await
                .and_then(|txs_by_addr| self.build_account_tasks(txs_by_addr)),
            _ => Err(anyhow::Error::msg(format!(
                "[AccountHandler::create_tasks] unexpected state_id: {:?}",
                self.state_id
            ))),
        }
        .map_err(SyncCoreError::system)?;

        let synced_height = match self.state_id {
            StateID::AccountFixSync
                if self
                    .current_fix_sync_upper_bound()
                    .await
                    .map_err(SyncCoreError::system)?
                    .is_some_and(|upper_bound| to_seqno >= upper_bound) =>
            {
                SyncHeight::from(self.initial_synced_height)
            }
            StateID::AccountSync | StateID::AccountResync => {
                SyncHeight::from(regular_synced_height)
            }
            _ => to,
        };

        Ok(Some(TaskBatch::new(synced_height, tasks)))
    }

    async fn process_task(&self, task: Self::Task) -> SyncCoreResult<Self::TaskResult> {
        Box::pin(self.process(task))
            .await
            .map_err(SyncCoreError::system)
    }

    fn task_priority(&self) -> TaskPriority {
        self.task_priority
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn min_batch_size(&self) -> usize {
        1
    }

    fn max_batch_size(&self) -> usize {
        match self.state_id {
            StateID::AccountFixSync => self.fix_batch_size as usize,
            _ => self.max_batch_size as usize,
        }
    }

    fn sync_timeout(&self) -> std::time::Duration {
        self.task_timeout
    }

    fn allow_rewind(&self) -> bool {
        self.state_id == StateID::AccountFixSync
    }
}
