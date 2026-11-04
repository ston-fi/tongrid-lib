use crate::handler::task::JettonSyncTask;
use crate::handler::JettonHandler;
use stonfi_distributed_sync::coordinator::TaskPriority;
use stonfi_distributed_sync::stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};
use stonfi_distributed_sync::stonfi_sync_core::sync_engine::SyncHeight;
use stonfi_distributed_sync::task::EmptyTaskResult;
use stonfi_distributed_sync::traits::{DistributedHandler, TaskBatch};
use tongrid_db::entity::StateID;

#[async_trait::async_trait]
impl DistributedHandler for JettonHandler {
    type Task = JettonSyncTask;
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
        let synced_height = match self.state_id {
            StateID::JettonSync | StateID::JettonResync => from_seqno
                .saturating_add(self.max_batch_size - 1)
                .min(to_seqno),
            _ => {
                return Err(SyncCoreError::system(anyhow::anyhow!(
                    "[JettonHandler::create_tasks] unexpected state_id: {:?}",
                    self.state_id
                )));
            }
        };

        let mut tasks = vec![];
        for seqno in from_seqno..=synced_height {
            let mut seq_tasks = self
                .collect_tasks(seqno)
                .await
                .map_err(SyncCoreError::system)?
                .into_iter()
                .collect::<Vec<_>>();
            seq_tasks.sort_by(|(left_addr, _), (right_addr, _)| left_addr.cmp(right_addr));
            let mut seq_tasks = seq_tasks
                .into_iter()
                .map(|(_, task)| task)
                .collect::<Vec<Self::Task>>();
            tasks.append(&mut seq_tasks);
        }

        Ok(Some(TaskBatch::new(SyncHeight::from(synced_height), tasks)))
    }

    async fn process_task(&self, task: Self::Task) -> SyncCoreResult<Self::TaskResult> {
        self.process(task).await.map_err(SyncCoreError::system)
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
        self.max_batch_size as usize
    }

    fn sync_timeout(&self) -> std::time::Duration {
        self.task_timeout
    }
}
