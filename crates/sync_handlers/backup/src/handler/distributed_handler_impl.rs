use crate::handler::task::BackupSyncTask;
use crate::handler::BackupHandler;
use stonfi_distributed_sync::coordinator::TaskPriority;
use stonfi_distributed_sync::stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};
use stonfi_distributed_sync::stonfi_sync_core::sync_engine::SyncHeight;
use stonfi_distributed_sync::task::EmptyTaskResult;
use stonfi_distributed_sync::traits::{DistributedHandler, TaskBatch};

#[async_trait::async_trait]
impl DistributedHandler for BackupHandler {
    type Task = BackupSyncTask;
    type TaskResult = EmptyTaskResult;

    fn id(&self) -> &str {
        &self.sync_id
    }

    async fn create_tasks(
        &self,
        from: SyncHeight,
        to: SyncHeight,
    ) -> SyncCoreResult<Option<TaskBatch<Self::Task>>> {
        let batch_size = u64::from(self.batch_size);
        let batch_end = from
            .checked_div(batch_size)
            .and_then(|batch| batch.checked_add(1))
            .and_then(|batch| batch.checked_mul(batch_size))
            .and_then(|end| end.checked_sub(1))
            .ok_or_else(|| SyncCoreError::invalid_args("backup batch range overflow"))?;
        if batch_end > to {
            return Ok(None);
        }
        let synced_height = batch_end.min(to);
        if synced_height < from {
            return Err(SyncCoreError::logic(format!(
                "[BackupHandler::create_tasks] invalid synced height {synced_height} for range [{from}, {to}]"
            )));
        }

        let task = BackupSyncTask {
            mc_seqno_from: u32::try_from(from).map_err(SyncCoreError::invalid_args)?,
            mc_seqno_to: u32::try_from(synced_height).map_err(SyncCoreError::invalid_args)?,
            state_id: self.state_id.to_string(),
        };

        Ok(Some(TaskBatch::new(synced_height, vec![task])))
    }

    async fn process_task(&self, task: Self::Task) -> SyncCoreResult<Self::TaskResult> {
        self.process(task).await.map_err(SyncCoreError::system)
    }

    fn task_priority(&self) -> TaskPriority {
        TaskPriority::Lowest
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn min_batch_size(&self) -> usize {
        1
    }

    fn max_batch_size(&self) -> usize {
        self.batch_size as usize
    }

    fn sync_timeout(&self) -> std::time::Duration {
        self.task_timeout
    }

    fn is_service_task(&self) -> bool {
        true
    }
}
