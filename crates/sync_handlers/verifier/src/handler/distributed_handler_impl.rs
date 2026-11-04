use crate::handler::VerifierHandler;
use stonfi_distributed_sync::coordinator::TaskPriority;
use stonfi_distributed_sync::stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};
use stonfi_distributed_sync::stonfi_sync_core::sync_engine::SyncHeight;
use stonfi_distributed_sync::task::{EmptyTaskResult, RangeTask};
use stonfi_distributed_sync::traits::{DistributedHandler, TaskBatch};

#[async_trait::async_trait]
impl DistributedHandler for VerifierHandler {
    type Task = RangeTask;
    type TaskResult = EmptyTaskResult;

    fn id(&self) -> &str {
        &self.sync_id
    }

    async fn create_tasks(
        &self,
        from: SyncHeight,
        to: SyncHeight,
    ) -> SyncCoreResult<Option<TaskBatch<Self::Task>>> {
        let task = RangeTask { from, to };

        Ok(Some(TaskBatch::new(task.to, vec![task])))
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

    fn is_service_task(&self) -> bool {
        true
    }
}
