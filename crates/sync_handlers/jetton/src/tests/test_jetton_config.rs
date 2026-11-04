use crate::config::JettonHandlerConfig;
use anyhow::Result;
use std::time::Duration;
use stonfi_distributed_sync::coordinator::TaskPriority;
use tongrid_db::entity::StateID;

fn test_config() -> JettonHandlerConfig {
    JettonHandlerConfig {
        enabled: false,
        resync_enabled: true,
        task_timeout: Duration::from_secs(30),
        max_batch_size: 5,
    }
}

#[test]
fn test_jetton_resync_uses_resync_config() -> Result<()> {
    let (enabled, task_priority) = test_config().state_settings(StateID::JettonResync)?;

    assert!(enabled);
    assert_eq!(task_priority, TaskPriority::Low);
    Ok(())
}
