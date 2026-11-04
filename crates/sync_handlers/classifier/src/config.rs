//! Component settings; application composition and private presets remain caller-owned.
use serde::Deserialize;
use std::time::Duration;

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct ClassifierHandlerConfig {
    pub enabled: bool,
    pub resync_enabled: bool,
    #[serde(with = "humantime_serde")]
    pub task_timeout: Duration,
    pub max_batch_size: u32,
}

impl Default for ClassifierHandlerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            resync_enabled: false,
            task_timeout: Duration::from_secs(30),
            max_batch_size: 1,
        }
    }
}

impl ClassifierHandlerConfig {
    pub(crate) fn state_settings(
        &self,
        state_id: tongrid_db::entity::StateID,
    ) -> anyhow::Result<(bool, stonfi_distributed_sync::coordinator::TaskPriority)> {
        use stonfi_distributed_sync::coordinator::TaskPriority;
        use tongrid_db::entity::StateID;
        match state_id {
            StateID::ClassifierSync => Ok((self.enabled, TaskPriority::High)),
            StateID::ClassifierResync => Ok((self.resync_enabled, TaskPriority::Low)),
            _ => anyhow::bail!("Unsupported handler state: {state_id}"),
        }
    }
}
