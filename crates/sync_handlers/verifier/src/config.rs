//! Component settings; application composition and private presets remain caller-owned.
use serde::Deserialize;
use std::time::Duration;

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct VerifierHandlerConfig {
    pub enabled: bool,
    #[serde(with = "humantime_serde")]
    pub task_timeout: Duration,
    pub max_batch_size: u32,
}

impl Default for VerifierHandlerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            task_timeout: Duration::from_secs(30),
            max_batch_size: 1,
        }
    }
}
