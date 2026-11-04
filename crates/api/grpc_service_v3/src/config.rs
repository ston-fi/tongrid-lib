//! Component settings; application composition and private presets remain caller-owned.
use crate::tx_tree::TreeLimits;
use serde::Deserialize;
use std::num::NonZeroUsize;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct ServicesConfig {
    pub send_msg_service: SendMsgServiceConfig,
    pub tx_service: TxServiceConfig,
    pub emulation_tree_limits: TreeLimits,
    pub jetton_service: BatchServiceConfig,
    pub library_service: BatchServiceConfig,
}

#[derive(Debug, Copy, Clone, Deserialize)]
#[serde(default)]
pub struct BatchServiceConfig {
    pub max_batch_size: usize,
    pub concurrency: NonZeroUsize,
}

impl Default for BatchServiceConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000,
            concurrency: NonZeroUsize::MIN.saturating_add(127),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct TxServiceConfig {
    pub get_initial_tx_from_tx_chain: bool,
    pub tree_limits: TreeLimits,
}

/// Bounds each concurrent broadcast round independently of the request deadline.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SendMsgServiceConfig {
    #[serde(with = "humantime_serde")]
    pub attempt_timeout: std::time::Duration,
}

impl Default for SendMsgServiceConfig {
    fn default() -> Self {
        Self {
            attempt_timeout: std::time::Duration::from_secs(2),
        }
    }
}

impl SendMsgServiceConfig {
    /// Rejects an attempt bound that would prevent any sender from being polled.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.attempt_timeout.is_zero(),
            "send_msg_service.attempt_timeout must be positive"
        );
        Ok(())
    }
}
