//! Component settings; application composition and private presets remain caller-owned.
use serde::Deserialize;
use std::time::Duration;

#[derive(Clone, Debug, Deserialize)]
pub struct BackupConfig {
    pub all_backups_dst_path: String,
    pub all_backups_restore_path: String,
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct BackupHandlerConfig {
    pub batch_size: u32,
    pub block_enabled: bool,
    pub account_enabled: bool,
    pub classifier_enabled: bool,
    pub jetton_enabled: bool,
    #[serde(with = "humantime_serde")]
    pub block_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub account_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub classifier_timeout: Duration,
    #[serde(with = "humantime_serde")]
    pub jetton_timeout: Duration,
}

impl Default for BackupHandlerConfig {
    fn default() -> Self {
        Self {
            batch_size: 10,
            block_enabled: true,
            account_enabled: true,
            classifier_enabled: true,
            jetton_enabled: true,
            block_timeout: Duration::from_secs(120),
            account_timeout: Duration::from_secs(120),
            classifier_timeout: Duration::from_secs(120),
            jetton_timeout: Duration::from_secs(120),
        }
    }
}

impl BackupHandlerConfig {
    /// Selects settings for a supported backup stream before initializing resources.
    pub fn state_settings(
        &self,
        state_id: tongrid_db::entity::StateID,
    ) -> anyhow::Result<(bool, Duration)> {
        use tongrid_db::entity::StateID;
        match state_id {
            StateID::BackupBlockSync => Ok((self.block_enabled, self.block_timeout)),
            StateID::BackupAccountSync => Ok((self.account_enabled, self.account_timeout)),
            StateID::BackupClassifierSync => Ok((self.classifier_enabled, self.classifier_timeout)),
            StateID::BackupJettonSync => Ok((self.jetton_enabled, self.jetton_timeout)),
            _ => anyhow::bail!("Unsupported backup state: {state_id}"),
        }
    }
}
