//! Component settings; application composition and private presets remain caller-owned.
use serde::Deserialize;
use std::collections::HashSet;
use std::time::Duration;
use ton::ton_core::cell::TonHash;

#[derive(Clone, Debug, Deserialize)]
pub struct AccountHandlerConfig {
    pub enabled: bool,
    pub resync_enabled: bool,
    pub fix_enabled: bool,
    pub max_batch_size: u32,
    pub fix_batch_size: u32,
    #[serde(with = "humantime_serde")]
    pub task_timeout: Duration,
    pub latest_state_cache_size: u64,
    #[serde(with = "humantime_serde")]
    pub slow_emulation_warn_threshold: Duration,
    #[serde(with = "humantime_serde")]
    pub gen_address_states_timeout: Duration,
    pub sequential_emulation: bool,
    #[serde(with = "humantime_serde")]
    pub get_state_from_bc_timeout: Duration,
    #[serde(skip)]
    pub critical_contracts_override: Option<HashSet<TonHash>>,
}

impl Default for AccountHandlerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            resync_enabled: false,
            fix_enabled: false,
            max_batch_size: 10,
            fix_batch_size: 500,
            task_timeout: Duration::from_secs(120),
            latest_state_cache_size: 10_000,
            slow_emulation_warn_threshold: Duration::from_millis(200),
            gen_address_states_timeout: Duration::from_secs(60),
            sequential_emulation: false,
            get_state_from_bc_timeout: Duration::from_secs(5),
            critical_contracts_override: None,
        }
    }
}

impl AccountHandlerConfig {
    pub(crate) fn state_settings(
        &self,
        state_id: tongrid_db::entity::StateID,
    ) -> anyhow::Result<(bool, stonfi_distributed_sync::coordinator::TaskPriority)> {
        use stonfi_distributed_sync::coordinator::TaskPriority;
        use tongrid_db::entity::StateID;
        match state_id {
            StateID::AccountSync => Ok((self.enabled, TaskPriority::High)),
            StateID::AccountResync => Ok((self.resync_enabled, TaskPriority::Low)),
            StateID::AccountFixSync => Ok((self.fix_enabled, TaskPriority::Low)),
            _ => anyhow::bail!("Unsupported account state: {state_id}"),
        }
    }
}
