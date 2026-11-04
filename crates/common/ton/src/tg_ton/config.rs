//! Component settings; application composition and private presets remain caller-owned.
use serde::Deserialize;
use std::time::Duration;
use ton::net_config::TON_NET_CONF_MAINNET_PUBLIC;

#[derive(Clone, Debug, Deserialize)]
pub struct LiteClientConfig {
    pub mainnet: bool,
    pub network_config: String,
    pub connections_per_node: u32,
    pub retry_count: u32,
    #[serde(with = "humantime_serde")]
    pub retry_waiting: Duration,
    #[serde(with = "humantime_serde")]
    pub query_timeout: Duration,
}

impl Default for LiteClientConfig {
    fn default() -> Self {
        Self {
            mainnet: true,
            network_config: TON_NET_CONF_MAINNET_PUBLIC.to_string(),
            connections_per_node: 1,
            retry_count: 20,
            retry_waiting: Duration::from_millis(400),
            query_timeout: Duration::from_millis(5000),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct TonConfig {
    pub lite: LiteClientConfig,
    pub config_cache_size: u64,
}
