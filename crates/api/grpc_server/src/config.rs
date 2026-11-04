//! Component settings; application composition and private presets remain caller-owned.
use serde::{Deserialize, Serialize};

const DEFAULT_MAX_CONCURRENT_STREAMS: u32 = 50;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GrpcServerConfig {
    pub listen_address: String,
    /// Maximum number of concurrent HTTP/2 streams accepted per connection.
    #[serde(default = "default_max_concurrent_streams")]
    pub max_concurrent_streams: u32,
    pub forward: ForwardConfig,
}

fn default_max_concurrent_streams() -> u32 {
    DEFAULT_MAX_CONCURRENT_STREAMS
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForwardConfig {
    pub address: String,
    pub percentage: f64,
    pub enabled: bool,
}
