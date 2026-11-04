//! Component settings; application composition and private presets remain caller-owned.
//! Portable metrics configuration; server lifecycle is caller-owned.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricsConfig {
    pub listen_address: String,
}
