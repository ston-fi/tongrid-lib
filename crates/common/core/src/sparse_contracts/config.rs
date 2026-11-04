//! Component settings; application composition and private presets remain caller-owned.
use serde::Deserialize;
use std::collections::HashSet;
use std::num::NonZeroU32;

#[derive(Clone, Debug, Deserialize)]
/// Caller-supplied sparse storage policy. The period must fit a positive u32.
pub struct SparseContractsConfig {
    pub store_period: NonZeroU32,
    // emulate & save states just once per store_period
    pub addresses: HashSet<String>,
}
