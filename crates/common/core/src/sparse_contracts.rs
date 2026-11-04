use std::collections::HashSet;
use std::num::NonZeroU32;

#[derive(Clone, Debug)]
/// Caller-owned sparse storage schedule.
pub struct SparseContracts {
    // if mc_seqno % store_period == 0, then save state
    pub store_period: NonZeroU32,
    // emulate & save states just once per store_period
    pub addresses: HashSet<String>,
}

impl SparseContracts {
    pub fn new(store_period: NonZeroU32, addresses: HashSet<String>) -> Self {
        Self {
            store_period,
            addresses,
        }
    }

    pub fn is_sparse_at_height(&self, addr: &str, mc_seqno: u32) -> bool {
        self.addresses.contains(addr) && !mc_seqno.is_multiple_of(self.store_period.get())
    }

    pub fn exists_at_height(&self, addr: &str, mc_seqno: u32) -> u32 {
        if self.addresses.contains(addr) {
            self.last_filled_height(mc_seqno)
        } else {
            mc_seqno
        }
    }

    pub fn last_filled_height(&self, mc_seqno: u32) -> u32 {
        mc_seqno - (mc_seqno % self.store_period.get())
    }
}

pub mod config;
