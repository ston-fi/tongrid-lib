use crate::v3::{LtRange, McSeqnoRange};
use tongrid_core::math::{clamp_u32_to_i32_max, saturating_i64_from_u64};

impl McSeqnoRange {
    pub fn clamp_to_i32_supported_range(self) -> (u32, u32) {
        (
            clamp_u32_to_i32_max(self.from_mc_seqno),
            clamp_u32_to_i32_max(self.to_mc_seqno),
        )
    }
}

impl From<LtRange> for (i64, i64) {
    fn from(value: LtRange) -> Self {
        (
            saturating_i64_from_u64(value.from_lt),
            saturating_i64_from_u64(value.to_lt),
        )
    }
}
