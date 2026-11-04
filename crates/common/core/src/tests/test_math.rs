use crate::math::{clamp_u32_to_i32_max, saturating_i64_from_u64};

#[test]
fn test_numeric_storage_bounds_saturate_without_wrapping() {
    for (input, expected) in [
        (0, 0),
        (42, 42),
        (i64::MAX as u64, i64::MAX),
        (i64::MAX as u64 + 1, i64::MAX),
        (u64::MAX, i64::MAX),
    ] {
        assert_eq!(saturating_i64_from_u64(input), expected);
    }
    for (input, expected) in [
        (0, 0),
        (42, 42),
        (i32::MAX as u32, i32::MAX as u32),
        (i32::MAX as u32 + 1, i32::MAX as u32),
        (u32::MAX, i32::MAX as u32),
    ] {
        assert_eq!(clamp_u32_to_i32_max(input), expected);
    }
}
