//! Explicit conversion policies for signed storage and unsigned wire values.

/// Converts an unsigned value to signed storage, saturating at `i64::MAX`.
/// Use this for numeric bounds; use [`preserving_i64_bit_pattern_from_u64`]
/// for identifiers whose high bit must remain significant.
pub fn saturating_i64_from_u64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

/// Interprets all 64 bits as a signed value without changing the bit pattern.
/// Values above `i64::MAX` become negative, as required for stored TON shard IDs.
pub fn preserving_i64_bit_pattern_from_u64(value: u64) -> i64 {
    value as i64
}

/// Clamps an unsigned bound to the range supported by signed 32-bit storage.
/// Retains the unsigned type for callers operating on wire/query bounds.
pub fn clamp_u32_to_i32_max(value: u32) -> u32 {
    value.min(i32::MAX as u32)
}
