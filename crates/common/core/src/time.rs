//! Unix timestamps with explicit clock errors.
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};
pub fn cur_ts_sec() -> Result<u64, SystemTimeError> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}
pub fn cur_ts_ms() -> Result<u128, SystemTimeError> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
}
