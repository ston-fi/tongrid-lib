//! TON-to-storage conversion, account/transaction access and emulation adapters.
//! Application configuration and live orchestration belong to the caller.
pub mod ser_de;
pub mod ton_db_adapter;

#[cfg(feature = "test-support")]
pub mod test_support;
