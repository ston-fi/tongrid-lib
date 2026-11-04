//! Adapter test construction, separate from generic Scylla lifecycle.
use crate::ton_db_adapter::TonDBAdapter;
use tongrid_test_utils::db::ScyllaTestEnv;
use tongrid_ton::tg_ton::TGTon;

/// Creates a local test DB and attaches the caller-owned TON provider.
/// Does not choose a network or initialize TON.
///
/// # Errors
/// Propagates setup/adapter errors.
pub async fn init_test_adapter(
    ton: &TGTon,
    genesis_config: ton::emulators::emul_bc_config::EmulBCConfig,
) -> anyhow::Result<(ScyllaTestEnv, TonDBAdapter)> {
    let (env, db) = tongrid_db::test_support::init_test_db().await?;
    let adapter = TonDBAdapter::new(db, ton.clone(), genesis_config, 1000).await?;
    Ok((env, adapter))
}
