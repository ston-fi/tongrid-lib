use crate::tg_ton::TGTon;
use std::sync::Arc;
use stonfi_sync_core::errors::SyncCoreError;
use stonfi_sync_core::sync_engine::HeightLoader;
use ton::block_tlb::BlockInfo;
use ton::emulators::emulator_pool::EmulatorPool;
use ton::lite_client::LiteClient;
use ton::net_config::{TonNetConfig, TON_NET_CONF_MAINNET_PUBLIC};

fn offline_ton(seqno: u32) -> anyhow::Result<TGTon> {
    let mut network = TonNetConfig::new(TON_NET_CONF_MAINNET_PUBLIC)?;
    network.lite_endpoints.clear();
    let client = LiteClient::builder()?.with_net_config(network).build()?;
    let pool = EmulatorPool::builder()?.with_threads_count(0).build()?;
    Ok(TGTon::new(
        client,
        pool,
        Arc::new(BlockInfo {
            seqno,
            ..BlockInfo::default()
        }),
    ))
}

#[tokio::test]
async fn test_cached_height_avoids_network_lookup() -> anyhow::Result<()> {
    let mut ton = offline_ton(42)?;
    // No endpoints exist: a regression into the lookup path cannot complete.
    let height =
        tokio::time::timeout(std::time::Duration::from_secs(1), ton.latest_height(41)).await??;
    assert_eq!(height, 42);
    Ok(())
}

#[tokio::test]
async fn test_unrepresentable_next_height_fails_before_lookup() -> anyhow::Result<()> {
    let mut ton = offline_ton(42)?;
    for after in [u64::from(u32::MAX), u64::from(u32::MAX) + 1, u64::MAX] {
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(1), ton.latest_height(after))
                .await?;
        assert!(matches!(result, Err(SyncCoreError::InvalidArgs(_))));
    }
    Ok(())
}
