use serde::Deserialize;
use tongrid_core::config::read_config;
use tongrid_core::sparse_contracts::SparseContracts;

#[derive(Deserialize)]
struct Settings {
    poll_interval_ms: u64,
    store_period: std::num::NonZeroU32,
    addresses: std::collections::HashSet<String>,
}

fn main() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let file = directory.path().join("config.yaml");
    std::fs::write(&file, "poll_interval_ms: 250")?;
    let path = file
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("invalid example path"))?;
    let defaults = "poll_interval_ms: 100\nstore_period: 10\naddresses: [example-account]";
    let settings: Settings = read_config(path, defaults, "TONGRID_CORE_EXAMPLE")?;
    println!(
        "Poll interval: {} ms; stored checkpoint for height 27: {}",
        settings.poll_interval_ms,
        SparseContracts::new(settings.store_period, settings.addresses)
            .exists_at_height("example-account", 27),
    );
    Ok(())
}
