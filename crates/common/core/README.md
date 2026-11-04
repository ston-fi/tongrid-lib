# tongrid_core

Generic `math`, Unix `time`, configuration parsing and sparse-storage scheduling.
The crate has no protocol/backend/native dependencies and no feature switches.
It inherits Apache-2.0, the shared workspace version and MSRV; registry publishing
is disabled.

`math` saturates numeric bounds and preserves signed shard bit patterns.
`config::read_config` merges caller defaults, an optional YAML file, and environment
values in that order. A single underscore separates the prefix; double underscores
separate nested keys. Errors omit configuration contents and parser sources.
`config::resolve_secret` resolves one environment variable lookup and otherwise
preserves the literal; errors do not expose non-Unicode values.

`SparseContracts::new` takes an explicit `NonZeroU32` period and address set.
The component schema is `sparse_contracts::config::SparseContractsConfig`; it
rejects zero/overflowing periods and has no deployment preset or Default.
`metrics::MetricsConfig` carries caller-provided listener settings. Application
aggregate schemas, sparse presets, logger initialization and signal handling remain caller-owned.

```sh
cargo run --locked -p tongrid_core --example configuration
cargo test --locked -p tongrid_core --lib
cargo test --locked -p tongrid_core --doc
```

The example uses a temporary file and inert values. Run the root
[testing guide](../../../docs/testing.md) for source/dependency changes.
