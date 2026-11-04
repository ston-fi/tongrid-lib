# tongrid_tda

Owns TON-to-storage conversion, account/transaction access and emulation adapters.

See the [backend contract](../../../docs/specs/backend.md) for configuration,
resource ownership and compatibility, and [testing](../../../docs/testing.md)
for native prerequisites and offline/disposable-service test tiers.

`ton_db_adapter::config::genesis_config()` supplies the canonical zero-state
emulation config from TON's upstream constant. Pass it to `TonDBAdapter::new`,
or supply a custom genesis config. No copied genesis payload is embedded here.
