# tongrid_lib

A complete, unconditional facade over the 16 Tongrid component crates. It adds
no types, aliases, defaults or Cargo features. Each crate retains its original
name, such as `tongrid_lib::tongrid_ton::ton_msg_sender::TonMsgSender`.

This dependency includes native TON, storage backends and inert test-support
capabilities. A lightweight RPC application should use `tongrid_client` directly.
Application composition, credentials and presets stay caller-owned.

`examples/consumer.rs` implements a sender and constructs public component configs
without network calls:

```sh
cargo run --locked -p tongrid_lib --example consumer
```

Registry publishing is disabled. The facade and component crates share the
workspace version, license and MSRV. See the repository testing contract for
native build prerequisites.

The explicitly run [tongrid-sync-demo](../../../examples/tongrid-sync-demo/README.md) composes
mainnet synchronization and the v3 API in one binary with a local Compose database.
It is a live development example; CI compiles it without running it.
