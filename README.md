# Tongrid libraries

Rust libraries for querying, indexing and serving TON data. The workspace contains
16 component crates and the `tongrid_lib` facade. Application binaries, deployment
configuration, credentials and live-service orchestration are maintained separately.

- [`tongrid_client`](crates/public/tongrid_client/README.md): async v3 RPC client,
  shared rate limiting, backend balancing, and TON state/emulation providers.
- [`tongrid_proto`](crates/public/tongrid_proto/README.md): protobuf messages,
  generated tonic clients/servers and reflection descriptors.
- [`tongrid_core`](crates/common/core/README.md): numeric conversions, configuration
  parsing, time helpers and sparse-storage scheduling.
- [`tongrid_lib`](crates/public/tongrid_lib/README.md): all components, including
  native TON integration, storage, synchronization and gRPC services.

See the [crate map](docs/architecture.md) for backend responsibilities. Direct
client/protocol/core dependencies do not require native TON. The facade does.

## Use

Dependencies are distributed through Git; crates.io publishing is disabled.
Reviewed release PRs produce one shared `vX.Y.Z` tag and GitHub release after CI.
Set `TONGRID_LIB_REV` to a reviewed immutable commit, then add the client:

```sh
cargo add --git https://github.com/ston-fi/tongrid-lib --rev "$TONGRID_LIB_REV" tongrid_client
```

Use the same URL and revision for all Tongrid dependencies. Build prerequisites
are Rust 1.95.0 or newer and protoc 29.3; see [CONTRIBUTING](docs/contributing.md).
The client requires a caller-supplied endpoint and Tokio runtime. Its
[README](crates/public/tongrid_client/README.md) documents connection setup,
service accessors, quotas and provider guarantees.

Run an example without external services:

```sh
cargo run --locked -p tongrid_client --example loopback
```

It binds an ephemeral loopback port, receives status updates, exercises state and
emulation providers, and shuts down. Backend consumers provide component configs
and initialized clients; libraries do not select deployment presets or credentials.

## Development and compatibility

All packages inherit the version in [Cargo.toml](Cargo.toml) and use Apache-2.0. Development uses Rust
1.97.1 with MSRV 1.95.0. CI runs one Rust job on x86_64 Ubuntu 24.04. Backend builds also require the [native prerequisites](docs/testing.md#native-prerequisites).
See the [compatibility policy](docs/releasing.md#compatibility) for API versioning
and v3 wire/storage guarantees.

- [Contributing](docs/contributing.md)
- [Architecture and crate responsibilities](docs/architecture.md)
- [Protocol/client contract](docs/specs/client.md) and [backend contract](docs/specs/backend.md)
- [Testing and CI](docs/testing.md)
- [Release policy](docs/releasing.md)
- [Maintainer guidance](AGENTS.md)

See [LICENSE](LICENSE) for the Apache-2.0 license and
[dependency policy](docs/dependencies.md) for third-party attribution.

## Sync and API demo

Run [tongrid-sync-demo](examples/tongrid-sync-demo/README.md)
for a single binary with mainnet block/account/classifier/jetton sync and the v3 API.
Start its local Scylla with `docker compose up -d --wait scylla`, then run
`cargo run --locked -p tongrid_lib --example tongrid-sync-demo`.
