# tongrid_client

Async Tongrid v3 client with generated service accessors, shared rate limiting,
DNS backend balancing, and separate TON `StateProvider` and `EmulationProvider`
implementations. Requires a Tokio runtime. It uses `ton` 0.4 without native
`tonlibjson`; no node, database, or emulator is initialized locally.

Use a reviewed Git revision from `ston-fi/tongrid-lib`; registry publication is
disabled. Build prerequisites are Rust 1.95.0 or newer and protoc 29.3; see the
[workspace instructions](../../../docs/contributing.md). There are no crate-level optional
features. Backend/native features must not leak through development dependencies.

```rust,no_run
use tongrid_client::tongrid_client::TongridClient;
use ton::contracts::ContractClient;

# async fn connect(endpoint: String) -> Result<(), Box<dyn std::error::Error>> {
let client = TongridClient::builder("my-indexer")?
    .with_tongrid_url(endpoint)
    .with_client_timeout_ms(10_000)
    .with_rate_limit(100)
    .build().await?;
let contracts = ContractClient::builder(client.clone(), client.clone())?.build()?;
let mut updates = client.last_sync_status_stream();
updates.changed().await?;
let synced = updates.borrow_and_update().full_synced_seqno;
client.wait_seqno(synced, None).await?;
# drop(contracts);
# Ok(())
# }
```

The caller supplies the endpoint. Omitting it returns `InvalidArguments` before
DNS/network access. `build` fetches an initial sync status before returning, then
owns status/reconnect and DNS refresh tasks. Dropping one clone keeps them alive;
dropping the final client clone aborts both. Generated service handles can outlive
the client, but do not own its background status workers. The watch stream retains
only the latest value; slow consumers may miss intermediate statuses.

The default quota is 1,000 requests/second, shared across clones and generated
services, including initial status and stream reconnect calls. Explicit zero
selects 5,000; higher values warn and clamp to 5,000. Quota waits count toward
`client_timeout_ms`, return `DeadlineExceeded` on expiration, and do not reserve
capacity after cancellation. Default concurrency is 500 per backend, with an
8 MiB response limit and a 60-second DNS refresh interval. Startup DNS resolution
uses the operating system resolver; transport/quota timeouts do not bound that
resolver call.

`StateProvider` checks both LT and hash for exact state queries and returns that
transaction's masterchain seqno. Latest queries capture one fully-synced snapshot
and return the same snapshot even if status advances. Missing/malformed state and
negative/overflowing transaction identifiers fail. `EmulationProvider` requires
resolved custom state for exact history; `ContractClient` performs this resolution.
Custom-state emulation preserves its snapshot and balance. A direct address/LT
emulation request cannot carry a hash; use the resolved-state path for exact
identity. Server rejection details remain typed TON errors.

Generated services are available under
`tongrid_client::tongrid_proto` path or from the protocol crate.
`library().get_libraries` returns stored code by hash; it makes no claim about
historical deployment and ignores deprecated seqno filtering.

Run the complete offline/loopback example:

```sh
cargo run --locked -p tongrid_client --example loopback
```

Tests cover transport, state identity/snapshots, malformed responses, quota
cancellation/deadlines, reconnect and final-clone shutdown. The retained large
balancing stress experiment is explicitly ignored; it does not contact a chain.
