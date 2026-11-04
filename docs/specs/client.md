# Protocol and client contract

The workspace implements `tongrid_proto` and `tongrid_client` at the shared workspace version.
The client is `tongrid_client::tongrid_client::TongridClient` and re-exports
`tongrid_proto` without an alias. Generated protobuf messages derive `Setters`
with consuming `with_` setters and `strip_option`; construct requests from
`Default::default()` rather than handwritten request constructors.
The protocol owns every proto input/import and generates v3 tonic services plus a
reflection descriptor using protoc 29.3. Existing service paths, tags, reserved
names and enum values are preserved. Primitive numeric policies live in
`tongrid_core::math`; protocol-specific LT/seqno range adapters delegate to it.
Core generic configuration, time and sparse runtime utilities are always available,
with public dependencies and no protocol, async-runtime or backend dependency.
Component configuration schemas and metrics settings live with their owners;
application aggregate schemas and deployment presets remain caller-owned. Library lookup is content lookup by hash, without deployment-history filtering.

The client is a lightweight Tokio consumer: `ton` 0.4 without `tonlibjson` or
backend dev dependencies. Callers must set an endpoint explicitly; an omitted
endpoint fails before DNS. Startup fetches a status, then owns two background
tasks: status stream/reconnect and DNS refresh. The final client clone drop aborts
both; retained generated RPC handles do not keep background workers alive.
Status watches can coalesce updates. The OS resolver is not currently covered by
the RPC timeout; transport and quota waits are bounded by `client_timeout_ms`.

One quota covers startup, service clones, providers and reconnects. Default 1,000
RPS; zero selects 5,000; larger values warn and clamp. Cancellation while waiting
does not reserve a future slot. Quota deadline failure is `DeadlineExceeded`.
Per-backend connection concurrency remains bounded.

The `StateProvider` validates both LT and hash against returned shard-account
identity. Exact queries return the transaction's masterchain snapshot. Latest
queries capture one fully-synced snapshot before RPC and retain it despite newer
status. Negative/overflowing LT, missing fields and malformed BOCs return errors.
`EmulationProvider` declares that resolved state is required; `ContractClient`
loads exact state first, then sends custom state, preserving snapshot/balance.
Direct address/LT emulation cannot represent the hash and must not be advertised
as hash-exact. Missing code, negative balance, missing results and server rejection
return errors.

Default/all-feature tests and examples use synthetic inputs or ephemeral
loopback. No signing, broadcasting, node/Scylla/S3 access occurs. Larger balancing
stress coverage is explicitly ignored.

Validation uses wire/descriptor tests, deterministic state/quota tests, loopback
reconnect/shutdown checks, executable examples and rustdoc. Validate MSRV and
downstream compatibility when changing those contracts. Each
crate has inherited metadata and all resources in its source archive.
