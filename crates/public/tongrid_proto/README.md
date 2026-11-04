# tongrid_proto

Tongrid v3 protobuf messages, generated tonic clients/servers, and the reflection
file-descriptor set `TONGRID_API_GRPC_DESCRIPTOR`. Schema inputs and their imports
are owned by this crate under `proto/`. No database, node, or native TON dependency
is required. Requires Rust 1.95.0 or newer and protoc 29.3; use the
[workspace installer](../../../docs/contributing.md).

Rustdoc source embedding is disabled for this generated-code crate: generated
source links can contain local build paths. The schemas and helper sources remain
available in Git. The CI artifact check rejects developer/home paths in docs.

Use the generated modules under `tongrid_proto::v3`, such as
`library_grpc_client::LibraryGrpcClient` and
`sync_status_grpc_client::SyncStatusGrpcClient`. Supply your own tonic `Channel`;
there is no built-in endpoint. The crate-level rustdoc demonstrates library lookup.

All generated message structs support consuming `with_<field>` setters through
`derive_setters`, starting from `Default::default()`. Optional field setters accept
the inner value:

```rust
use tongrid_proto::v3::SendMsgReturnHashRequest;

let request = SendMsgReturnHashRequest::default()
    .with_body(vec![1, 2, 3])
    .with_timeout_ms(1000);
```

`GetLibraries` is content lookup by hash, including ordinary contract/private
library cells. Unknown hashes are omitted; known duplicates and order are retained
by the server contract. Deprecated seqno filtering is ignored. This protocol does
not certify deployment or fetch missing code. Emulation resolves libraries by
content; removed fields remain reserved.

All service paths, message field numbers, reserved tags/names, enum values and
range adapters preserve the reviewed v3 wire contract. Primitive conversions
live in `tongrid_core::math`. Changing a schema
requires descriptor/wire regression checks and consumer compatibility review.
All proto imports must be included in Git/source archives. No optional features
or native build tools are required beyond protoc. Registry publishing is disabled.

```sh
cargo test --locked -p tongrid_proto --lib
cargo test --locked -p tongrid_proto --doc
```
