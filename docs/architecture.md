# Architecture

The explicit workspace contains 16 component crates and one facade. Component configuration schemas and portable defaults belong to their owning
libraries. Application aggregate schemas, operational presets and process composition
remain caller-owned. See the [backend contract](specs/backend.md) for constructor and
resource boundaries.

| Area                 | Package directories under `crates/`                | Responsibility                                                            |
| -------------------- | -------------------------------------------------- | ------------------------------------------------------------------------- |
| Facade               | `public/tongrid_lib`                               | Unconditional access to all components, including native backends         |
| Protocol/client      | `public/tongrid_proto`, `public/tongrid_client`    | Protobuf types and lightweight Rust client; no native emulator dependency |
| Shared foundation    | `common/core`, `common/test_utils`                 | Generic helpers and parsing; leaf fixture/disposable-service support      |
| Server/TON           | `api/grpc_server`, `common/ton`                    | gRPC lifecycle/middleware and explicit TON/native integration             |
| Persistence/adapter  | `common/db`, `common/tda`                          | Schema, query/storage behavior, and TON-to-storage conversion             |
| Synchronization      | `sync_handlers/block`, `sync_handlers/account`     | Block/account synchronization and resumable progress                      |
| Classification       | `sync_handlers/classifier`, `sync_handlers/jetton` | Contract classification and jetton state handling                         |
| Archive/verification | `sync_handlers/backup`, `sync_handlers/verifier`   | Backup compatibility and verification logic                               |
| API state/services   | `api/mem_store`, `api/grpc_service_v3`             | Readiness, in-memory state, and v3 service implementation                 |

The client consumes protocol types and provider interfaces. Protocol range
adapters depend on `tongrid_core::math`; core must never depend on protocol types. Server/service
composition, handlers, adapters, and persistence form the backend side. Backend
development dependencies must not leak native requirements into client builds.
Test helpers depend downward on reusable interfaces; they must not initialize a
complete application or synchronize a live chain.

Each crate owns all resources necessary to compile, package, and test it. The DB
crate owns migrations. The classifier receives caller-owned predefined-code data.
Protocol and backup crates own their protobuf inputs. No include or runtime
default may depend on a sibling checkout or operator configuration.

Application binaries, operational load tools, live end-to-end orchestration,
deployment secrets, and environment configuration stay in the separately
maintained application. That application's library dependencies must use one
canonical public repository URL and a single common immutable revision, avoiding
duplicate Rust type identities from mixed dependency sources.

The package `tongrid_lib` at `crates/public/tongrid_lib` unconditionally re-exports
all 16 component crates without aliases or feature switches. It includes native
backends and enables the existing inert DB/TDA test-support and Scylla helper
capabilities. Underlying crates never depend on the umbrella. Backend applications can use
this dependency; standalone clients can use `tongrid_client` directly.
