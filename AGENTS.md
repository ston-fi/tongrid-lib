# Tongrid library maintenance

Read [the repository contract](docs/specs/repository.md) before changing project
structure, CI, or distribution. See the
[client contract](docs/specs/client.md) and [backend contract](docs/specs/backend.md).

## Boundaries

- Keep the responsibility groups under `crates/` and explicit
  workspace membership. The public pair is `crates/public/tongrid_client` and
  `crates/public/tongrid_proto` (packages `tongrid_client` and `tongrid_proto`).
  Component Config schemas and portable defaults live in their owning libraries.
  Application aggregate schemas, secret resolution and deployment presets stay caller-owned.
  Constructors accept component configs and initialized clients.
  Shared helpers live in `crates/common/core` (`tongrid_core`): `math`, `time`,
  generic configuration utilities and sparse runtime policy. Keep core independent
  of protocol/backend crates and private dependencies.
  See [architecture](docs/architecture.md) for ownership.
- The explicitly invoked `tongrid-sync-demo` example and root `docker-compose.yml`
  are the development-only composition exception. Keep its CQL/API endpoints
  loopback-only, use public ton-rs node configuration, and never execute it in
  ordinary CI. See [demo instructions](examples/tongrid-sync-demo/README.md).
- Keep production application composition, deployment, operator documentation,
  live test orchestration, credentials, and node topology outside this repository.
  Never copy another repository's history or hidden local state.
- Client/protocol dependencies must remain independent of native emulation and
  backend dev dependencies. Shared test support is a leaf. Crates own their
  schemas, proto inputs, reviewed fixtures, and examples.
- Every package inherits Apache-2.0, the shared version, MSRV, attribution, and
  disabled publishing. Preserve third-party notices; unknown provenance blocks
  export. A future crate must explicitly document any narrower platform support.
- Default unit/doc/example runs are offline or loopback-only. No signing,
  broadcasting, chain fetch, Scylla, or S3 access in ordinary CI. Keep disposable
  service harnesses separate; `--all-features` must not activate live tests.

## Implementation and extension

Use narrow typed interfaces and explicit errors. The umbrella re-exports each whole crate under its own name. Elsewhere, do not add duplicate public
re-exports, speculative abstractions, or panics to production paths. Async workers
need explicit ownership, cancellation, bounded startup, and shutdown behavior.
Preserve transaction LT/hash identity and storage/wire compatibility across changes. Put pure Rust unit tests in `src/tests/test_*.rs`.

Update the existing specification when behavior changes. Add focused regression
tests for project-owned behavior, not constants or third-party wiring. Public
API/dependency/resource changes need downstream consumer and artifact checks in
addition to workspace tests. Review the actual final diff and report verification
gaps separately from implementation completion.

## Validation

Run the [contribution checks](docs/contributing.md) and [Rust checks and test tiers](docs/testing.md)
selected for the affected package. The single CI job runs workspace-wide Clippy,
build and unit tests; Cargo workspace membership is the coverage source.

Never publish raw scanner findings, private paths, node addresses, or credentials
in diagnostics. Preserve unrelated changes. No commit, push, tag, registry upload,
visibility change, deployment, or remote-database operation is implied by local
validation. Follow the surrounding task's Git review and acceptance workflow.

## Sender and umbrella changes

`TonMsgSender` is an open async-trait extension point at
`tongrid_ton::ton_msg_sender`. Implementations return errors safe to log and own
no detached retries. The service owns bounded concurrent rounds and request
cancellation. Additional transports are application-owned. The unconditional `tongrid_lib`
umbrella includes native backends; direct client dependencies remain lightweight.
Validate sender changes with `cargo test --locked -p tongrid_api_grpc_service_v3 --lib`,
then the relevant extended checks in docs/testing.md and the umbrella `consumer` example.

`TGTon` implements `stonfi_sync_core::sync_engine::HeightLoader` directly; keep
its stable provider ID and checked height conversion. TDA builds the canonical
genesis config from TON's upstream constant, without copying encoded data.
Callers may still supply a custom genesis config. Validate height loading with
`cargo test --locked -p tongrid_ton --lib` and the umbrella `consumer` example.

## GitHub releases

[Release policy](docs/releasing.md) uses release-plz in Git-only mode. Keep Cargo
and release-plz registry publishing disabled. All components inherit one version;
only the umbrella creates the shared `vX.Y.Z` tag/release and root changelog.
Release runs after successful Clippy, build and unit tests on trusted `main`, only
for merged release PRs. Keep explicit CI dispatch for bot-created version PRs.
Validate release changes with `actionlint`, a review of the configuration, and a
disposable release-plz update simulation covering initial and subsequent versions.
Never run a real release command merely to validate workflow changes.
