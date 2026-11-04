# Client maintenance

Read the [client contract](../../../docs/specs/client.md). Preserve public module
paths and generated accessor transports; do not add facade aliases. Keep `ton`
aligned with the application's reviewed version and never enable `tonlibjson` in
this crate or its dev graph. State and emulation are separate capabilities.

Keep exact LT/hash checks, captured latest snapshots, custom-state balance,
shared clone/service quota and last-client-drop worker ownership. New workers
need explicit cancellation and bounded operations. Errors must remain typed;
examples return `Result`. Endpoints are caller supplied. Tests may use synthetic
BOCs and ephemeral loopback services; no chain/default production endpoint.
Pure unit tests belong in `src/tests/test_*.rs`; transport contracts belong in
integration tests. Keep the explicitly ignored balancing experiment out of fast CI.

After changes, run `cargo test --locked -p tongrid_client --all-targets`,
`cargo test --locked -p tongrid_client --doc`, and
`cargo run --locked -p tongrid_client --example loopback`. Complete the root
[testing guide](../../../docs/testing.md), including downstream checks for API changes.
Update README/rustdoc/specifications with behavioral changes.
