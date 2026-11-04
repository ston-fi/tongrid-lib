# Protocol maintenance

Own all `proto/` inputs here. Preserve v3 service/message paths, field numbers,
reserved tags/names and enum values. `GetLibraries` is content lookup, with no
historical deployment filtering. Keep LT/seqno range adapters here and primitive
numeric policies in `tongrid_core::math`; do not add a core dependency on protocol
types or introduce convenience re-exports. Generated code is produced by protoc
29.3 and tonic/prost during the build, never copied from an unreviewed artifact.
Generate message builders with `derive_setters::Setters`, `with_` setters and
`strip_option` in `build.rs`.

Run `cargo test --locked -p tongrid_proto --lib`,
`cargo test --locked -p tongrid_proto --doc`, and the root
[testing guide](../../../docs/testing.md). Wire changes need the descriptor and
fixed-byte tests in `src/tests/test_wire.rs`, source archive inputs, and downstream
consumer validation. No live services belong in this crate's tests/examples.
