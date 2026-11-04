# Shared core maintenance

Core owns primitive `math`, build metadata, and always-available
generic configuration utilities, time helpers and sparse runtime policy. Keep these modules unconditional and
use public dependencies only.
Never depend on protocol/client/backend crates, application crates, storage drivers,
native emulation or async runtimes. Application logger/signal setup stays caller-owned.

Preserve numeric-bound saturation versus shard bit preservation, config precedence
and secret-safe errors. Sparse periods remain positive u32 values. MetricsConfig and SparseContractsConfig are component-owned; application aggregate
schemas and sparse presets stay caller-owned; checkpoint semantics remain stable. Do not add root aliases or duplicate implementations.

Unit tests live in `src/tests/test_*.rs`; test boundaries, precedence, redaction and
schedule decisions rather than compiler guarantees. Run the commands in
[README](README.md) and the root [testing guide](../../../docs/testing.md), including
default/no-default builds and the offline configuration example.
Review exact exported source/artifacts; no private resources or history belongs here.
