# Backend contract

The workspace contains 16 component packages and the unconditional umbrella.
Applications own production composition and deployment; the
[demo](../../examples/tongrid-sync-demo/README.md) shows development setup.

## Ownership boundary

Component configuration schemas, serde shapes, portable defaults and handler
state selection belong to the corresponding libraries. Constructors accept these
configs directly. Application aggregate schemas, CLI, network/bootstrap composition,
credential resolution and deployment configuration belong to the caller. Core owns MetricsConfig and
SparseContractsConfig; the latter requires explicit addresses and period and has
no deployment preset or Default. TreeLimits has one shared serde/default definition.
Blockchain ConfigParams and EmulBCConfig remain domain data.

Callers supply classifier predefined-code lookup and account-handler critical-contract presets.
TON callers supply initialized LiteClient/EmulatorPool and an
initial block. TDA's `ton_db_adapter::config::genesis_config` builds the canonical
zero-state configuration from `ton::ton_core::constants::TON_ZERO_CONFIG_BOC_B64`,
without a copied payload. TDA callers supply this or a custom genesis configuration
and cache capacity. Libraries must not depend on application crates.

## Constructors and compatibility

Protocol and backup protobuf inputs, CQL migrations, persisted field conversions,
transaction LT/hash identity and normal successful algorithms are preserved.
Changing these formats requires explicit compatibility and data-migration review.

Component constructors accept config references alongside initialized resources.
DB accepts a caller-initialized Scylla client. FileManager wraps caller-owned
FsOps. GrpcServer accepts its config, optional initialized forwarding layer and
reflection descriptor; add_service returns an error after startup. TON bootstrap,
secret resolution, deployment presets and forwarding connection setup remain app-owned.
Consumers pin one coherent source revision; see the
[compatibility policy](../releasing.md#compatibility).

Forwarding is best-effort: the primary request drives body reads and never waits
for the shadow. A full 16-frame shadow queue cancels that shadow request. At most
128 shadow requests run per layer (shared by clones), each with a 30-second
limit. Early primary-body cancellation or dropping the last layer cancels its
workers; after body EOF the layer owns forwarding through completion or timeout.
S3 prefetches are cancelled when replaced, discarded, or when their owner or
waiting read is dropped.

## Message broadcast

The open `tongrid_ton::ton_msg_sender::TonMsgSender: Send + Sync` interface uses
async-trait and `send_msg(&self, boc: &[u8]) -> anyhow::Result<()>`. LiteClient
implements it; applications own additional transports, endpoint selection and tokens.
Implementations return errors safe to log, with no credential-bearing URLs or BOCs.

ServiceContext accepts a nonempty Vec of Arc trait objects and ServicesConfig.
It rejects zero attempt_timeout; omitted send_msg_service defaults to 2 seconds.
The request validates its BOC and returns the existing normalized message hash.
Zero request timeout performs no broadcast. Every round calls every sender
concurrently, bounded by the smaller of attempt_timeout and remaining request time.
Every failure/timeout warns. The round awaits all calls even after a success;
any success makes the request successful. All-failed rounds retry after 100 ms,
clipped to the remaining deadline. Terminal all-sender failure logs one error and
returns TimedOut (gRPC DeadlineExceeded). No provider-specific error-string rule
can override another sender's success. Cancellation drops all request-owned calls;
there are no detached retries. Transport acceptance does not imply chain inclusion.

## Storage and worker compatibility

TGTon directly implements stonfi_sync_core's HeightLoader. It returns the monitored
height when ahead of the caller's checkpoint, otherwise looks up the next
masterchain block. Unrepresentable next heights return InvalidArgs and lookup
failures remain NetError. Its stable provider ID is `TONInitiator`. The sync engine owns retry scheduling
and cancellation.

Backup ProtoSerial::from_proto returns Result. Invalid hashes, trait labels and
numeric balance strings fail before insertion, rather than panicking or silently
dropping invalid balances. Signed storage bit representations remain unchanged.
DBMsg::default preserves the exact empty-message wire bytes, checked against TON
serialization. TON monitor workers and the memory-store updater stop when their
last external owner is dropped; dropping an intermediate store clone cannot stop
updates. BlocksProvider also owns and cancels its producer during failed or
pending fetches; zero buffer/batch bounds return errors. Application sender implementations must omit credential-bearing endpoint URLs
from errors returned to the library.

## One-process demo

The [sync demo](../../examples/tongrid-sync-demo/README.md)
runs mainnet block, account, classifier and jetton stages, with an in-process
coordinator/worker and automatically started v3 API. It checks loopback Scylla
using CQL before contacting TON, exits with Compose instructions when unavailable,
and uses only public node configuration supplied by ton-rs. First startup seeds
the current masterchain checkpoint and its key block; restarts preserve progress.
It excludes backup, verifier, resync and account-fix stages. Hardcoded development
settings and the Compose lifecycle belong to the demo, not library constructors.
Live execution is manual; CI only compiles it. The demo README documents its
local offline regression test.

## Source, resources and dependencies

DB owns byte-preserved CQL migrations; backup owns byte-preserved protobuf inputs.
No include escapes its crate. Library fixtures are synthetic.
The TON storage-price adaptation retains TON Core's complete MIT notice.
Registry publishing is disabled for every package.

Native integration uses ton 0.4 and locked tonlib-sys 2026.10.0. Linux requires
Clang 21; macOS requires the upstream native dependencies. Source distributions
exclude native build caches.
The client dependency graph must remain free of tonlib-sys/backend packages.

See [testing](../testing.md) for validation tiers and [dependency policy](../dependencies.md)
for exact legacy source pins and maintenance advisory dispositions.
