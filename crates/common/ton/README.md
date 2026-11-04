# tongrid_ton

Owns TON extraction and monitoring over caller-initialized TON clients; caller-supplied TonMsgSender transports.

See the [backend contract](../../../docs/specs/backend.md) for configuration,
resource ownership and compatibility, and [testing](../../../docs/testing.md)
for native prerequisites and offline/disposable-service test tiers.

The canonical open sender trait is `ton_msg_sender::TonMsgSender`. Implement with
async-trait for `Arc<dyn TonMsgSender>` dispatch; errors must be safe to log. The
service owns deadlines, retry rounds and cancellation. LiteClient implements the
trait; the application owns TonAPI HTTP transport and credentials.

`tg_ton::TGTon` implements `stonfi_sync_core::sync_engine::HeightLoader` directly.
It uses the monitored height when ahead, otherwise looks up the next masterchain
block. The sync engine owns retries and cancellation. Its stable provider ID
is `TONInitiator`.
