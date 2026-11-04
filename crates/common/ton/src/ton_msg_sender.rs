//! Caller-supplied TON message broadcast transports.

/// Broadcasts a serialized message. Acceptance does not guarantee chain inclusion.
///
/// Implementations must be safe to call concurrently and return errors safe to log
/// (no credentials, endpoint URLs or message payloads). The caller owns deadlines
/// and retries. Dropping a call must release its local work; an already submitted
/// message may still be delivered. No detached retry loop should outlive the call.
#[async_trait::async_trait]
pub trait TonMsgSender: Send + Sync {
    /// Attempts one broadcast of the supplied BOC, returning transport failures.
    async fn send_msg(&self, boc: &[u8]) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl TonMsgSender for ton::lite_client::LiteClient {
    async fn send_msg(&self, boc: &[u8]) -> anyhow::Result<()> {
        self.send_msg(boc.to_vec(), None).await?;
        Ok(())
    }
}
