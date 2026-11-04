use crate::config::SendMsgServiceConfig;
use crate::context::{validate_senders, ServiceContext};
use crate::error::GrpcServiceError;
use crate::error::GrpcServiceError::InvalidArgument;
use futures::future::join_all;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep_until, timeout_at, Instant};
use ton::block_tlb::Msg;
use ton::ton_core::cell::TonCell;
use ton::ton_core::traits::tlb::TLB;
use tongrid_proto::v3::{SendMsgReturnHashRequest, SendMsgReturnHashResponse};
use tongrid_ton::ton_msg_sender::TonMsgSender;

const RETRY_INTERVAL: Duration = Duration::from_millis(100);

pub struct SendMsgService {
    ctx: Arc<ServiceContext>,
}

impl SendMsgService {
    pub fn new(ctx: Arc<ServiceContext>) -> Self {
        Self { ctx }
    }

    pub async fn send_msg_return_hash(
        &self,
        req: SendMsgReturnHashRequest,
    ) -> Result<SendMsgReturnHashResponse, GrpcServiceError> {
        send_with_senders(
            &self.ctx.msg_senders,
            &self.ctx.config.send_msg_service,
            req,
        )
        .await
    }
}

// Separate from DB/TON composition so tests exercise the actual request boundary.
pub(crate) async fn send_with_senders(
    senders: &[Arc<dyn TonMsgSender>],
    config: &SendMsgServiceConfig,
    req: SendMsgReturnHashRequest,
) -> Result<SendMsgReturnHashResponse, GrpcServiceError> {
    let msg = Msg::<TonCell>::from_boc(req.body.clone())
        .map_err(|err| InvalidArgument(format!("Invalid TON message BOC: {err}")))?;
    let msg_hash = msg.cell_hash_normalized()?.into_vec();
    validate_senders(senders, config)?;
    let deadline = Instant::now() + Duration::from_millis(u64::from(req.timeout_ms));
    let body = req.body.as_slice();
    let mut attempt = 0_u64;
    while Instant::now() < deadline {
        attempt += 1;
        let round_deadline = Instant::now()
            .checked_add(config.attempt_timeout)
            .unwrap_or(deadline)
            .min(deadline);
        // Await every bounded call: a fast success must not cancel its peers.
        let results = join_all(
            senders
                .iter()
                .enumerate()
                .map(|(index, sender)| async move {
                    let result = timeout_at(round_deadline, sender.send_msg(body)).await;
                    match result {
                        Ok(Ok(())) => true,
                        Ok(Err(err)) => {
                            log::warn!("TON sender {index}, attempt {attempt} failed: {err}");
                            false
                        }
                        Err(_) => {
                            log::warn!("TON sender {index}, attempt {attempt} timed out");
                            false
                        }
                    }
                }),
        )
        .await;
        if results.into_iter().any(|success| success) {
            return Ok(SendMsgReturnHashResponse { msg_hash });
        }
        sleep_until((Instant::now() + RETRY_INTERVAL).min(deadline)).await;
    }
    log::error!("All TON message senders failed before the request deadline ({attempt} attempts)");
    Err(GrpcServiceError::TimedOut(
        "TON message broadcast deadline reached".into(),
    ))
}

#[cfg(test)]
#[path = "../tests/test_send_msg.rs"]
mod tests;
