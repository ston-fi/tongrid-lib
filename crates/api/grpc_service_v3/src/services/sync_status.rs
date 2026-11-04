use crate::context::ServiceContext;
use crate::error::GrpcServiceError;
use crate::grpc_impl::sync_status::SyncStatusStream;
use crate::utils::ResponseWithWaitTime;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;
use tongrid_proto::v3::{SyncStatusResponse, WaitSyncedSeqnoRequest};
const MAX_WAIT_TIMEOUT_MS: u32 = 10_000; // 10 sec;

pub struct SyncStatusService {
    ctx: Arc<ServiceContext>,
}

impl SyncStatusService {
    pub fn new(ctx: Arc<ServiceContext>) -> Self {
        Self { ctx }
    }

    pub async fn get_sync_state(
        &self,
    ) -> Result<ResponseWithWaitTime<SyncStatusResponse>, GrpcServiceError> {
        let sync_status = self.ctx.store.get_recent_data().sync_status;
        let response = SyncStatusResponse {
            sync_status: self.ctx.serial.sync_status_to_proto(&sync_status),
        };
        Ok(ResponseWithWaitTime {
            response,
            time_waited: Duration::default(),
        })
    }

    pub async fn wait_synced_seqno(
        &self,
        req: WaitSyncedSeqnoRequest,
    ) -> Result<ResponseWithWaitTime<SyncStatusResponse>, GrpcServiceError> {
        let start_ts = Instant::now();
        let timeout_ms = req.wait_timeout_ms;
        if timeout_ms > MAX_WAIT_TIMEOUT_MS {
            let err_msg = format!("Max timeout={MAX_WAIT_TIMEOUT_MS}ms, got {timeout_ms}ms");
            return Err(GrpcServiceError::InvalidArgument(err_msg));
        }

        let time_waited = loop {
            let recent_data = self.ctx.store.get_recent_data();
            if recent_data.sync_status.full_synced_seqno >= req.mc_seqno {
                break start_ts.elapsed();
            }

            if start_ts.elapsed().as_millis() >= timeout_ms as u128 {
                let msg = format!(
                    "Timeout={timeout_ms} during waiting for seqno={}",
                    req.mc_seqno
                );
                return Err(GrpcServiceError::TimedOut(msg));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        };

        let mut response = self.get_sync_state().await?;
        response.add_wait_time(time_waited);
        Ok(response)
    }

    pub async fn stream_sync_status(&self) -> Result<SyncStatusStream, GrpcServiceError> {
        let rx = self.ctx.store.subscribe();
        let ctx = self.ctx.clone();

        let stream = WatchStream::new(rx).map(move |recent_data| {
            let response = SyncStatusResponse {
                sync_status: ctx.serial.sync_status_to_proto(&recent_data.sync_status),
            };
            Ok(response)
        });

        Ok(Box::pin(stream))
    }
}
