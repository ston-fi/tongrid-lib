use crate::context::ServiceContext;
use crate::services::sync_status::SyncStatusService;
use futures::stream::BoxStream;
use std::sync::Arc;
use tongrid_proto::v3::sync_status_grpc_server::SyncStatusGrpc;
use tongrid_proto::v3::{SyncStatusResponse, WaitSyncedSeqnoRequest};
use tonic::{Request, Response, Status};

pub type SyncStatusStream = BoxStream<'static, Result<SyncStatusResponse, Status>>;

pub struct SyncStatusGrpcImpl {
    service: SyncStatusService,
}

impl SyncStatusGrpcImpl {
    pub fn new(ctx: Arc<ServiceContext>) -> Self {
        Self {
            service: SyncStatusService::new(ctx),
        }
    }
}

#[tonic::async_trait]
impl SyncStatusGrpc for SyncStatusGrpcImpl {
    async fn get_sync_status(
        &self,
        _req: Request<()>,
    ) -> Result<Response<SyncStatusResponse>, Status> {
        self.service
            .get_sync_state()
            .await
            .map(|res| res.into_tonic_response())
            .map_err(Into::into)
    }

    async fn wait_synced_seqno(
        &self,
        req: Request<WaitSyncedSeqnoRequest>,
    ) -> Result<Response<SyncStatusResponse>, Status> {
        self.service
            .wait_synced_seqno(req.into_inner())
            .await
            .map(|res| res.into_tonic_response())
            .map_err(Into::into)
    }

    type StreamSyncStatusStream = SyncStatusStream;

    async fn stream_sync_status(
        &self,
        _req: Request<()>,
    ) -> Result<Response<SyncStatusStream>, Status> {
        self.service
            .stream_sync_status()
            .await
            .map(Response::new)
            .map_err(Into::into)
    }
}
