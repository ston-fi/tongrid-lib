use crate::context::ServiceContext;
use crate::services::send_msg::SendMsgService;
use std::sync::Arc;
use tongrid_proto::v3::send_msg_grpc_server::SendMsgGrpc;
use tongrid_proto::v3::{SendMsgReturnHashRequest, SendMsgReturnHashResponse};
use tonic::{Request, Response, Status};

pub struct SendMsgGrpcImpl {
    service: SendMsgService,
}

impl SendMsgGrpcImpl {
    pub fn new(ctx: Arc<ServiceContext>) -> Self {
        Self {
            service: SendMsgService::new(ctx),
        }
    }
}

#[tonic::async_trait]
impl SendMsgGrpc for SendMsgGrpcImpl {
    async fn send_msg_return_hash(
        &self,
        req: Request<SendMsgReturnHashRequest>,
    ) -> Result<Response<SendMsgReturnHashResponse>, Status> {
        self.service
            .send_msg_return_hash(req.into_inner())
            .await
            .map(|res| res.into())
            .map_err(Into::into)
    }
}
