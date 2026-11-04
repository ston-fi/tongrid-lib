use crate::context::ServiceContext;
use crate::services::emulation::EmulationService;
use std::sync::Arc;
use tongrid_proto::v3::emulation_grpc_server::EmulationGrpc;
use tongrid_proto::v3::{
    EmulateGetMethodRequest, EmulateGetMethodResponse, EmulateTxTreeRequest, EmulateTxTreeResponse,
};
use tonic::{Request, Response, Status};

pub struct EmulationGrpcImpl {
    service: EmulationService,
}

impl EmulationGrpcImpl {
    pub fn new(ctx: Arc<ServiceContext>) -> anyhow::Result<Self> {
        Ok(Self {
            service: EmulationService::new(ctx),
        })
    }
}

#[tonic::async_trait]
impl EmulationGrpc for EmulationGrpcImpl {
    async fn emulate_get_method(
        &self,
        req: Request<EmulateGetMethodRequest>,
    ) -> Result<Response<EmulateGetMethodResponse>, Status> {
        self.service
            .emulate_get_method(req.into_inner())
            .await
            .map(|res| res.into_tonic_response())
            .map_err(Into::into)
    }

    async fn emulate_tx_tree(
        &self,
        req: Request<EmulateTxTreeRequest>,
    ) -> Result<Response<EmulateTxTreeResponse>, Status> {
        self.service
            .emulate_tx_tree(req.into_inner())
            .await
            .map(|res| res.into_tonic_response())
            .map_err(Into::into)
    }
}
