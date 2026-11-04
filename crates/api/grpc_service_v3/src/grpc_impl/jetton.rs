use crate::context::ServiceContext;
use crate::services::jetton::JettonService;
use std::sync::Arc;
use tongrid_proto::v3::jetton_grpc_server::JettonGrpc;
use tongrid_proto::v3::*;
use tonic::{Request, Response, Status};

pub struct JettonGrpcImpl {
    service: JettonService,
}

impl JettonGrpcImpl {
    pub fn new(ctx: Arc<ServiceContext>) -> anyhow::Result<Self> {
        Ok(Self {
            service: JettonService::new(ctx),
        })
    }
}

#[tonic::async_trait]
impl JettonGrpc for JettonGrpcImpl {
    async fn get_balances_by_master(
        &self,
        req: Request<GetBalancesByMasterRequest>,
    ) -> Result<Response<BalancesResponse>, Status> {
        self.service
            .get_balances_by_master(req.into_inner())
            .await
            .map(|res| res.into_tonic_response())
            .map_err(Into::into)
    }

    async fn get_balances_by_owner(
        &self,
        req: Request<GetBalancesByOwnerRequest>,
    ) -> Result<Response<BalancesResponse>, Status> {
        self.service
            .get_balances_by_owner(req.into_inner())
            .await
            .map(|res| res.into_tonic_response())
            .map_err(Into::into)
    }

    async fn get_balances(
        &self,
        req: Request<GetBalancesRequest>,
    ) -> Result<Response<BalancesResponse>, Status> {
        self.service
            .get_balances(req.into_inner())
            .await
            .map(|res| res.into_tonic_response())
            .map_err(Into::into)
    }

    async fn get_balance_history(
        &self,
        req: Request<GetBalanceHistoryRequest>,
    ) -> Result<Response<BalanceHistoryResponse>, Status> {
        self.service
            .get_balance_history(req.into_inner())
            .await
            .map(|res| res.into_tonic_response())
            .map_err(Into::into)
    }
}
