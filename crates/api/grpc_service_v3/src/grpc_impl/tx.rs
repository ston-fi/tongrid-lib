use crate::context::ServiceContext;
use crate::services::tx::TxService;
use std::sync::Arc;
use tongrid_proto::v3::tx_grpc_server::TxGrpc;
use tongrid_proto::v3::*;
use tonic::{Request, Response, Status};

pub struct TxGrpcImpl {
    service: TxService,
}

impl TxGrpcImpl {
    pub fn new(ctx: Arc<ServiceContext>) -> Self {
        Self {
            service: TxService::new(ctx),
        }
    }
}

#[tonic::async_trait]
impl TxGrpc for TxGrpcImpl {
    async fn get_blocks_txs(
        &self,
        req: Request<GetBlocksTxsRequest>,
    ) -> Result<Response<TxsResponse>, Status> {
        self.service
            .get_blocks_txs(req.into_inner())
            .await
            .map(|res| res.into_tonic_response())
            .map_err(Into::into)
    }

    async fn get_tx(&self, req: Request<GetTxRequest>) -> Result<Response<TxResponse>, Status> {
        self.service
            .get_tx(req.into_inner())
            .await
            .map(|res| res.into_tonic_response())
            .map_err(Into::into)
    }

    async fn query_txs(
        &self,
        req: Request<QueryTxsRequest>,
    ) -> Result<Response<TxsResponse>, Status> {
        self.service
            .query_txs(req.into_inner())
            .await
            .map(|res| res.into_tonic_response())
            .map_err(Into::into)
    }

    async fn get_tx_tree(
        &self,
        req: Request<GetTxTreeRequest>,
    ) -> Result<Response<GetTxTreeResponse>, Status> {
        self.service
            .get_tx_tree(req.into_inner())
            .await
            .map(|res| res.into_tonic_response())
            .map_err(Into::into)
    }

    async fn get_initial_tx(
        &self,
        req: Request<GetInitialTxRequest>,
    ) -> Result<Response<TxResponse>, Status> {
        self.service
            .get_initial_tx(req.into_inner())
            .await
            .map(|res| res.into_tonic_response())
            .map_err(Into::into)
    }

    async fn get_txs_by_address(
        &self,
        req: Request<GetTxsByAddressRequest>,
    ) -> Result<Response<GetTxsByAddressResponse>, Status> {
        self.service
            .get_txs_by_address(req.into_inner())
            .await
            .map(|res| res.into_tonic_response())
            .map_err(Into::into)
    }

    async fn get_ongoing_txs(
        &self,
        req: Request<GetOngoingTxsRequest>,
    ) -> Result<Response<TxsResponse>, Status> {
        self.service
            .get_ongoing_txs(req.into_inner())
            .await
            .map(|res| res.into_tonic_response())
            .map_err(Into::into)
    }

    async fn get_last_tx_at_mc_seqno(
        &self,
        req: Request<GetLastTxAtMcSeqnoRequest>,
    ) -> Result<Response<TxResponse>, Status> {
        self.service
            .get_last_tx_at_mc_seqno(req.into_inner())
            .await
            .map(|res| res.into_tonic_response())
            .map_err(Into::into)
    }
}
