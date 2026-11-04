use crate::context::ServiceContext;
use crate::services::block::BlockService;
use std::sync::Arc;
use tongrid_proto::v3::block_grpc_server::BlockGrpc;
use tongrid_proto::v3::*;
use tonic::{Request, Response, Status};

pub struct BlockGrpcImpl {
    service: BlockService,
}

impl BlockGrpcImpl {
    pub fn new(ctx: Arc<ServiceContext>) -> Self {
        Self {
            service: BlockService::new(ctx),
        }
    }
}

#[tonic::async_trait]
impl BlockGrpc for BlockGrpcImpl {
    async fn get_blocks_shards(
        &self,
        req: Request<GetBlocksShardsRequest>,
    ) -> Result<Response<GetBlocksShardsResponse>, Status> {
        self.service
            .get_block_shards(req.into_inner())
            .await
            .map(|res| res.into_tonic_response())
            .map_err(Into::into)
    }

    async fn get_shard(
        &self,
        request: Request<GetShardRequest>,
    ) -> Result<Response<GetShardResponse>, Status> {
        self.service
            .get_shard(request.into_inner())
            .await
            .map(|res| res.into_tonic_response())
            .map_err(Into::into)
    }
}
