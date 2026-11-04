use crate::context::ServiceContext;
use crate::services::library::LibraryService;
use std::sync::Arc;
use tongrid_proto::v3::library_grpc_server::LibraryGrpc;
use tongrid_proto::v3::{GetLibrariesRequest, GetLibrariesResponse};
use tonic::{Request, Response, Status};

pub struct LibraryGrpcImpl {
    service: LibraryService,
}

impl LibraryGrpcImpl {
    pub fn new(ctx: Arc<ServiceContext>) -> Self {
        Self {
            service: LibraryService::new(ctx),
        }
    }
}

#[tonic::async_trait]
impl LibraryGrpc for LibraryGrpcImpl {
    async fn get_libraries(
        &self,
        req: Request<GetLibrariesRequest>,
    ) -> Result<Response<GetLibrariesResponse>, Status> {
        self.service
            .get_libraries(req.into_inner())
            .await
            .map(|res| res.into_tonic_response())
            .map_err(Into::into)
    }
}
