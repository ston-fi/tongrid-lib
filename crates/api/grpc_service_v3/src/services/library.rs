use crate::context::ServiceContext;
use crate::error::GrpcServiceError;
use crate::utils::ResponseWithWaitTime;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::sync::Arc;
use ton::ton_core::cell::TonHash;
use tongrid_proto::v3::{GetLibrariesRequest, GetLibrariesResponse, Library};

pub struct LibraryService {
    ctx: Arc<ServiceContext>,
}

impl LibraryService {
    pub fn new(ctx: Arc<ServiceContext>) -> Self {
        Self { ctx }
    }

    pub async fn get_libraries(
        &self,
        req: GetLibrariesRequest,
    ) -> Result<ResponseWithWaitTime<GetLibrariesResponse>, GrpcServiceError> {
        if req.lib_ids.len() > self.ctx.config.library_service.max_batch_size {
            return Err(GrpcServiceError::InvalidArgument(format!(
                "Too many lib_ids: {} (max: {})",
                req.lib_ids.len(),
                self.ctx.config.library_service.max_batch_size
            )));
        }
        let db = self.ctx.tda.db();
        let code_futs = req.lib_ids.into_iter().map(|hash| async move {
            let hash = TonHash::from_slice(&hash).map_err(|err| {
                GrpcServiceError::InvalidArgument(format!("Invalid library hash: {err}"))
            })?;
            let code = db.tvm_code().get_opt(&hash).await?;
            Ok::<_, GrpcServiceError>(code.map(|code| Library {
                hash: code.code_hash.into_vec(),
                boc: code.boc,
            }))
        });
        let libraries = stream::iter(code_futs)
            .buffered(self.ctx.config.library_service.concurrency.get())
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect();
        Ok(ResponseWithWaitTime {
            response: GetLibrariesResponse { libraries },
            time_waited: Default::default(),
        })
    }
}
