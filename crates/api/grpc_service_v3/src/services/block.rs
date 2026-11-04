use crate::utils::ResponseWithWaitTime;
use crate::{context::ServiceContext, error::GrpcServiceError};
use std::sync::Arc;
use tongrid_core::math::preserving_i64_bit_pattern_from_u64;
use tongrid_proto::v3::*;

const GET_BLOCKS_SHARDS_MAX_RANGE: u64 = 100;

pub struct BlockService {
    ctx: Arc<ServiceContext>,
}

impl BlockService {
    pub fn new(ctx: Arc<ServiceContext>) -> Self {
        Self { ctx }
    }

    pub async fn get_block_shards(
        &self,
        req: GetBlocksShardsRequest,
    ) -> Result<ResponseWithWaitTime<GetBlocksShardsResponse>, GrpcServiceError> {
        let seqnos = get_shards_seqnos(req.mc_seqno_range)?;
        let db_blocks = self.ctx.tda.db().block().get_by_mc_seqnos(&seqnos).await?;
        let proto_shards = db_blocks
            .into_iter()
            .map(|shard| {
                self.ctx
                    .serial
                    .block_to_proto(shard, req.boc_required.unwrap_or(false))
            })
            .collect();
        Ok(ResponseWithWaitTime {
            response: GetBlocksShardsResponse {
                shards: proto_shards,
            },
            time_waited: Default::default(),
        })
    }

    pub async fn get_shard(
        &self,
        req: GetShardRequest,
    ) -> Result<ResponseWithWaitTime<GetShardResponse>, GrpcServiceError> {
        let requested = self
            .ctx
            .tda
            .db()
            .block()
            .get_shard(
                req.mc_seqno,
                req.workchain,
                preserving_i64_bit_pattern_from_u64(req.shard),
                req.seqno,
            )
            .await
            .map_err(|err| {
                let err_str = err.to_string();
                if err_str.contains("block not found for") {
                    GrpcServiceError::NotFound("No such block".to_string())
                } else {
                    GrpcServiceError::InternalError(err_str)
                }
            })?;
        let proto_block = self.ctx.serial.block_to_proto(
            requested.as_ref().clone(),
            req.boc_required.unwrap_or(false),
        );

        Ok(ResponseWithWaitTime {
            response: GetShardResponse {
                shard: Some(proto_block),
            },
            time_waited: Default::default(),
        })
    }
}

fn get_shards_seqnos(range: Option<McSeqnoRange>) -> Result<Vec<u32>, GrpcServiceError> {
    let range = range
        .ok_or_else(|| GrpcServiceError::InvalidArgument("MC range was not provided".into()))?;
    let range_len = u64::from(range.to_mc_seqno)
        .checked_sub(u64::from(range.from_mc_seqno))
        .ok_or_else(|| {
            GrpcServiceError::InvalidArgument(format!(
                "Invalid mc seqno range: from={} is greater than to={}",
                range.from_mc_seqno, range.to_mc_seqno
            ))
        })?
        + 1;
    if range_len > GET_BLOCKS_SHARDS_MAX_RANGE {
        return Err(GrpcServiceError::InvalidArgument(format!(
            "Requested range is too big: {range_len} (max: {GET_BLOCKS_SHARDS_MAX_RANGE})"
        )));
    }
    let (from, to) = range.clamp_to_i32_supported_range();
    Ok((from..=to).collect())
}

#[cfg(test)]
#[path = "../tests/test_block_service.rs"]
mod tests;
