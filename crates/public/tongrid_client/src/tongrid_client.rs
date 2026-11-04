mod builder;
mod emulation_provider_impl;
pub mod rate_limiter;
mod state_provider_impl;

use crate::errors::{TongridApiError, TongridApiResult};
use crate::interceptor::TongridClientInterceptor;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch::Receiver;
use tokio::task::JoinHandle;
use ton::ton_core::errors::TonCoreError;
use tongrid_proto::v3::block_grpc_client::BlockGrpcClient;
use tongrid_proto::v3::emulation_grpc_client::EmulationGrpcClient;
use tongrid_proto::v3::jetton_grpc_client::JettonGrpcClient;
use tongrid_proto::v3::library_grpc_client::LibraryGrpcClient;
use tongrid_proto::v3::send_msg_grpc_client::SendMsgGrpcClient;
use tongrid_proto::v3::sync_status_grpc_client::SyncStatusGrpcClient;
use tongrid_proto::v3::tx_grpc_client::TxGrpcClient;
use tongrid_proto::v3::SyncStatus;
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;

type TowerChannel = InterceptedService<rate_limiter::RateLimitedChannel, TongridClientInterceptor>;

#[derive(Clone)]
/// Shared RPC services, status watch, quota and two owned background workers.
/// Dropping the final client clone aborts both workers.
pub struct TongridClient(Arc<Inner>);

#[rustfmt::skip]
impl TongridClient {
    /// Starts configuring a client. Building requires a caller-supplied endpoint.
    pub fn builder<T: Into<String>>(client_id: T) -> TongridApiResult<builder::Builder> {
        Ok(builder::Builder::new(client_id))
    }

    /// Always returns the most recent sync status. Updated in background.
    pub fn last_sync_status(&self) -> SyncStatus {
        *self.0.last_sync_status_rcv.borrow()
    }

    /// Returns a receiver subscribed to sync status updates
    /// It's watch channel, so some statuses could be lost
    pub fn last_sync_status_stream(&self) -> Receiver<SyncStatus> {
        self.0.last_sync_status_rcv.clone()
    }

    /// You likely don't need this method - we keep it here only to mirror available services
    /// use `last_sync_status()` instead
    pub fn sync_status(&self) -> SyncStatusGrpcClient<TowerChannel> { self.0.sync_status.clone() }
    /// Block queries sharing this client's transport and quota.
    pub fn block(&self) -> BlockGrpcClient<TowerChannel> { self.0.block.clone() }
    /// Transaction queries sharing this client's transport and quota.
    pub fn tx(&self) -> TxGrpcClient<TowerChannel> { self.0.tx.clone() }
    /// Remote emulation RPCs; no native emulator is initialized by this client.
    pub fn emulation(&self) -> EmulationGrpcClient<TowerChannel> { self.0.emulation.clone() }
    /// Stored library content lookup by hash, independent of deployment history.
    pub fn library(&self) -> LibraryGrpcClient<TowerChannel> { self.0.library.clone() }
    /// Jetton queries sharing this client's transport and quota.
    pub fn jetton(&self) -> JettonGrpcClient<TowerChannel> { self.0.jetton.clone() }
    /// Broadcast RPC access. The caller is responsible for explicitly invoking it.
    pub fn send_msg(&self) -> SendMsgGrpcClient<TowerChannel> { self.0.send_msg.clone() }

    /// Waits on the local status watch until the requested seqno is fully synced.
    /// `None` uses a ten-second deadline. Returns watch-closure or timeout errors.
    pub async fn wait_seqno(&self, mc_seqno: u32, timeout: Option<Duration>) -> TongridApiResult<SyncStatus> {
        let mut rcv_clone = self.0.last_sync_status_rcv.clone();
        let exec_timeout = timeout.unwrap_or(Duration::from_secs(10));

        let wait_result = tokio::time::timeout(
            exec_timeout,
            rcv_clone.wait_for(|x| x.full_synced_seqno >= mc_seqno),
        ).await;

        let sync_status = match wait_result {
            Ok(Ok(res)) => res,
            Ok(Err(err)) => return Err(TongridApiError::RecvError(err)),
            Err(err) => return Err(TongridApiError::TimeoutError(err)),
        };

        Ok(*sync_status)
    }

}

pub(super) struct Inner {
    sync_status: SyncStatusGrpcClient<TowerChannel>,
    block: BlockGrpcClient<TowerChannel>,
    tx: TxGrpcClient<TowerChannel>,
    emulation: EmulationGrpcClient<TowerChannel>,
    library: LibraryGrpcClient<TowerChannel>,
    jetton: JettonGrpcClient<TowerChannel>,
    send_msg: SendMsgGrpcClient<TowerChannel>,

    last_sync_status_rcv: Receiver<SyncStatus>,
    background_tasks: [JoinHandle<()>; 2],
}

impl Drop for Inner {
    fn drop(&mut self) {
        for task in &self.background_tasks {
            task.abort();
        }
    }
}

fn to_ton_core_boxed<E: std::error::Error + Send + Sync + 'static>(err: E) -> TonCoreError {
    TonCoreError::BoxedError(Box::new(err))
}
