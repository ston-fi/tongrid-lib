use anyhow::Context;
use std::pin::Pin;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch};
use tokio_stream::wrappers::{TcpListenerStream, WatchStream};
use tokio_stream::{Stream, StreamExt};
use tongrid_client::tongrid_client::TongridClient;
use tongrid_proto::v3::sync_status_grpc_server::{SyncStatusGrpc, SyncStatusGrpcServer};
use tongrid_proto::v3::{SyncStatus, SyncStatusResponse, WaitSyncedSeqnoRequest};
use tonic::{Request, Response, Status};

struct StatusService(watch::Receiver<SyncStatus>);

#[tonic::async_trait]
impl SyncStatusGrpc for StatusService {
    async fn get_sync_status(
        &self,
        _: Request<()>,
    ) -> Result<Response<SyncStatusResponse>, Status> {
        Ok(Response::new(SyncStatusResponse {
            sync_status: Some(*self.0.borrow()),
        }))
    }

    async fn wait_synced_seqno(
        &self,
        request: Request<WaitSyncedSeqnoRequest>,
    ) -> Result<Response<SyncStatusResponse>, Status> {
        let request = request.into_inner();
        let mut status = self.0.clone();
        tokio::time::timeout(
            Duration::from_millis(u64::from(request.wait_timeout_ms)),
            async {
                loop {
                    let current = *status.borrow_and_update();
                    if current.full_synced_seqno >= request.mc_seqno {
                        return Ok(Response::new(SyncStatusResponse {
                            sync_status: Some(current),
                        }));
                    }
                    status
                        .changed()
                        .await
                        .map_err(|_| Status::unavailable("status producer stopped"))?;
                }
            },
        )
        .await
        .map_err(|_| Status::deadline_exceeded("sync deadline"))?
    }

    type StreamSyncStatusStream =
        Pin<Box<dyn Stream<Item = Result<SyncStatusResponse, Status>> + Send>>;

    async fn stream_sync_status(
        &self,
        _: Request<()>,
    ) -> Result<Response<Self::StreamSyncStatusStream>, Status> {
        let stream = WatchStream::new(self.0.clone()).map(|status| {
            Ok(SyncStatusResponse {
                sync_status: Some(status),
            })
        });
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Protects background status propagation and wait_seqno against deterministic responses.
#[tokio::test]
async fn test_sync_status_and_wait_seqno_over_loopback() -> anyhow::Result<()> {
    let initial = SyncStatus {
        full_synced_seqno: 42,
        ..Default::default()
    };
    let (status_tx, status_rx) = watch::channel(initial);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop_tx, stop_rx) = oneshot::channel();
    let mut server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(SyncStatusGrpcServer::new(StatusService(status_rx)))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = stop_rx.await;
            })
            .await
    });
    let result = async {
        let client = TongridClient::builder("loopback-status-test")?
            .with_tongrid_url(format!("http://{address}"))
            .with_client_timeout_ms(1000)
            .build()
            .await?;
        assert_eq!(client.last_sync_status().full_synced_seqno, 42);
        status_tx.send_replace(SyncStatus {
            full_synced_seqno: 43,
            ..Default::default()
        });
        let status = client.wait_seqno(43, Some(Duration::from_secs(1))).await?;
        assert_eq!(status.full_synced_seqno, 43);
        tokio::time::timeout(Duration::from_secs(2), async {
            while client.last_sync_status().full_synced_seqno != 43 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("background stream did not propagate status")?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let _ = stop_tx.send(());
    let shutdown = tokio::time::timeout(Duration::from_secs(3), &mut server).await;
    if shutdown.is_err() {
        server.abort();
        let _ = server.await;
    }
    result?;
    shutdown.context("loopback status server shutdown timed out")???;
    Ok(())
}
