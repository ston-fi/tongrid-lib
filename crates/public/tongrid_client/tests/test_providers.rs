use anyhow::{Context, Result};
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch};
use tokio_stream::{
    wrappers::{TcpListenerStream, WatchStream},
    Stream,
};
use ton::block_tlb::ShardAccount;
use ton::ton_core::{
    cell::TonHash,
    traits::{state_provider::StateProvider, tlb::TLB},
    types::{TonAddress, TxLTHash},
};
use tongrid_client::{errors::TongridApiError, tongrid_client::TongridClient};
use tongrid_proto::v3::{
    self,
    sync_status_grpc_server::{SyncStatusGrpc, SyncStatusGrpcServer},
    tx_grpc_server::{TxGrpc, TxGrpcServer},
};
use tonic::{Request, Response, Status};

#[derive(Clone)]
struct Services {
    response: Arc<Mutex<v3::TxResponse>>,
    last_request: Arc<Mutex<Option<v3::GetLastTxAtMcSeqnoRequest>>>,
    tx_calls: Arc<AtomicUsize>,
    status: watch::Sender<v3::SyncStatus>,
    streams: Arc<AtomicUsize>,
    attempts: Arc<AtomicUsize>,
}

struct StatusStream {
    inner: WatchStream<v3::SyncStatus>,
    active: Arc<AtomicUsize>,
}
impl Stream for StatusStream {
    type Item = Result<v3::SyncStatusResponse, Status>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx).map(|item| {
            item.map(|status| {
                Ok(v3::SyncStatusResponse {
                    sync_status: Some(status),
                })
            })
        })
    }
}
impl Drop for StatusStream {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[tonic::async_trait]
impl SyncStatusGrpc for Services {
    async fn get_sync_status(
        &self,
        _: Request<()>,
    ) -> Result<Response<v3::SyncStatusResponse>, Status> {
        Ok(Response::new(v3::SyncStatusResponse {
            sync_status: Some(*self.status.borrow()),
        }))
    }
    async fn wait_synced_seqno(
        &self,
        _: Request<v3::WaitSyncedSeqnoRequest>,
    ) -> Result<Response<v3::SyncStatusResponse>, Status> {
        Err(Status::unimplemented("use the client's status watch"))
    }
    type StreamSyncStatusStream =
        Pin<Box<dyn Stream<Item = Result<v3::SyncStatusResponse, Status>> + Send>>;
    async fn stream_sync_status(
        &self,
        _: Request<()>,
    ) -> Result<Response<Self::StreamSyncStatusStream>, Status> {
        // The first stream ends; the client must reconnect and own the replacement.
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Ok(Response::new(Box::pin(tokio_stream::empty())));
        }
        self.streams.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(Box::pin(StatusStream {
            inner: WatchStream::new(self.status.subscribe()),
            active: self.streams.clone(),
        })))
    }
}

#[tonic::async_trait]
impl TxGrpc for Services {
    async fn get_tx(
        &self,
        _: Request<v3::GetTxRequest>,
    ) -> Result<Response<v3::TxResponse>, Status> {
        self.tx_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(
            self.response
                .lock()
                .map_err(|_| Status::internal("fixture lock"))?
                .clone(),
        ))
    }
    async fn get_last_tx_at_mc_seqno(
        &self,
        request: Request<v3::GetLastTxAtMcSeqnoRequest>,
    ) -> Result<Response<v3::TxResponse>, Status> {
        *self
            .last_request
            .lock()
            .map_err(|_| Status::internal("fixture lock"))? = Some(request.into_inner());
        self.status.send_replace(v3::SyncStatus {
            full_synced_seqno: 101,
            ..Default::default()
        });
        self.get_tx(Request::new(v3::GetTxRequest::default())).await
    }
    async fn get_blocks_txs(
        &self,
        _: Request<v3::GetBlocksTxsRequest>,
    ) -> Result<Response<v3::TxsResponse>, Status> {
        Err(Status::unimplemented("unused fixture RPC"))
    }
    async fn query_txs(
        &self,
        _: Request<v3::QueryTxsRequest>,
    ) -> Result<Response<v3::TxsResponse>, Status> {
        Err(Status::unimplemented("unused fixture RPC"))
    }
    async fn get_tx_tree(
        &self,
        _: Request<v3::GetTxTreeRequest>,
    ) -> Result<Response<v3::GetTxTreeResponse>, Status> {
        Err(Status::unimplemented("unused fixture RPC"))
    }
    async fn get_initial_tx(
        &self,
        _: Request<v3::GetInitialTxRequest>,
    ) -> Result<Response<v3::TxResponse>, Status> {
        Err(Status::unimplemented("unused fixture RPC"))
    }
    async fn get_txs_by_address(
        &self,
        _: Request<v3::GetTxsByAddressRequest>,
    ) -> Result<Response<v3::GetTxsByAddressResponse>, Status> {
        Err(Status::unimplemented("unused fixture RPC"))
    }
    async fn get_ongoing_txs(
        &self,
        _: Request<v3::GetOngoingTxsRequest>,
    ) -> Result<Response<v3::TxsResponse>, Status> {
        Err(Status::unimplemented("unused fixture RPC"))
    }
}

struct Fixture {
    client: TongridClient,
    service: Services,
    server: ServerTask,
    stop: Option<oneshot::Sender<()>>,
}
impl Fixture {
    async fn start() -> Result<Self> {
        let (status, _) = watch::channel(v3::SyncStatus {
            full_synced_seqno: 100,
            ..Default::default()
        });
        let service = Services {
            response: Arc::new(Mutex::new(response(7, 70)?)),
            last_request: Arc::new(Mutex::new(None)),
            tx_calls: Arc::new(AtomicUsize::new(0)),
            status,
            streams: Arc::new(AtomicUsize::new(0)),
            attempts: Arc::new(AtomicUsize::new(0)),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (stop, stopping) = oneshot::channel();
        let state = service.clone();
        let server = ServerTask(Some(tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(SyncStatusGrpcServer::new(state.clone()))
                .add_service(TxGrpcServer::new(state))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stopping.await;
                })
                .await
        })));
        let client = TongridClient::builder("provider-contract-test")?
            .with_tongrid_url(format!("http://{address}"))
            .with_client_timeout_ms(1000)
            .build()
            .await?;
        Ok(Self {
            client,
            service,
            server,
            stop: Some(stop),
        })
    }
    async fn finish(self) -> Result<()> {
        let Self {
            client,
            service: _,
            server,
            mut stop,
        } = self;
        drop(client);
        if let Some(stop) = stop.take() {
            let _ = stop.send(());
        }
        server.finish().await
    }
}

struct ServerTask(Option<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>);
impl ServerTask {
    async fn finish(mut self) -> Result<()> {
        let task = self.0.as_mut().context("server task missing")?;
        tokio::time::timeout(Duration::from_secs(2), task).await???;
        self.0.take();
        Ok(())
    }
}
impl Drop for ServerTask {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}

fn address() -> Result<TonAddress> {
    Ok("0:0000000000000000000000000000000000000000000000000000000000000000".parse()?)
}
fn identity(hash: u8, lt: i64) -> Result<TxLTHash> {
    Ok(TxLTHash::new(lt, TonHash::from_vec(vec![hash; 32])?))
}
fn response(hash: u8, lt: u64) -> Result<v3::TxResponse> {
    let account = ShardAccount {
        last_tx_hash: TonHash::from_vec(vec![hash; 32])?,
        last_tx_lt: lt,
        ..ShardAccount::NON_EXIST
    };
    Ok(v3::TxResponse {
        tx: Some(v3::Tx {
            mc_seqno: 80,
            tx_lt: lt,
            tx_hash: vec![hash; 32],
            account_state: Some(v3::AccountState {
                shard_account_boc: account.to_boc()?,
                ..Default::default()
            }),
            ..Default::default()
        }),
    })
}

#[tokio::test]
async fn test_exact_state_requires_both_lt_and_hash() -> Result<()> {
    let fixture = Fixture::start().await?;
    let state = fixture
        .client
        .load_state(address()?, Some(identity(7, 70)?))
        .await?;
    assert_eq!(state.last_tx_id, identity(7, 70)?);
    assert_eq!(state.mc_seqno, Some(80));
    assert!(fixture
        .client
        .load_state(address()?, Some(identity(8, 70)?))
        .await
        .is_err());
    assert!(fixture
        .client
        .load_state(address()?, Some(identity(7, 71)?))
        .await
        .is_err());
    let before = fixture.service.tx_calls.load(Ordering::SeqCst);
    assert!(fixture
        .client
        .load_state(address()?, Some(identity(7, -1)?))
        .await
        .is_err());
    assert_eq!(
        fixture.service.tx_calls.load(Ordering::SeqCst),
        before,
        "negative LT reached the server"
    );
    fixture.finish().await
}

#[tokio::test]
async fn test_latest_state_keeps_the_request_snapshot() -> Result<()> {
    let fixture = Fixture::start().await?;
    let state = fixture.client.load_state(address()?, None).await?;
    let request = fixture
        .service
        .last_request
        .lock()
        .map_err(|_| anyhow::anyhow!("fixture lock"))?
        .clone()
        .context("latest RPC not called")?;
    assert_eq!(request.mc_seqno, 100);
    assert_eq!(state.mc_seqno, Some(100));
    assert_eq!(state.last_tx_id, identity(7, 70)?);
    fixture
        .client
        .wait_seqno(101, Some(Duration::from_secs(2)))
        .await?;
    assert_eq!(
        state.mc_seqno,
        Some(100),
        "background status changed the captured snapshot"
    );
    fixture.finish().await
}

#[tokio::test]
async fn test_missing_and_malformed_state_responses_fail() -> Result<()> {
    let fixture = Fixture::start().await?;
    for value in [
        v3::TxResponse { tx: None },
        v3::TxResponse {
            tx: Some(v3::Tx::default()),
        },
        v3::TxResponse {
            tx: Some(v3::Tx {
                account_state: Some(v3::AccountState {
                    shard_account_boc: vec![0, 1, 2],
                    ..Default::default()
                }),
                ..Default::default()
            }),
        },
    ] {
        *fixture
            .service
            .response
            .lock()
            .map_err(|_| anyhow::anyhow!("fixture lock"))? = value;
        assert!(fixture
            .client
            .load_state(address()?, Some(identity(7, 70)?))
            .await
            .is_err());
    }
    fixture.finish().await
}

#[tokio::test]
async fn test_status_reconnects_and_last_clone_drop_stops_stream() -> Result<()> {
    let fixture = Fixture::start().await?;
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture.service.streams.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    assert!(fixture.service.attempts.load(Ordering::SeqCst) >= 2);
    let client = fixture.client.clone();
    // Keep the service and server alive after dropping every client clone.
    let Fixture {
        client: original,
        service,
        server,
        mut stop,
    } = fixture;
    drop(original);
    assert_eq!(
        service.streams.load(Ordering::SeqCst),
        1,
        "dropping one clone stopped the worker"
    );
    drop(client);
    tokio::time::timeout(Duration::from_secs(2), async {
        while service.streams.load(Ordering::SeqCst) != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    if let Some(stop) = stop.take() {
        let _ = stop.send(());
    }
    server.finish().await?;
    Ok(())
}

#[tokio::test]
async fn test_builder_requires_an_explicit_endpoint() -> Result<()> {
    let result = TongridClient::builder("explicit-endpoint")?.build().await;
    assert!(matches!(result, Err(TongridApiError::InvalidArguments(_))));
    Ok(())
}
