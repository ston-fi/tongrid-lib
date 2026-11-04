use super::*;
use anyhow::Context;
use futures_util::future::try_join_all;
use futures_util::stream::{self, Stream};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_stream::wrappers::TcpListenerStream;
use tongrid_proto::v3::block_grpc_server::{BlockGrpc, BlockGrpcServer};
use tongrid_proto::v3::library_grpc_server::{LibraryGrpc, LibraryGrpcServer};
use tongrid_proto::v3::sync_status_grpc_server::{SyncStatusGrpc, SyncStatusGrpcServer};
use tongrid_proto::v3::tx_grpc_server::{TxGrpc, TxGrpcServer};
use tongrid_proto::v3::{
    BlockShard, GetBlocksShardsRequest, GetBlocksShardsResponse, GetBlocksTxsRequest,
    GetInitialTxRequest, GetLastTxAtMcSeqnoRequest, GetLibrariesRequest, GetLibrariesResponse,
    GetOngoingTxsRequest, GetShardRequest, GetShardResponse, GetTxRequest, GetTxTreeRequest,
    GetTxTreeResponse, GetTxsByAddressRequest, GetTxsByAddressResponse, Library, McSeqnoRange,
    QueryTxsRequest, SyncStatus, SyncStatusResponse, Tx, TxResponse, TxsResponse,
    WaitSyncedSeqnoRequest,
};
use tonic::{Request, Response, Status};

const TEST_SEQNO: u32 = 42;
const BACKEND_COUNT: usize = 4;
const REQUESTS_PER_ROUND: usize = 4000;
const MAX_ROUNDS: usize = 20;
const TX_DELAY_MS: u64 = 40;
const BLOCK_DELAY_MS: u64 = 15;
const LIBRARY_DELAY_MS: u64 = 5;
const TEST_MAX_CONCURRENT_STREAMS: u32 = 8;
const BURST_REQUEST_COUNT: usize = 200;

#[derive(Default)]
struct RequestStats {
    sync_state_calls: AtomicUsize,
    tx_calls: AtomicUsize,
    active_tx_calls: AtomicUsize,
    max_active_tx_calls: AtomicUsize,
    block_calls: AtomicUsize,
    library_calls: AtomicUsize,
    route_hits: Mutex<BTreeMap<String, BTreeMap<String, usize>>>,
}

impl RequestStats {
    fn record_tx<T>(self: &Arc<Self>, backend_id: &str, _req: &Request<T>) -> ActiveTxGuard {
        self.tx_calls.fetch_add(1, Ordering::Relaxed);
        self.record_hit("tx", backend_id);
        let active = self.active_tx_calls.fetch_add(1, Ordering::Relaxed) + 1;
        self.max_active_tx_calls
            .fetch_max(active, Ordering::Relaxed);
        ActiveTxGuard {
            stats: self.clone(),
        }
    }

    fn record_block<T>(&self, backend_id: &str, _req: &Request<T>) {
        self.block_calls.fetch_add(1, Ordering::Relaxed);
        self.record_hit("block", backend_id);
    }

    fn record_library<T>(&self, backend_id: &str, _req: &Request<T>) {
        self.library_calls.fetch_add(1, Ordering::Relaxed);
        self.record_hit("library", backend_id);
    }

    fn route_hits(&self) -> BTreeMap<String, BTreeMap<String, usize>> {
        self.route_hits.lock().unwrap().clone()
    }

    fn summary(&self) -> String {
        format!(
            "tonic-balancing stats:\n  sync_state_calls={}\n  tx_calls={}\n  block_calls={}\n  library_calls={}\n  route_hits={:?}",
            self.sync_state_calls.load(Ordering::Relaxed),
            self.tx_calls.load(Ordering::Relaxed),
            self.block_calls.load(Ordering::Relaxed),
            self.library_calls.load(Ordering::Relaxed),
            self.route_hits()
        )
    }

    fn record_hit(&self, route: &str, backend_id: &str) {
        let mut hits = self.route_hits.lock().unwrap();
        let per_route = hits.entry(route.to_string()).or_default();
        *per_route.entry(backend_id.to_string()).or_insert(0) += 1;
    }
}

struct ActiveTxGuard {
    stats: Arc<RequestStats>,
}

impl Drop for ActiveTxGuard {
    fn drop(&mut self) {
        self.stats.active_tx_calls.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct TestSyncStatusService;

#[tonic::async_trait]
impl SyncStatusGrpc for TestSyncStatusService {
    async fn get_sync_status(
        &self,
        _req: Request<()>,
    ) -> Result<Response<SyncStatusResponse>, Status> {
        Ok(Response::new(SyncStatusResponse {
            sync_status: Some(test_sync_status(TEST_SEQNO)),
        }))
    }

    async fn wait_synced_seqno(
        &self,
        req: Request<WaitSyncedSeqnoRequest>,
    ) -> Result<Response<SyncStatusResponse>, Status> {
        Ok(Response::new(SyncStatusResponse {
            sync_status: Some(test_sync_status(req.get_ref().mc_seqno.max(TEST_SEQNO))),
        }))
    }

    type StreamSyncStatusStream =
        Pin<Box<dyn Stream<Item = Result<SyncStatusResponse, Status>> + Send + 'static>>;

    async fn stream_sync_status(
        &self,
        _req: Request<()>,
    ) -> Result<Response<Self::StreamSyncStatusStream>, Status> {
        let stream = stream::iter([Ok(SyncStatusResponse {
            sync_status: Some(test_sync_status(TEST_SEQNO)),
        })]);
        Ok(Response::new(Box::pin(stream)))
    }
}

#[derive(Clone)]
struct TestTxService {
    stats: Arc<RequestStats>,
    backend_id: String,
}

#[tonic::async_trait]
impl TxGrpc for TestTxService {
    async fn get_blocks_txs(
        &self,
        req: Request<GetBlocksTxsRequest>,
    ) -> Result<Response<TxsResponse>, Status> {
        let _active_tx_guard = self.stats.record_tx(&self.backend_id, &req);
        sleep(Duration::from_millis(TX_DELAY_MS)).await;

        let seqno = req
            .get_ref()
            .seqno_range
            .as_ref()
            .map(|range| range.from_mc_seqno)
            .unwrap_or_default();
        Ok(Response::new(TxsResponse {
            txs: vec![Tx {
                mc_seqno: seqno,
                block_seqno: seqno,
                address: self.backend_id.clone(),
                tx_hash: vec![seqno as u8],
                tx_lt: seqno as u64,
                ..Default::default()
            }],
        }))
    }

    async fn get_tx(&self, _req: Request<GetTxRequest>) -> Result<Response<TxResponse>, Status> {
        Err(Status::unimplemented("not needed for tonic balancing test"))
    }

    async fn query_txs(
        &self,
        _req: Request<QueryTxsRequest>,
    ) -> Result<Response<TxsResponse>, Status> {
        Err(Status::unimplemented("not needed for tonic balancing test"))
    }

    async fn get_tx_tree(
        &self,
        _req: Request<GetTxTreeRequest>,
    ) -> Result<Response<GetTxTreeResponse>, Status> {
        Err(Status::unimplemented("not needed for tonic balancing test"))
    }

    async fn get_initial_tx(
        &self,
        _req: Request<GetInitialTxRequest>,
    ) -> Result<Response<TxResponse>, Status> {
        Err(Status::unimplemented("not needed for tonic balancing test"))
    }

    async fn get_txs_by_address(
        &self,
        _req: Request<GetTxsByAddressRequest>,
    ) -> Result<Response<GetTxsByAddressResponse>, Status> {
        Err(Status::unimplemented("not needed for tonic balancing test"))
    }

    async fn get_ongoing_txs(
        &self,
        _req: Request<GetOngoingTxsRequest>,
    ) -> Result<Response<TxsResponse>, Status> {
        Err(Status::unimplemented("not needed for tonic balancing test"))
    }

    async fn get_last_tx_at_mc_seqno(
        &self,
        _req: Request<GetLastTxAtMcSeqnoRequest>,
    ) -> Result<Response<TxResponse>, Status> {
        Err(Status::unimplemented("not needed for tonic balancing test"))
    }
}

#[derive(Clone)]
struct TestBlockService {
    stats: Arc<RequestStats>,
    backend_id: String,
}

#[tonic::async_trait]
impl BlockGrpc for TestBlockService {
    async fn get_blocks_shards(
        &self,
        req: Request<GetBlocksShardsRequest>,
    ) -> Result<Response<GetBlocksShardsResponse>, Status> {
        self.stats.record_block(&self.backend_id, &req);
        sleep(Duration::from_millis(BLOCK_DELAY_MS)).await;

        Ok(Response::new(GetBlocksShardsResponse {
            shards: vec![BlockShard {
                master_seqno: req
                    .get_ref()
                    .mc_seqno_range
                    .as_ref()
                    .map(|range| range.from_mc_seqno)
                    .unwrap_or_default(),
                seqno: req
                    .get_ref()
                    .mc_seqno_range
                    .as_ref()
                    .map(|range| range.to_mc_seqno)
                    .unwrap_or_default(),
                root_hash: self.backend_id.as_bytes().to_vec(),
                ..Default::default()
            }],
        }))
    }

    async fn get_shard(
        &self,
        _req: Request<GetShardRequest>,
    ) -> Result<Response<GetShardResponse>, Status> {
        Err(Status::unimplemented("not needed for tonic balancing test"))
    }
}

#[derive(Clone)]
struct TestLibraryService {
    stats: Arc<RequestStats>,
    backend_id: String,
}

#[tonic::async_trait]
impl LibraryGrpc for TestLibraryService {
    async fn get_libraries(
        &self,
        req: Request<GetLibrariesRequest>,
    ) -> Result<Response<GetLibrariesResponse>, Status> {
        self.stats.record_library(&self.backend_id, &req);
        sleep(Duration::from_millis(LIBRARY_DELAY_MS)).await;

        Ok(Response::new(GetLibrariesResponse {
            libraries: vec![Library {
                hash: self.backend_id.as_bytes().to_vec(),
                boc: self.backend_id.as_bytes().to_vec(),
            }],
        }))
    }
}

struct TestBackend {
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: JoinHandle<()>,
}

impl TestBackend {
    async fn shutdown(mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        let _ = self.handle.await;
    }
}

struct TestCluster {
    stats: Arc<RequestStats>,
    tongrid_url: String,
    resolved_backends: Vec<SocketAddr>,
    backends: Vec<TestBackend>,
}

impl TestCluster {
    async fn spawn() -> anyhow::Result<Self> {
        let stats = Arc::new(RequestStats::default());
        let mut resolved_backends = Vec::with_capacity(BACKEND_COUNT);
        let mut backends = Vec::with_capacity(BACKEND_COUNT);

        for idx in 0..BACKEND_COUNT {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .with_context(|| format!("failed to bind backend listener #{idx}"))?;
            let addr = listener
                .local_addr()
                .with_context(|| format!("failed to get backend addr #{idx}"))?;
            resolved_backends.push(addr);
            let backend_id = format!("backend-{}", idx + 1);
            backends.push(spawn_backend(listener, stats.clone(), backend_id));
        }

        let mut running_backends = Vec::with_capacity(backends.len());
        for backend in backends {
            running_backends.push(backend.await?);
        }

        Ok(Self {
            stats,
            tongrid_url: "http://localhost".to_string(),
            resolved_backends,
            backends: running_backends,
        })
    }

    async fn shutdown(self) {
        for backend in self.backends {
            backend.shutdown().await;
        }
    }
}

async fn spawn_backend(
    listener: TcpListener,
    stats: Arc<RequestStats>,
    backend_id: String,
) -> anyhow::Result<TestBackend> {
    let sync_status_service = TestSyncStatusService;
    let tx_service = TestTxService {
        stats: stats.clone(),
        backend_id: backend_id.clone(),
    };
    let block_service = TestBlockService {
        stats: stats.clone(),
        backend_id: backend_id.clone(),
    };
    let library_service = TestLibraryService { stats, backend_id };
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(SyncStatusGrpcServer::new(sync_status_service))
            .add_service(TxGrpcServer::new(tx_service))
            .add_service(BlockGrpcServer::new(block_service))
            .add_service(LibraryGrpcServer::new(library_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("tonic balancing backend should shut down cleanly");
    });

    Ok(TestBackend {
        shutdown_tx: Some(shutdown_tx),
        handle,
    })
}

fn test_sync_status(seqno: u32) -> SyncStatus {
    SyncStatus {
        initial_seqno: seqno as i32,
        full_synced_seqno: seqno,
        full_synced_seqno_utime: seqno,
        last_seen_seqno: seqno,
        last_seen_seqno_utime: seqno,
        sync_utime: seqno,
    }
}

#[tokio::test]
async fn test_client_respects_server_max_concurrent_streams() -> anyhow::Result<()> {
    let stats = Arc::new(RequestStats::default());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let listen_addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_stats = stats.clone();
    let mut server_task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .max_concurrent_streams(Some(TEST_MAX_CONCURRENT_STREAMS))
            .add_service(SyncStatusGrpcServer::new(TestSyncStatusService))
            .add_service(TxGrpcServer::new(TestTxService {
                stats: server_stats,
                backend_id: "configured-backend".to_string(),
            }))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let test_result = async {
        let client = TongridClient::builder("test_server_stream_limit")?
            .with_tongrid_url("http://localhost".to_string())
            .with_connections_refresh_interval(Duration::from_secs(3600))
            .with_resolved_backend_addrs(vec![listen_addr])
            .build()
            .await?;

        let responses = try_join_all((0..BURST_REQUEST_COUNT).map(|_| {
            let mut tx_client = client.tx();
            async move {
                tx_client
                    .get_blocks_txs(GetBlocksTxsRequest {
                        seqno_range: Some(McSeqnoRange {
                            from_mc_seqno: TEST_SEQNO,
                            to_mc_seqno: TEST_SEQNO,
                        }),
                        ..Default::default()
                    })
                    .await
            }
        }))
        .await?;

        assert_eq!(responses.len(), BURST_REQUEST_COUNT);
        assert!(
            stats.max_active_tx_calls.load(Ordering::Relaxed)
                <= TEST_MAX_CONCURRENT_STREAMS as usize,
            "client exceeded the server's negotiated stream limit"
        );
        assert!(
            stats.max_active_tx_calls.load(Ordering::Relaxed) > 1,
            "burst was unexpectedly serialized"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;

    let _ = shutdown_tx.send(());
    let shutdown_result = tokio::time::timeout(Duration::from_secs(5), &mut server_task).await;
    if shutdown_result.is_err() {
        server_task.abort();
        let _ = server_task.await;
    }
    test_result?;
    shutdown_result.context("loopback concurrency server shutdown timed out")???;
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_tonic_balancing_spreads_multiple_rpc_types_across_many_backends() -> anyhow::Result<()>
{
    let cluster = TestCluster::spawn().await?;
    let stats = cluster.stats.clone();

    let client = TongridClient::builder("test_tonic_balancing")?
        .with_tongrid_url(cluster.tongrid_url.clone())
        .with_connections_refresh_interval(Duration::from_secs(3600))
        .with_resolved_backend_addrs(cluster.resolved_backends.clone())
        .build()
        .await?;

    assert_eq!(client.last_sync_status().full_synced_seqno, TEST_SEQNO);

    let mut observed_tx_hits = BTreeMap::<String, usize>::new();
    let mut observed_block_hits = BTreeMap::<String, usize>::new();
    let mut observed_library_hits = BTreeMap::<String, usize>::new();

    for round in 0..MAX_ROUNDS {
        let round_observations = try_join_all((0..REQUESTS_PER_ROUND).map(|idx| {
            let mut tx_client = client.tx();
            let mut block_client = client.block();
            let mut library_client = client.library();

            async move {
                let tx_rsp = tx_client
                    .get_blocks_txs(GetBlocksTxsRequest {
                        seqno_range: Some(McSeqnoRange {
                            from_mc_seqno: TEST_SEQNO,
                            to_mc_seqno: TEST_SEQNO,
                        }),
                        ..Default::default()
                    })
                    .await?
                    .into_inner();
                let tx_backend = tx_rsp
                    .txs
                    .first()
                    .context("missing tx in tx response")?
                    .address
                    .clone();

                let block_rsp = block_client
                    .get_blocks_shards(GetBlocksShardsRequest {
                        mc_seqno_range: Some(McSeqnoRange {
                            from_mc_seqno: TEST_SEQNO + idx as u32 + round as u32,
                            to_mc_seqno: TEST_SEQNO + idx as u32 + round as u32,
                        }),
                        boc_required: None,
                    })
                    .await?
                    .into_inner();
                let block_backend = String::from_utf8(
                    block_rsp
                        .shards
                        .first()
                        .context("missing shard in block response")?
                        .root_hash
                        .clone(),
                )
                .context("invalid backend id in block response")?;

                let library_rsp = library_client
                    .get_libraries(GetLibrariesRequest {
                        lib_ids: vec![vec![idx as u8]],
                        ..Default::default()
                    })
                    .await?
                    .into_inner();
                let library_backend = String::from_utf8(
                    library_rsp
                        .libraries
                        .first()
                        .context("missing library in library response")?
                        .hash
                        .clone(),
                )
                .context("invalid backend id in library response")?;

                Ok::<_, anyhow::Error>((tx_backend, block_backend, library_backend))
            }
        }))
        .await?;

        for (tx_backend, block_backend, library_backend) in round_observations {
            *observed_tx_hits.entry(tx_backend).or_insert(0) += 1;
            *observed_block_hits.entry(block_backend).or_insert(0) += 1;
            *observed_library_hits.entry(library_backend).or_insert(0) += 1;
        }

        if observed_tx_hits.len() == BACKEND_COUNT
            && observed_block_hits.len() == BACKEND_COUNT
            && observed_library_hits.len() == BACKEND_COUNT
        {
            break;
        }
    }

    eprintln!(
        "configured_backends={:?}\nobserved_tx_hits={:?}\nobserved_block_hits={:?}\nobserved_library_hits={:?}\n{}",
        cluster.resolved_backends,
        observed_tx_hits,
        observed_block_hits,
        observed_library_hits,
        stats.summary()
    );

    assert_eq!(
        observed_tx_hits.len(),
        BACKEND_COUNT,
        "expected tx requests to hit every backend; stats: {}",
        stats.summary()
    );
    assert_eq!(
        observed_block_hits.len(),
        BACKEND_COUNT,
        "expected block requests to hit every backend; stats: {}",
        stats.summary()
    );
    assert_eq!(
        observed_library_hits.len(),
        BACKEND_COUNT,
        "expected library requests to hit every backend; stats: {}",
        stats.summary()
    );

    let route_hits = stats.route_hits();
    assert_eq!(
        route_hits
            .get("tx")
            .map(|hits| hits.len())
            .unwrap_or_default(),
        BACKEND_COUNT,
        "expected server-side tx hits on every backend; stats: {}",
        stats.summary()
    );
    assert_eq!(
        route_hits
            .get("block")
            .map(|hits| hits.len())
            .unwrap_or_default(),
        BACKEND_COUNT,
        "expected server-side block hits on every backend; stats: {}",
        stats.summary()
    );
    assert_eq!(
        route_hits
            .get("library")
            .map(|hits| hits.len())
            .unwrap_or_default(),
        BACKEND_COUNT,
        "expected server-side library hits on every backend; stats: {}",
        stats.summary()
    );

    cluster.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_shared_rpc_rate_limit_and_deadline_across_generated_clients() -> anyhow::Result<()> {
    let cluster = TestCluster::spawn().await?;
    let started = tokio::time::Instant::now();
    let client = TongridClient::builder("test_shared_rate_limit")?
        .with_tongrid_url(cluster.tongrid_url.clone())
        .with_resolved_backend_addrs(cluster.resolved_backends.clone())
        .with_rate_limit(10)
        .with_connections_refresh_interval(Duration::from_secs(3600))
        .build()
        .await?;
    let mut tx = client.clone().tx();
    let mut block = client.clone().block();
    let mut library = client.clone().library();
    let (a, b, c) = tokio::join!(
        tx.get_blocks_txs(GetBlocksTxsRequest {
            seqno_range: Some(McSeqnoRange {
                from_mc_seqno: TEST_SEQNO,
                to_mc_seqno: TEST_SEQNO
            }),
            ..Default::default()
        }),
        block.get_blocks_shards(GetBlocksShardsRequest {
            mc_seqno_range: Some(McSeqnoRange {
                from_mc_seqno: TEST_SEQNO,
                to_mc_seqno: TEST_SEQNO
            }),
            ..Default::default()
        }),
        library.get_libraries(GetLibrariesRequest::default()),
    );
    a?;
    b?;
    c?;
    assert!(
        started.elapsed() >= Duration::from_millis(290),
        "startup and generated clients bypassed shared rate limiter"
    );
    drop(client);
    drop((tx, block, library));
    cluster.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_queued_rpc_deadline_is_explicit() -> anyhow::Result<()> {
    let cluster = TestCluster::spawn().await?;
    let client = TongridClient::builder("test_rate_limit_timeout")?
        .with_tongrid_url(cluster.tongrid_url.clone())
        .with_resolved_backend_addrs(cluster.resolved_backends.clone())
        .with_rate_limit(1)
        .with_client_timeout_ms(100)
        .build()
        .await?;
    let error = client
        .tx()
        .get_tx(GetTxRequest::default())
        .await
        .err()
        .context("queued request must time out")?;
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    drop(client);
    cluster.shutdown().await;
    Ok(())
}
