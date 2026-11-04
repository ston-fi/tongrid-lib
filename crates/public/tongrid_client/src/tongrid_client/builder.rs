use crate::errors::{TongridApiError, TongridApiResult};
use crate::tongrid_client::Channel;
use crate::tongrid_client::Inner;
use crate::tongrid_client::{TongridClient, TongridClientInterceptor, TowerChannel};
use derive_setters::Setters;
use std::cmp::max;
use std::collections::HashSet;
#[cfg(test)]
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::lookup_host;
use tokio::sync::mpsc::Sender as BalanceSender;
use tokio::sync::watch::Sender;
use tokio::time::sleep;
use tongrid_proto::v3::block_grpc_client::BlockGrpcClient;
use tongrid_proto::v3::emulation_grpc_client::EmulationGrpcClient;
use tongrid_proto::v3::jetton_grpc_client::JettonGrpcClient;
use tongrid_proto::v3::library_grpc_client::LibraryGrpcClient;
use tongrid_proto::v3::send_msg_grpc_client::SendMsgGrpcClient;
use tongrid_proto::v3::sync_status_grpc_client::SyncStatusGrpcClient;
use tongrid_proto::v3::tx_grpc_client::TxGrpcClient;
use tongrid_proto::v3::{SyncStatus, SyncStatusResponse};
use tonic::codegen::http::Uri;
use tonic::transport::channel::Change;
use tonic::transport::Endpoint;

const UPDATE_LOOP_SLEEP_IN_ERROR_MS: Duration = Duration::from_millis(200);
const BALANCER_BUFFER_SIZE: usize = 128;

#[cfg(test)]
#[path = "../tests/test_balancing.rs"]
mod tests;

#[derive(Setters, Clone)]
#[setters(prefix = "with_", strip_option)]
/// Client configuration. Set an endpoint explicitly before calling [`Self::build`].
pub struct Builder {
    /// Caller-owned HTTP endpoint; empty until configured, with no production fallback.
    pub tongrid_url: String,
    /// Largest decoded RPC response; defaults to 8 MiB.
    pub max_response_size_bytes: usize,
    /// Transport concurrency limit for each resolved backend; defaults to 500.
    pub max_parallel_requests_per_backend: usize,
    /// Transport/quota deadline, in milliseconds; defaults to 10,000. Excludes OS DNS resolution.
    pub client_timeout_ms: u64,
    /// Delay between DNS refreshes; defaults to 60 seconds.
    pub connections_refresh_interval: Duration,
    #[cfg(test)]
    pub resolved_backend_addrs: Option<Vec<SocketAddr>>,
    /// ASCII metadata value sent as `x-client-id` on all RPCs.
    pub client_id: String,
    /// Requests per second across all services and clones, with burst one. Defaults to 1000.
    /// Zero selects the maximum of 5000; higher values warn and clamp to 5000 when built.
    pub rate_limit: u32,
}

#[derive(Debug)]
struct ResolvedEndpoint {
    key: String,
    endpoint: Endpoint,
}

macro_rules! make_grpc_client {
    ($client_type:ident, $channel:expr, $interceptor:expr, $builder:expr) => {
        $client_type::with_interceptor($channel.clone(), $interceptor.clone())
            .max_decoding_message_size($builder.max_response_size_bytes)
    };
}

impl Builder {
    /// Initializes defaults without an endpoint. No network access occurs here.
    pub fn new<T: Into<String>>(client_id: T) -> Self {
        Self {
            client_id: client_id.into(),
            rate_limit: 1000,
            tongrid_url: String::new(),
            max_response_size_bytes: 8 * 1024 * 1024,
            max_parallel_requests_per_backend: 500,
            client_timeout_ms: 10000,
            connections_refresh_interval: Duration::from_secs(60),
            #[cfg(test)]
            resolved_backend_addrs: None,
        }
    }

    /// Resolves backends, fetches the initial status, then starts owned background workers.
    ///
    /// # Errors
    /// Returns configuration/DNS/transport/status errors. A missing endpoint fails
    /// before DNS; transport and quota deadlines exclude the OS resolver call.
    pub async fn build(self) -> TongridApiResult<TongridClient> {
        let endpoints = resolve_endpoints(&self).await?;
        log_resolved_endpoints("[tongrid_client][build]", &self.tongrid_url, &endpoints);
        let interceptor = TongridClientInterceptor::new(&self.client_id)?;
        let (channel, balance_tx) = Channel::balance_channel::<String>(BALANCER_BUFFER_SIZE);

        let mut active_keys = HashSet::new();
        apply_endpoint_changes(&balance_tx, &mut active_keys, endpoints).await?;

        let channel = super::rate_limiter::RateLimitedChannel::new(
            channel,
            self.rate_limit,
            Duration::from_millis(self.client_timeout_ms),
        );

        let sync_status = make_grpc_client!(SyncStatusGrpcClient, &channel, &interceptor, &self);
        let block = make_grpc_client!(BlockGrpcClient, &channel, &interceptor, &self);
        let tx = make_grpc_client!(TxGrpcClient, &channel, &interceptor, &self);
        let emulation = make_grpc_client!(EmulationGrpcClient, &channel, &interceptor, &self);
        let library = make_grpc_client!(LibraryGrpcClient, &channel, &interceptor, &self);
        let jetton = make_grpc_client!(JettonGrpcClient, &channel, &interceptor, &self);
        let send_msg = make_grpc_client!(SendMsgGrpcClient, &channel, &interceptor, &self);

        let mut sync_status_clone = sync_status.clone();
        let fresh_status = sync_status_clone
            .get_sync_status(())
            .await?
            .into_inner()
            .sync_status
            .ok_or_else(|| {
                TongridApiError::UnexpectedResponse(
                    "[tongrid_client] sync_status not found in response".to_string(),
                )
            })?;

        let (sender, last_sync_status_rcv) = tokio::sync::watch::channel(fresh_status);
        let background_tasks = [
            tokio::spawn(update_sync_status_loop(sync_status_clone, sender)),
            tokio::spawn(refresh_balancer_loop(self.clone(), balance_tx, active_keys)),
        ];

        let inner = Inner {
            sync_status,
            block,
            tx,
            emulation,
            library,
            jetton,
            send_msg,
            last_sync_status_rcv,
            background_tasks,
        };
        Ok(TongridClient(Arc::new(inner)))
    }
}

async fn refresh_balancer_loop(
    builder: Builder,
    balance_tx: BalanceSender<Change<String, Endpoint>>,
    mut active_keys: HashSet<String>,
) {
    loop {
        sleep(builder.connections_refresh_interval).await;

        let endpoints = match resolve_endpoints(&builder).await {
            Ok(endpoints) => endpoints,
            Err(err) => {
                log::warn!(
                    "[tongrid_client][refresh_balancer_loop] failed to resolve endpoints for {}: {}",
                    builder.tongrid_url,
                    err
                );
                continue;
            }
        };

        if endpoints_changed(&active_keys, &endpoints) {
            log_resolved_endpoints(
                "[tongrid_client][refresh_balancer_loop]",
                &builder.tongrid_url,
                &endpoints,
            );
        }

        if let Err(err) = apply_endpoint_changes(&balance_tx, &mut active_keys, endpoints).await {
            log::info!(
                "[tongrid_client][refresh_balancer_loop] failed to update balanced channel for {}: {}",
                builder.tongrid_url,
                err
            );
            break;
        }
    }
}

async fn apply_endpoint_changes(
    balance_tx: &BalanceSender<Change<String, Endpoint>>,
    active_keys: &mut HashSet<String>,
    endpoints: Vec<ResolvedEndpoint>,
) -> TongridApiResult<()> {
    let next_keys = endpoints
        .iter()
        .map(|endpoint| endpoint.key.clone())
        .collect::<HashSet<_>>();

    let removed_keys = active_keys
        .difference(&next_keys)
        .cloned()
        .collect::<Vec<_>>();

    for endpoint in endpoints {
        if active_keys.contains(&endpoint.key) {
            continue;
        }

        balance_tx
            .send(Change::Insert(endpoint.key.clone(), endpoint.endpoint))
            .await
            .map_err(|_| {
                TongridApiError::CustomError("balanced channel update loop was dropped".to_string())
            })?;
        active_keys.insert(endpoint.key);
    }

    for key in removed_keys {
        balance_tx
            .send(Change::Remove(key.clone()))
            .await
            .map_err(|_| {
                TongridApiError::CustomError("balanced channel update loop was dropped".to_string())
            })?;
        active_keys.remove(&key);
    }

    Ok(())
}

async fn resolve_endpoints(builder: &Builder) -> TongridApiResult<Vec<ResolvedEndpoint>> {
    if builder.tongrid_url.is_empty() {
        return Err(TongridApiError::InvalidArguments(
            "Set a caller-supplied endpoint with with_tongrid_url before building".to_owned(),
        ));
    }
    let uri: Uri = builder.tongrid_url.parse().map_err(|err| {
        TongridApiError::InvalidArguments(format!(
            "Invalid tongrid_url '{}': {err}",
            builder.tongrid_url
        ))
    })?;
    let host = uri.host().ok_or_else(|| {
        TongridApiError::InvalidArguments(format!(
            "Missing host in tongrid_url '{}'",
            builder.tongrid_url
        ))
    })?;
    let port = match uri.port_u16() {
        Some(port) => port,
        None if uri.scheme_str() == Some("https") => 443,
        None => 80,
    };
    let scheme = uri.scheme_str().unwrap_or("http");

    #[cfg(test)]
    let addrs = match &builder.resolved_backend_addrs {
        Some(addrs) => addrs.iter().copied().collect::<HashSet<_>>(),
        None => lookup_host((host, port))
            .await
            .map_err(|err| {
                TongridApiError::NetError(format!(
                    "Failed to resolve backends for {host}:{port}: {err}"
                ))
            })?
            .collect::<HashSet<_>>(),
    };
    #[cfg(not(test))]
    let addrs = lookup_host((host, port))
        .await
        .map_err(|err| {
            TongridApiError::NetError(format!(
                "Failed to resolve backends for {host}:{port}: {err}"
            ))
        })?
        .collect::<HashSet<_>>();
    if addrs.is_empty() {
        return Err(TongridApiError::NetError(format!(
            "No backend addresses resolved for {host}:{port}"
        )));
    }

    let mut endpoints = Vec::with_capacity(addrs.len());
    for addr in addrs {
        let endpoint_uri = format!("{scheme}://{addr}");
        let endpoint = Endpoint::from_shared(endpoint_uri.clone())
            .map_err(|err| {
                TongridApiError::InvalidArguments(format!(
                    "Invalid backend URI '{endpoint_uri}': {err}"
                ))
            })?
            .origin(uri.clone())
            .connect_timeout(Duration::from_millis(builder.client_timeout_ms))
            .timeout(Duration::from_millis(builder.client_timeout_ms))
            .concurrency_limit(builder.max_parallel_requests_per_backend);

        endpoints.push(ResolvedEndpoint {
            key: addr.to_string(),
            endpoint,
        });
    }

    Ok(endpoints)
}

async fn update_sync_status_loop(
    mut service: SyncStatusGrpcClient<TowerChannel>,
    sender: Sender<SyncStatus>,
) {
    loop {
        let stream = match service.stream_sync_status(()).await {
            Ok(response) => response.into_inner(),
            Err(err) => {
                log::warn!("[tongrid_client][update_sync_status_loop] Failed to open sync status stream: {err}");
                sleep(UPDATE_LOOP_SLEEP_IN_ERROR_MS).await;
                continue;
            }
        };
        match process_sync_status_stream(stream, &sender).await {
            Ok(()) => {
                log::info!("[tongrid_client][update_sync_status_loop] no receivers left, exiting");
                break;
            }
            Err(err) => {
                log::warn!("[tongrid_client][update_sync_status_loop] restarting stream because of error: {err}")
            }
        }
        sleep(UPDATE_LOOP_SLEEP_IN_ERROR_MS).await;
    }
    log::info!("[tongrid_client][update_sync_status_loop] completed");
}

async fn process_sync_status_stream(
    mut stream: tonic::Streaming<SyncStatusResponse>,
    sender: &Sender<SyncStatus>,
) -> TongridApiResult<()> {
    loop {
        let msg = match stream.message().await {
            Ok(Some(msg)) => msg,
            Ok(None) => {
                let err_msg =
                    "[tongrid_client][update_sync_status_loop] completed sync_status stream ended by server"
                        .to_string();
                return Err(TongridApiError::CustomError(err_msg));
            }
            Err(err) => {
                let err_msg =
                    format!("[tongrid_client][update_sync_status_loop] completed sync_status stream error: {err}");
                return Err(TongridApiError::CustomError(err_msg));
            }
        };

        let sync_status = match msg.sync_status {
            Some(status) => status,
            None => {
                let err_msg =
                    "[tongrid_client][update_sync_status_loop] completed Received update with missing sync_status"
                        .to_string();
                return Err(TongridApiError::UnexpectedResponse(err_msg));
            }
        };

        sender.send_if_modified(|old| {
            let old_clone = *old;
            merge_sync_status(old, &sync_status);
            &old_clone != old
        });
        if sender.receiver_count() == 0 {
            return Ok(());
        }
    }
}

fn merge_sync_status(target: &mut SyncStatus, other: &SyncStatus) {
    target.initial_seqno = max(target.initial_seqno, other.initial_seqno);
    target.full_synced_seqno = max(target.full_synced_seqno, other.full_synced_seqno);
    target.full_synced_seqno_utime = max(
        target.full_synced_seqno_utime,
        other.full_synced_seqno_utime,
    );
    target.last_seen_seqno = max(target.last_seen_seqno, other.last_seen_seqno);
    target.last_seen_seqno_utime = max(target.last_seen_seqno_utime, other.last_seen_seqno_utime);
    target.sync_utime = max(target.sync_utime, other.sync_utime);
}

fn endpoints_changed(active_keys: &HashSet<String>, endpoints: &[ResolvedEndpoint]) -> bool {
    if active_keys.len() != endpoints.len() {
        return true;
    }

    endpoints
        .iter()
        .any(|endpoint| !active_keys.contains(&endpoint.key))
}

fn log_resolved_endpoints(prefix: &str, tongrid_url: &str, endpoints: &[ResolvedEndpoint]) {
    let mut keys = endpoints
        .iter()
        .map(|endpoint| endpoint.key.clone())
        .collect::<Vec<_>>();
    keys.sort_unstable();
    log::info!(
        "{prefix} resolved {} backend(s) for {}: {}",
        keys.len(),
        tongrid_url,
        keys.join(", ")
    );
}
