use prometheus::{
    register_histogram_vec, register_int_counter_vec, HistogramOpts, HistogramVec, IntCounterVec,
    Opts,
};
use std::time::Duration;
use stonfi_metrics::constants::DURATION_BUCKETS_1MS_20S;
use stonfi_metrics::MetricsCell;

static METRICS: MetricsCell<Metrics> = MetricsCell::new();
stonfi_metrics::register_metrics!(Metrics, METRICS);

struct Metrics {
    grpc_requests: IntCounterVec,
    grpc_errors_total: IntCounterVec,
    grpc_requests_duration_ms: HistogramVec,
}

impl Metrics {
    fn new() -> anyhow::Result<Self> {
        let request_labels = ["service_name", "method_name", "status", "client_id"];
        let duration_labels = [
            "service_name",
            "method_name",
            "status",
            "client_id",
            "request_phase",
        ];

        let res = Self {
            grpc_requests: register_int_counter_vec!(
                Opts::new("grpc_requests", "Number of requests"),
                &request_labels
            )?,
            grpc_errors_total: register_int_counter_vec!(
                Opts::new(
                    "grpc_errors_total",
                    "gRPC errors observed in headers, trailers, or transport"
                ),
                &["service_name", "method_name", "grpc_code", "client_id"]
            )?,
            grpc_requests_duration_ms: register_histogram_vec!(
                HistogramOpts::new("grpc_requests_duration_ms", "Requests duration")
                    .buckets(DURATION_BUCKETS_1MS_20S.clone()),
                &duration_labels
            )?,
        };
        Ok(res)
    }
}

pub(crate) fn record_error(service: &str, method: &str, code: tonic::Code, client_id: &str) {
    METRICS
        .grpc_errors_total
        .with_label_values(&[service, method, &format!("{code:?}"), client_id])
        .inc();
}

pub(crate) fn update(
    service: &str,
    method: &str,
    is_ok: bool,
    wait_duration: Duration,
    execution_duration: Duration,
    client_id: &str,
) {
    let status = if is_ok { "ok" } else { "error" };
    let counter_labels = [service, method, status, client_id];
    METRICS
        .grpc_requests
        .with_label_values(&counter_labels)
        .inc();

    let wait_labels = [service, method, status, client_id, "wait_mc_seqno"];
    METRICS
        .grpc_requests_duration_ms
        .with_label_values(&wait_labels)
        .observe(wait_duration.as_micros() as f64 / 1000.0);

    let execution_labels = [service, method, status, client_id, "execution"];
    METRICS
        .grpc_requests_duration_ms
        .with_label_values(&execution_labels)
        .observe(execution_duration.as_micros() as f64 / 1000.0);
}
