use prometheus::{
    register_int_counter, register_int_counter_vec, register_int_gauge, IntCounter, IntCounterVec,
    IntGauge, Opts,
};
use std::collections::HashMap;
use stonfi_metrics::MetricsCell;
use ton::block_tlb::BlockInfo;
use ton::lite_client::metrics::LiteClientMetricsSnapshot;

static METRICS: MetricsCell<Metrics> = MetricsCell::new();
stonfi_metrics::register_metrics!(Metrics, METRICS);

struct Metrics {
    lite_max_seen_mc_seqno: IntGauge,
    lite_max_seen_mc_seqno_utime: IntGauge,
    lite_known_nodes_count: IntGauge,
    lite_connections_per_node: IntGauge,
    lite_wait_connection_ms_total: IntCounter,
    lite_ongoing_requests: IntGauge,
    lite_requests_ok_total: IntCounterVec,
    lite_requests_failed_total: IntCounterVec,
    lite_requests_count_total: IntCounterVec,
    lite_requests_duration_ms_total: IntCounterVec,
    lite_retries_count_total: IntCounterVec,
}

impl Metrics {
    fn new() -> anyhow::Result<Self> {
        Ok(Self {
            lite_max_seen_mc_seqno: register_int_gauge!(Opts::new(
                "lite_max_seen_mc_seqno",
                "Maximal masterchain seqno seen",
            ))?,
            lite_max_seen_mc_seqno_utime: register_int_gauge!(Opts::new(
                "lite_max_seen_mc_seqno_utime",
                "Block utime of maximal masterchain seqno seen",
            ))?,
            lite_known_nodes_count: register_int_gauge!(Opts::new(
                "lite_known_nodes_count",
                "Number of known lite server nodes",
            ))?,
            lite_connections_per_node: register_int_gauge!(Opts::new(
                "lite_connections_per_node",
                "Configured number of lite client connections per node",
            ))?,
            lite_wait_connection_ms_total: register_int_counter!(Opts::new(
                "lite_wait_connection_ms_total",
                "Total milliseconds spent waiting for lite client connections",
            ))?,
            lite_ongoing_requests: register_int_gauge!(Opts::new(
                "lite_ongoing_requests",
                "Number of lite client requests currently in progress",
            ))?,
            lite_requests_ok_total: register_int_counter_vec!(
                Opts::new(
                    "lite_requests_ok_total",
                    "Total successful lite client requests"
                ),
                &["method"]
            )?,
            lite_requests_failed_total: register_int_counter_vec!(
                Opts::new(
                    "lite_requests_failed_total",
                    "Total failed lite client requests"
                ),
                &["method"]
            )?,
            lite_requests_count_total: register_int_counter_vec!(
                Opts::new("lite_requests_count_total", "Total lite client requests"),
                &["method"]
            )?,
            lite_requests_duration_ms_total: register_int_counter_vec!(
                Opts::new(
                    "lite_requests_duration_ms_total",
                    "Total lite client request duration in milliseconds",
                ),
                &["method"]
            )?,
            lite_retries_count_total: register_int_counter_vec!(
                Opts::new(
                    "lite_retries_count_total",
                    "Total retried lite client requests"
                ),
                &["method"]
            )?,
        })
    }
}

pub(super) fn set_last_seen_info(mc_info: &BlockInfo) {
    METRICS.lite_max_seen_mc_seqno.set(mc_info.seqno as i64);
    METRICS
        .lite_max_seen_mc_seqno_utime
        .set(mc_info.gen_utime as i64);
}

pub(super) fn set_lite_client_metrics(
    snapshot: &LiteClientMetricsSnapshot,
    prev_snapshot: Option<&LiteClientMetricsSnapshot>,
) {
    METRICS
        .lite_known_nodes_count
        .set(snapshot.known_nodes_count as i64);
    METRICS
        .lite_connections_per_node
        .set(snapshot.connections_per_node as i64);
    METRICS
        .lite_ongoing_requests
        .set(snapshot.ongoing_requests as i64);

    METRICS.lite_wait_connection_ms_total.inc_by(delta(
        snapshot.wait_connection_ms_total,
        prev_snapshot.map(|prev| prev.wait_connection_ms_total),
    ));
    inc_counter_vec_by_delta(
        &METRICS.lite_requests_ok_total,
        &snapshot.requests_ok,
        prev_snapshot.map(|prev| &prev.requests_ok),
    );
    inc_counter_vec_by_delta(
        &METRICS.lite_requests_failed_total,
        &snapshot.requests_failed,
        prev_snapshot.map(|prev| &prev.requests_failed),
    );
    inc_counter_vec_by_delta(
        &METRICS.lite_requests_count_total,
        &snapshot.requests_count_total,
        prev_snapshot.map(|prev| &prev.requests_count_total),
    );
    inc_counter_vec_by_delta(
        &METRICS.lite_requests_duration_ms_total,
        &snapshot.requests_duration_ms_total,
        prev_snapshot.map(|prev| &prev.requests_duration_ms_total),
    );
    inc_counter_vec_by_delta(
        &METRICS.lite_retries_count_total,
        &snapshot.retries_count_total,
        prev_snapshot.map(|prev| &prev.retries_count_total),
    );
}

fn inc_counter_vec_by_delta(
    counter: &IntCounterVec,
    values: &HashMap<String, u64>,
    prev_values: Option<&HashMap<String, u64>>,
) {
    for (method, value) in values {
        let prev_value = prev_values.and_then(|prev| prev.get(method)).copied();
        let increment = delta(*value, prev_value);
        if increment > 0 {
            counter.with_label_values(&[method]).inc_by(increment);
        }
    }
}

fn delta(value: u64, prev_value: Option<u64>) -> u64 {
    value.saturating_sub(prev_value.unwrap_or_default())
}
