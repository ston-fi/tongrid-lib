use crate::types::sync_status::SyncStatus;
use prometheus::{
    register_gauge, register_histogram, register_histogram_vec, register_int_gauge_vec, Gauge,
    Histogram, HistogramOpts, HistogramVec, IntGaugeVec, Opts,
};
use std::time::Duration;
use stonfi_metrics::constants::DURATION_BUCKETS_1MS_20S;
use stonfi_metrics::MetricsCell;
use tongrid_core::time::cur_ts_sec;

static METRICS: MetricsCell<Metrics> = MetricsCell::new();
stonfi_metrics::register_metrics!(Metrics, METRICS);

struct Metrics {
    sync_status_mc_seqno: IntGaugeVec,
    update_duration_ms: HistogramVec,
    block_delay_ms: Histogram,
    full_synced_seqno_delay_sec: Gauge,
}

impl Metrics {
    fn new() -> anyhow::Result<Self> {
        let sync_status_mc_seqno = register_int_gauge_vec!(
            Opts::new(
                "mem_store_sync_status_mc_seqno",
                "Last received seqnos from scylla"
            ),
            &["sync_type"]
        )?;

        let update_duration_ms = register_histogram_vec!(
            HistogramOpts::new("mem_store_updater_duration_ms", "Time spent during update")
                .buckets(DURATION_BUCKETS_1MS_20S.clone()),
            &["stage"]
        )?;

        let block_delay_ms = register_histogram!(HistogramOpts::new(
            "mem_store_block_delay_ms",
            "Diff betwen seqno shows in memstore and block_utime",
        )
        .buckets(DURATION_BUCKETS_1MS_20S.clone()),)?;

        let full_synced_seqno_delay_sec = register_gauge!(Opts::new(
            "mem_store_full_synced_seqno_delay_sec",
            "Diff betwen full_seqno.utime and current time",
        ))?;

        let metrics = Self {
            sync_status_mc_seqno,
            update_duration_ms,
            block_delay_ms,
            full_synced_seqno_delay_sec,
        };
        Ok(metrics)
    }
}

pub(super) fn update_sync_status(status: &SyncStatus) {
    METRICS
        .sync_status_mc_seqno
        .with_label_values(&["full_synced"])
        .set(status.full_synced_seqno as i64);

    METRICS
        .sync_status_mc_seqno
        .with_label_values(&["last_seen"])
        .set(status.last_seen_seqno as i64);
}

pub(super) fn update_full_synced_seqno_delay(block_utime: u64) -> anyhow::Result<()> {
    METRICS
        .full_synced_seqno_delay_sec
        .set(cur_ts_sec()?.saturating_sub(block_utime) as f64);
    Ok(())
}

pub(super) fn update_block_sync_delay_ms(delay_ms: u64) {
    METRICS.block_delay_ms.observe(delay_ms as f64);
}

pub(super) fn update_duration(stage: UpdateStage, duration: Duration) {
    METRICS
        .update_duration_ms
        .with_label_values(&[stage])
        .observe(duration.as_micros() as f64 / 1000.0);
}

#[derive(strum::IntoStaticStr, Copy, Clone, Debug)]
pub(super) enum UpdateStage {
    SyncStatus,
    Total,
}

impl AsRef<str> for UpdateStage {
    fn as_ref(&self) -> &str {
        self.into()
    }
}
