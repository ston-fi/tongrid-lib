use prometheus::{register_histogram, Histogram, HistogramOpts};
use std::time::Duration;
use stonfi_metrics::constants::DURATION_BUCKETS_1MS_20S;
use stonfi_metrics::MetricsCell;

static METRICS: MetricsCell<Metrics> = MetricsCell::new();
stonfi_metrics::register_metrics!(Metrics, METRICS);

struct Metrics {
    emulation_duration_ms: Histogram,
}

impl Metrics {
    fn new() -> anyhow::Result<Self> {
        let emulation_duration_ms = register_histogram!(HistogramOpts::new(
            "emulation_v3_duration_ms",
            "Time spent running a v3 transaction emulation in ms",
        )
        .buckets(DURATION_BUCKETS_1MS_20S.clone()),)?;

        Ok(Self {
            emulation_duration_ms,
        })
    }
}

pub(super) fn observe_emulation_duration(duration: Duration) {
    METRICS
        .emulation_duration_ms
        .observe(duration.as_micros() as f64 / 1_000.0);
}
