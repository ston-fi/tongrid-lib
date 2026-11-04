use prometheus::{
    register_histogram, register_histogram_vec, register_int_counter_vec, register_int_gauge_vec,
    HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts,
};
use stonfi_metrics::constants::DURATION_BUCKETS_01MS_20S;
use stonfi_metrics::MetricsCell;
use tongrid_db::entity::StateID;

static METRICS: MetricsCell<Metrics> = MetricsCell::new();
stonfi_metrics::register_metrics!(Metrics, METRICS);

struct Metrics {
    block_processor_objects_processed: IntCounterVec,
    block_processor_objects_per_mc_block: IntGaugeVec,
    block_processor_block_delay_ms: HistogramVec,
}

impl Metrics {
    fn new() -> anyhow::Result<Self> {
        let labels = &["type", "state_id"];
        register_histogram!(HistogramOpts::new(
            "block_processor_wait_items_duration_ms",
            "how much time we're waiting for new block items",
        )
        .buckets(DURATION_BUCKETS_01MS_20S.clone()),)?;

        let res = Self {
            block_processor_objects_processed: register_int_counter_vec!(
                Opts::new(
                    "block_processor_objects_processed",
                    "Number of objects processed split by type"
                ),
                labels
            )?,
            block_processor_objects_per_mc_block: register_int_gauge_vec!(
                Opts::new(
                    "block_processor_objects_per_mc_block",
                    "Number of objects in one mc block"
                ),
                labels
            )?,
            block_processor_block_delay_ms: register_histogram_vec!(
                HistogramOpts::new(
                    "block_processor_block_delay_ms",
                    "Delay between time when processor got block and block_utime",
                ),
                &["sync_type"]
            )?,
        };
        Ok(res)
    }

    fn update_processed_objects(
        &self,
        shards: usize,
        txs: usize,
        msgs: usize,
        tvm_codes: usize,
        state_id: StateID,
    ) {
        for (label, value) in &[
            ("mc_shards", 1),
            ("total_shards", shards),
            ("txs", txs),
            ("msgs", msgs),
            ("tvm_codes", tvm_codes),
        ] {
            self.block_processor_objects_processed
                .with_label_values(&[*label, state_id.as_ref()])
                .inc_by(*value as u64);
            self.block_processor_objects_per_mc_block
                .with_label_values(&[*label, state_id.as_ref()])
                .set(*value as i64);
        }
    }

    fn update_block_delay(&self, delay_ms: u128, sync_type: StateID) {
        self.block_processor_block_delay_ms
            .with_label_values(&[sync_type.as_ref()])
            .observe(delay_ms as f64);
    }
}

pub(super) fn update_processed_objects(
    shards: usize,
    txs: usize,
    msgs: usize,
    tvm_codes: usize,
    state_id: StateID,
) {
    METRICS.update_processed_objects(shards, txs, msgs, tvm_codes, state_id);
}

pub(super) fn update_block_delay(delay_ms: u128, sync_type: StateID) {
    METRICS.update_block_delay(delay_ms, sync_type);
}
