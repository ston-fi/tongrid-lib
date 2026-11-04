use std::collections::HashMap;

use prometheus::{
    register_histogram_vec, register_int_counter_vec, register_int_gauge_vec, HistogramOpts,
    IntCounterVec, IntGaugeVec, Opts,
};
use stonfi_metrics::constants::DURATION_BUCKETS_1MS_20S;
use stonfi_metrics::MetricsCell;
use tongrid_proto::v3::ContractTrait;

static METRICS: MetricsCell<Metrics> = MetricsCell::new();
stonfi_metrics::register_metrics!(Metrics, METRICS);

struct Metrics {
    classifier_processor_objects_processed: IntCounterVec,
    classifier_processor_objects_per_mc_block: IntGaugeVec,
}

impl Metrics {
    fn new() -> anyhow::Result<Self> {
        register_histogram_vec!(
            HistogramOpts::new(
                "classifier_processor_process_duration_ms",
                "Process duration by different stages",
            )
            .buckets(DURATION_BUCKETS_1MS_20S.clone()),
            &["stage"]
        )?;

        let res = Self {
            classifier_processor_objects_processed: register_int_counter_vec!(
                Opts::new(
                    "classifier_processor_objects_processed",
                    "Number of objects processed split by type",
                ),
                &["type"]
            )?,
            classifier_processor_objects_per_mc_block: register_int_gauge_vec!(
                Opts::new(
                    "classifier_processor_objects_per_mc_block",
                    "Number of objects in one mc block"
                ),
                &["type"]
            )?,
        };

        Ok(res)
    }
}

pub(super) fn update_objects_processed(contract_traits: Vec<ContractTrait>) {
    let mut counts: HashMap<ContractTrait, usize> = HashMap::new();
    for contract_trait in &contract_traits {
        *counts.entry(*contract_trait).or_insert(0) += 1;
    }

    for (trait_variant, count) in counts {
        let label = &trait_variant.as_str_name().to_string();
        METRICS
            .classifier_processor_objects_processed
            .with_label_values(&[label])
            .inc_by(count as u64);
        METRICS
            .classifier_processor_objects_per_mc_block
            .with_label_values(&[label])
            .set(count as i64);
    }
}
