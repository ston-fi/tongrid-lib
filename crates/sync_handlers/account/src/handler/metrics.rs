use prometheus::{
    register_histogram_vec, register_int_counter_vec, register_int_gauge_vec, HistogramOpts,
    HistogramVec, IntCounterVec, IntGaugeVec, Opts,
};
use stonfi_metrics::constants::DURATION_BUCKETS_1MS_20S;
use stonfi_metrics::MetricsCell;
use strum::IntoEnumIterator;
use tongrid_db::entity::StateID;
use tongrid_tda::ton_db_adapter::libs::LibSearchStats;

static METRICS: MetricsCell<Metrics> = MetricsCell::new();
stonfi_metrics::register_metrics!(Metrics, METRICS);

struct Metrics {
    account_processor_objects_processed: IntCounterVec,
    account_processor_objects_per_mc_block: IntGaugeVec,
    account_processor_process_duration_ms: HistogramVec,
    account_processor_emulation_stat: IntCounterVec,
    account_processor_libs_keeper_stat: IntCounterVec,
}

#[derive(strum::IntoStaticStr, Clone, Debug, Copy)]
pub(super) enum ProcessingStage {
    ProcessAll,
    CollectData,
    BuildTxsInfo,
    CollectTxsInfo,
    CollectOneTxInfo,
    CollectBlockInfo,
    BuildTonTx,
    TryGetKnownState,
    EmulateAll,
    EmulateAddress,
    GetFirstState,
    GetFirstStateFromBC,
    GetLastStateFromBC,
    TryGenState,
    EmulateTx,
    GetLastState,
    SaveAddressStates,
}

impl AsRef<str> for ProcessingStage {
    fn as_ref(&self) -> &str {
        self.into()
    }
}

#[derive(strum::IntoStaticStr, strum::EnumIter, Debug)]
pub(super) enum EmulationCase {
    Known,
    UnknownPrevState,
    Emulated,
    VerifyAfterFailed,
    EmulationFailed,
    GotFromCache,
    GotFromScylla,
    GotFromBC,
    Timeout,
    LostState,
    SparseContract,
}

impl AsRef<str> for EmulationCase {
    fn as_ref(&self) -> &str {
        self.into()
    }
}

#[derive(strum::IntoStaticStr, strum::EnumIter, Debug)]
pub(super) enum LibsCase {
    NoLibs,
    NotInNode,
    NoNodeSearch,
    GotFromScylla,
    GotFromBC,
}

impl AsRef<str> for LibsCase {
    fn as_ref(&self) -> &str {
        self.into()
    }
}

pub(super) struct StateDurationUpdater<'a> {
    stage: ProcessingStage,
    sync_type: StateID,
    start: std::time::Instant,
    metrics: &'a Metrics,
}

impl Drop for StateDurationUpdater<'_> {
    fn drop(&mut self) {
        self.metrics
            .account_processor_process_duration_ms
            .with_label_values(&[self.stage.as_ref(), self.sync_type.as_ref()])
            .observe(self.start.elapsed().as_micros() as f64 / 1000.0);
    }
}

impl Metrics {
    fn new() -> anyhow::Result<Self> {
        let res = Self {
            account_processor_objects_processed: register_int_counter_vec!(
                Opts::new(
                    "account_processor_objects_processed",
                    "Number of objects processed split by type",
                ),
                &["type", "sync_type"]
            )?,
            account_processor_objects_per_mc_block: register_int_gauge_vec!(
                Opts::new(
                    "account_processor_objects_per_mc_block",
                    "Number of objects in one mc block"
                ),
                &["type", "sync_type"]
            )?,
            account_processor_process_duration_ms: register_histogram_vec!(
                HistogramOpts::new(
                    "account_processor_process_duration_ms",
                    "Process duration by different stages"
                )
                .buckets(DURATION_BUCKETS_1MS_20S.clone()),
                &["stage", "sync_type"]
            )?,
            account_processor_emulation_stat: register_int_counter_vec!(
                Opts::new(
                    "account_processor_emulation_stat",
                    "Number of different cases during state generation",
                ),
                &["case_name", "sync_type"]
            )?,
            account_processor_libs_keeper_stat: register_int_counter_vec!(
                Opts::new(
                    "account_processor_libs_keeper_stat",
                    "Number of different cases during libs handling",
                ),
                &["case_name", "sync_type"]
            )?,
        };

        // initialize all counters with 0
        for case in EmulationCase::iter() {
            res.account_processor_emulation_stat
                .with_label_values(&[case.as_ref(), ""])
                .inc_by(0);
        }

        // initialize all counters with 0
        for case in LibsCase::iter() {
            res.account_processor_libs_keeper_stat
                .with_label_values(&[case.as_ref(), ""])
                .inc_by(0);
        }

        Ok(res)
    }

    fn track_stage_duration(
        &self,
        stage: ProcessingStage,
        sync_type: StateID,
    ) -> StateDurationUpdater<'_> {
        StateDurationUpdater {
            stage,
            sync_type,
            start: std::time::Instant::now(),
            metrics: self,
        }
    }

    fn tick_emulation_case(&self, case: EmulationCase, count: usize, sync_type: StateID) {
        self.account_processor_emulation_stat
            .with_label_values(&[case.as_ref(), sync_type.as_ref()])
            .inc_by(count as u64);
    }

    fn tick_lib(&self, cntrs: LibSearchStats, sync_type: StateID) {
        if cntrs.no_lib.load(std::sync::atomic::Ordering::Relaxed) > 0 {
            self.account_processor_libs_keeper_stat
                .with_label_values(&[LibsCase::NoLibs.as_ref(), sync_type.as_ref()])
                .inc_by(cntrs.no_lib.load(std::sync::atomic::Ordering::Relaxed));
        }
        if cntrs.from_scylla.load(std::sync::atomic::Ordering::Relaxed) > 0 {
            self.account_processor_libs_keeper_stat
                .with_label_values(&[LibsCase::GotFromScylla.as_ref(), sync_type.as_ref()])
                .inc_by(cntrs.from_scylla.load(std::sync::atomic::Ordering::Relaxed));
        }
        if cntrs.from_bc.load(std::sync::atomic::Ordering::Relaxed) > 0 {
            self.account_processor_libs_keeper_stat
                .with_label_values(&[LibsCase::GotFromBC.as_ref(), sync_type.as_ref()])
                .inc_by(cntrs.from_bc.load(std::sync::atomic::Ordering::Relaxed));
        }
        if cntrs
            .not_found_onchain
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
        {
            self.account_processor_libs_keeper_stat
                .with_label_values(&[LibsCase::NotInNode.as_ref(), sync_type.as_ref()])
                .inc_by(
                    cntrs
                        .not_found_onchain
                        .load(std::sync::atomic::Ordering::Relaxed),
                );
        }
        if cntrs
            .node_search_disabled
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
        {
            self.account_processor_libs_keeper_stat
                .with_label_values(&[LibsCase::NoNodeSearch.as_ref(), sync_type.as_ref()])
                .inc_by(
                    cntrs
                        .node_search_disabled
                        .load(std::sync::atomic::Ordering::Relaxed),
                );
        }
    }

    fn update_objects_processed(
        &self,
        mc_shards: usize,
        total_shards: usize,
        accounts: usize,
        txs: usize,
        sync_type: StateID,
    ) {
        for (label, value) in &[
            ("mc_shards", mc_shards),
            ("total_shards", total_shards),
            ("accounts", accounts),
            ("txs", txs),
        ] {
            self.account_processor_objects_processed
                .with_label_values(&[*label, sync_type.as_ref()])
                .inc_by(*value as u64);
            self.account_processor_objects_per_mc_block
                .with_label_values(&[*label, sync_type.as_ref()])
                .set(*value as i64);
        }
    }
}

pub(super) fn track_stage_duration(
    stage: ProcessingStage,
    sync_type: StateID,
) -> StateDurationUpdater<'static> {
    METRICS.track_stage_duration(stage, sync_type)
}

pub(super) fn tick_emulation_case(case: EmulationCase, count: usize, sync_type: StateID) {
    METRICS.tick_emulation_case(case, count, sync_type);
}

pub(super) fn tick_lib(cntrs: LibSearchStats, sync_type: StateID) {
    METRICS.tick_lib(cntrs, sync_type);
}

pub(super) fn update_objects_processed(
    mc_shards: usize,
    total_shards: usize,
    accounts: usize,
    txs: usize,
    sync_type: StateID,
) {
    METRICS.update_objects_processed(mc_shards, total_shards, accounts, txs, sync_type);
}
