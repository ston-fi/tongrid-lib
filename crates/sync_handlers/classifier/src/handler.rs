use std::{collections::HashSet, vec};

use crate::Classifier;
use anyhow::Result;
use classifier::CONTRACT_CLASSIFIER_CURRENT_TRAIT_VERSION;
use futures::future::try_join_all;
use std::time::Duration;
use stonfi_distributed_sync::coordinator::TaskPriority;
use stonfi_distributed_sync::task::{EmptyTaskResult, RangeTask};
use ton::emulators::tvm_emulator::TVMState;
use ton::ton_core::cell::TonHash;
use tongrid_db::entity::account::db_account::DBAccount;
use tongrid_db::entity::tvm_code_traits::tvm_code_traits::TvmCodeTraits;
use tongrid_db::entity::{StateID, TxKeyLookup};
use tongrid_proto::v3::ContractTrait;
use tongrid_tda::ton_db_adapter::emulation::calculate_code_hash;
use tongrid_tda::ton_db_adapter::TonDBAdapter;

pub mod classifier;
pub mod distributed_handler_impl;
mod metrics;

pub struct ClassifierHandler {
    tda: TonDBAdapter,
    #[allow(
        clippy::type_complexity,
        reason = "Caller-owned classification lookup keeps private presets outside this crate"
    )]
    predefined: std::sync::Arc<dyn Fn(&TonHash) -> Vec<ContractTrait> + Send + Sync>,
    pub(crate) sync_id: String,
    pub(crate) enabled: bool,
    pub(crate) task_timeout: Duration,
    pub(crate) task_priority: TaskPriority,
    pub(crate) max_batch_size: u32,
}

impl ClassifierHandler {
    #[allow(
        clippy::type_complexity,
        reason = "Caller-owned classification lookup keeps private presets outside this crate"
    )]
    pub async fn new(
        tda: TonDBAdapter,
        config: &crate::config::ClassifierHandlerConfig,
        state_id: StateID,
        predefined: std::sync::Arc<dyn Fn(&TonHash) -> Vec<ContractTrait> + Send + Sync>,
    ) -> Result<Self> {
        let (enabled, task_priority) = config.state_settings(state_id)?;
        let task_timeout = config.task_timeout;
        let max_batch_size = config.max_batch_size;
        let handler = Self {
            tda,
            predefined,
            sync_id: state_id.to_string(),
            enabled,
            task_timeout,
            task_priority,
            max_batch_size,
        };
        log::info!("[CLASSIFIER_PROCESSOR] created");
        Ok(handler)
    }

    pub async fn process(&self, task: RangeTask) -> Result<EmptyTaskResult> {
        log::debug!("[CLASSIFIER_PROCESSOR] task={task:?}: start processing");

        let from = u32::try_from(task.from)?;
        let to = u32::try_from(task.to)?;

        let mut traits_vec = vec![];

        for mc_seqno in from..=to {
            let unclassified_code_hashes = self.get_unclassified_code_hashes(mc_seqno).await?;
            let db_states = self
                .get_db_states(mc_seqno, unclassified_code_hashes.clone())
                .await?;

            let key_block_seqno = self
                .tda
                .db()
                .block()
                .get_master_by_mc_seqno(mc_seqno)
                .await?
                .key_block_seqno;
            let tvm_state_futs = db_states.iter().map(|x| async move {
                self.tda
                    .emulation()
                    .collect_tvm_states(x, mc_seqno, key_block_seqno)
                    .await
            });

            let tvm_states = try_join_all(tvm_state_futs).await?;
            // ===
            let futures = tvm_states
                .iter()
                .map(|state| self.classify_smc(mc_seqno, state));
            traits_vec.extend(try_join_all(futures).await?.into_iter().flatten());
        }

        log::debug!("[CLASSIFIER_PROCESSOR] task={task:?}: completed");
        metrics::update_objects_processed(traits_vec);
        Ok(EmptyTaskResult)
    }

    async fn get_unclassified_code_hashes(&self, mc_seqno: u32) -> Result<Vec<TonHash>> {
        let all_account_states = self
            .tda
            .db()
            .account_lite()
            .get_by_mc_seqno(mc_seqno)
            .await?;

        let code_hashes = all_account_states
            .into_iter()
            .flat_map(|x| x.code_hash)
            .collect::<Vec<_>>();

        let traits_futs = code_hashes
            .iter()
            .map(|code_hash| self.tda.db().tvm_code_traits().get_by_code_hash(code_hash));

        let traits = try_join_all(traits_futs).await?;

        let mut unclassified_code_hashes = Vec::new();
        let mut delete_old_futs = Vec::new();

        for (code_hash, trait_result) in code_hashes.into_iter().zip(traits) {
            match trait_result {
                Some(traits) => {
                    let delete_fut = self.tda.db().tvm_code_traits().delete_old_versions(
                        code_hash,
                        CONTRACT_CLASSIFIER_CURRENT_TRAIT_VERSION - 2,
                    );
                    delete_old_futs.push(delete_fut);

                    if traits.version < CONTRACT_CLASSIFIER_CURRENT_TRAIT_VERSION {
                        unclassified_code_hashes.push(code_hash);
                    }
                }
                None => unclassified_code_hashes.push(code_hash),
            }
        }

        try_join_all(delete_old_futs).await?;
        Ok(unclassified_code_hashes)
    }

    async fn get_db_states(
        &self,
        mc_seqno: u32,
        code_hashes: Vec<TonHash>,
    ) -> Result<HashSet<DBAccount>> {
        let code_hashes_slices = code_hashes.iter().map(|x| x.as_slice()).collect::<Vec<_>>();
        let mc_seqno_slice = &[mc_seqno];
        let tx_lookup_key = TxKeyLookup::seqnos_code_hashes(
            mc_seqno_slice.as_slice(),
            code_hashes_slices.as_slice(),
        );
        let states = self
            .tda
            .db()
            .account()
            .lookup_many(tx_lookup_key)
            .await?
            .into_iter()
            .collect();
        Ok(states)
    }

    async fn classify_smc(
        &self,
        mc_seqno: u32,
        emul_state: &TVMState,
    ) -> Result<HashSet<ContractTrait>> {
        let classifier = Classifier::new(emul_state, &self.tda, self.predefined.as_ref());

        let contract_traits = classifier.classify().await?;

        let tvm_code_traits = TvmCodeTraits {
            code_hash: calculate_code_hash(emul_state)?,
            version: CONTRACT_CLASSIFIER_CURRENT_TRAIT_VERSION,
            traits: contract_traits.clone(),
            mc_seqno,
        };

        self.tda
            .db()
            .tvm_code_traits()
            .insert_many([tvm_code_traits])
            .await?;

        Ok(contract_traits)
    }
}
