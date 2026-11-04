use crate::ton_db_adapter::TonDBAdapter;
use anyhow::bail;
use std::str::FromStr;
use std::sync::Arc;
use tokio::time::Duration;
use ton::bail_ton;
use ton::emulators::emulator_pool::TVMGetMethodTask;
use ton::emulators::tvm_emulator::TVMState;
use ton::emulators::tvm_emulator::{TVMEmulatorC7, TVMGetMethodID, TVMGetMethodSuccess};
use ton::emulators::tx_emulator::TXEmulOrdArgs;
use ton::emulators::tx_emulator::TXEmulTickTockArgs;
use ton::emulators::tx_emulator::TXEmulationResponse;
use ton::errors::TonError;
use ton::errors::TonResult;
use ton::libs_dict::LibsDict;
use ton::ton_core::cell::{TonCell, TonHash};
use ton::ton_core::traits::tlb::TLB;
use ton::ton_core::types::TonAddress;
use tongrid_db::entity::account::db_account::DBAccount;
const MAX_DYN_LIBS: usize = 100;

pub struct EmulatorAdapter<'a> {
    tda: &'a TonDBAdapter,
}

impl<'a> From<&'a TonDBAdapter> for EmulatorAdapter<'a> {
    fn from(tda: &'a TonDBAdapter) -> Self {
        Self { tda }
    }
}

impl<'a> EmulatorAdapter<'a> {
    pub async fn emulate_get_method<T>(
        &self,
        state: TVMState,
        method: T,
        stack_boc: Vec<u8>,
    ) -> TonResult<TVMGetMethodSuccess>
    where
        T: Into<TVMGetMethodID>,
    {
        let method_id = method.into();
        let stack_boc = Arc::new(stack_boc);
        let mut libs_dict = match &state.libs_boc {
            Some(libs_boc) => {
                let libs_cell = TonCell::from_boc(libs_boc.as_ref().clone())?;
                LibsDict::from_cell(&libs_cell)?
            }
            None => LibsDict::default(),
        };
        let mut iteration = 0;

        let ton = &self.tda.inner().ton;

        loop {
            if iteration > MAX_DYN_LIBS {
                return Err(TonError::EmulatorTooManyLibraries(MAX_DYN_LIBS));
            }
            let libs_boc = if libs_dict.is_empty() {
                None
            } else {
                Some(libs_dict.to_boc()?)
            };
            let mut task_state = state.clone();
            task_state.libs_boc = libs_boc.map(Arc::new);
            task_state.debug_enabled = Some(true);
            let task = TVMGetMethodTask {
                state: task_state,
                method: method_id.clone(),
                stack_boc: stack_boc.clone(),
            };
            // Each pool request gets a two-second timeout; each missing-library retry gets a new timeout.
            // TODO: accept a caller deadline and share its remaining budget across library retries.
            let emul_result = ton
                .emul_pool
                .emul_get_method(task, Some(Duration::from_secs(2)))
                .await?;

            let Some(missing_lib_hash) = emul_result.missing_lib()? else {
                return emul_result.into_success();
            };

            let lib_boc = match self.tda.libs().get_lib_by_hash(&missing_lib_hash).await {
                Ok(Some(boc)) => boc,
                Ok(None) => return Err(TonError::EmulatorMissingLibrary(missing_lib_hash)),
                Err(err) => bail_ton!("Error loading library {missing_lib_hash} onchain: {err}"),
            };

            let lib_cell = TonCell::from_boc(lib_boc)?;
            libs_dict.insert(missing_lib_hash, lib_cell.into());
            iteration += 1;
        }
    }

    pub async fn emul_ord_tx(
        &self,
        task: TXEmulOrdArgs,
        timeout: Option<Duration>,
    ) -> TonResult<TXEmulationResponse> {
        self.tda
            .inner()
            .ton
            .emul_pool
            .emul_ord_tx(task, timeout)
            .await
    }
    pub async fn emul_ticktock_tx(
        &self,
        task: TXEmulTickTockArgs,
        timeout: Option<Duration>,
    ) -> TonResult<TXEmulationResponse> {
        // Only the supplied libraries are available here; missing-library failures propagate to the caller.
        // TODO: add dynamic retries when the transaction emulator exposes the missing library hash.
        self.tda
            .inner()
            .ton
            .emul_pool
            .emul_ticktock_tx(task, timeout)
            .await
    }

    pub async fn collect_tvm_states(
        &self,
        db_state: &DBAccount,
        mc_seqno: u32,
        key_seqno: u32,
    ) -> anyhow::Result<TVMState> {
        let (maybe_account, lib_ids) = self.tda.account().build_account(db_state).await?;
        let address = TonAddress::from_str(&db_state.address)?;

        let code = match maybe_account.get_code() {
            Some(code) => code.to_boc()?,
            None => {
                bail!("[collect_emul_data]: no code for address {address}, mc_seqno {mc_seqno}")
            }
        };

        let data = match maybe_account.get_data() {
            Some(data) => data.to_boc()?,
            None => vec![],
        };

        let bc_config = self.tda.conf().get_config(key_seqno).await?;
        let c7 = TVMEmulatorC7::new(address, bc_config)?;

        let db_lib_ids = lib_ids.into_iter().collect::<Vec<_>>();
        let libs_boc = self.tda.libs().get_libs_dict(&db_lib_ids).await?;

        Ok(build_tvm_state(code, data, c7, libs_boc))
    }
}

pub fn build_tvm_state(
    code: Vec<u8>,
    data: Vec<u8>,
    c7: TVMEmulatorC7,
    libs_boc: Option<Vec<u8>>,
) -> TVMState {
    let libs_arc = libs_boc.as_ref().map(|boc| Arc::new(boc.clone()));
    TVMState {
        code_boc: Arc::new(code),
        data_boc: Arc::new(data),
        c7,
        libs_boc: libs_arc,
        debug_enabled: Some(false),
        gas_limit: None,
    }
}

pub fn calculate_code_hash(state: &TVMState) -> anyhow::Result<TonHash> {
    Ok(*TonCell::from_boc(state.code_boc.clone())?.hash()?)
}
