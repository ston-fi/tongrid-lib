use crate::handler::metrics;
use crate::handler::types::EmulTxInfo;
use anyhow::bail;
use anyhow::Result;
use std::collections::HashSet;
use ton::block_tlb::MaybeAccount;
use ton::ton_core::cell::{TonCellUtils, TonHash};
use ton::ton_core::traits::tlb::TLB;
use tongrid_db::entity::StateID;
use tongrid_tda::ton_db_adapter::TonDBAdapter;

/// Helper to extract/store/cache required libraries from arbitrary blockchain data
pub struct TonLibsKeeper {
    tda: TonDBAdapter,
}

impl TonLibsKeeper {
    pub fn new(tda: TonDBAdapter) -> Result<Self> {
        let keeper = Self { tda };
        Ok(keeper)
    }

    pub async fn get_libs(
        &self,
        account: &MaybeAccount,
        emul_tx: &EmulTxInfo,
        sync_type: StateID,
    ) -> Result<Option<Vec<u8>>> {
        let mut all_lib_ids: HashSet<TonHash> = TonCellUtils::extract_lib_ids(
            [account.get_code(), account.get_data()]
                .into_iter()
                .flatten(),
        )?;
        let before = self.tda.libs().stats().snapshot();
        all_lib_ids.extend(emul_tx.tx_lib_ids.iter().cloned());
        let (libs, _unknown_libs) =
            match self.tda.libs().get_libs_by_hashes(all_lib_ids.iter()).await {
                Ok(result) => result,
                Err(err) => {
                    bail!(
                        "error: {err}. Address: {}, Tx: {}:{:X}",
                        emul_tx.addr,
                        emul_tx.ton_tx.lt,
                        emul_tx.ton_tx.cell_hash()?
                    );
                }
            };
        let mut result_cntrs = self.tda.libs().stats().snapshot();
        result_cntrs -= &before;

        metrics::tick_lib(result_cntrs, sync_type);

        Ok(libs)
    }
}
