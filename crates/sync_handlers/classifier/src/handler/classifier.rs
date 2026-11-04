use std::pin::Pin;
use std::{collections::HashSet, future::Future};

use anyhow::Result;
use futures::future::try_join_all;
use ton::block_tlb::TVMStack;
use ton::emulators::tvm_emulator::TVMGetMethodSuccess;
use ton::emulators::tvm_emulator::TVMState;
use ton::errors::{TonError, TonResult};
use ton::ton_core::traits::tlb::TLB;
use ton::ton_core::types::TonAddress;
use tongrid_proto::v3::ContractTrait;
use tongrid_tda::ton_db_adapter::emulation::calculate_code_hash;
use tongrid_tda::ton_db_adapter::TonDBAdapter;

pub const CONTRACT_CLASSIFIER_CURRENT_TRAIT_VERSION: i32 = 6;

pub struct Classifier<'a> {
    tvm_state: &'a TVMState,
    tda: &'a TonDBAdapter,
    predefined: &'a (dyn Fn(&ton::ton_core::cell::TonHash) -> Vec<ContractTrait> + Send + Sync),
}

impl<'a> Classifier<'a> {
    pub fn new(
        tvm_state: &'a TVMState,
        tda: &'a TonDBAdapter,
        predefined: &'a (dyn Fn(&ton::ton_core::cell::TonHash) -> Vec<ContractTrait> + Send + Sync),
    ) -> Self {
        Self {
            tvm_state,
            tda,
            predefined,
        }
    }

    async fn check_method_success(&self, method: &'static str, stack_boc: Vec<u8>) -> Result<bool> {
        let result = self
            .tda
            .emulation()
            .emulate_get_method(self.tvm_state.clone(), method, stack_boc)
            .await;

        is_result_success(&self.tvm_state.c7.address, result)
    }

    async fn check_trait(
        &self,
        method: &'static str,
        trait_variant: ContractTrait,
        stack_boc: Vec<u8>,
    ) -> Result<Vec<ContractTrait>> {
        if self.check_method_success(method, stack_boc).await? {
            return Ok(vec![trait_variant]);
        }

        Ok(vec![])
    }

    pub async fn classify(&self) -> Result<HashSet<ContractTrait>> {
        type BoxedFuture<'a> =
            Pin<Box<dyn Future<Output = Result<Vec<ContractTrait>>> + Send + 'a>>;

        let detect_futs: Vec<BoxedFuture> = vec![
            Box::pin(self.detect_predefined_contract()),
            Box::pin(self.detect_jetton_master()),
            Box::pin(self.check_trait(
                "get_wallet_data",
                ContractTrait::JettonWallet,
                TVMStack::EMPTY_BOC.to_owned(),
            )),
            Box::pin(self.check_trait(
                "get_collection_data",
                ContractTrait::NftCollection,
                TVMStack::EMPTY_BOC.to_owned(),
            )),
            Box::pin(self.check_trait(
                "get_nft_data",
                ContractTrait::NftItem,
                TVMStack::EMPTY_BOC.to_owned(),
            )),
        ];

        let result_traits: HashSet<_> = try_join_all(detect_futs)
            .await?
            .into_iter()
            .flatten()
            .collect();

        if result_traits.is_empty() {
            Ok(HashSet::from([ContractTrait::Unknown]))
        } else {
            Ok(result_traits)
        }
    }

    async fn detect_predefined_contract(&self) -> Result<Vec<ContractTrait>> {
        Ok((self.predefined)(&calculate_code_hash(self.tvm_state)?))
    }

    async fn detect_jetton_master(&self) -> Result<Vec<ContractTrait>> {
        let mut can_be_jetton_master = self
            .check_method_success("get_jetton_data", TVMStack::EMPTY_BOC.to_owned())
            .await?;

        let mut stack = TVMStack::default();
        let owner_address = TonAddress::ZERO;
        stack.push_cell_slice(owner_address.to_cell()?);
        let stack_boc = stack.to_boc()?;

        let can_call_wallet_address = self
            .check_method_success("get_wallet_address", stack_boc)
            .await?;
        can_be_jetton_master &= can_call_wallet_address;

        if can_be_jetton_master {
            return Ok(vec![ContractTrait::JettonMaster]);
        }

        Ok(vec![])
    }
}

fn is_result_success(address: &TonAddress, result: TonResult<TVMGetMethodSuccess>) -> Result<bool> {
    let err = match result {
        Ok(_) => return Ok(true),
        Err(err) => err,
    };

    let success = match err {
        TonError::EmulatorCreationFailed => false,
        TonError::EmulatorEmulationError {
            vm_exit_code: Some(exit_code),
            ..
        } => {
            match exit_code {
                11 => false,      // exit code most commonly raised for unknown method
                32 | 34 => false, // exit codes for incorrect action list
                _ => true,
            }
        }
        TonError::EmulatorMissingLibrary(hash) => {
            log::warn!("Fail to load lib {hash} for account: {:?}", address);
            false
        }
        TonError::EmulatorTooManyLibraries(limit) => {
            log::warn!(
                "Reached dynamic library limit {limit} for account: {:?}",
                address
            );
            false
        }
        _ => return Err(err.into()),
    };
    Ok(success)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_too_many_libraries_is_not_classifier_success() -> Result<()> {
        let result = is_result_success(
            &TonAddress::ZERO,
            Err(TonError::EmulatorTooManyLibraries(100)),
        )?;

        assert!(!result);

        Ok(())
    }
}
