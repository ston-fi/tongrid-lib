use std::time::Duration;

use ton::errors::TonError;
use ton::ton_core::{
    errors::{TonCoreError, TonCoreResult},
    traits::emulation_provider::{
        EmulationProvider, EmulatorContractState, EmulatorGetMethodRequest,
        EmulatorGetMethodSuccess,
    },
};
use tonic::async_trait;

use tongrid_proto::v3::emulate_get_method_request::{
    ContractState as EmulationContractState, MethodId,
};
use tongrid_proto::v3::emulate_get_method_response::EmulateGetMethodResult;
use tongrid_proto::v3::{
    AddressLt, EmulateGetMethodRequest as GrpcEmulateGetMethodRequest, EmulateRunMethodState,
};

use super::{to_ton_core_boxed, TongridClient};

#[async_trait]
impl EmulationProvider for TongridClient {
    async fn emulate_get_method(
        &self,
        request: EmulatorGetMethodRequest,
        timeout: Option<Duration>,
    ) -> TonCoreResult<EmulatorGetMethodSuccess> {
        let (contract_state, mc_seqno) = map_contract_state(
            request.contract_state,
            self.last_sync_status().full_synced_seqno,
        )?;

        let grpc_request = GrpcEmulateGetMethodRequest {
            mc_seqno,
            contract_state: Some(contract_state),
            method_id: Some(MethodId::MethodIdInt(request.method_id)),
            stack_boc: Some(request.stack_boc.as_ref().clone()),
        };
        let mut emulation_client = self.emulation();
        let emulation = emulation_client.emulate_get_method(grpc_request);
        let response = match timeout {
            Some(timeout) => tokio::time::timeout(timeout, emulation)
                .await
                .map_err(|_| to_ton_core_boxed(TonError::EmulatorTimeout(timeout)))?,
            None => emulation.await,
        }
        .map_err(to_ton_core_boxed)?
        .into_inner();

        map_result(response.emulate_get_method_result)
    }

    fn requires_resolved_state(&self) -> bool {
        true
    }
}

fn map_contract_state(
    state: EmulatorContractState,
    full_synced_seqno: u32,
) -> TonCoreResult<(EmulationContractState, u32)> {
    match state {
        EmulatorContractState::Address { address, tx_id } => {
            let contract_state = match tx_id {
                Some(tx_id) => EmulationContractState::AddressLt(AddressLt {
                    address: address.to_string(),
                    lt: u64::try_from(tx_id.lt).map_err(|_| {
                        TonCoreError::Custom(format!("negative transaction LT: {}", tx_id.lt))
                    })?,
                }),
                None => EmulationContractState::Address(address.to_string()),
            };
            Ok((contract_state, full_synced_seqno))
        }
        EmulatorContractState::Custom(state) => {
            let mc_seqno = state.mc_seqno.unwrap_or(full_synced_seqno);
            let code_boc = state.code_boc.as_ref().ok_or_else(|| {
                TonCoreError::Custom(format!(
                    "contract {} has no code for emulation",
                    state.address
                ))
            })?;
            let balance = u64::try_from(state.balance).map_err(|_| {
                TonCoreError::Custom(format!("contract {} has negative balance", state.address))
            })?;
            let contract_state = EmulationContractState::Custom(EmulateRunMethodState {
                address: state.address.to_string(),
                code_boc: code_boc.as_ref().clone(),
                data_boc: state
                    .data_boc
                    .as_ref()
                    .map(|data| data.as_ref().clone())
                    .unwrap_or_default(),
                balance,
            });
            Ok((contract_state, mc_seqno))
        }
        _ => Err(TonCoreError::Custom(
            "unsupported emulator contract state".to_owned(),
        )),
    }
}

fn map_result(result: Option<EmulateGetMethodResult>) -> TonCoreResult<EmulatorGetMethodSuccess> {
    match result {
        Some(EmulateGetMethodResult::Success(success)) => Ok(EmulatorGetMethodSuccess::new(
            success.exit_code,
            success.stack_boc,
        )),
        Some(EmulateGetMethodResult::Error(error)) => Err(TonCoreError::BoxedError(Box::new(
            TonError::EmulatorEmulationError {
                vm_exit_code: error.exit_code,
                response_raw: error.emulator_response_json,
            },
        ))),
        None => Err(TonCoreError::Custom(
            "empty get-method emulation response".to_owned(),
        )),
    }
}

#[cfg(test)]
#[path = "../tests/test_emulation.rs"]
mod tests;
