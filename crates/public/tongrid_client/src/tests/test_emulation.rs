use std::str::FromStr;
use std::sync::Arc;

use anyhow::bail;
use ton::ton_core::{
    cell::TonHash,
    traits::state_provider::ContractState,
    types::{TonAddress, TxLTHash},
};
use tongrid_proto::v3::EmulateGetMethodError;

use super::*;

#[test]
fn test_emulation_rejection_preserves_ton_error() -> anyhow::Result<()> {
    let result = map_result(Some(EmulateGetMethodResult::Error(EmulateGetMethodError {
        exit_code: Some(9),
        emulator_response_json: "stack underflow".to_owned(),
    })));
    let Err(TonCoreError::BoxedError(error)) = result else {
        bail!("emulator rejection was not returned as a boxed TON error");
    };
    let Ok(error) = error.downcast::<TonError>() else {
        bail!("boxed emulator rejection was not a TonError");
    };
    match *error {
        TonError::EmulatorEmulationError {
            vm_exit_code: Some(9),
            response_raw,
        } if response_raw == "stack underflow" => Ok(()),
        error => bail!("unexpected emulator rejection: {error:?}"),
    }
}

#[test]
fn test_emulation_request_maps_custom_snapshot_and_historical_address_lt() -> anyhow::Result<()> {
    let address =
        TonAddress::from_str("0:0000000000000000000000000000000000000000000000000000000000000000")?;
    let tx_hash = TonHash::from_vec(vec![7; TonHash::BYTES_LEN])?;
    let tx_id = TxLTHash::new(7, tx_hash);
    let (historical_state, historical_mc_seqno) = map_contract_state(
        EmulatorContractState::Address {
            address,
            tx_id: Some(tx_id.clone()),
        },
        120,
    )?;
    let EmulationContractState::AddressLt(address_lt) = historical_state else {
        bail!("historical emulation did not use an address/LT key");
    };
    assert_eq!(address_lt.address, address.to_string());
    assert_eq!(address_lt.lt, 7);
    assert_eq!(historical_mc_seqno, 120);

    let state = ContractState::new(address, tx_id, 123)
        .with_mc_seqno(100)
        .with_code_boc(Arc::new(vec![1, 2]))
        .with_data_boc(Arc::new(vec![3, 4]));
    let (custom_state, custom_mc_seqno) =
        map_contract_state(EmulatorContractState::Custom(Arc::new(state)), 120)?;
    let EmulationContractState::Custom(custom) = custom_state else {
        bail!("custom state did not reach the emulation request");
    };
    assert_eq!(custom.balance, 123);
    assert_eq!(custom.code_boc, [1, 2]);
    assert_eq!(custom.data_boc, [3, 4]);
    assert_eq!(custom_mc_seqno, 100);
    Ok(())
}

#[test]
fn test_invalid_emulation_state_and_missing_response_fail() -> anyhow::Result<()> {
    let address: TonAddress =
        "0:0000000000000000000000000000000000000000000000000000000000000000".parse()?;
    let state = ContractState::new(address, TxLTHash::new(1, TonHash::ZERO), 1);
    assert!(
        map_contract_state(EmulatorContractState::Custom(Arc::new(state.clone())), 100).is_err()
    );
    let negative = state.with_code_boc(Arc::new(vec![]));
    let mut negative = negative;
    negative.balance = -1;
    assert!(map_contract_state(EmulatorContractState::Custom(Arc::new(negative)), 100).is_err());
    assert!(map_contract_state(
        EmulatorContractState::Address {
            address,
            tx_id: Some(TxLTHash::new(-1, TonHash::ZERO))
        },
        100
    )
    .is_err());
    assert!(map_result(None).is_err());
    Ok(())
}
