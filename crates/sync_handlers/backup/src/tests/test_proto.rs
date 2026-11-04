use crate::backup_ser_de::ProtoSerial;
use crate::proto::v1::{JettonBalanceProto, TvmCodeProto, TvmCodeTraitsProto};
use tongrid_db::entity::jetton_balance::DBJettonBalance;
use tongrid_db::entity::tvm_code_traits::tvm_code_traits::TvmCodeTraits;
use tongrid_db::entity::{DBTx, TvmCode};

#[test]
fn test_corrupt_backup_hashes_and_traits_return_errors() {
    assert!(TvmCode::from_proto(TvmCodeProto {
        hash: vec![1; 31],
        ..Default::default()
    })
    .is_err());
    assert!(TvmCode::from_proto(TvmCodeProto {
        hash: vec![1; 32],
        lib_ids: vec![vec![2; 33]],
        ..Default::default()
    })
    .is_err());
    assert!(TvmCodeTraits::from_proto(TvmCodeTraitsProto {
        code_hash: vec![1; 32],
        traits: vec!["unrecognized-trait".into()],
        ..Default::default()
    })
    .is_err());
    assert!(DBJettonBalance::from_proto(JettonBalanceProto {
        balance: Some("invalid-balance".into()),
        ..Default::default()
    })
    .is_err());
}

#[test]
fn test_backup_preserves_transaction_hash_and_unsigned_storage_bits() -> anyhow::Result<()> {
    let tx = DBTx {
        tx_lt: u64::MAX,
        mc_seqno: u32::MAX,
        tx_hash: ton::ton_core::cell::TonHash::from(&[7u8; 32]),
        ..Default::default()
    };
    let restored = DBTx::from_proto(tx.clone().into_proto())?;
    assert_eq!(restored.tx_lt, tx.tx_lt);
    assert_eq!(restored.mc_seqno, tx.mc_seqno);
    assert_eq!(restored.tx_hash, tx.tx_hash);
    Ok(())
}
