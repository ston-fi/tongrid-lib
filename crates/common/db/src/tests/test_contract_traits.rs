#[test]
fn test_contract_trait_storage_names_stay_stable() {
    use tongrid_proto::v3::ContractTrait as V3ContractTrait;

    let expected = [
        (V3ContractTrait::Unknown, 0, "Unknown"),
        (V3ContractTrait::System, 1, "System"),
        (V3ContractTrait::Elector, 2, "Elector"),
        (V3ContractTrait::Config, 3, "Config"),
        (V3ContractTrait::LogTest, 4, "LogTest"),
        (V3ContractTrait::JettonMaster, 5, "JettonMaster"),
        (V3ContractTrait::JettonWallet, 6, "JettonWallet"),
        (V3ContractTrait::NftItem, 7, "NftItem"),
        (V3ContractTrait::NftCollection, 8, "NftCollection"),
        (V3ContractTrait::WalletAny, 9, "WalletAny"),
        (V3ContractTrait::WalletV1, 10, "WalletV1"),
        (V3ContractTrait::WalletV2, 11, "WalletV2"),
        (V3ContractTrait::WalletV3, 12, "WalletV3"),
        (V3ContractTrait::WalletV4, 13, "WalletV4"),
        (V3ContractTrait::WalletV5, 14, "WalletV5"),
        (V3ContractTrait::WalletHighloadV1, 15, "WalletHighloadV1"),
        (V3ContractTrait::WalletHighloadV2, 16, "WalletHighloadV2"),
    ];

    for (contract_trait, number, name) in expected {
        assert_eq!(contract_trait as i32, number);
        assert_eq!(contract_trait.as_str_name(), name);
        assert_eq!(V3ContractTrait::from_str_name(name), Some(contract_trait));
    }
}
