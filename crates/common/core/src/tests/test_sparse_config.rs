use crate::sparse_contracts::config::SparseContractsConfig;

#[test]
fn test_sparse_period_rejects_zero_and_truncating_values() {
    for period in [0_u64, u64::from(u32::MAX) + 1] {
        let input = format!(r#"{{"store_period":{period},"addresses":[]}}"#);
        assert!(serde_json::from_str::<SparseContractsConfig>(&input).is_err());
    }
}
