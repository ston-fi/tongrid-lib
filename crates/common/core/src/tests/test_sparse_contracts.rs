use crate::sparse_contracts::SparseContracts;

#[test]
fn test_sparse_schedule_preserves_selected_address_checkpoints() -> anyhow::Result<()> {
    let period =
        std::num::NonZeroU32::new(10).ok_or_else(|| anyhow::anyhow!("invalid test period"))?;
    let policy = SparseContracts::new(
        period,
        std::collections::HashSet::from(["selected".to_string()]),
    );
    assert!(!policy.is_sparse_at_height("selected", 20));
    assert!(policy.is_sparse_at_height("selected", 27));
    assert!(!policy.is_sparse_at_height("other", 27));
    assert_eq!(policy.exists_at_height("selected", 27), 20);
    assert_eq!(policy.exists_at_height("other", 27), 27);
    assert_eq!(policy.last_filled_height(9), 0);
    Ok(())
}
