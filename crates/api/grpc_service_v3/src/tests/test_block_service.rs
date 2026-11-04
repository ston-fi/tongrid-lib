use super::get_shards_seqnos;
use tongrid_proto::v3::McSeqnoRange;

fn range(from_mc_seqno: u32, to_mc_seqno: u32) -> Option<McSeqnoRange> {
    Some(McSeqnoRange {
        from_mc_seqno,
        to_mc_seqno,
    })
}

#[test]
fn test_shard_ranges_are_bounded_before_allocation_and_clamping() -> anyhow::Result<()> {
    let max = i32::MAX as u32;
    for (from, to, expected) in [
        (0, 0, vec![0]),
        (42, 43, vec![42, 43]),
        (0, 99, (0..100).collect()),
        (max - 99, max, (max - 99..=max).collect()),
        (max - 1, max + 1, vec![max - 1, max]),
        (u32::MAX, u32::MAX, vec![max]),
        (u32::MAX - 99, u32::MAX, vec![max]),
    ] {
        assert_eq!(get_shards_seqnos(range(from, to))?, expected);
    }
    for range in [
        None,
        range(0, 100),
        range(0, u32::MAX),
        range(1, 0),
        range(u32::MAX, u32::MAX - 1),
        range(u32::MAX - 100, u32::MAX),
    ] {
        let error = match get_shards_seqnos(range) {
            Ok(_) => anyhow::bail!("invalid range must fail before allocation"),
            Err(error) => error,
        };
        assert_eq!(
            tonic::Status::from(error).code(),
            tonic::Code::InvalidArgument
        );
    }
    Ok(())
}
