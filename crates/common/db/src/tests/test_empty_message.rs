use crate::entity::scylla_types::DBMsg;
use ton::{
    block_tlb::{CommonMsgInfo, Msg},
    ton_core::{cell::TonCell, traits::tlb::TLB},
};
#[test]
fn test_default_message_retains_ton_wire_encoding() -> anyhow::Result<()> {
    let encoded = Msg::new(CommonMsgInfo::default(), TonCell::empty().clone()).to_boc()?;
    assert_eq!(DBMsg::default().boc_stripped, encoded);
    Ok(())
}
