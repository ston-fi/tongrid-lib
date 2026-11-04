use ton::ton_core::types::tlb_core::MsgAddress;
use ton::ton_core::types::TonAddress;

pub fn tlb_address_to_str<T: Into<MsgAddress>>(addr: T) -> anyhow::Result<String> {
    match addr.into() {
        MsgAddress::Int(int) => Ok(TonAddress::from_msg_address(int)?.to_string()),
        _ => Ok("".to_string()),
    }
}
