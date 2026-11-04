use stonfi_scylla_client::client::DeserRow;

pub trait AccountEntity: DeserRow + Clone {
    const FIELDS: &'static str;
    const FIELDS_CNT: usize;
    const TX_KEY_FIELDS: &'static str = "mc_seqno, address, tx_lt";
}
