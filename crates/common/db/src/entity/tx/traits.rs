use stonfi_scylla_client::client::DeserRow;

pub trait TxEntity: DeserRow + Clone + Sized {
    const FIELDS: &'static str;
}
