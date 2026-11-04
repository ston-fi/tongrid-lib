use crate::db::{Inner, DB};
use crate::entity::out_msg_query_id::out_msg_query_id::OutMsgQueryId;
use function_name::named;
use stonfi_scylla_client::client::ScyllaClient;

#[derive(Clone, Copy)]
pub struct OutMsgQueryIdMapper<'a> {
    db_inner: &'a Inner,
}

// mapper support only "insert". Select is done by TxKeyLookup
impl<'a> OutMsgQueryIdMapper<'a> {
    const TABLE_NAME: &'static str = "tx_by_out_msg_query_id";

    fn scylla(&self) -> &ScyllaClient {
        &self.db_inner.scylla
    }

    #[named]
    pub async fn insert(self, out_msg: &OutMsgQueryId) -> anyhow::Result<()> {
        let query_str = format!(
            "INSERT INTO {} ({}) VALUES (?, ?, ?, ?, ?)",
            Self::TABLE_NAME,
            OutMsgQueryId::FIELDS
        );
        self.scylla()
            .insert(query_str, out_msg, function_name!())
            .await
            .map_err(anyhow::Error::from)
    }
}

impl<'a> From<&'a DB> for OutMsgQueryIdMapper<'a> {
    fn from(db: &'a DB) -> Self {
        Self {
            db_inner: db.inner(),
        }
    }
}
