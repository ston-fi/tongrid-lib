use crate::db::{Inner, DB};
use crate::entity::msg_txs::msg_txs::MsgTxs;
use function_name::named;
use stonfi_scylla_client::client::ScyllaClient;

#[derive(Clone, Copy)]
pub struct MsgTxsMapper<'a> {
    db_inner: &'a Inner,
}

impl<'a> MsgTxsMapper<'a> {
    const MSG_TXS_TABLE_NAME: &'static str = "msg_txs";

    fn scylla(&self) -> &ScyllaClient {
        &self.db_inner.scylla
    }

    #[named]
    pub async fn get(
        self,
        src: &str,
        dst: &str,
        created_lt: u64,
    ) -> anyhow::Result<Option<MsgTxs>> {
        let query_str = format!(
            "SELECT {} FROM {} WHERE src = ? AND dst = ? AND created_lt = ?",
            MsgTxs::FIELDS,
            Self::MSG_TXS_TABLE_NAME
        );
        self.scylla()
            .select_one(query_str, (src, dst, created_lt as i64), function_name!())
            .await
            .map_err(anyhow::Error::from)
    }

    // When we run insert_in_msg, in fact we update the row which was inserted during "insert_out_msg"
    // (if it's not first tx from tree)

    #[named]
    pub async fn insert_in_msg(self, msg_txs: &MsgTxs) -> anyhow::Result<()> {
        let query_str = format!(
            "INSERT INTO {} (src, dst, created_lt, dst_mc_seqno, dst_tx_lt) VALUES (?, ?, ?, ?, ?)",
            Self::MSG_TXS_TABLE_NAME
        );
        let dst_mc_seqno = msg_txs.dst_mc_seqno.map(i32::try_from).transpose()?;
        let dst_tx_lt: Option<i64> = msg_txs.dst_tx_lt.map(|v| v as i64);

        self.scylla()
            .insert(
                query_str,
                (
                    &msg_txs.src,
                    &msg_txs.dst,
                    msg_txs.created_lt as i64,
                    dst_mc_seqno,
                    dst_tx_lt,
                ),
                function_name!(),
            )
            .await?;
        Ok(())
    }

    #[named]
    pub async fn insert_out_msg(self, msg_txs: &MsgTxs) -> anyhow::Result<()> {
        let query_str = format!(
            "INSERT INTO {} (src, dst, created_lt, src_mc_seqno, src_tx_lt) VALUES (?, ?, ?, ?, ?)",
            Self::MSG_TXS_TABLE_NAME
        );

        let src_mc_seqno = msg_txs.src_mc_seqno.map(i32::try_from).transpose()?;
        let src_tx_lt: Option<i64> = msg_txs.src_tx_lt.map(|v| v as i64);
        self.scylla()
            .insert(
                query_str,
                (
                    &msg_txs.src,
                    &msg_txs.dst,
                    msg_txs.created_lt as i64,
                    src_mc_seqno,
                    src_tx_lt,
                ),
                function_name!(),
            )
            .await?;
        Ok(())
    }
}

impl<'a> From<&'a DB> for MsgTxsMapper<'a> {
    fn from(db: &'a DB) -> Self {
        Self {
            db_inner: db.inner(),
        }
    }
}
