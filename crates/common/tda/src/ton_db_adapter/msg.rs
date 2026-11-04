use crate::ton_db_adapter::TonDBAdapter;
use futures_util::try_join;
use futures_util::{stream, TryStreamExt};
use ton::block_tlb::Msg;
use ton::ton_core::traits::tlb::TLB;
use tongrid_db::entity::out_msg_query_id::out_msg_query_id::OutMsgQueryId;
use tongrid_db::entity::{msg_txs::msg_txs::MsgTxs, DBTx};
use tongrid_db::DB_WRITE_CONCURRENCY;
use tongrid_ton::extractor::TonExtractor;

// covers message_txs & out_msg_query_id table
#[derive(Clone, Copy)]
pub struct MsgAdapter<'a> {
    tda: &'a TonDBAdapter,
}

impl<'a> From<&'a TonDBAdapter> for MsgAdapter<'a> {
    fn from(tda: &'a TonDBAdapter) -> Self {
        Self { tda }
    }
}

impl<'a> MsgAdapter<'a> {
    pub async fn save_message_data(self, txs: &[DBTx]) -> anyhow::Result<usize> {
        let msg_txs_fut = self.save_msg_txs(txs);
        let out_msg_query_id_fut = self.save_out_msg_query_id(txs);

        let (msg_txs_count, _) = try_join!(msg_txs_fut, out_msg_query_id_fut)?;
        Ok(msg_txs_count)
    }

    async fn save_msg_txs(self, txs: &[DBTx]) -> anyhow::Result<usize> {
        let msgs_count = txs
            .iter()
            .map(|tx| tx.in_msg.is_some() as usize + tx.out_msgs.len())
            .sum();
        stream::iter(txs.iter().map(Ok::<_, anyhow::Error>))
            .try_for_each_concurrent(DB_WRITE_CONCURRENCY, |tx| async {
                let tx_key = tx.tx_key();
                if let Some(in_msg) = &tx.in_msg {
                    let msg_txs = MsgTxs::new_in_msg(in_msg, tx_key.clone());
                    self.tda.db().msg_txs().insert_in_msg(&msg_txs).await?;
                }

                for out_msg in &tx.out_msgs {
                    let msg_txs = MsgTxs::new_out_msg(out_msg, tx_key.clone());
                    self.tda.db().msg_txs().insert_out_msg(&msg_txs).await?;
                }

                Ok(())
            })
            .await?;
        Ok(msgs_count)
    }

    async fn save_out_msg_query_id(self, txs: &[DBTx]) -> anyhow::Result<()> {
        stream::iter(txs.iter().map(Ok::<_, anyhow::Error>))
            .try_for_each_concurrent(DB_WRITE_CONCURRENCY, |tx| async {
                for out_msg in &tx.out_msgs {
                    let ton_msg = Msg::from_boc(out_msg.boc_stripped.to_vec())?;
                    if let Some(query_id) = TonExtractor::msg_query_id(&ton_msg.body)? {
                        let out_msg_query_id = OutMsgQueryId {
                            address: tx.address.clone(),
                            query_id: query_id as i64,
                            tx_utime: tx.tx_utime,
                            tx_lt: tx.tx_lt,
                            mc_seqno: tx.mc_seqno,
                        };
                        self.tda
                            .db()
                            .out_msg_query_id()
                            .insert(&out_msg_query_id)
                            .await?;
                    }
                }
                Ok(())
            })
            .await
    }
}
