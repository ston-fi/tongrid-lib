use crate::backup_ser_de::ProtoSerial;
use crate::proto::v1::{DbTxKeyProto, DbTxProto};
use ton::ton_core::cell::TonHash;
use tongrid_db::entity::scylla_types::DBMsg;
use tongrid_db::entity::tx::tx_key::{DBTxKey, DB_TX_KEY_ZERO};
use tongrid_db::entity::DBTx;

impl ProtoSerial for DBTx {
    type ProtoType = DbTxProto;

    fn into_proto(self) -> Self::ProtoType {
        let in_msg = self.in_msg.map(|x| x.into_proto());
        let out_msgs = self.out_msgs.into_iter().map(|x| x.into_proto()).collect();

        Self::ProtoType {
            mc_seqno: self.mc_seqno as i32,
            shard_workchain: self.block_workchain,
            shard_id: self.block_shard,
            shard_seqno: self.block_seqno as i32,
            tx_lt: self.tx_lt as i64,
            address: self.address,
            tx_hash: self.tx_hash.into_vec(),
            tx_utime: self.tx_utime as i64,
            boc_stripped: self.boc_stripped.to_vec(),
            in_msg,
            out_msgs,
            in_msg_hash: self.in_msg_hash.map(|x| x.into_vec()),
            in_msg_hash_normalized: self.in_msg_hash_normalized.map(|x| x.into_vec()),
            initial_tx: Some(DbTxKeyProto::from(self.initial_tx)),
        }
    }

    fn from_proto(proto: Self::ProtoType) -> anyhow::Result<Self> {
        let in_msg = proto.in_msg.map(DBMsg::from_proto).transpose()?;
        let out_msgs = proto
            .out_msgs
            .into_iter()
            .map(DBMsg::from_proto)
            .collect::<anyhow::Result<_>>()?;

        Ok(Self {
            mc_seqno: proto.mc_seqno as u32,
            block_workchain: proto.shard_workchain,
            block_shard: proto.shard_id,
            block_seqno: proto.shard_seqno as u32,
            tx_lt: proto.tx_lt as u64,
            address: proto.address,
            tx_hash: TonHash::from_vec(proto.tx_hash)?,
            tx_utime: proto.tx_utime as u32,
            boc_stripped: proto.boc_stripped.into(),
            in_msg,
            out_msgs,
            in_msg_hash: proto.in_msg_hash.map(TonHash::from_vec).transpose()?,
            in_msg_hash_normalized: proto
                .in_msg_hash_normalized
                .map(TonHash::from_vec)
                .transpose()?,
            initial_tx: proto
                .initial_tx
                .map(DBTxKey::from)
                .unwrap_or(DB_TX_KEY_ZERO.clone()),
        })
    }
}

impl From<DBTxKey> for DbTxKeyProto {
    fn from(value: DBTxKey) -> Self {
        DbTxKeyProto {
            mc_seqno: value.mc_seqno as i32,
            address: value.address,
            tx_lt: value.tx_lt as i64,
        }
    }
}

impl From<DbTxKeyProto> for DBTxKey {
    fn from(value: DbTxKeyProto) -> Self {
        DBTxKey {
            mc_seqno: value.mc_seqno as u32,
            address: value.address,
            tx_lt: value.tx_lt as u64,
        }
    }
}
