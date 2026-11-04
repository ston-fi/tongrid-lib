use crate::backup_ser_de::ProtoSerial;
use crate::proto::v1::DbAccountProto;
use ton::ton_core::cell::TonHash;
use tongrid_db::entity::account::db_account::DBAccount;
use tongrid_db::entity::scylla_types::DBSimpleLib;

impl ProtoSerial for DBAccount {
    type ProtoType = DbAccountProto;

    fn into_proto(self) -> Self::ProtoType {
        Self::ProtoType {
            mc_seqno: self.mc_seqno as i32,
            tx_hash: self.tx_hash.into_vec(),
            tx_lt: self.tx_lt as i64,
            address: self.address,
            is_valid: self.is_valid,
            boc_stripped: self.boc_stripped.to_vec(),
            code_hash: self.code_hash.map(|x| x.into_vec()),
            last_code_hash: self.last_code_hash.map(|x| x.into_vec()),
            data_lib_ids: self
                .data_lib_ids
                .into_iter()
                .map(|x| x.into_vec())
                .collect(),
            simple_libs: self
                .simple_libs
                .into_iter()
                .map(DBSimpleLib::into_proto)
                .collect(),
        }
    }

    fn from_proto(proto: Self::ProtoType) -> anyhow::Result<Self> {
        Ok(Self {
            mc_seqno: proto.mc_seqno as u32,
            tx_hash: TonHash::from_vec(proto.tx_hash)?,
            tx_lt: proto.tx_lt as u64,
            address: proto.address,
            is_valid: proto.is_valid,
            boc_stripped: proto.boc_stripped.into(),
            code_hash: proto.code_hash.map(TonHash::from_vec).transpose()?,
            data_lib_ids: proto
                .data_lib_ids
                .into_iter()
                .map(TonHash::from_vec)
                .collect::<Result<_, _>>()?,
            simple_libs: proto
                .simple_libs
                .into_iter()
                .map(DBSimpleLib::from_proto)
                .collect::<anyhow::Result<_>>()?,
            last_code_hash: proto.last_code_hash.map(TonHash::from_vec).transpose()?,
        })
    }
}
