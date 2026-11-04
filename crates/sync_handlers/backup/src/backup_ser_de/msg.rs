use crate::backup_ser_de::ProtoSerial;
use crate::proto::v1::{DbMsgProto, DbSimpleLibProto};
use ton::ton_core::cell::TonHash;
use tongrid_db::entity::scylla_types::{DBMsg, DBSimpleLib};

impl ProtoSerial for DBMsg {
    type ProtoType = DbMsgProto;

    fn into_proto(self) -> Self::ProtoType {
        Self::ProtoType {
            src: self.src,
            dst: self.dst,
            created_lt: self.created_lt as i64,
            boc_stripped: self.boc_stripped,
            code_hash: self.code_hash.map(|x| x.into_vec()),
            data_lib_ids: self
                .data_lib_ids
                .into_iter()
                .map(|x| x.into_vec())
                .collect(),
            body_lib_ids: self
                .body_lib_ids
                .into_iter()
                .map(|x| x.into_vec())
                .collect(),
            simple_libs: self
                .simple_libs
                .into_iter()
                .map(|lib| DbSimpleLibProto {
                    hash: lib.code_hash.into_vec(),
                    public: lib.public,
                })
                .collect(),
        }
    }

    fn from_proto(proto: Self::ProtoType) -> anyhow::Result<Self> {
        Ok(Self {
            src: proto.src,
            dst: proto.dst,
            created_lt: proto.created_lt as u64,
            code_hash: proto.code_hash.map(TonHash::from_vec).transpose()?,
            boc_stripped: proto.boc_stripped,
            data_lib_ids: proto
                .data_lib_ids
                .into_iter()
                .map(TonHash::from_vec)
                .collect::<Result<_, _>>()?,
            body_lib_ids: proto
                .body_lib_ids
                .into_iter()
                .map(TonHash::from_vec)
                .collect::<Result<_, _>>()?,
            simple_libs: proto
                .simple_libs
                .into_iter()
                .map(|lib| {
                    Ok(DBSimpleLib {
                        code_hash: TonHash::from_vec(lib.hash)?,
                        public: lib.public,
                    })
                })
                .collect::<anyhow::Result<_>>()?,
        })
    }
}
