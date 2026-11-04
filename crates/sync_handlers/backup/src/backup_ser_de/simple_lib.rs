use crate::backup_ser_de::ProtoSerial;
use crate::proto::v1::DbSimpleLibProto;
use ton::ton_core::cell::TonHash;
use tongrid_db::entity::scylla_types::DBSimpleLib;

impl ProtoSerial for DBSimpleLib {
    type ProtoType = DbSimpleLibProto;

    fn into_proto(self) -> Self::ProtoType {
        Self::ProtoType {
            hash: self.code_hash.into_vec(),
            public: self.public,
        }
    }

    fn from_proto(proto: Self::ProtoType) -> anyhow::Result<Self> {
        Ok(Self {
            code_hash: TonHash::from_vec(proto.hash)?,
            public: proto.public,
        })
    }
}
