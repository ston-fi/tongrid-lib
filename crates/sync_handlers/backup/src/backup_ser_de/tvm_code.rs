use crate::backup_ser_de::ProtoSerial;
use crate::proto::v1::TvmCodeProto;
use ton::ton_core::cell::TonHash;
use tongrid_db::entity::TvmCode;

impl ProtoSerial for TvmCode {
    type ProtoType = TvmCodeProto;

    fn into_proto(self) -> Self::ProtoType {
        Self::ProtoType {
            hash: self.code_hash.as_slice().to_vec(),
            boc: self.boc,
            lib_ids: self.lib_ids.into_iter().map(|x| x.into_vec()).collect(),
        }
    }

    fn from_proto(proto: Self::ProtoType) -> anyhow::Result<Self> {
        Ok(Self {
            code_hash: TonHash::from_vec(proto.hash)?,
            boc: proto.boc,
            lib_ids: proto
                .lib_ids
                .into_iter()
                .map(TonHash::from_vec)
                .collect::<Result<_, _>>()?,
        })
    }
}
