use crate::backup_ser_de::ProtoSerial;
use crate::proto::v1::TvmCodeTraitsProto;
use std::collections::HashSet;
use ton::ton_core::cell::TonHash;
use tongrid_db::entity::tvm_code_traits::tvm_code_traits::{
    ContractTraitParseError, TvmCodeTraits,
};
use tongrid_proto::v3::ContractTrait;

impl ProtoSerial for TvmCodeTraits {
    type ProtoType = TvmCodeTraitsProto;

    fn into_proto(self) -> Self::ProtoType {
        Self::ProtoType {
            code_hash: self.code_hash.into_vec(),
            version: self.version,
            traits: self
                .traits
                .into_iter()
                .map(|t| t.as_str_name().to_string())
                .collect(),
            mc_seqno: self.mc_seqno as i32,
        }
    }

    fn from_proto(proto: Self::ProtoType) -> anyhow::Result<Self> {
        Ok(Self {
            code_hash: TonHash::from_vec(proto.code_hash)?,
            version: proto.version,
            traits: proto
                .traits
                .into_iter()
                .map(|s| {
                    ContractTrait::from_str_name(&s)
                        .ok_or_else(|| ContractTraitParseError::new(s.to_string(), None))
                })
                .collect::<Result<HashSet<_>, _>>()?,
            mc_seqno: proto.mc_seqno as u32,
        })
    }
}
