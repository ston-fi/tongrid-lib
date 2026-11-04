use std::collections::HashSet;
use std::fmt;

use ton::ton_core::cell::TonHash;
use tongrid_proto::v3::ContractTrait;

#[derive(Debug)]
pub struct ContractTraitParseError {
    unknown_trait: String,
    code_hash: Option<TonHash>,
}

impl ContractTraitParseError {
    pub fn new(unknown_trait: String, code_hash: Option<TonHash>) -> Self {
        Self {
            unknown_trait,
            code_hash,
        }
    }
}

impl fmt::Display for ContractTraitParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Failed to parse ContractTrait: {}", self.unknown_trait)?;
        if let Some(ref code_hash) = self.code_hash {
            write!(f, " (code_hash: {})", code_hash)?;
        }
        Ok(())
    }
}

impl std::error::Error for ContractTraitParseError {}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct TvmCodeTraits {
    pub code_hash: TonHash,
    pub version: i32,
    pub traits: HashSet<ContractTrait>,
    pub mc_seqno: u32,
}

impl TvmCodeTraits {
    pub const CACHE_NAME: &'static str = "DbTvmCodeTraits";
    pub const FIELDS: &'static str = "code_hash, version, traits, mc_seqno";
}
