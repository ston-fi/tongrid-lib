use crate::entity::tvm_code_traits::tvm_code_traits::{ContractTraitParseError, TvmCodeTraits};
use crate::util::{deserialize_ton_hash, scylla_check_blob_type};
use crate::{
    scylla_deserialize_val, scylla_typecheck_deserialize_len_check_end_return,
    scylla_typecheck_native, scylla_typecheck_serialize_columns_len,
};
use scylla::_macro_internal::{
    ColumnIterator, ColumnSpec, ColumnType, DeserializationError, SerializationError,
    TypeCheckError,
};
use scylla::deserialize::row::DeserializeRow;
use scylla::deserialize::value::DeserializeValue;
use scylla::serialize::row::{RowSerializationContext, SerializeRow};
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::RowWriter;
use std::collections::HashSet;
use std::fmt;
use tongrid_proto::v3::ContractTrait;

#[derive(Debug)]
struct MissingColumnError(String);

impl fmt::Display for MissingColumnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Missing column: {}", self.0)
    }
}

impl std::error::Error for MissingColumnError {}

const TVM_CODE_TRAITS_FIELDS_CNT: usize = 4;

impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for TvmCodeTraits {
    fn type_check(specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        use scylla::cluster::metadata::NativeType;
        use scylla::deserialize::value::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};

        scylla_typecheck_deserialize_len_check_end_return!(
            specs,
            TVM_CODE_TRAITS_FIELDS_CNT,
            "TvmCodeTraits"
        );

        scylla_check_blob_type("TonHash", specs[0].typ())?;

        scylla_typecheck_native!(specs, version_typ, 1, "i32", NativeType::Int);

        // traits: HashSet<ContractTrait>
        HashSet::<String>::type_check(specs[2].typ())?;

        scylla_typecheck_native!(specs, mc_seqno_typ, 3, "i32", NativeType::Int);

        Ok(())
    }

    fn deserialize(
        mut row: ColumnIterator<'frame, 'metadata>,
    ) -> Result<Self, DeserializationError> {
        let code_hash = deserialize_ton_hash(scylla_deserialize_val!(row, code_hash, Vec<u8>))?;
        let version = scylla_deserialize_val!(row, version, i32);
        let set_string = scylla_deserialize_val!(row, traits, HashSet<String>);

        let traits = set_string
            .into_iter()
            .map(|s| {
                ContractTrait::from_str_name(&s).ok_or_else(|| {
                    DeserializationError::new(ContractTraitParseError::new(s.clone(), None))
                })
            })
            .collect::<Result<HashSet<ContractTrait>, _>>()?;

        let mc_seqno = scylla_deserialize_val!(row, mc_seqno, i32) as u32;

        Ok(Self {
            code_hash,
            version,
            traits,
            mc_seqno,
        })
    }
}

impl SerializeRow for TvmCodeTraits {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        use scylla::_macro_internal::ser::row::serialize_column;

        scylla_typecheck_serialize_columns_len!(ctx, TVM_CODE_TRAITS_FIELDS_CNT, "TvmCodeTraits");

        let traits_string: HashSet<String> = self
            .traits
            .iter()
            .map(|x| x.as_str_name().to_string())
            .collect();
        let mc_seqno = self.mc_seqno as i32;
        let code_hash = self.code_hash.as_slice();

        let fields: [&dyn SerializeValue; TVM_CODE_TRAITS_FIELDS_CNT] =
            [&code_hash, &self.version, &traits_string, &mc_seqno];

        for (col, val) in ctx.columns().iter().zip(fields.iter()) {
            serialize_column::<Self>(val, col, writer)?;
        }

        Ok(())
    }

    fn is_empty(&self) -> bool {
        false
    }
}
