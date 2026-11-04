use super::tx_key::DBTxKey;
use crate::{
    scylla_deserialize_val, scylla_typecheck_deserialize_len_check_end_return,
    scylla_typecheck_native, scylla_typecheck_serialize_columns_len,
};
use scylla::_macro_internal::{
    ColumnIterator, ColumnSpec, ColumnType, DeserializationError, SerializationError,
    TypeCheckError,
};
use scylla::deserialize::row::DeserializeRow;
use scylla::serialize::row::{RowSerializationContext, SerializeRow};
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::RowWriter;
use std::fmt;

const DB_TX_KEY_FIELDS_CNT: usize = 3;

#[derive(Debug)]
struct MissingColumnError(String);

impl fmt::Display for MissingColumnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Missing column: {}", self.0)
    }
}

impl std::error::Error for MissingColumnError {}

impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for DBTxKey {
    fn type_check(specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        use scylla::cluster::metadata::NativeType;
        use scylla::deserialize::value::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};

        scylla_typecheck_deserialize_len_check_end_return!(specs, DB_TX_KEY_FIELDS_CNT, "DBTxKey");

        // mc_seqno: i32
        scylla_typecheck_native!(specs, mc_seqno_typ, 0, "i32", NativeType::Int);

        // address: String
        scylla_typecheck_native!(specs, address_typ, 1, "String", NativeType::Text);

        // tx_lt: i64
        scylla_typecheck_native!(specs, tx_lt_typ, 2, "i64", NativeType::BigInt);

        Ok(())
    }

    fn deserialize(
        mut row: ColumnIterator<'frame, 'metadata>,
    ) -> Result<Self, DeserializationError> {
        let mc_seqno = scylla_deserialize_val!(row, mc_seqno, i32) as u32;
        let address = scylla_deserialize_val!(row, address, String);
        let tx_lt = scylla_deserialize_val!(row, tx_lt, i64) as u64;

        Ok(Self {
            mc_seqno,
            address,
            tx_lt,
        })
    }
}

impl SerializeRow for DBTxKey {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        use scylla::_macro_internal::ser::row::serialize_column;

        scylla_typecheck_serialize_columns_len!(ctx, DB_TX_KEY_FIELDS_CNT, "DBTxKey");

        let mc_seqno = self.mc_seqno as i32;
        let tx_lt = self.tx_lt as i64;
        let fields: [&dyn SerializeValue; DB_TX_KEY_FIELDS_CNT] =
            [&mc_seqno, &self.address, &tx_lt];

        for (col, val) in ctx.columns().iter().zip(fields.iter()) {
            serialize_column::<Self>(val, col, writer)?;
        }

        Ok(())
    }

    fn is_empty(&self) -> bool {
        false
    }
}
