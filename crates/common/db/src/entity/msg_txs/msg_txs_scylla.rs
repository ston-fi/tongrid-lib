use crate::entity::msg_txs::msg_txs::MsgTxs;
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
use std::fmt;

#[derive(Debug)]
struct MissingColumnError(String);

impl fmt::Display for MissingColumnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Missing column: {}", self.0)
    }
}

impl std::error::Error for MissingColumnError {}

const MSG_TXS_FIELDS_CNT: usize = 7;

impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for MsgTxs {
    fn type_check(specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        use scylla::cluster::metadata::NativeType;
        use scylla::deserialize::value::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};

        scylla_typecheck_deserialize_len_check_end_return!(specs, MSG_TXS_FIELDS_CNT, "MsgTxs");

        scylla_typecheck_native!(specs, src_typ, 0, "String", NativeType::Text);
        scylla_typecheck_native!(specs, dst_typ, 1, "String", NativeType::Text);
        scylla_typecheck_native!(specs, created_lt_typ, 2, "i64", NativeType::BigInt);

        // src_mc_seqno: Option<i32>
        Option::<i32>::type_check(specs[3].typ())?;

        // src_tx_lt: Option<i64>
        Option::<i64>::type_check(specs[4].typ())?;

        // dst_mc_seqno: Option<i32>
        Option::<i32>::type_check(specs[5].typ())?;

        // dst_tx_lt: Option<i64>
        Option::<i64>::type_check(specs[6].typ())?;

        Ok(())
    }

    fn deserialize(
        mut row: ColumnIterator<'frame, 'metadata>,
    ) -> Result<Self, DeserializationError> {
        let src = scylla_deserialize_val!(row, src, String);
        let dst = scylla_deserialize_val!(row, dst, String);
        let created_lt = scylla_deserialize_val!(row, created_lt, i64) as u64;
        let src_mc_seqno =
            scylla_deserialize_val!(row, src_mc_seqno, Option<i32>).map(|v| v as u32);
        let src_tx_lt = scylla_deserialize_val!(row, src_tx_lt, Option<i64>).map(|v| v as u64);
        let dst_mc_seqno =
            scylla_deserialize_val!(row, dst_mc_seqno, Option<i32>).map(|v| v as u32);
        let dst_tx_lt = scylla_deserialize_val!(row, dst_tx_lt, Option<i64>).map(|v| v as u64);

        Ok(Self {
            src,
            dst,
            created_lt,
            src_mc_seqno,
            src_tx_lt,
            dst_mc_seqno,
            dst_tx_lt,
        })
    }
}

impl SerializeRow for MsgTxs {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        use scylla::_macro_internal::ser::row::serialize_column;

        scylla_typecheck_serialize_columns_len!(ctx, MSG_TXS_FIELDS_CNT, "MsgTxs");
        let src_mc_seqno = self.src_mc_seqno.map(|v| v as i32);
        let dst_mc_seqno = self.dst_mc_seqno.map(|v| v as i32);
        let created_lt = self.created_lt as i64;
        let src_tx_lt = self.src_tx_lt.map(|v| v as i64);
        let dst_tx_lt = self.dst_tx_lt.map(|v| v as i64);

        let fields: [&dyn SerializeValue; MSG_TXS_FIELDS_CNT] = [
            &self.src,
            &self.dst,
            &created_lt,
            &src_mc_seqno,
            &src_tx_lt,
            &dst_mc_seqno,
            &dst_tx_lt,
        ];

        for (col, val) in ctx.columns().iter().zip(fields.iter()) {
            serialize_column::<Self>(val, col, writer)?;
        }

        Ok(())
    }

    fn is_empty(&self) -> bool {
        false
    }
}
