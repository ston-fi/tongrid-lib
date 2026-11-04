use crate::entity::out_msg_query_id::out_msg_query_id::OutMsgQueryId;
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

#[derive(Debug)]
struct MissingColumnError(String);

impl fmt::Display for MissingColumnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Missing column: {}", self.0)
    }
}

impl std::error::Error for MissingColumnError {}

const OUT_MSG_QUERY_ID_FIELDS_CNT: usize = 5;

impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for OutMsgQueryId {
    fn type_check(specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        use scylla::cluster::metadata::NativeType;
        use scylla::deserialize::value::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};

        scylla_typecheck_deserialize_len_check_end_return!(
            specs,
            OUT_MSG_QUERY_ID_FIELDS_CNT,
            "OutMsgQueryId"
        );

        scylla_typecheck_native!(specs, address_typ, 0, "String", NativeType::Text);
        scylla_typecheck_native!(specs, query_id_typ, 1, "i64", NativeType::BigInt);
        scylla_typecheck_native!(specs, tx_utime_typ, 2, "i64", NativeType::BigInt);
        scylla_typecheck_native!(specs, tx_lt_typ, 3, "i64", NativeType::BigInt);
        scylla_typecheck_native!(specs, mc_seqno_typ, 4, "i32", NativeType::Int);

        Ok(())
    }

    fn deserialize(
        mut row: ColumnIterator<'frame, 'metadata>,
    ) -> Result<Self, DeserializationError> {
        let address = scylla_deserialize_val!(row, address, String);
        let query_id = scylla_deserialize_val!(row, query_id, i64);
        let tx_utime = scylla_deserialize_val!(row, tx_utime, i64) as u32;
        let tx_lt = scylla_deserialize_val!(row, tx_lt, i64) as u64;
        let mc_seqno = scylla_deserialize_val!(row, mc_seqno, i32) as u32;

        Ok(Self {
            address,
            query_id,
            tx_utime,
            tx_lt,
            mc_seqno,
        })
    }
}

impl SerializeRow for OutMsgQueryId {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        use scylla::_macro_internal::ser::row::serialize_column;

        scylla_typecheck_serialize_columns_len!(ctx, OUT_MSG_QUERY_ID_FIELDS_CNT, "OutMsgQueryId");
        let mc_seqno = self.mc_seqno as i32;
        let tx_lt = self.tx_lt as i64;
        let tx_utime = self.tx_utime as i64;

        let fields: [&dyn SerializeValue; OUT_MSG_QUERY_ID_FIELDS_CNT] =
            [&self.address, &self.query_id, &tx_utime, &tx_lt, &mc_seqno];

        for (col, val) in ctx.columns().iter().zip(fields.iter()) {
            serialize_column::<Self>(val, col, writer)?;
        }

        Ok(())
    }

    fn is_empty(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scylla_table_test_spec;
    use bytes::Bytes;
    use scylla::deserialize::FrameSlice;
    use scylla::frame::response::result::TableSpec;
    use scylla::serialize::row::RowSerializationContext;
    use scylla::serialize::writers::RowWriter;

    use scylla::DeserializeRow;
    use scylla::SerializeRow;
    #[derive(Default, Debug, Clone, PartialEq, Eq, Hash, SerializeRow, DeserializeRow)]
    pub struct TestOutMsgQueryId {
        pub address: String,
        pub query_id: i64, // query_id is u64 in TON, but scylla does not support unsigned types
        pub tx_utime: i64,
        pub tx_lt: i64,
        pub mc_seqno: i32,
    }

    #[test]
    fn test_serialization_block_in_row_test() -> anyhow::Result<()> {
        let table_spec =
            TableSpec::owned("test_keyspace".to_string(), "out_msg_query_id".to_string());
        let specs = vec![
            scylla_table_test_spec!(String, "address", table_spec),
            scylla_table_test_spec!(i64, "query_id", table_spec),
            scylla_table_test_spec!(i64, "tx_utime", table_spec),
            scylla_table_test_spec!(i64, "tx_lt", table_spec),
            scylla_table_test_spec!(i32, "mc_seqno", table_spec),
        ];
        let ctx = RowSerializationContext::from_specs(&specs);

        let db_query = OutMsgQueryId {
            address: "addr_1".to_string(),
            query_id: 1_234_567,
            tx_utime: 1_700_000_000,
            tx_lt: 9_000_000,
            mc_seqno: 7,
        };
        let test_query = TestOutMsgQueryId {
            address: "addr_1".to_string(),
            query_id: 1_234_567,
            tx_utime: 1_700_000_000,
            tx_lt: 9_000_000,
            mc_seqno: 7,
        };

        let mut db_bytes = Vec::new();
        let mut db_writer = RowWriter::new(&mut db_bytes);
        db_query.serialize(&ctx, &mut db_writer)?;

        let mut test_bytes = Vec::new();
        let mut test_writer = RowWriter::new(&mut test_bytes);
        test_query.serialize(&ctx, &mut test_writer)?;

        assert_eq!(db_bytes, test_bytes);
        Ok(())
    }

    #[test]
    fn test_deserialization_block_in_row_test() -> anyhow::Result<()> {
        let table_spec =
            TableSpec::owned("test_keyspace".to_string(), "out_msg_query_id".to_string());
        let specs = vec![
            scylla_table_test_spec!(String, "address", table_spec),
            scylla_table_test_spec!(i64, "query_id", table_spec),
            scylla_table_test_spec!(i64, "tx_utime", table_spec),
            scylla_table_test_spec!(i64, "tx_lt", table_spec),
            scylla_table_test_spec!(i32, "mc_seqno", table_spec),
        ];
        let ctx = RowSerializationContext::from_specs(&specs);

        let db_query = OutMsgQueryId {
            address: "addr_1".to_string(),
            query_id: 1_234_567,
            tx_utime: 1_700_000_000,
            tx_lt: 9_000_000,
            mc_seqno: 7,
        };
        let test_query = TestOutMsgQueryId {
            address: "addr_1".to_string(),
            query_id: 1_234_567,
            tx_utime: 1_700_000_000,
            tx_lt: 9_000_000,
            mc_seqno: 7,
        };

        let mut db_bytes = Vec::new();
        let mut db_writer = RowWriter::new(&mut db_bytes);
        db_query.serialize(&ctx, &mut db_writer)?;

        OutMsgQueryId::type_check(&specs)?;
        TestOutMsgQueryId::type_check(&specs)?;

        let db_bytes = Bytes::from(db_bytes);
        let db_row = ColumnIterator::new(&specs, FrameSlice::new(&db_bytes));
        let test_row = ColumnIterator::new(&specs, FrameSlice::new(&db_bytes));

        let decoded_db_query = OutMsgQueryId::deserialize(db_row)?;
        let decoded_test_query = TestOutMsgQueryId::deserialize(test_row)?;

        assert_eq!(decoded_db_query, db_query);
        assert_eq!(decoded_test_query, test_query);
        Ok(())
    }
}
