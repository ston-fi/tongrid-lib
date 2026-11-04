use crate::entity::block::db_block::DBBlock;
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
use scylla::serialize::row::{RowSerializationContext, SerializeRow};
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::RowWriter;
use std::fmt;
use std::sync::Arc;

#[derive(Debug)]
struct MissingColumnError(String);

impl fmt::Display for MissingColumnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Missing column: {}", self.0)
    }
}

impl std::error::Error for MissingColumnError {}

const DB_BLOCK_FIELDS_CNT: usize = 11;

impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for DBBlock {
    fn type_check(specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        use scylla::cluster::metadata::NativeType;
        use scylla::deserialize::value::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};

        scylla_typecheck_deserialize_len_check_end_return!(specs, DB_BLOCK_FIELDS_CNT, "DBBlock");

        scylla_typecheck_native!(specs, mc_seqno_typ, 0, "i32", NativeType::Int);
        scylla_typecheck_native!(specs, workchain_typ, 1, "i32", NativeType::Int);
        scylla_typecheck_native!(specs, shard_typ, 2, "i64", NativeType::BigInt);
        scylla_typecheck_native!(specs, seqno_typ, 3, "i32", NativeType::Int);

        scylla_check_blob_type("TonHash", specs[4].typ())?;

        scylla_check_blob_type("TonHash", specs[5].typ())?;

        scylla_typecheck_native!(specs, boc_typ, 6, "Vec<u8>", NativeType::Blob);
        scylla_typecheck_native!(specs, end_lt_typ, 7, "i64", NativeType::BigInt);
        scylla_typecheck_native!(specs, key_block_seqno_typ, 8, "i32", NativeType::Int);
        scylla_typecheck_native!(specs, utime_typ, 9, "i64", NativeType::BigInt);

        scylla_check_blob_type("TonHash", specs[10].typ())?;

        Ok(())
    }

    fn deserialize(
        mut row: ColumnIterator<'frame, 'metadata>,
    ) -> Result<Self, DeserializationError> {
        let mc_seqno = scylla_deserialize_val!(row, mc_seqno, i32) as u32;
        let workchain = scylla_deserialize_val!(row, workchain, i32);
        let shard = scylla_deserialize_val!(row, shard, i64);
        let seqno = scylla_deserialize_val!(row, seqno, i32) as u32;
        let root_hash = deserialize_ton_hash(scylla_deserialize_val!(row, root_hash, Vec<u8>))?;
        let file_hash = deserialize_ton_hash(scylla_deserialize_val!(row, file_hash, Vec<u8>))?;
        let boc_stripped = Arc::new(scylla_deserialize_val!(row, boc_stripped, Vec<u8>));
        let end_lt = scylla_deserialize_val!(row, end_lt, i64) as u64;
        let key_block_seqno = scylla_deserialize_val!(row, key_block_seqno, i32) as u32;
        let utime = scylla_deserialize_val!(row, utime, i64) as u32;
        let rand_seed = deserialize_ton_hash(scylla_deserialize_val!(row, rand_seed, Vec<u8>))?;

        Ok(Self {
            mc_seqno,
            workchain,
            shard,
            seqno,
            root_hash,
            file_hash,
            boc_stripped,
            end_lt,
            key_block_seqno,
            utime,
            rand_seed,
        })
    }
}

impl SerializeRow for DBBlock {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        use scylla::_macro_internal::ser::row::serialize_column;

        scylla_typecheck_serialize_columns_len!(ctx, DB_BLOCK_FIELDS_CNT, "DBBlock");

        let boc_stripped = &*self.boc_stripped;
        let mc_seqno = self.mc_seqno as i32;
        let seqno = self.seqno as i32;
        let key_block_seqno = self.key_block_seqno as i32;
        let end_lt = self.end_lt as i64;
        let utime = self.utime as i64;
        let root_hash = self.root_hash.as_slice();
        let file_hash = self.file_hash.as_slice();
        let rand_seed = self.rand_seed.as_slice();

        let fields: [&dyn SerializeValue; DB_BLOCK_FIELDS_CNT] = [
            &mc_seqno,
            &self.workchain,
            &self.shard,
            &seqno,
            &root_hash,
            &file_hash,
            &boc_stripped,
            &end_lt,
            &key_block_seqno,
            &utime,
            &rand_seed,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scylla_table_test_spec;
    use bytes::Bytes;
    use scylla::cluster::metadata::NativeType;
    use scylla::deserialize::FrameSlice;
    use scylla::frame::response::result::TableSpec;
    use scylla::DeserializeRow;
    use scylla::SerializeRow;
    use std::str::FromStr;
    use ton::ton_core::cell::TonHash;
    #[derive(Default, Debug, Clone, PartialEq, Eq, Hash, SerializeRow, DeserializeRow)]
    pub struct TestDBBlock {
        pub mc_seqno: i32,
        pub workchain: i32,
        pub shard: i64,
        pub seqno: i32,
        pub root_hash: Vec<u8>,
        pub file_hash: Vec<u8>,
        pub boc_stripped: Arc<Vec<u8>>, // shared with TonCell when parsing
        pub end_lt: i64, // can't be used to determinate last address tx by lt at specified mc_seqno
        pub key_block_seqno: i32,
        pub utime: i64,
        pub rand_seed: Vec<u8>,
    }

    #[test]
    fn test_serialization_block_in_row_test() -> anyhow::Result<()> {
        use scylla::frame::response::result::TableSpec;
        use scylla::serialize::writers::RowWriter;

        let table_spec = TableSpec::owned("test_keyspace".to_string(), "blocks".to_string());
        let specs = vec![
            scylla_table_test_spec!(i32, "mc_seqno", table_spec),
            scylla_table_test_spec!(i32, "workchain", table_spec),
            scylla_table_test_spec!(i64, "shard", table_spec),
            scylla_table_test_spec!(i32, "seqno", table_spec),
            scylla_table_test_spec!(Vec<u8>, "root_hash", table_spec),
            scylla_table_test_spec!(Vec<u8>, "file_hash", table_spec),
            scylla_table_test_spec!(Vec<u8>, "boc_stripped", table_spec),
            scylla_table_test_spec!(i64, "end_lt", table_spec),
            scylla_table_test_spec!(i32, "key_block_seqno", table_spec),
            scylla_table_test_spec!(i64, "utime", table_spec),
            scylla_table_test_spec!(Vec<u8>, "rand_seed", table_spec),
        ];
        let ctx = RowSerializationContext::from_specs(&specs);

        let boc_stripped = Arc::new(vec![1_u8, 4, 9, 16, 25, 36]);
        let root_hash =
            TonHash::from_str("8b5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea22")?;
        let file_hash =
            TonHash::from_str("2a5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea2b")?;
        let rand_seed =
            TonHash::from_str("3c5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea2c")?;

        let db_block = DBBlock {
            mc_seqno: 7,
            workchain: -1,
            shard: 1_234_567_890,
            seqno: 42,
            root_hash,
            file_hash,
            boc_stripped: Arc::clone(&boc_stripped),
            end_lt: 900_000,
            key_block_seqno: 7,
            utime: 1_700_000_000,
            rand_seed,
        };
        let test_block = TestDBBlock {
            mc_seqno: 7,
            workchain: -1,
            shard: 1_234_567_890,
            seqno: 42,
            root_hash: root_hash.to_vec(),
            file_hash: file_hash.to_vec(),
            boc_stripped,
            end_lt: 900_000,
            key_block_seqno: 7,
            utime: 1_700_000_000,
            rand_seed: rand_seed.to_vec(),
        };

        let mut db_bytes = Vec::new();
        let mut db_writer = RowWriter::new(&mut db_bytes);
        db_block.serialize(&ctx, &mut db_writer)?;

        let mut test_bytes = Vec::new();
        let mut test_writer = RowWriter::new(&mut test_bytes);
        test_block.serialize(&ctx, &mut test_writer)?;

        assert_eq!(db_bytes, test_bytes);
        Ok(())
    }

    #[test]
    fn test_deserialization_block_in_row_test() -> anyhow::Result<()> {
        let table_spec = TableSpec::owned("test_keyspace".to_string(), "blocks".to_string());
        let specs = vec![
            ColumnSpec::owned(
                "mc_seqno".to_string(),
                ColumnType::Native(NativeType::Int),
                table_spec.clone(),
            ),
            ColumnSpec::owned(
                "workchain".to_string(),
                ColumnType::Native(NativeType::Int),
                table_spec.clone(),
            ),
            ColumnSpec::owned(
                "shard".to_string(),
                ColumnType::Native(NativeType::BigInt),
                table_spec.clone(),
            ),
            ColumnSpec::owned(
                "seqno".to_string(),
                ColumnType::Native(NativeType::Int),
                table_spec.clone(),
            ),
            ColumnSpec::owned(
                "root_hash".to_string(),
                ColumnType::Native(NativeType::Blob),
                table_spec.clone(),
            ),
            ColumnSpec::owned(
                "file_hash".to_string(),
                ColumnType::Native(NativeType::Blob),
                table_spec.clone(),
            ),
            ColumnSpec::owned(
                "boc_stripped".to_string(),
                ColumnType::Native(NativeType::Blob),
                table_spec.clone(),
            ),
            ColumnSpec::owned(
                "end_lt".to_string(),
                ColumnType::Native(NativeType::BigInt),
                table_spec.clone(),
            ),
            ColumnSpec::owned(
                "key_block_seqno".to_string(),
                ColumnType::Native(NativeType::Int),
                table_spec.clone(),
            ),
            ColumnSpec::owned(
                "utime".to_string(),
                ColumnType::Native(NativeType::BigInt),
                table_spec.clone(),
            ),
            ColumnSpec::owned(
                "rand_seed".to_string(),
                ColumnType::Native(NativeType::Blob),
                table_spec,
            ),
        ];
        let ctx = RowSerializationContext::from_specs(&specs);

        let boc_stripped = Arc::new(vec![1_u8, 4, 9, 16, 25, 36]);
        let root_hash =
            TonHash::from_str("8b5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea22")?;
        let file_hash =
            TonHash::from_str("2a5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea2b")?;
        let rand_seed =
            TonHash::from_str("3c5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea2c")?;

        let db_block = DBBlock {
            mc_seqno: 7,
            workchain: -1,
            shard: 1_234_567_890,
            seqno: 42,
            root_hash,
            file_hash,
            boc_stripped: Arc::clone(&boc_stripped),
            end_lt: 900_000,
            key_block_seqno: 7,
            utime: 1_700_000_000,
            rand_seed,
        };
        let test_block = TestDBBlock {
            mc_seqno: 7,
            workchain: -1,
            shard: 1_234_567_890,
            seqno: 42,
            root_hash: root_hash.to_vec(),
            file_hash: file_hash.to_vec(),
            boc_stripped,
            end_lt: 900_000,
            key_block_seqno: 7,
            utime: 1_700_000_000,
            rand_seed: rand_seed.to_vec(),
        };

        let mut db_bytes = Vec::new();
        let mut db_writer = RowWriter::new(&mut db_bytes);
        db_block.serialize(&ctx, &mut db_writer)?;

        DBBlock::type_check(&specs)?;
        TestDBBlock::type_check(&specs)?;

        let db_bytes = Bytes::from(db_bytes);
        let db_row = ColumnIterator::new(&specs, FrameSlice::new(&db_bytes));
        let test_row = ColumnIterator::new(&specs, FrameSlice::new(&db_bytes));

        let decoded_db_block = DBBlock::deserialize(db_row)?;
        let decoded_test_block = TestDBBlock::deserialize(test_row)?;

        assert_eq!(decoded_db_block, db_block);
        assert_eq!(decoded_test_block, test_block);
        Ok(())
    }
}
