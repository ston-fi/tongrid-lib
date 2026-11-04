use crate::entity::scylla_types::DBMsg;
use crate::entity::tx::db_tx::DBTx;
use crate::entity::tx::db_tx_lite::DBTxLite;
use crate::entity::tx::tx_key::DBTxKey;
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
use scylla::deserialize::value::DeserializeValue as ScyllaDeserializeValue;
use scylla::serialize::row::{RowSerializationContext, SerializeRow};
use scylla::serialize::value::SerializeValue as ScyllaSerializeValue;
use scylla::serialize::writers::RowWriter;
use std::fmt;
use std::sync::Arc;
use ton::ton_core::cell::TonHash;

#[derive(Debug)]
struct MissingColumnError(String);

impl fmt::Display for MissingColumnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Missing column: {}", self.0)
    }
}

impl std::error::Error for MissingColumnError {}

impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for DBTx {
    fn type_check(specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        use scylla::cluster::metadata::NativeType;
        use scylla::deserialize::value::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};

        scylla_typecheck_deserialize_len_check_end_return!(specs, DBTx::FIELDS_CNT, "DBTx");
        scylla_typecheck_native!(specs, mc_seqno_typ, 0, "i32", NativeType::Int);
        scylla_typecheck_native!(specs, address_typ, 1, "String", NativeType::Text);
        scylla_typecheck_native!(specs, tx_lt_typ, 2, "i64", NativeType::BigInt);
        scylla_typecheck_native!(specs, tx_utime_typ, 3, "i64", NativeType::BigInt);

        scylla_check_blob_type("TonHash", specs[4].typ())?;

        // initial_tx: DBTxKey
        <DBTxKey as ScyllaDeserializeValue>::type_check(specs[5].typ())?;

        scylla_typecheck_native!(specs, block_workchain_typ, 6, "i32", NativeType::Int);
        scylla_typecheck_native!(specs, block_shard_typ, 7, "i64", NativeType::BigInt);
        scylla_typecheck_native!(specs, block_seqno_typ, 8, "i32", NativeType::Int);
        scylla_typecheck_native!(specs, boc_typ, 9, "Vec<u8>", NativeType::Blob);

        // in_msg: Option<DBMsg>
        Option::<DBMsg>::type_check(specs[10].typ())?;

        // out_msgs: Vec<DBMsg>
        Vec::<DBMsg>::type_check(specs[11].typ())?;

        Option::<Vec<u8>>::type_check(specs[12].typ())?;

        Option::<Vec<u8>>::type_check(specs[13].typ())?;

        Ok(())
    }

    fn deserialize(
        mut row: ColumnIterator<'frame, 'metadata>,
    ) -> Result<Self, DeserializationError> {
        let mc_seqno = scylla_deserialize_val!(row, mc_seqno, i32) as u32;
        let address = scylla_deserialize_val!(row, address, String);
        let tx_lt = scylla_deserialize_val!(row, tx_lt, i64) as u64;
        let tx_utime = scylla_deserialize_val!(row, tx_utime, i64) as u32;
        let tx_hash = deserialize_ton_hash(scylla_deserialize_val!(row, tx_hash, Vec<u8>))?;
        let initial_tx = scylla_deserialize_val!(row, initial_tx, DBTxKey);
        let block_workchain = scylla_deserialize_val!(row, block_workchain, i32);
        let block_shard = scylla_deserialize_val!(row, block_shard, i64);
        let block_seqno = scylla_deserialize_val!(row, block_seqno, i32) as u32;
        let boc_stripped = Arc::new(scylla_deserialize_val!(row, boc_stripped, Vec<u8>));
        let in_msg = scylla_deserialize_val!(row, in_msg, Option<DBMsg>);
        let out_msgs = scylla_deserialize_val!(row, out_msgs, Vec<DBMsg>);
        let in_msg_hash = scylla_deserialize_val!(row, in_msg_hash, Option<Vec<u8>>)
            .map(deserialize_ton_hash)
            .transpose()?;
        let in_msg_hash_normalized =
            scylla_deserialize_val!(row, in_msg_hash_normalized, Option<Vec<u8>>)
                .map(deserialize_ton_hash)
                .transpose()?;

        Ok(Self {
            mc_seqno,
            address,
            tx_lt,
            tx_utime,
            tx_hash,
            initial_tx,
            block_workchain,
            block_shard,
            block_seqno,
            boc_stripped,
            in_msg,
            out_msgs,
            in_msg_hash,
            in_msg_hash_normalized,
        })
    }
}

impl SerializeRow for DBTx {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        use scylla::_macro_internal::ser::row::serialize_column;

        scylla_typecheck_serialize_columns_len!(ctx, DBTx::FIELDS_CNT, "DBTx");

        let boc_stripped = &*self.boc_stripped;
        let mc_seqno = self.mc_seqno as i32;
        let block_seqno = self.block_seqno as i32;
        let tx_lt = self.tx_lt as i64;
        let tx_utime = self.tx_utime as i64;
        let tx_hash = self.tx_hash.as_slice();
        let in_msg_hash = self.in_msg_hash.as_ref().map(TonHash::to_vec);
        let in_msg_hash_normalized = self.in_msg_hash_normalized.as_ref().map(TonHash::to_vec);

        let fields: [&dyn ScyllaSerializeValue; DBTx::FIELDS_CNT] = [
            &mc_seqno,
            &self.address,
            &tx_lt,
            &tx_utime,
            &tx_hash,
            &self.initial_tx,
            &self.block_workchain,
            &self.block_shard,
            &block_seqno,
            &boc_stripped,
            &self.in_msg,
            &self.out_msgs,
            &in_msg_hash,
            &in_msg_hash_normalized,
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

impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for DBTxLite {
    fn type_check(specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        use scylla::cluster::metadata::NativeType;
        use scylla::deserialize::value::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};

        if specs.len() != DBTxLite::FIELDS_CNT {
            return Err(TypeCheckError::new(BuiltinTypeCheckError {
                rust_name: "DBTxLite",
                cql_type: ColumnType::Native(NativeType::Text),
                kind: BuiltinTypeCheckErrorKind::MismatchedType { expected: &[] },
            }));
        }

        scylla_typecheck_native!(specs, mc_seqno_typ, 0, "i32", NativeType::Int);
        scylla_typecheck_native!(specs, address_typ, 1, "String", NativeType::Text);
        scylla_typecheck_native!(specs, tx_lt_typ, 2, "i64", NativeType::BigInt);
        scylla_typecheck_native!(specs, tx_utime_typ, 3, "i64", NativeType::BigInt);

        scylla_check_blob_type("TonHash", specs[4].typ())?;

        // initial_tx: Option<DBTxKey>
        Option::<DBTxKey>::type_check(specs[5].typ())?;

        scylla_typecheck_native!(specs, block_workchain_typ, 6, "i32", NativeType::Int);
        scylla_typecheck_native!(specs, block_shard_typ, 7, "i64", NativeType::BigInt);
        scylla_typecheck_native!(specs, block_seqno_typ, 8, "i32", NativeType::Int);

        Ok(())
    }

    fn deserialize(
        mut row: ColumnIterator<'frame, 'metadata>,
    ) -> Result<Self, DeserializationError> {
        let mc_seqno = scylla_deserialize_val!(row, mc_seqno, i32) as u32;
        let address = scylla_deserialize_val!(row, address, String);
        let tx_lt = scylla_deserialize_val!(row, tx_lt, i64) as u64;
        let tx_utime = scylla_deserialize_val!(row, tx_utime, i64) as u32;
        let tx_hash = deserialize_ton_hash(scylla_deserialize_val!(row, tx_hash, Vec<u8>))?;
        let initial_tx = scylla_deserialize_val!(row, initial_tx, Option<DBTxKey>);
        let block_workchain = scylla_deserialize_val!(row, block_workchain, i32);
        let block_shard = scylla_deserialize_val!(row, block_shard, i64);
        let block_seqno = scylla_deserialize_val!(row, block_seqno, i32) as u32;

        Ok(Self {
            mc_seqno,
            address,
            tx_lt,
            tx_utime,
            tx_hash,
            initial_tx,
            block_workchain,
            block_shard,
            block_seqno,
        })
    }
}

impl SerializeRow for DBTxLite {
    fn is_empty(&self) -> bool {
        false
    }

    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        use scylla::_macro_internal::ser::row::serialize_column;

        scylla_typecheck_serialize_columns_len!(ctx, DBTxLite::FIELDS_CNT, "DBTxLite");

        let mc_seqno = self.mc_seqno as i32;
        let block_seqno = self.block_seqno as i32;
        let tx_lt = self.tx_lt as i64;
        let tx_utime = self.tx_utime as i64;
        let tx_hash = self.tx_hash.as_slice();

        let fields: [&dyn ScyllaSerializeValue; DBTxLite::FIELDS_CNT] = [
            &mc_seqno,
            &self.address,
            &tx_lt,
            &tx_utime,
            &tx_hash,
            &self.initial_tx,
            &self.block_workchain,
            &self.block_shard,
            &block_seqno,
        ];

        for (col, val) in ctx.columns().iter().zip(fields.iter()) {
            serialize_column::<Self>(val, col, writer)?;
        }

        Ok(())
    }
}
