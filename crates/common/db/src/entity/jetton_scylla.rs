use crate::entity::jetton_balance::DBJettonBalance;
use crate::entity::jetton_wallet::DBJettonWallet;
use crate::{
    scylla_deserialize_val, scylla_typecheck_deserialize_len_check_end_return,
    scylla_typecheck_native, scylla_typecheck_serialize_columns_len,
};
use num_bigint::BigInt;
use scylla::_macro_internal::{
    ColumnIterator, ColumnSpec, ColumnType, DeserializationError, SerializationError,
    TypeCheckError,
};
use scylla::deserialize::row::DeserializeRow;
use scylla::deserialize::value::DeserializeValue;
use scylla::serialize::row::{RowSerializationContext, SerializeRow};
use scylla::serialize::value::SerializeValue as ScyllaSerializeValue;
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

impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for DBJettonBalance {
    fn type_check(specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        use scylla::cluster::metadata::NativeType;
        use scylla::deserialize::value::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};

        if specs.len() != DBJettonBalance::FIELDS_CNT {
            return Err(TypeCheckError::new(BuiltinTypeCheckError {
                rust_name: "DBJettonBalance",
                cql_type: ColumnType::Native(NativeType::Text),
                kind: BuiltinTypeCheckErrorKind::MismatchedType { expected: &[] },
            }));
        }

        scylla_typecheck_native!(specs, wallet_address_typ, 0, "String", NativeType::Text);
        scylla_typecheck_native!(specs, tx_lt_typ, 1, "i64", NativeType::BigInt);
        scylla_typecheck_native!(specs, is_valid_typ, 2, "bool", NativeType::Boolean);
        Option::<BigInt>::type_check(specs[3].typ())?;
        Option::<BigInt>::type_check(specs[4].typ())?;
        scylla_typecheck_native!(specs, tx_utime_typ, 5, "i64", NativeType::BigInt);
        scylla_typecheck_native!(specs, mc_seqno_typ, 6, "i32", NativeType::Int);

        Ok(())
    }

    fn deserialize(
        mut row: ColumnIterator<'frame, 'metadata>,
    ) -> Result<Self, DeserializationError> {
        let wallet_address = scylla_deserialize_val!(row, wallet_address, String);
        let tx_lt = scylla_deserialize_val!(row, tx_lt, i64) as u64;
        let is_valid = scylla_deserialize_val!(row, is_valid, bool);
        let prev_balance = scylla_deserialize_val!(row, prev_balance, Option<BigInt>);
        let balance = scylla_deserialize_val!(row, balance, Option<BigInt>);
        let tx_utime = scylla_deserialize_val!(row, tx_utime, i64) as u32;
        let mc_seqno = scylla_deserialize_val!(row, mc_seqno, i32) as u32;

        Ok(Self {
            wallet_address,
            tx_lt,
            is_valid,
            prev_balance,
            balance,
            tx_utime,
            mc_seqno,
        })
    }
}

impl SerializeRow for DBJettonBalance {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        use scylla::_macro_internal::ser::row::serialize_column;

        scylla_typecheck_serialize_columns_len!(
            ctx,
            DBJettonBalance::FIELDS_CNT,
            "DBJettonBalance"
        );

        let mc_seqno = self.mc_seqno as i32;
        let tx_lt = self.tx_lt as i64;
        let tx_utime = self.tx_utime as i64;
        let fields: [&dyn ScyllaSerializeValue; DBJettonBalance::FIELDS_CNT] = [
            &self.wallet_address,
            &tx_lt,
            &self.is_valid,
            &self.prev_balance,
            &self.balance,
            &tx_utime,
            &mc_seqno,
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

impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for DBJettonWallet {
    fn type_check(specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        use scylla::cluster::metadata::NativeType;
        use scylla::deserialize::value::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};

        scylla_typecheck_deserialize_len_check_end_return!(
            specs,
            DBJettonWallet::FIELDS_CNT,
            "DBJettonWallet"
        );

        scylla_typecheck_native!(specs, wallet_address_typ, 0, "String", NativeType::Text);
        scylla_typecheck_native!(specs, is_legit_typ, 1, "bool", NativeType::Boolean);
        scylla_typecheck_native!(specs, master_address_typ, 2, "String", NativeType::Text);
        scylla_typecheck_native!(specs, owner_address_typ, 3, "String", NativeType::Text);
        scylla_typecheck_native!(specs, mc_seqno_typ, 4, "i32", NativeType::Int);

        Ok(())
    }

    fn deserialize(
        mut row: ColumnIterator<'frame, 'metadata>,
    ) -> Result<Self, DeserializationError> {
        let wallet_address = scylla_deserialize_val!(row, wallet_address, String);
        let is_legit = scylla_deserialize_val!(row, is_legit, bool);
        let master_address = scylla_deserialize_val!(row, master_address, String);
        let owner_address = scylla_deserialize_val!(row, owner_address, String);
        let mc_seqno = scylla_deserialize_val!(row, mc_seqno, i32) as u32;

        Ok(Self {
            wallet_address,
            is_legit,
            master_address,
            owner_address,
            mc_seqno,
        })
    }
}

impl SerializeRow for DBJettonWallet {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        use scylla::_macro_internal::ser::row::serialize_column;

        scylla_typecheck_serialize_columns_len!(ctx, DBJettonWallet::FIELDS_CNT, "DBJettonWallet");
        let mc_seqno = self.mc_seqno as i32;

        let fields: [&dyn ScyllaSerializeValue; DBJettonWallet::FIELDS_CNT] = [
            &self.wallet_address,
            &self.is_legit,
            &self.master_address,
            &self.owner_address,
            &mc_seqno,
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
