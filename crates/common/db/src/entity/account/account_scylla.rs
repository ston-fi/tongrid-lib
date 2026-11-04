use crate::entity::account::db_account::DBAccount;
use crate::entity::account::db_account_lite::DBAccountLite;
use crate::entity::account::traits::AccountEntity;
use crate::entity::scylla_types::DBSimpleLib;
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

const DB_ACCOUNT_FIELDS_CNT: usize = DBAccountLite::FIELDS_CNT + 2;
const DB_ACCOUNT_LITE_FIELDS_CNT: usize = DBAccountLite::FIELDS_CNT;

impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for DBAccount {
    fn type_check(specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        use scylla::cluster::metadata::NativeType;
        use scylla::deserialize::value::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};

        scylla_typecheck_deserialize_len_check_end_return!(
            specs,
            DB_ACCOUNT_FIELDS_CNT,
            "DBAccount"
        );

        scylla_typecheck_native!(specs, mc_seqno_typ, 0, "i32", NativeType::Int);
        scylla_typecheck_native!(specs, address_typ, 1, "String", NativeType::Text);
        scylla_typecheck_native!(specs, tx_lt_typ, 2, "i64", NativeType::BigInt);

        scylla_check_blob_type("TonHash", specs[3].typ())?;

        scylla_typecheck_native!(specs, is_valid_typ, 4, "bool", NativeType::Boolean);

        Option::<Vec<u8>>::type_check(specs[5].typ())?;

        Option::<Vec<u8>>::type_check(specs[6].typ())?;

        // simple_libs: Vec<DBSimpleLib>
        Vec::<DBSimpleLib>::type_check(specs[7].typ())?;

        Vec::<Vec<u8>>::type_check(specs[8].typ())?;

        scylla_typecheck_native!(specs, boc_typ, 9, "Vec<u8>", NativeType::Blob);

        Ok(())
    }

    fn deserialize(
        mut row: ColumnIterator<'frame, 'metadata>,
    ) -> Result<Self, DeserializationError> {
        let mc_seqno = scylla_deserialize_val!(row, mc_seqno, i32) as u32;
        let address = scylla_deserialize_val!(row, address, String);
        let tx_lt = scylla_deserialize_val!(row, tx_lt, i64);
        let tx_hash = deserialize_ton_hash(scylla_deserialize_val!(row, tx_hash, Vec<u8>))?;
        let is_valid = scylla_deserialize_val!(row, is_valid, bool);
        let code_hash = scylla_deserialize_val!(row, code_hash, Option<Vec<u8>>)
            .map(deserialize_ton_hash)
            .transpose()?;
        let last_code_hash = scylla_deserialize_val!(row, last_code_hash, Option<Vec<u8>>)
            .map(deserialize_ton_hash)
            .transpose()?;
        let simple_libs = scylla_deserialize_val!(row, simple_libs, Vec<DBSimpleLib>);
        let data_lib_ids = scylla_deserialize_val!(row, data_lib_ids, Vec<Vec<u8>>)
            .into_iter()
            .map(deserialize_ton_hash)
            .collect::<Result<Vec<_>, _>>()?;
        let boc_stripped = Arc::new(scylla_deserialize_val!(row, boc_stripped, Vec<u8>));

        Ok(Self {
            mc_seqno,
            address,
            tx_lt: tx_lt as u64,
            tx_hash,
            is_valid,
            code_hash,
            last_code_hash,
            simple_libs,
            data_lib_ids,
            boc_stripped,
        })
    }
}

impl SerializeRow for DBAccount {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        use scylla::_macro_internal::ser::row::serialize_column;

        scylla_typecheck_serialize_columns_len!(ctx, DB_ACCOUNT_FIELDS_CNT, "DBAccount");

        let boc_stripped = &*self.boc_stripped;
        let mc_seqno = self.mc_seqno as i32;
        let tx_lt = self.tx_lt as i64;
        let tx_hash = self.tx_hash.as_slice();
        let code_hash = self.code_hash.as_ref().map(TonHash::to_vec);
        let last_code_hash = self.last_code_hash.as_ref().map(TonHash::to_vec);
        let data_lib_ids = self
            .data_lib_ids
            .iter()
            .map(TonHash::to_vec)
            .collect::<Vec<_>>();

        let fields: [&dyn SerializeValue; DB_ACCOUNT_FIELDS_CNT] = [
            &mc_seqno,
            &self.address,
            &tx_lt,
            &tx_hash,
            &self.is_valid,
            &code_hash,
            &last_code_hash,
            &self.simple_libs,
            &data_lib_ids,
            &boc_stripped,
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

impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for DBAccountLite {
    fn type_check(specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        use scylla::cluster::metadata::NativeType;
        use scylla::deserialize::value::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};

        scylla_typecheck_deserialize_len_check_end_return!(
            specs,
            DB_ACCOUNT_LITE_FIELDS_CNT,
            "DBAccountLite"
        );

        scylla_typecheck_native!(specs, mc_seqno_typ, 0, "i32", NativeType::Int);
        scylla_typecheck_native!(specs, address_typ, 1, "String", NativeType::Text);
        scylla_typecheck_native!(specs, tx_lt_typ, 2, "i64", NativeType::BigInt);

        scylla_check_blob_type("TonHash", specs[3].typ())?;

        scylla_typecheck_native!(specs, is_valid_typ, 4, "bool", NativeType::Boolean);

        Option::<Vec<u8>>::type_check(specs[5].typ())?;

        Option::<Vec<u8>>::type_check(specs[6].typ())?;

        // simple_libs: Vec<DBSimpleLib>
        Vec::<DBSimpleLib>::type_check(specs[7].typ())?;

        Ok(())
    }

    fn deserialize(
        mut row: ColumnIterator<'frame, 'metadata>,
    ) -> Result<Self, DeserializationError> {
        let mc_seqno = scylla_deserialize_val!(row, mc_seqno, i32) as u32;
        let address = scylla_deserialize_val!(row, address, String);
        let tx_lt = scylla_deserialize_val!(row, tx_lt, i64) as u64;
        let tx_hash = deserialize_ton_hash(scylla_deserialize_val!(row, tx_hash, Vec<u8>))?;
        let is_valid = scylla_deserialize_val!(row, is_valid, bool);
        let code_hash = scylla_deserialize_val!(row, code_hash, Option<Vec<u8>>)
            .map(deserialize_ton_hash)
            .transpose()?;
        let last_code_hash = scylla_deserialize_val!(row, last_code_hash, Option<Vec<u8>>)
            .map(deserialize_ton_hash)
            .transpose()?;
        let simple_libs = scylla_deserialize_val!(row, simple_libs, Vec<DBSimpleLib>);

        Ok(Self {
            mc_seqno,
            address,
            tx_lt,
            tx_hash,
            is_valid,
            code_hash,
            last_code_hash,
            simple_libs,
        })
    }
}

impl SerializeRow for DBAccountLite {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        use scylla::_macro_internal::ser::row::serialize_column;

        scylla_typecheck_serialize_columns_len!(ctx, DB_ACCOUNT_LITE_FIELDS_CNT, "DBAccountLite");

        let mc_seqno = self.mc_seqno as i32;
        let tx_lt = self.tx_lt as i64;
        let tx_hash = self.tx_hash.as_slice();
        let code_hash = self.code_hash.as_ref().map(TonHash::to_vec);
        let last_code_hash = self.last_code_hash.as_ref().map(TonHash::to_vec);

        let fields: [&dyn SerializeValue; DB_ACCOUNT_LITE_FIELDS_CNT] = [
            &mc_seqno,
            &self.address,
            &tx_lt,
            &tx_hash,
            &self.is_valid,
            &code_hash,
            &last_code_hash,
            &self.simple_libs,
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
    use scylla::deserialize::FrameSlice;
    use scylla::frame::response::result::{
        CollectionType, ColumnSpec, ColumnType, NativeType, TableSpec, UserDefinedType,
    };
    use scylla::serialize::row::RowSerializationContext;
    use scylla::serialize::writers::RowWriter;
    use std::borrow::Cow;
    use std::str::FromStr;

    use scylla::DeserializeRow;
    use scylla::SerializeRow;

    #[derive(Default, Debug, Clone, PartialEq, Eq, Hash, SerializeRow, DeserializeRow)]
    pub struct TestDBAccount {
        pub mc_seqno: i32,
        pub address: String,
        pub tx_lt: i64,
        pub tx_hash: Vec<u8>,
        pub is_valid: bool,
        pub code_hash: Option<Vec<u8>>,
        pub last_code_hash: Option<Vec<u8>>,
        pub simple_libs: Vec<DBSimpleLib>,
        pub data_lib_ids: Vec<Vec<u8>>,
        pub boc_stripped: Arc<Vec<u8>>, // shared with TonCell when parsing
    }

    fn account_specs() -> Vec<ColumnSpec<'static>> {
        let table_spec = TableSpec::owned("test_keyspace".to_string(), "accounts".to_string());
        let simple_lib_type = ColumnType::UserDefinedType {
            frozen: true,
            definition: Arc::new(UserDefinedType {
                name: Cow::Borrowed("simple_lib"),
                keyspace: Cow::Borrowed("test_keyspace"),
                field_types: vec![
                    (
                        Cow::Borrowed("code_hash"),
                        ColumnType::Native(NativeType::Blob),
                    ),
                    (
                        Cow::Borrowed("public"),
                        ColumnType::Native(NativeType::Boolean),
                    ),
                ],
            }),
        };
        let list_blob = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(ColumnType::Native(NativeType::Blob))),
        };
        let list_simple_lib = ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(simple_lib_type)),
        };

        vec![
            scylla_table_test_spec!(i32, "mc_seqno", table_spec),
            scylla_table_test_spec!(String, "address", table_spec),
            scylla_table_test_spec!(i64, "tx_lt", table_spec),
            scylla_table_test_spec!(Vec<u8>, "tx_hash", table_spec),
            scylla_table_test_spec!(bool, "is_valid", table_spec),
            scylla_table_test_spec!(Vec<u8>, "code_hash", table_spec),
            scylla_table_test_spec!(Vec<u8>, "last_code_hash", table_spec),
            ColumnSpec::owned(
                "simple_libs".to_string(),
                list_simple_lib,
                table_spec.clone(),
            ),
            ColumnSpec::owned("data_lib_ids".to_string(), list_blob, table_spec.clone()),
            scylla_table_test_spec!(Vec<u8>, "boc_stripped", table_spec),
        ]
    }

    #[test]
    fn test_serialization_account_in_row_test() -> anyhow::Result<()> {
        let specs = account_specs();
        let ctx = RowSerializationContext::from_specs(&specs);

        let boc_stripped = Arc::new(vec![1_u8, 4, 9, 16, 25, 36]);
        let tx_hash =
            TonHash::from_str("8b5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea22")?;
        let code_hash =
            TonHash::from_str("2a5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea2b")?;
        let last_code_hash =
            TonHash::from_str("3c5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea2c")?;
        let lib_id =
            TonHash::from_str("4d5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea2d")?;

        let db_account = DBAccount {
            mc_seqno: 7,
            address: "account_addr".to_string(),
            tx_lt: 900_000,
            tx_hash,
            is_valid: true,
            code_hash: Some(code_hash),
            last_code_hash: Some(last_code_hash),
            simple_libs: vec![
                DBSimpleLib::new(code_hash, true),
                DBSimpleLib::new(last_code_hash, false),
            ],
            data_lib_ids: vec![lib_id, tx_hash],
            boc_stripped: Arc::clone(&boc_stripped),
        };
        let test_account = TestDBAccount {
            mc_seqno: 7,
            address: "account_addr".to_string(),
            tx_lt: 900_000,
            tx_hash: tx_hash.to_vec(),
            is_valid: true,
            code_hash: Some(code_hash.to_vec()),
            last_code_hash: Some(last_code_hash.to_vec()),
            simple_libs: vec![
                DBSimpleLib::new(
                    TonHash::from_str(
                        "2a5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea2b",
                    )?,
                    true,
                ),
                DBSimpleLib::new(
                    TonHash::from_str(
                        "3c5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea2c",
                    )?,
                    false,
                ),
            ],
            data_lib_ids: vec![
                lib_id.to_vec(),
                TonHash::from_str(
                    "8b5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea22",
                )?
                .to_vec(),
            ],
            boc_stripped,
        };

        let mut db_bytes = Vec::new();
        let mut db_writer = RowWriter::new(&mut db_bytes);
        db_account.serialize(&ctx, &mut db_writer)?;

        let mut test_bytes = Vec::new();
        let mut test_writer = RowWriter::new(&mut test_bytes);
        test_account.serialize(&ctx, &mut test_writer)?;

        assert_eq!(db_bytes, test_bytes);
        Ok(())
    }

    #[test]
    fn test_deserialization_account_in_row_test() -> anyhow::Result<()> {
        let specs = account_specs();
        let ctx = RowSerializationContext::from_specs(&specs);

        let boc_stripped = Arc::new(vec![1_u8, 4, 9, 16, 25, 36]);
        let tx_hash =
            TonHash::from_str("8b5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea22")?;
        let code_hash =
            TonHash::from_str("2a5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea2b")?;
        let last_code_hash =
            TonHash::from_str("3c5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea2c")?;
        let lib_id =
            TonHash::from_str("4d5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea2d")?;

        let db_account = DBAccount {
            mc_seqno: 7,
            address: "account_addr".to_string(),
            tx_lt: 900_000,
            tx_hash,
            is_valid: true,
            code_hash: Some(code_hash),
            last_code_hash: Some(last_code_hash),
            simple_libs: vec![
                DBSimpleLib::new(code_hash, true),
                DBSimpleLib::new(last_code_hash, false),
            ],
            data_lib_ids: vec![lib_id, tx_hash],
            boc_stripped: Arc::clone(&boc_stripped),
        };
        let test_account = TestDBAccount {
            mc_seqno: 7,
            address: "account_addr".to_string(),
            tx_lt: 900_000,
            tx_hash: tx_hash.to_vec(),
            is_valid: true,
            code_hash: Some(code_hash.to_vec()),
            last_code_hash: Some(last_code_hash.to_vec()),
            simple_libs: vec![
                DBSimpleLib::new(
                    TonHash::from_str(
                        "2a5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea2b",
                    )?,
                    true,
                ),
                DBSimpleLib::new(
                    TonHash::from_str(
                        "3c5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea2c",
                    )?,
                    false,
                ),
            ],
            data_lib_ids: vec![
                lib_id.to_vec(),
                TonHash::from_str(
                    "8b5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea22",
                )?
                .to_vec(),
            ],
            boc_stripped,
        };

        let mut db_bytes = Vec::new();
        let mut db_writer = RowWriter::new(&mut db_bytes);
        db_account.serialize(&ctx, &mut db_writer)?;

        DBAccount::type_check(&specs)?;
        TestDBAccount::type_check(&specs)?;

        let db_bytes = Bytes::from(db_bytes);
        let db_row = ColumnIterator::new(&specs, FrameSlice::new(&db_bytes));
        let test_row = ColumnIterator::new(&specs, FrameSlice::new(&db_bytes));

        let decoded_db_account = DBAccount::deserialize(db_row)?;
        let decoded_test_account = TestDBAccount::deserialize(test_row)?;

        assert_eq!(decoded_db_account, db_account);
        assert_eq!(decoded_test_account, test_account);
        Ok(())
    }
}
