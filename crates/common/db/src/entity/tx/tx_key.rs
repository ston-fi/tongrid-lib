use scylla::_macro_internal::{
    ColumnType, DeserializationError, SerializationError, TypeCheckError,
};
use scylla::deserialize::value::DeserializeValue;
use scylla::serialize::value::SerializeValue;
use scylla::value::CqlValue;

use scylla::deserialize::FrameSlice;
use scylla::serialize::writers::{CellWriter, WrittenCellProof};

use std::fmt;

pub static DB_TX_KEY_ZERO: DBTxKey = DBTxKey {
    mc_seqno: 0,
    address: String::new(),
    tx_lt: 0,
};

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct DBTxKey {
    pub mc_seqno: u32,
    pub address: String,
    pub tx_lt: u64,
}

impl DBTxKey {
    pub(crate) const FIELDS: &'static str = "mc_seqno, address, tx_lt";
    pub(crate) const CQL_WHERE: &'static str = "mc_seqno = ? AND address = ? AND tx_lt = ?";
    pub fn new<T: ToString>(mc_seqno: u32, address: T, tx_lt: u64) -> Self {
        Self {
            mc_seqno,
            address: address.to_string(),
            tx_lt,
        }
    }
    pub fn is_zero(&self) -> bool {
        self == &DB_TX_KEY_ZERO
    }
}

// Support both the frozen tx_key UDT and positional values used by existing queries.
impl SerializeValue for DBTxKey {
    fn serialize<'b>(
        &self,
        typ: &ColumnType<'_>,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match typ {
            ColumnType::UserDefinedType { definition, .. } => {
                let udt = CqlValue::UserDefinedType {
                    keyspace: definition.keyspace.to_string(),
                    name: definition.name.to_string(),
                    fields: vec![
                        (
                            "mc_seqno".to_string(),
                            Some(CqlValue::Int(self.mc_seqno as i32)),
                        ),
                        (
                            "address".to_string(),
                            Some(CqlValue::Text(self.address.clone())),
                        ),
                        (
                            "tx_lt".to_string(),
                            Some(CqlValue::BigInt(self.tx_lt as i64)),
                        ),
                    ],
                };
                SerializeValue::serialize(&udt, typ, writer)
            }
            _ => {
                let mc_seqno = self.mc_seqno as i32;
                let tx_lt = self.tx_lt as i64;
                SerializeValue::serialize(&(&mc_seqno, &self.address, &tx_lt), typ, writer)
            }
        }
    }
}
impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for DBTxKey {
    fn type_check(typ: &ColumnType<'_>) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::UserDefinedType { definition, .. } => {
                use scylla::cluster::metadata::NativeType;
                use scylla::deserialize::value::{
                    BuiltinTypeCheckError, BuiltinTypeCheckErrorKind,
                };

                let fields = &definition.field_types;
                if fields.len() != DB_TX_KEY_FIELDS_CNT {
                    return Err(TypeCheckError::new(BuiltinTypeCheckError {
                        rust_name: "DBTxKey",
                        cql_type: ColumnType::Native(NativeType::Text),
                        kind: BuiltinTypeCheckErrorKind::MismatchedType { expected: &[] },
                    }));
                }

                let (name, typ) = &fields[0];
                if name.as_ref() != "mc_seqno" {
                    return Err(TypeCheckError::new(BuiltinTypeCheckError {
                        rust_name: "DBTxKey",
                        cql_type: typ.clone().into_owned(),
                        kind: BuiltinTypeCheckErrorKind::MismatchedType { expected: &[] },
                    }));
                }
                match typ {
                    ColumnType::Native(NativeType::Int) => Ok(()),
                    _ => Err(TypeCheckError::new(BuiltinTypeCheckError {
                        rust_name: "i32",
                        cql_type: typ.clone().into_owned(),
                        kind: BuiltinTypeCheckErrorKind::MismatchedType {
                            expected: &[ColumnType::Native(NativeType::Int)],
                        },
                    })),
                }?;

                let (name, typ) = &fields[1];
                if name.as_ref() != "address" {
                    return Err(TypeCheckError::new(BuiltinTypeCheckError {
                        rust_name: "DBTxKey",
                        cql_type: typ.clone().into_owned(),
                        kind: BuiltinTypeCheckErrorKind::MismatchedType { expected: &[] },
                    }));
                }
                match typ {
                    ColumnType::Native(NativeType::Text) => Ok(()),
                    _ => Err(TypeCheckError::new(BuiltinTypeCheckError {
                        rust_name: "String",
                        cql_type: typ.clone().into_owned(),
                        kind: BuiltinTypeCheckErrorKind::MismatchedType {
                            expected: &[ColumnType::Native(NativeType::Text)],
                        },
                    })),
                }?;

                let (name, typ) = &fields[2];
                if name.as_ref() != "tx_lt" {
                    return Err(TypeCheckError::new(BuiltinTypeCheckError {
                        rust_name: "DBTxKey",
                        cql_type: typ.clone().into_owned(),
                        kind: BuiltinTypeCheckErrorKind::MismatchedType { expected: &[] },
                    }));
                }
                match typ {
                    ColumnType::Native(NativeType::BigInt) => Ok(()),
                    _ => Err(TypeCheckError::new(BuiltinTypeCheckError {
                        rust_name: "i64",
                        cql_type: typ.clone().into_owned(),
                        kind: BuiltinTypeCheckErrorKind::MismatchedType {
                            expected: &[ColumnType::Native(NativeType::BigInt)],
                        },
                    })),
                }?;

                Ok(())
            }
            _ => <(i32, String, i64) as DeserializeValue<'frame, 'metadata>>::type_check(typ),
        }
    }

    fn deserialize(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        match typ {
            ColumnType::UserDefinedType { .. } => {
                let cql = <CqlValue as DeserializeValue<'frame, 'metadata>>::deserialize(typ, v)?;
                let CqlValue::UserDefinedType { fields, .. } = cql else {
                    return Err(DeserializationError::new(MissingColumnError(
                        "tx_key".to_string(),
                    )));
                };

                let mut mc_seqno = None;
                let mut address = None;
                let mut tx_lt = None;
                for (name, value) in fields {
                    match (name.as_str(), value) {
                        ("mc_seqno", Some(CqlValue::Int(val))) => mc_seqno = Some(val as u32),
                        ("address", Some(CqlValue::Text(val))) => address = Some(val),
                        ("tx_lt", Some(CqlValue::BigInt(val))) => tx_lt = Some(val as u64),
                        _ => {}
                    }
                }

                let mc_seqno = mc_seqno.ok_or_else(|| {
                    DeserializationError::new(MissingColumnError("mc_seqno".to_string()))
                })?;
                let address = address.ok_or_else(|| {
                    DeserializationError::new(MissingColumnError("address".to_string()))
                })?;
                let tx_lt = tx_lt.ok_or_else(|| {
                    DeserializationError::new(MissingColumnError("tx_lt".to_string()))
                })?;

                Ok(Self {
                    mc_seqno,
                    address,
                    tx_lt,
                })
            }
            _ => {
                let (mc_seqno, address, tx_lt) = <(i32, String, i64) as DeserializeValue<
                    'frame,
                    'metadata,
                >>::deserialize(typ, v)?;

                Ok(Self {
                    mc_seqno: mc_seqno as u32,
                    address,
                    tx_lt: tx_lt as u64,
                })
            }
        }
    }
}

#[derive(Debug)]
struct MissingColumnError(String);

impl fmt::Display for MissingColumnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Missing column: {}", self.0)
    }
}

impl std::error::Error for MissingColumnError {}

const DB_TX_KEY_FIELDS_CNT: usize = 3;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use scylla::deserialize::FrameSlice;
    use scylla::frame::response::result::{ColumnType, NativeType, UserDefinedType};
    use scylla::serialize::writers::CellWriter;
    use std::borrow::Cow;
    use std::sync::Arc;

    #[derive(
        Default, Debug, Clone, PartialEq, Eq, Hash, scylla::SerializeValue, scylla::DeserializeValue,
    )]
    pub struct TestDBTxKey {
        pub mc_seqno: i32,
        pub address: String,
        pub tx_lt: i64,
    }

    fn tx_key_column_type() -> ColumnType<'static> {
        ColumnType::UserDefinedType {
            frozen: false,
            definition: Arc::new(UserDefinedType {
                name: Cow::Borrowed("tx_key"),
                keyspace: Cow::Borrowed("test_keyspace"),
                field_types: vec![
                    (
                        Cow::Borrowed("mc_seqno"),
                        ColumnType::Native(NativeType::Int),
                    ),
                    (
                        Cow::Borrowed("address"),
                        ColumnType::Native(NativeType::Text),
                    ),
                    (
                        Cow::Borrowed("tx_lt"),
                        ColumnType::Native(NativeType::BigInt),
                    ),
                ],
            }),
        }
    }

    fn serialize_value<T: SerializeValue>(
        value: &T,
        typ: &ColumnType<'_>,
    ) -> anyhow::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        SerializeValue::serialize(value, typ, CellWriter::new_without_size(&mut bytes))?;
        Ok(bytes)
    }

    #[test]
    fn test_deserialization_dbtxkey_in_value_test() -> anyhow::Result<()> {
        let test_key = TestDBTxKey {
            mc_seqno: 42,
            address: "address".to_string(),
            tx_lt: 12345,
        };

        let expected_db_key = DBTxKey {
            mc_seqno: test_key.mc_seqno as u32,
            address: test_key.address.clone(),
            tx_lt: test_key.tx_lt as u64,
        };

        let typ = tx_key_column_type();
        let encoded = serialize_value(&test_key, &typ)?;
        let bytes = Bytes::from(encoded);

        let decoded_db_key = DBTxKey::deserialize(&typ, Some(FrameSlice::new(&bytes)))?;
        let decoded_test_key = TestDBTxKey::deserialize(&typ, Some(FrameSlice::new(&bytes)))?;

        assert_eq!(decoded_db_key, expected_db_key);
        assert_eq!(decoded_test_key, test_key);

        Ok(())
    }

    #[test]
    fn test_serialization_dbtxkey_in_value_test() -> anyhow::Result<()> {
        let db_key = DBTxKey {
            mc_seqno: 42,
            address: "address".to_string(),
            tx_lt: 12345,
        };
        let test_key = TestDBTxKey {
            mc_seqno: db_key.mc_seqno as i32,
            address: db_key.address.clone(),
            tx_lt: db_key.tx_lt as i64,
        };

        let typ = tx_key_column_type();
        let db_key_bytes = serialize_value(&db_key, &typ)?;
        let test_key_bytes = serialize_value(&test_key, &typ)?;

        assert_eq!(db_key_bytes, test_key_bytes);
        Ok(())
    }
}
