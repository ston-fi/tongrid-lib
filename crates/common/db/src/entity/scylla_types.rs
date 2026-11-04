use crate::util::deserialize_ton_hash;
use scylla::_macro_internal::{
    CellWriter, ColumnType, DeserializationError, FrameSlice, SerializationError, TypeCheckError,
    WrittenCellProof,
};
use scylla::deserialize::value::DeserializeValue;
use scylla::serialize::value::SerializeValue;
use scylla::value::CqlValue;
use std::fmt;
use ton::ton_core::cell::TonHash;
use ton::ton_core::types::TonAddress;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct DBMsgKey {
    pub src: String,
    pub dst: String,
    pub created_lt: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DBMsg {
    pub src: String,
    pub dst: String,
    pub created_lt: u64,
    pub boc_stripped: Vec<u8>,
    pub body_lib_ids: Vec<TonHash>,
    pub code_hash: Option<TonHash>,
    pub data_lib_ids: Vec<TonHash>,
    pub simple_libs: Vec<DBSimpleLib>,
}

impl Default for DBMsg {
    fn default() -> Self {
        Self {
            src: TonAddress::ZERO.to_string(),
            dst: TonAddress::ZERO.to_string(),
            created_lt: 0,
            boc_stripped: vec![
                181, 238, 156, 114, 1, 1, 2, 1, 0, 20, 0, 1, 29, 96, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 3, 1, 0, 0,
            ],
            body_lib_ids: Vec::new(),
            code_hash: None,
            data_lib_ids: Vec::new(),
            simple_libs: Vec::new(),
        }
    }
}

impl SerializeValue for DBMsg {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match typ {
            ColumnType::UserDefinedType { definition, .. } => {
                let simple_libs = self
                    .simple_libs
                    .iter()
                    .map(|lib| CqlValue::UserDefinedType {
                        keyspace: definition.keyspace.to_string(),
                        name: "simple_lib".to_string(),
                        fields: vec![
                            (
                                "code_hash".to_string(),
                                Some(CqlValue::Blob(lib.code_hash.to_vec())),
                            ),
                            ("public".to_string(), Some(CqlValue::Boolean(lib.public))),
                        ],
                    })
                    .collect();
                let udt = CqlValue::UserDefinedType {
                    keyspace: definition.keyspace.to_string(),
                    name: definition.name.to_string(),
                    fields: vec![
                        ("src".to_string(), Some(CqlValue::Text(self.src.clone()))),
                        ("dst".to_string(), Some(CqlValue::Text(self.dst.clone()))),
                        (
                            "created_lt".to_string(),
                            Some(CqlValue::BigInt(self.created_lt as i64)),
                        ),
                        (
                            "boc_stripped".to_string(),
                            Some(CqlValue::Blob(self.boc_stripped.clone())),
                        ),
                        (
                            "body_lib_ids".to_string(),
                            Some(CqlValue::List(
                                self.body_lib_ids
                                    .iter()
                                    .map(|hash| CqlValue::Blob(hash.to_vec()))
                                    .collect(),
                            )),
                        ),
                        (
                            "code_hash".to_string(),
                            self.code_hash
                                .as_ref()
                                .map(|hash| CqlValue::Blob(hash.to_vec())),
                        ),
                        (
                            "data_lib_ids".to_string(),
                            Some(CqlValue::List(
                                self.data_lib_ids
                                    .iter()
                                    .map(|hash| CqlValue::Blob(hash.to_vec()))
                                    .collect(),
                            )),
                        ),
                        ("simple_libs".to_string(), Some(CqlValue::List(simple_libs))),
                    ],
                };
                SerializeValue::serialize(&udt, typ, writer)
            }
            _ => {
                let created_lt = self.created_lt as i64;
                let body_lib_ids = self
                    .body_lib_ids
                    .iter()
                    .map(TonHash::to_vec)
                    .collect::<Vec<_>>();
                let code_hash = self.code_hash.as_ref().map(TonHash::to_vec);
                let data_lib_ids = self
                    .data_lib_ids
                    .iter()
                    .map(TonHash::to_vec)
                    .collect::<Vec<_>>();
                SerializeValue::serialize(
                    &(
                        &self.src,
                        &self.dst,
                        &created_lt,
                        &self.boc_stripped,
                        &body_lib_ids,
                        &code_hash,
                        &data_lib_ids,
                        &self.simple_libs,
                    ),
                    typ,
                    writer,
                )
            }
        }
    }
}

#[derive(Debug)]
struct UdtFieldError(String);

impl fmt::Display for UdtFieldError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UDT field error: {}", self.0)
    }
}

impl std::error::Error for UdtFieldError {}

impl DeserializeValue<'_, '_> for DBMsg {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::UserDefinedType { .. } => Ok(()),
            _ => <(
                String,
                String,
                i64,
                Vec<u8>,
                Vec<Vec<u8>>,
                Option<Vec<u8>>,
                Vec<Vec<u8>>,
                Vec<DBSimpleLib>,
            ) as DeserializeValue>::type_check(typ),
        }
    }

    fn deserialize<'a>(
        typ: &'a ColumnType<'a>,
        v: Option<FrameSlice<'_>>,
    ) -> Result<Self, DeserializationError> {
        match typ {
            ColumnType::UserDefinedType { .. } => {
                let cql = <CqlValue as DeserializeValue>::deserialize(typ, v)?;
                let CqlValue::UserDefinedType { fields, .. } = cql else {
                    return Err(DeserializationError::new(UdtFieldError("msg".to_string())));
                };

                let mut src = None;
                let mut dst = None;
                let mut created_lt = None;
                let mut boc_stripped = None;
                let mut body_lib_ids = None;
                let mut code_hash = None;
                let mut data_lib_ids = None;
                let mut simple_libs = None;

                for (name, value) in fields {
                    match (name.as_str(), value) {
                        ("src", Some(CqlValue::Text(val))) => src = Some(val),
                        ("dst", Some(CqlValue::Text(val))) => dst = Some(val),
                        ("created_lt", Some(CqlValue::BigInt(val))) => {
                            created_lt = Some(val as u64)
                        }
                        ("boc_stripped", Some(CqlValue::Blob(val))) => boc_stripped = Some(val),
                        ("body_lib_ids", Some(CqlValue::List(list))) => {
                            let hashes =
                                list.into_iter()
                                    .map(|item| match item {
                                        CqlValue::Blob(bytes) => deserialize_ton_hash(bytes)
                                            .map_err(|err| {
                                                DeserializationError::new(UdtFieldError(format!(
                                                    "body_lib_ids decode error: {err}"
                                                )))
                                            }),
                                        _ => Err(DeserializationError::new(UdtFieldError(
                                            "body_lib_ids item type mismatch".to_string(),
                                        ))),
                                    })
                                    .collect::<Result<Vec<_>, _>>()?;
                            body_lib_ids = Some(hashes);
                        }
                        ("code_hash", Some(CqlValue::Blob(bytes))) => {
                            let hash = deserialize_ton_hash(bytes).map_err(|err| {
                                DeserializationError::new(UdtFieldError(format!(
                                    "code_hash decode error: {err}"
                                )))
                            })?;
                            code_hash = Some(Some(hash));
                        }
                        ("code_hash", None) => code_hash = Some(None),
                        ("data_lib_ids", Some(CqlValue::List(list))) => {
                            let hashes =
                                list.into_iter()
                                    .map(|item| match item {
                                        CqlValue::Blob(bytes) => deserialize_ton_hash(bytes)
                                            .map_err(|err| {
                                                DeserializationError::new(UdtFieldError(format!(
                                                    "data_lib_ids decode error: {err}"
                                                )))
                                            }),
                                        _ => Err(DeserializationError::new(UdtFieldError(
                                            "data_lib_ids item type mismatch".to_string(),
                                        ))),
                                    })
                                    .collect::<Result<Vec<_>, _>>()?;
                            data_lib_ids = Some(hashes);
                        }
                        ("simple_libs", Some(CqlValue::List(list))) => {
                            let libs = list
                                .into_iter()
                                .map(|item| {
                                    let CqlValue::UserDefinedType { fields, .. } = item else {
                                        return Err(DeserializationError::new(UdtFieldError(
                                            "simple_libs item type mismatch".to_string(),
                                        )));
                                    };

                                    let mut code_hash = None;
                                    let mut public = None;
                                    for (fname, fvalue) in fields {
                                        match (fname.as_str(), fvalue) {
                                            ("code_hash", Some(CqlValue::Blob(bytes))) => {
                                                let hash =
                                                    deserialize_ton_hash(bytes).map_err(|err| {
                                                        DeserializationError::new(UdtFieldError(
                                                            format!(
                                                        "simple_libs code_hash decode error: {err}"
                                                    ),
                                                        ))
                                                    })?;
                                                code_hash = Some(hash);
                                            }
                                            ("public", Some(CqlValue::Boolean(val))) => {
                                                public = Some(val)
                                            }
                                            _ => {}
                                        }
                                    }

                                    Ok(DBSimpleLib {
                                        code_hash: code_hash.ok_or_else(|| {
                                            DeserializationError::new(UdtFieldError(
                                                "simple_libs missing code_hash".to_string(),
                                            ))
                                        })?,
                                        public: public.ok_or_else(|| {
                                            DeserializationError::new(UdtFieldError(
                                                "simple_libs missing public".to_string(),
                                            ))
                                        })?,
                                    })
                                })
                                .collect::<Result<Vec<_>, _>>()?;
                            simple_libs = Some(libs);
                        }
                        _ => {}
                    }
                }

                Ok(Self {
                    src: src.ok_or_else(|| {
                        DeserializationError::new(UdtFieldError("src".to_string()))
                    })?,
                    dst: dst.ok_or_else(|| {
                        DeserializationError::new(UdtFieldError("dst".to_string()))
                    })?,
                    created_lt: created_lt.ok_or_else(|| {
                        DeserializationError::new(UdtFieldError("created_lt".to_string()))
                    })?,
                    boc_stripped: boc_stripped.ok_or_else(|| {
                        DeserializationError::new(UdtFieldError("boc_stripped".to_string()))
                    })?,
                    body_lib_ids: body_lib_ids.ok_or_else(|| {
                        DeserializationError::new(UdtFieldError("body_lib_ids".to_string()))
                    })?,
                    code_hash: code_hash.ok_or_else(|| {
                        DeserializationError::new(UdtFieldError("code_hash".to_string()))
                    })?,
                    data_lib_ids: data_lib_ids.ok_or_else(|| {
                        DeserializationError::new(UdtFieldError("data_lib_ids".to_string()))
                    })?,
                    simple_libs: simple_libs.ok_or_else(|| {
                        DeserializationError::new(UdtFieldError("simple_libs".to_string()))
                    })?,
                })
            }
            _ => {
                let (
                    src,
                    dst,
                    created_lt,
                    boc_stripped,
                    body_lib_ids,
                    code_hash,
                    data_lib_ids,
                    simple_libs,
                ) = <(
                    String,
                    String,
                    i64,
                    Vec<u8>,
                    Vec<Vec<u8>>,
                    Option<Vec<u8>>,
                    Vec<Vec<u8>>,
                    Vec<DBSimpleLib>,
                ) as DeserializeValue>::deserialize(typ, v)?;

                Ok(Self {
                    src,
                    dst,
                    created_lt: created_lt as u64,
                    boc_stripped,
                    body_lib_ids: body_lib_ids
                        .into_iter()
                        .map(deserialize_ton_hash)
                        .collect::<Result<Vec<_>, _>>()?,
                    code_hash: code_hash.map(deserialize_ton_hash).transpose()?,
                    data_lib_ids: data_lib_ids
                        .into_iter()
                        .map(deserialize_ton_hash)
                        .collect::<Result<Vec<_>, _>>()?,
                    simple_libs,
                })
            }
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub struct DBSimpleLib {
    pub code_hash: TonHash,
    pub public: bool,
}

impl SerializeValue for DBSimpleLib {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match typ {
            ColumnType::UserDefinedType { definition, .. } => {
                let udt = CqlValue::UserDefinedType {
                    keyspace: definition.keyspace.to_string(),
                    name: definition.name.to_string(),
                    fields: vec![
                        (
                            "code_hash".to_string(),
                            Some(CqlValue::Blob(self.code_hash.to_vec())),
                        ),
                        ("public".to_string(), Some(CqlValue::Boolean(self.public))),
                    ],
                };
                SerializeValue::serialize(&udt, typ, writer)
            }
            _ => {
                let code_hash = self.code_hash.to_vec();
                SerializeValue::serialize(&(&code_hash, &self.public), typ, writer)
            }
        }
    }
}

impl DeserializeValue<'_, '_> for DBSimpleLib {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        match typ {
            ColumnType::UserDefinedType { .. } => Ok(()),
            _ => <(Vec<u8>, bool) as DeserializeValue>::type_check(typ),
        }
    }

    fn deserialize<'a>(
        typ: &'a ColumnType<'a>,
        v: Option<FrameSlice<'_>>,
    ) -> Result<Self, DeserializationError> {
        match typ {
            ColumnType::UserDefinedType { .. } => {
                let cql = <CqlValue as DeserializeValue>::deserialize(typ, v)?;
                let CqlValue::UserDefinedType { fields, .. } = cql else {
                    return Err(DeserializationError::new(UdtFieldError(
                        "simple_lib".to_string(),
                    )));
                };

                let mut code_hash = None;
                let mut public = None;
                for (field_name, value) in fields {
                    match (field_name.as_str(), value) {
                        ("code_hash", Some(CqlValue::Blob(bytes))) => {
                            code_hash = Some(deserialize_ton_hash(bytes)?)
                        }
                        ("public", Some(CqlValue::Boolean(val))) => public = Some(val),
                        _ => {}
                    }
                }

                Ok(Self {
                    code_hash: code_hash.ok_or_else(|| {
                        DeserializationError::new(UdtFieldError("simple_lib code_hash".to_string()))
                    })?,
                    public: public.ok_or_else(|| {
                        DeserializationError::new(UdtFieldError("simple_lib public".to_string()))
                    })?,
                })
            }
            _ => {
                let (code_hash, public) =
                    <(Vec<u8>, bool) as DeserializeValue>::deserialize(typ, v)?;
                Ok(Self {
                    code_hash: deserialize_ton_hash(code_hash)?,
                    public,
                })
            }
        }
    }
}

impl DBSimpleLib {
    pub fn new<T: Into<TonHash>>(hash: T, public: bool) -> Self {
        Self {
            code_hash: hash.into(),
            public,
        }
    }
}

impl DBMsg {
    pub fn key(&self) -> DBMsgKey {
        DBMsgKey {
            src: self.src.clone(),
            dst: self.dst.clone(),
            created_lt: self.created_lt,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use scylla::frame::response::result::{CollectionType, NativeType, UserDefinedType};
    use scylla::{DeserializeValue, SerializeValue};
    use std::borrow::Cow;
    use std::str::FromStr;
    use std::sync::Arc;

    #[derive(Debug, Clone, Eq, PartialEq, Hash, SerializeValue, DeserializeValue)]
    pub struct TestDBMsgKey {
        pub src: String,
        pub dst: String,
        pub created_lt: i64,
    }

    #[derive(Debug, Clone, PartialEq, Eq, SerializeValue, DeserializeValue)]
    pub struct TestDBSimpleLib {
        pub code_hash: Vec<u8>,
        pub public: bool,
    }

    #[derive(Debug, Clone, PartialEq, Eq, SerializeValue, DeserializeValue)]
    pub struct TestDBMsg {
        pub src: String,
        pub dst: String,
        pub created_lt: i64,
        pub boc_stripped: Vec<u8>,
        pub body_lib_ids: Vec<Vec<u8>>,
        pub code_hash: Option<Vec<u8>>,
        pub data_lib_ids: Vec<Vec<u8>>,
        pub simple_libs: Vec<TestDBSimpleLib>,
    }

    fn msg_column_type() -> ColumnType<'static> {
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
        ColumnType::UserDefinedType {
            frozen: true,
            definition: Arc::new(UserDefinedType {
                name: Cow::Borrowed("db_msg"),
                keyspace: Cow::Borrowed("test_keyspace"),
                field_types: vec![
                    (Cow::Borrowed("src"), ColumnType::Native(NativeType::Text)),
                    (Cow::Borrowed("dst"), ColumnType::Native(NativeType::Text)),
                    (
                        Cow::Borrowed("created_lt"),
                        ColumnType::Native(NativeType::BigInt),
                    ),
                    (
                        Cow::Borrowed("boc_stripped"),
                        ColumnType::Native(NativeType::Blob),
                    ),
                    (Cow::Borrowed("body_lib_ids"), list_blob.clone()),
                    (
                        Cow::Borrowed("code_hash"),
                        ColumnType::Native(NativeType::Blob),
                    ),
                    (Cow::Borrowed("data_lib_ids"), list_blob),
                    (Cow::Borrowed("simple_libs"), list_simple_lib),
                ],
            }),
        }
    }

    fn serialize_value<T: SerializeValue>(value: &T, typ: &ColumnType) -> anyhow::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        SerializeValue::serialize(value, typ, CellWriter::new_without_size(&mut bytes))?;
        Ok(bytes)
    }

    #[test]
    fn test_dbmsg_dbmsgkey_serialization() -> anyhow::Result<()> {
        let hash_a =
            TonHash::from_str("8b5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea22")?;
        let hash_b =
            TonHash::from_str("2a5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea2b")?;
        let hash_c =
            TonHash::from_str("3c5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea2c")?;

        let db_msg = DBMsg {
            src: "src_addr".to_string(),
            dst: "dst_addr".to_string(),
            created_lt: 42,
            boc_stripped: vec![1_u8, 4, 9, 16, 25],
            body_lib_ids: vec![hash_a, hash_b],
            code_hash: Some(hash_c),
            data_lib_ids: vec![hash_b],
            simple_libs: vec![
                DBSimpleLib::new(hash_a, true),
                DBSimpleLib::new(hash_c, false),
            ],
        };
        let test_msg = TestDBMsg {
            src: db_msg.src.clone(),
            dst: db_msg.dst.clone(),
            created_lt: db_msg.created_lt as i64,
            boc_stripped: db_msg.boc_stripped.clone(),
            body_lib_ids: db_msg.body_lib_ids.iter().map(TonHash::to_vec).collect(),
            code_hash: db_msg.code_hash.as_ref().map(TonHash::to_vec),
            data_lib_ids: db_msg.data_lib_ids.iter().map(TonHash::to_vec).collect(),
            simple_libs: db_msg
                .simple_libs
                .iter()
                .map(|lib| TestDBSimpleLib {
                    code_hash: lib.code_hash.to_vec(),
                    public: lib.public,
                })
                .collect(),
        };

        let msg_typ = msg_column_type();
        let db_msg_bytes = serialize_value(&db_msg, &msg_typ)?;
        let test_msg_bytes = serialize_value(&test_msg, &msg_typ)?;
        assert_eq!(db_msg_bytes, test_msg_bytes);

        let db_key = db_msg.key();
        let test_key = TestDBMsgKey {
            src: db_key.src.clone(),
            dst: db_key.dst.clone(),
            created_lt: db_key.created_lt as i64,
        };
        assert_eq!(test_key.src, db_key.src);
        assert_eq!(test_key.dst, db_key.dst);
        assert_eq!(test_key.created_lt, db_key.created_lt as i64);

        Ok(())
    }

    #[test]
    fn test_dbmsg_dbmsgkey_deserialization() -> anyhow::Result<()> {
        let hash_a =
            TonHash::from_str("8b5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea22")?;
        let hash_b =
            TonHash::from_str("2a5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea2b")?;
        let hash_c =
            TonHash::from_str("3c5ba7da1d8c9d36a168988e1727326105b4812a48be7c0e018b27627878ea2c")?;

        let db_msg = DBMsg {
            src: "src_addr".to_string(),
            dst: "dst_addr".to_string(),
            created_lt: 42,
            boc_stripped: vec![1_u8, 4, 9, 16, 25],
            body_lib_ids: vec![hash_a, hash_b],
            code_hash: Some(hash_c),
            data_lib_ids: vec![hash_b],
            simple_libs: vec![
                DBSimpleLib::new(hash_a, true),
                DBSimpleLib::new(hash_c, false),
            ],
        };
        let test_msg = TestDBMsg {
            src: db_msg.src.clone(),
            dst: db_msg.dst.clone(),
            created_lt: db_msg.created_lt as i64,
            boc_stripped: db_msg.boc_stripped.clone(),
            body_lib_ids: db_msg.body_lib_ids.iter().map(TonHash::to_vec).collect(),
            code_hash: db_msg.code_hash.as_ref().map(TonHash::to_vec),
            data_lib_ids: db_msg.data_lib_ids.iter().map(TonHash::to_vec).collect(),
            simple_libs: db_msg
                .simple_libs
                .iter()
                .map(|lib| TestDBSimpleLib {
                    code_hash: lib.code_hash.to_vec(),
                    public: lib.public,
                })
                .collect(),
        };

        let msg_typ = msg_column_type();
        let msg_bytes = serialize_value(&test_msg, &msg_typ)?;
        let msg_bytes = Bytes::from(msg_bytes);
        let decoded_db_msg = DBMsg::deserialize(&msg_typ, Some(FrameSlice::new(&msg_bytes)))?;
        let decoded_test_msg = TestDBMsg::deserialize(&msg_typ, Some(FrameSlice::new(&msg_bytes)))?;
        assert_eq!(decoded_db_msg, db_msg);
        assert_eq!(decoded_test_msg, test_msg);

        let db_key = db_msg.key();
        let test_key = TestDBMsgKey {
            src: db_key.src.clone(),
            dst: db_key.dst.clone(),
            created_lt: db_key.created_lt as i64,
        };
        assert_eq!(test_key.src, db_key.src);
        assert_eq!(test_key.dst, db_key.dst);
        assert_eq!(test_key.created_lt, db_key.created_lt as i64);

        Ok(())
    }
}
