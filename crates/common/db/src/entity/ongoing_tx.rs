use crate::db::{Inner, DB};
use crate::entity::DBTxKey;
use anyhow::bail;
use function_name::named;
use futures::future::try_join_all;
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
use stonfi_scylla_client::client::ScyllaClient;

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct OngoingTx {
    pub address: String,
    pub tx_lt: u64,
    pub mc_seqno: u32,
    pub count: i32,
    pub last_processed_mc_seqno: u32,
}

impl OngoingTx {
    const FIELDS: &'static str = "address, tx_lt, mc_seqno, count, last_processed_mc_seqno";
    const FIELDS_CNT: usize = 5;
    pub fn tx_key(&self) -> DBTxKey {
        DBTxKey {
            mc_seqno: self.mc_seqno,
            address: self.address.clone(),
            tx_lt: self.tx_lt,
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

impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for OngoingTx {
    fn deserialize(
        mut row: ColumnIterator<'frame, 'metadata>,
    ) -> Result<Self, DeserializationError> {
        let address = {
            let col = row.next().ok_or_else(|| {
                DeserializationError::new(MissingColumnError("address".to_string()))
            })??;
            <String as ScyllaDeserializeValue>::deserialize(col.spec.typ(), col.slice)?
        };

        let tx_lt = {
            let col = row.next().ok_or_else(|| {
                DeserializationError::new(MissingColumnError("tx_lt".to_string()))
            })??;
            <i64 as ScyllaDeserializeValue>::deserialize(col.spec.typ(), col.slice)?
        } as u64;

        let mc_seqno = {
            let col = row.next().ok_or_else(|| {
                DeserializationError::new(MissingColumnError("mc_seqno".to_string()))
            })??;
            <i32 as ScyllaDeserializeValue>::deserialize(col.spec.typ(), col.slice)? as u32
        };

        let count = {
            let col = row.next().ok_or_else(|| {
                DeserializationError::new(MissingColumnError("count".to_string()))
            })??;
            <i32 as ScyllaDeserializeValue>::deserialize(col.spec.typ(), col.slice)?
        };

        let last_processed_mc_seqno = {
            let col = row.next().ok_or_else(|| {
                DeserializationError::new(MissingColumnError("last_processed_mc_seqno".to_string()))
            })??;
            <i32 as ScyllaDeserializeValue>::deserialize(col.spec.typ(), col.slice)? as u32
        };

        Ok(Self {
            address,
            tx_lt,
            mc_seqno,
            count,
            last_processed_mc_seqno,
        })
    }

    fn type_check(specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        use scylla::cluster::metadata::NativeType;
        use scylla::deserialize::value::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};

        if specs.len() != OngoingTx::FIELDS_CNT {
            return Err(TypeCheckError::new(BuiltinTypeCheckError {
                rust_name: "OngoingTx",
                cql_type: ColumnType::Native(NativeType::Text),
                kind: BuiltinTypeCheckErrorKind::MismatchedType { expected: &[] },
            }));
        }

        // address: String
        let address_typ = specs[0].typ();
        match address_typ {
            ColumnType::Native(NativeType::Text) => Ok(()),
            _ => Err(TypeCheckError::new(BuiltinTypeCheckError {
                rust_name: "String",
                cql_type: address_typ.clone().into_owned(),
                kind: BuiltinTypeCheckErrorKind::MismatchedType {
                    expected: &[ColumnType::Native(NativeType::Text)],
                },
            })),
        }?;

        // tx_lt: i64
        let tx_lt_typ = specs[1].typ();
        match tx_lt_typ {
            ColumnType::Native(NativeType::BigInt) => Ok(()),
            _ => Err(TypeCheckError::new(BuiltinTypeCheckError {
                rust_name: "i64",
                cql_type: tx_lt_typ.clone().into_owned(),
                kind: BuiltinTypeCheckErrorKind::MismatchedType {
                    expected: &[ColumnType::Native(NativeType::BigInt)],
                },
            })),
        }?;

        // mc_seqno: i32
        let mc_seqno_typ = specs[2].typ();
        match mc_seqno_typ {
            ColumnType::Native(NativeType::Int) => Ok(()),
            _ => Err(TypeCheckError::new(BuiltinTypeCheckError {
                rust_name: "i32",
                cql_type: mc_seqno_typ.clone().into_owned(),
                kind: BuiltinTypeCheckErrorKind::MismatchedType {
                    expected: &[ColumnType::Native(NativeType::Int)],
                },
            })),
        }?;

        // count: i32
        let count_typ = specs[3].typ();
        match count_typ {
            ColumnType::Native(NativeType::Int) => Ok(()),
            _ => Err(TypeCheckError::new(BuiltinTypeCheckError {
                rust_name: "i32",
                cql_type: count_typ.clone().into_owned(),
                kind: BuiltinTypeCheckErrorKind::MismatchedType {
                    expected: &[ColumnType::Native(NativeType::Int)],
                },
            })),
        }?;

        // last_processed_mc_seqno: i32
        let last_processed_mc_seqno_typ = specs[4].typ();
        match last_processed_mc_seqno_typ {
            ColumnType::Native(NativeType::Int) => Ok(()),
            _ => Err(TypeCheckError::new(BuiltinTypeCheckError {
                rust_name: "i32",
                cql_type: last_processed_mc_seqno_typ.clone().into_owned(),
                kind: BuiltinTypeCheckErrorKind::MismatchedType {
                    expected: &[ColumnType::Native(NativeType::Int)],
                },
            })),
        }?;

        Ok(())
    }
}

impl SerializeRow for OngoingTx {
    fn is_empty(&self) -> bool {
        false
    }

    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        use scylla::_macro_internal::ser::row::serialize_column;

        if ctx.columns().len() != OngoingTx::FIELDS_CNT {
            use scylla::serialize::row::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};
            return Err(SerializationError::new(BuiltinTypeCheckError {
                rust_name: "OngoingTx",
                kind: BuiltinTypeCheckErrorKind::WrongColumnCount {
                    rust_cols: OngoingTx::FIELDS_CNT,
                    cql_cols: ctx.columns().len(),
                },
            }));
        }
        let mc_seqno = self.mc_seqno as i32;
        let last_processed_mc_seqno = self.last_processed_mc_seqno as i32;
        let tx_lt = self.tx_lt as i64;

        let fields: [&dyn ScyllaSerializeValue; OngoingTx::FIELDS_CNT] = [
            &self.address,
            &tx_lt,
            &mc_seqno,
            &self.count,
            &last_processed_mc_seqno,
        ];

        for (col, val) in ctx.columns().iter().zip(fields.iter()) {
            serialize_column::<Self>(val, col, writer)?;
        }

        Ok(())
    }
}

#[derive(Clone, Copy)]
pub struct OngoingTxMapper<'a> {
    db_inner: &'a Inner,
}

impl<'a> OngoingTxMapper<'a> {
    const TABLE_NAME: &'static str = "ongoing_tx";

    fn scylla(&self) -> &ScyllaClient {
        &self.db_inner.scylla
    }

    #[named]
    pub async fn insert(self, ongoing_tx: &OngoingTx) -> anyhow::Result<()> {
        let query_str = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            Self::TABLE_NAME,
            OngoingTx::FIELDS,
            "?, ".repeat(OngoingTx::FIELDS_CNT - 1) + "?"
        );
        self.scylla()
            .insert(query_str, ongoing_tx, function_name!())
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn insert_many(self, ongoing_txs: &[OngoingTx]) -> anyhow::Result<()> {
        try_join_all(ongoing_txs.iter().map(|x| self.insert(x))).await?;
        Ok(())
    }

    #[named]
    pub async fn get_opt(self, address: &str, lt: i64) -> anyhow::Result<Option<OngoingTx>> {
        let query_str = format!(
            "SELECT {} FROM {} WHERE address = ? and tx_lt = ?",
            OngoingTx::FIELDS,
            Self::TABLE_NAME
        );
        self.scylla()
            .select_one(query_str, (address, lt), function_name!())
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get(self, address: &str, lt: i64) -> anyhow::Result<OngoingTx> {
        match self.get_opt(address, lt).await? {
            Some(ongoing_tx) => Ok(ongoing_tx),
            None => bail!("OngoingTx not found for {address} and {lt}"),
        }
    }

    #[named]
    pub async fn get_ongoing_txs(self, address: &str) -> anyhow::Result<Vec<OngoingTx>> {
        let query_str = format!(
            "SELECT {} FROM {} WHERE address = ?",
            OngoingTx::FIELDS,
            Self::TABLE_NAME
        );
        self.scylla()
            .select(query_str, (address,), function_name!())
            .await
            .map_err(anyhow::Error::from)
    }
    #[named]
    pub async fn get_ongoing_txs_for_lt(
        self,
        address: &str,
        lt: i64,
    ) -> anyhow::Result<Vec<OngoingTx>> {
        let query_str = format!(
            "SELECT {} FROM {} WHERE address = ? and tx_lt = ?",
            OngoingTx::FIELDS,
            Self::TABLE_NAME
        );
        self.scylla()
            .select(query_str, (address, lt), function_name!())
            .await
            .map_err(anyhow::Error::from)
    }

    #[named]
    pub async fn get_ongoing_txs_from_lt(
        self,
        address: &str,
        lt: i64,
    ) -> anyhow::Result<Vec<OngoingTx>> {
        let query_str = format!(
            "SELECT {} FROM {} WHERE address = ? and tx_lt >= ?",
            OngoingTx::FIELDS,
            Self::TABLE_NAME
        );
        self.scylla()
            .select(query_str, (address, lt), function_name!())
            .await
            .map_err(anyhow::Error::from)
    }

    #[named]
    pub async fn delete(self, address: &str, lt: i64) -> anyhow::Result<()> {
        let query_str = format!(
            "DELETE FROM {} WHERE address = ? and tx_lt = ?",
            Self::TABLE_NAME
        );
        self.scylla()
            .delete(query_str, (address, lt), function_name!())
            .await
            .map_err(anyhow::Error::from)
    }
}

impl<'a> From<&'a DB> for OngoingTxMapper<'a> {
    fn from(db: &'a DB) -> Self {
        Self {
            db_inner: db.inner(),
        }
    }
}
