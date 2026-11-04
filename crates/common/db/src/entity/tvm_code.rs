use crate::db::{Inner, DB};
use crate::metered_cache::MeteredCache;
use crate::util::{deserialize_ton_hash, scylla_check_blob_type};
use crate::{
    scylla_deserialize_val, scylla_typecheck_deserialize_len_check_end_return,
    scylla_typecheck_native, scylla_typecheck_serialize_columns_len,
};
use function_name::named;
use futures::future::try_join_all;
use scylla::_macro_internal::{
    ColumnIterator, ColumnSpec, ColumnType, DeserializationError, SerializationError,
    TypeCheckError,
};
use scylla::deserialize::row::DeserializeRow;
use scylla::deserialize::value::DeserializeValue;
use scylla::serialize::row::{RowSerializationContext, SerializeRow};
use scylla::serialize::value::SerializeValue;
use scylla::serialize::writers::RowWriter;
use std::collections::HashMap;
use std::fmt;
use stonfi_scylla_client::client::ScyllaClient;
use ton::ton_core::cell::TonHash;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TvmCode {
    pub code_hash: TonHash,
    pub boc: Vec<u8>,
    pub lib_ids: Vec<TonHash>,
}

#[derive(Debug)]
struct MissingColumnError(String);

impl fmt::Display for MissingColumnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Missing column: {}", self.0)
    }
}

impl std::error::Error for MissingColumnError {}

impl TvmCode {
    pub const CACHE_NAME: &'static str = "DbTvmCode";
    pub fn new<H, I>(hash: H, boc: Vec<u8>, lib_ids: I) -> Self
    where
        H: Into<TonHash>,
        I: IntoIterator<Item = H>,
    {
        Self {
            code_hash: hash.into(),
            boc,
            lib_ids: lib_ids.into_iter().map(|x| x.into()).collect(),
        }
    }
}

impl<'frame, 'metadata> DeserializeRow<'frame, 'metadata> for TvmCode {
    fn type_check(specs: &[ColumnSpec]) -> Result<(), TypeCheckError> {
        use scylla::cluster::metadata::NativeType;
        use scylla::deserialize::value::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};

        scylla_typecheck_deserialize_len_check_end_return!(specs, 3, "TvmCode");
        scylla_check_blob_type("TonHash", specs[0].typ())?;
        scylla_typecheck_native!(specs, boc_typ, 1, "Vec<u8>", NativeType::Blob);
        Vec::<Vec<u8>>::type_check(specs[2].typ())?;
        Ok(())
    }

    fn deserialize(
        mut row: ColumnIterator<'frame, 'metadata>,
    ) -> Result<Self, DeserializationError> {
        let code_hash = deserialize_ton_hash(scylla_deserialize_val!(row, code_hash, Vec<u8>))?;
        let boc = scylla_deserialize_val!(row, boc, Vec<u8>);
        let lib_ids = scylla_deserialize_val!(row, lib_ids, Vec<Vec<u8>>)
            .into_iter()
            .map(deserialize_ton_hash)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            code_hash,
            boc,
            lib_ids,
        })
    }
}

impl SerializeRow for TvmCode {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter<'_>,
    ) -> Result<(), SerializationError> {
        use scylla::_macro_internal::ser::row::serialize_column;

        scylla_typecheck_serialize_columns_len!(ctx, 3, "TvmCode");

        let code_hash = self.code_hash.as_slice();
        let lib_ids = self
            .lib_ids
            .iter()
            .map(|hash| hash.to_vec())
            .collect::<Vec<_>>();
        let fields: [&dyn SerializeValue; 3] = [&code_hash, &self.boc, &lib_ids];

        for (col, val) in ctx.columns().iter().zip(fields.iter()) {
            serialize_column::<Self>(val, col, writer)?;
        }

        Ok(())
    }

    fn is_empty(&self) -> bool {
        false
    }
}

#[derive(Clone, Copy)]
pub struct TvmCodeMapper<'a> {
    db_inner: &'a Inner,
}

impl<'a> TvmCodeMapper<'a> {
    const TABLE_NAME: &'static str = "tvm_code";

    fn scylla(&self) -> &ScyllaClient {
        &self.db_inner.scylla
    }

    fn cache(&self) -> &MeteredCache<TonHash, TvmCode> {
        &self.db_inner.tvm_code_cache
    }

    pub async fn get_opt(self, hash: &TonHash) -> anyhow::Result<Option<TvmCode>> {
        self.cache()
            .get_or_init_option(hash, self.get_by_hash(hash))
            .await
    }

    pub async fn get(self, hash: &TonHash) -> anyhow::Result<TvmCode> {
        self.get_opt(hash)
            .await?
            .ok_or_else(|| anyhow::anyhow!("TvmCode not found for hash: {}", hash))
    }

    #[named]
    async fn insert_db(self, tvm_code: &TvmCode) -> anyhow::Result<()> {
        let query_str = format!(
            "INSERT INTO {} (code_hash, boc, lib_ids) VALUES (?, ?, ?)",
            Self::TABLE_NAME,
        );
        self.scylla()
            .insert(query_str, tvm_code, function_name!())
            .await
            .map_err(anyhow::Error::from)
    }

    pub async fn get_by_hashes<'b, I>(self, hashes: I) -> anyhow::Result<HashMap<TonHash, TvmCode>>
    where
        I: IntoIterator<Item = &'b TonHash>,
    {
        let futures = hashes
            .into_iter()
            .map(|x| async move { self.get_opt(x).await });
        let tvm_codes = try_join_all(futures)
            .await?
            .into_iter()
            .flatten()
            .map(|x| (x.code_hash, x))
            .collect();

        Ok(tvm_codes)
    }

    pub async fn insert(self, codes: impl IntoIterator<Item = TvmCode>) -> anyhow::Result<()> {
        let futs = codes.into_iter().map(|x| async {
            if !self.cache().contains(&x.code_hash) {
                // insert to db first
                self.insert_db(&x).await?;
                self.cache().insert(x.code_hash, x).await;
            }
            Ok::<(), anyhow::Error>(())
        });
        try_join_all(futs).await?;
        Ok(())
    }

    #[named]
    async fn get_by_hash(self, hash: &TonHash) -> anyhow::Result<Option<TvmCode>> {
        let query_str = format!(
            "SELECT code_hash, boc, lib_ids FROM {} WHERE code_hash = ?",
            Self::TABLE_NAME
        );
        self.scylla()
            .select_one(query_str, (hash.as_slice(),), function_name!())
            .await
            .map_err(anyhow::Error::from)
    }
}

impl<'a> From<&'a DB> for TvmCodeMapper<'a> {
    fn from(db: &'a DB) -> Self {
        Self {
            db_inner: db.inner(),
        }
    }
}
