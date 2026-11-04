use fastnum::I512;
use num_bigint::{BigInt, BigUint, Sign};
use scylla::_macro_internal::{ColumnType, DeserializationError, TypeCheckError};
use scylla::cluster::metadata::NativeType;
use scylla::deserialize::value::{BuiltinTypeCheckError, BuiltinTypeCheckErrorKind};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::time::Duration;
use ton::ton_core::cell::TonHash;

use crate::metered_cache::MeteredCache;

pub(crate) fn scylla_check_blob_type(
    rust_name: &'static str,
    typ: &ColumnType,
) -> Result<(), TypeCheckError> {
    match typ {
        ColumnType::Native(NativeType::Blob) => Ok(()),
        _ => Err(TypeCheckError::new(BuiltinTypeCheckError {
            rust_name,
            cql_type: typ.clone().into_owned(),
            kind: BuiltinTypeCheckErrorKind::MismatchedType {
                expected: &[ColumnType::Native(NativeType::Blob)],
            },
        })),
    }
}

#[derive(Debug)]
struct TonHashDecodeError {
    bytes: Vec<u8>,
    err: String,
}

impl Display for TonHashDecodeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "can't convert {:?} to TonHash: {}", self.bytes, self.err)
    }
}

impl Error for TonHashDecodeError {}

pub(crate) fn deserialize_ton_hash(bytes: Vec<u8>) -> Result<TonHash, DeserializationError> {
    TonHash::from_slice(&bytes).map_err(|err| {
        DeserializationError::new(TonHashDecodeError {
            bytes,
            err: err.to_string(),
        })
    })
}

pub(crate) fn init_db_cache<K, V>(capacity: u64, name: &'static str) -> MeteredCache<K, V>
where
    K: Eq + Hash + Send + Sync + Clone + 'static,
    V: Clone + Send + Sync + 'static,
{
    let cache_impl = moka::future::Cache::builder()
        .max_capacity(capacity)
        .time_to_idle(Duration::from_secs(30))
        .build();
    MeteredCache::new(name, cache_impl)
}
#[inline]
pub fn bigint_from_i512(val: I512) -> anyhow::Result<BigInt> {
    if val.is_zero() {
        return Ok(BigInt::ZERO);
    }
    let sign = if val < I512::ZERO {
        Sign::Minus
    } else {
        Sign::Plus
    };
    let bytes_le = val.unsigned_abs().to_radix_le(256);
    Ok(BigInt::from_biguint(
        sign,
        BigUint::from_bytes_le(&bytes_le),
    ))
}
