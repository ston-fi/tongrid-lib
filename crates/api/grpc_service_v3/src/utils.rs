use crate::error::GrpcServiceError;
use std::collections::HashMap;
use std::time::Duration;
use ton::block_tlb::ShardAccount;
use ton::ton_core::cell::TonHash;
use ton::ton_core::types::TonAddress;
use tongrid_db::entity::DBTx;
use tongrid_db::{db::DB, entity::TxKeyLookup};
use tongrid_proto::v3::tx_query;
use tongrid_proto::v3::tx_query::Value::{
    AnyHash, InMsgHash, InMsgHashNormalized, QueryId, TxHash,
};
use tonic::Response;

/// Convert a protobuf address to the canonical string used by Scylla keys.
pub(crate) fn normalize_address(address: &str) -> Result<String, GrpcServiceError> {
    address
        .parse::<TonAddress>()
        .map(|address| address.to_string())
        .map_err(|err| GrpcServiceError::InvalidArgument(format!("Invalid address: {err}")))
}

#[derive(Debug)]
pub struct ResponseWithWaitTime<T> {
    pub response: T,
    pub time_waited: Duration,
}

impl<T> ResponseWithWaitTime<T> {
    pub fn into_tonic_response(self) -> Response<T> {
        let mut response = Response::new(self.response);
        response.extensions_mut().insert(self.time_waited);
        response
    }

    pub fn add_wait_time(&mut self, additional: Duration) {
        self.time_waited += additional;
    }
}

pub fn make_tx_key_lookup<'a>(
    tx_query_value: &'a tx_query::Value,
    tx_query_address: Option<&'a str>,
    from_utime: u32,
) -> Result<TxKeyLookup<'a>, GrpcServiceError> {
    let tx_lookup_key = match tx_query_value {
        TxHash(tx_hash) => TxKeyLookup::tx_hash(tx_hash),
        InMsgHash(hash) => TxKeyLookup::in_msg_hash(hash),
        InMsgHashNormalized(hash) => TxKeyLookup::in_msg_hash_norm(hash),
        AnyHash(hash) => TxKeyLookup::any_hash(hash),
        QueryId(query_id) => {
            let address = tx_query_address.ok_or_else(|| {
                GrpcServiceError::InvalidArgument(
                    "tx_query.address is required for query_id lookups".to_string(),
                )
            })?;
            TxKeyLookup::address_query_id(address, *query_id, from_utime)
        }
    };
    Ok(tx_lookup_key)
}

fn filter_tx(
    tx: &DBTx,
    tx_query_address: Option<&str>,
    from_utime: u32,
    mc_seqno: u32,
) -> Result<(), GrpcServiceError> {
    if tx.mc_seqno > mc_seqno {
        return Err(GrpcServiceError::NotFound(format!(
            "Tx not found for query: tx mc_seqno {} > requested mc_seqno {}",
            tx.mc_seqno, mc_seqno
        )));
    }

    if tx.tx_utime < from_utime {
        return Err(GrpcServiceError::NotFound(format!(
            "Tx not found for query: tx_utime {} < from_utime {}",
            tx.tx_utime, from_utime
        )));
    }

    if let Some(address_filter) = tx_query_address {
        if tx.address != address_filter {
            return Err(GrpcServiceError::NotFound(format!(
                "Tx not found for query: tx address {} does not match requested address {}",
                tx.address, address_filter
            )));
        }
    }

    Ok(())
}

pub async fn get_tx_by_query(
    db: &DB,
    tx_query_value: &tx_query::Value,
    tx_query_address: Option<&str>,
    from_utime: u32,
    mc_seqno: u32,
) -> Result<DBTx, GrpcServiceError> {
    let address = tx_query_address
        .filter(|address| !address.is_empty())
        .map(normalize_address)
        .transpose()?;
    let tx_query_address = address.as_deref();
    let lookup_key = make_tx_key_lookup(tx_query_value, tx_query_address, from_utime)?;

    let tx = if let QueryId(_) = tx_query_value {
        let txs = db
            .tx()
            .lookup_many(lookup_key)
            .await
            .map_err(|x| GrpcServiceError::NotFound(x.to_string()))?;

        txs.into_iter()
            .filter(|tx| tx.mc_seqno <= mc_seqno)
            .max_by_key(|tx| (tx.tx_utime, tx.mc_seqno, tx.tx_lt))
            .ok_or_else(|| {
                GrpcServiceError::NotFound(format!("Tx not found for key: {lookup_key:?}"))
            })?
    } else {
        db.tx()
            .lookup(lookup_key)
            .await
            .map_err(|x| GrpcServiceError::NotFound(x.to_string()))?
    };

    filter_tx(&tx, tx_query_address, from_utime, mc_seqno)?;
    Ok(tx)
}

/// Load traits for a single account if traits are required.
/// Returns an empty vector if traits are not required or if no traits are found.
pub async fn load_traits_for_account(
    shard_account: &ShardAccount,
    db: &DB,
    traits_required: bool,
) -> anyhow::Result<Vec<i32>> {
    if !traits_required {
        return Ok(vec![]);
    }

    let code_hash = match shard_account
        .account
        .get_code()
        .map(|x| x.hash())
        .transpose()?
    {
        Some(code_hash) => code_hash,
        None => return Ok(vec![]),
    };

    let traits = match db.tvm_code_traits().get_by_code_hash(code_hash).await? {
        Some(tvm_traits) => tvm_traits
            .traits
            .iter()
            .map(|t| *t as i32)
            .collect::<Vec<i32>>(),
        None => vec![],
    };

    Ok(traits)
}

/// Load trait IDs from a collection of code hashes.
/// Returns a vector of unique trait IDs (as i32) found for the given code hashes.
pub async fn load_trait_ids_from_code_hashes(
    db: &DB,
    code_hashes: &[TonHash],
) -> anyhow::Result<HashMap<TonHash, Vec<i32>>> {
    if code_hashes.is_empty() {
        return Ok(HashMap::new());
    }
    let traits_by_code_hash = db.tvm_code_traits().get_by_code_hashes(code_hashes).await?;
    let traits_int_by_code_hash: HashMap<TonHash, Vec<i32>> = traits_by_code_hash
        .into_iter()
        .map(|(code_hash, tvm_traits)| {
            let trait_ids = tvm_traits
                .traits
                .iter()
                .map(|t| *t as i32)
                .collect::<Vec<i32>>();
            (code_hash, trait_ids)
        })
        .collect();
    Ok(traits_int_by_code_hash)
}
