use crate::entity::tx::tx_mapper::TX_TABLE_NAME_BY_ADDRESS_MC_SEQNO;
use crate::{entity::tx::tx_key::DBTxKey, paginator::LTPage};
use anyhow::bail;
use futures::{stream::FuturesUnordered, StreamExt};
use std::cmp::min;
use stonfi_scylla_client::client::ScyllaClient;

pub const MAX_LIMIT: i64 = 1000;

#[derive(Debug, Clone, Copy)]
pub enum TxKeyLookup<'a> {
    Hash(&'a [u8]),
    AddressLT {
        address: &'a str,
        lt: u64, // returns tx with lt == lt
    },
    AddressLatest {
        address: &'a str,
        to_mc_seqno: u32, // returns latest tx_key with mc_seqno <= to_mc_seqno
    },
    AddressLTRange {
        address: &'a str,
        // returns tx_keys with key in [from, to]
        from_lt: u64,
        to_lt: u64,
        order_str: &'static str, // "ASC" or "DESC"
        limit: i64,
    },
    AddressUtimeRange {
        address: &'a str,
        // returns tx_keys with key in [from, to]
        from_utime: u32,
        to_utime: u32,
        order_str: &'static str, // "ASC" or "DESC"
        limit: i64,
    },
    AddressMcSeqnoRange {
        address: &'a str,
        // returns tx_keys with key in [from, to]
        from_mc_seqno: u32,
        to_mc_seqno: u32,
        order_str: &'static str, // "ASC" or "DESC"
        limit: i64,
    },
    AddressQueryId {
        address: &'a str,
        query_id: u64,
        from_utime: u32, // returns tx_keys for txs, if tx_out_msg utime >= from_utime
    },
    SeqnosCodeHashes {
        mc_seqnos: &'a [u32],
        code_hashes: &'a [&'a [u8]],
    },
    SeqnosLastCodeHashes {
        mc_seqnos: &'a [u32],
        last_code_hashes: &'a [&'a [u8]],
    },
    InMsgHash(&'a [u8]),
    InMsgHashNormalized(&'a [u8]),
    AnyHash(&'a [u8]),
}

impl<'a> TxKeyLookup<'a> {
    pub fn tx_hash(hash: &'a impl AsRef<[u8]>) -> Self {
        TxKeyLookup::Hash(hash.as_ref())
    }
    pub fn any_hash(hash: &'a impl AsRef<[u8]>) -> Self {
        TxKeyLookup::AnyHash(hash.as_ref())
    }
    pub fn address_lt(address: &'a str, lt: u64) -> Self {
        TxKeyLookup::AddressLT { address, lt }
    }
    pub fn address_latest(address: &'a str, to_mc_seqno: u32) -> Self {
        TxKeyLookup::AddressLatest {
            address,
            to_mc_seqno,
        }
    }
    pub fn address_lt_range(address: &'a str, lt_page: &LTPage) -> Self {
        TxKeyLookup::AddressLTRange {
            address,
            from_lt: lt_page.from_lt,
            to_lt: lt_page.to_lt,
            order_str: if lt_page.order_desc { "DESC" } else { "ASC" },
            limit: min(lt_page.limit, MAX_LIMIT),
        }
    }

    pub fn address_utime_range(
        address: &'a str,
        from_utime: u32,
        to_utime: u32,
        order_desc: bool,
        limit: i64,
    ) -> Self {
        TxKeyLookup::AddressUtimeRange {
            address,
            from_utime,
            to_utime,
            order_str: if order_desc { "DESC" } else { "ASC" },
            limit: min(limit, MAX_LIMIT),
        }
    }

    pub fn address_mc_seqno_range(
        address: &'a str,
        from_mc_seqno: u32,
        to_mc_seqno: u32,
        order_desc: bool,
        limit: i64,
    ) -> Self {
        TxKeyLookup::AddressMcSeqnoRange {
            address,
            from_mc_seqno,
            to_mc_seqno,
            order_str: if order_desc { "DESC" } else { "ASC" },
            limit: min(limit, MAX_LIMIT),
        }
    }

    pub fn address_query_id(address: &'a str, query_id: u64, from_utime: u32) -> Self {
        TxKeyLookup::AddressQueryId {
            address,
            query_id,
            from_utime,
        }
    }
    pub fn in_msg_hash(hash: &'a [u8]) -> Self {
        TxKeyLookup::InMsgHash(hash)
    }

    pub fn in_msg_hash_norm(hash: &'a [u8]) -> Self {
        TxKeyLookup::InMsgHashNormalized(hash)
    }

    pub fn seqnos_code_hashes(mc_seqnos: &'a [u32], code_hashes: &'a [&'a [u8]]) -> Self {
        TxKeyLookup::SeqnosCodeHashes {
            mc_seqnos,
            code_hashes,
        }
    }

    pub fn seqnos_last_code_hashes(mc_seqnos: &'a [u32], last_code_hashes: &'a [&'a [u8]]) -> Self {
        TxKeyLookup::SeqnosLastCodeHashes {
            mc_seqnos,
            last_code_hashes,
        }
    }

    pub(crate) async fn find_key_by_any_hash(
        hash: impl AsRef<[u8]>,
        scylla: &ScyllaClient,
    ) -> anyhow::Result<Option<DBTxKey>> {
        let l1 = TxKeyLookup::tx_hash(&hash);
        let l2 = TxKeyLookup::in_msg_hash(hash.as_ref());
        let l3 = TxKeyLookup::in_msg_hash_norm(hash.as_ref());

        let mut tasks = FuturesUnordered::new();
        tasks.push(l1.find_key(scylla));
        tasks.push(l2.find_key(scylla));
        tasks.push(l3.find_key(scylla));

        let mut last_error = None;

        while let Some(result) = tasks.next().await {
            match result {
                Ok(Some(key)) => return Ok(Some(key)),
                Ok(None) => continue,
                Err(e) => last_error = Some(e),
            }
        }

        if let Some(e) = last_error {
            return Err(e);
        }
        Ok(None)
    }

    pub(crate) async fn find_key(&self, scylla: &ScyllaClient) -> anyhow::Result<Option<DBTxKey>> {
        if let TxKeyLookup::AnyHash(hash) = self {
            return Self::find_key_by_any_hash(hash, scylla).await;
        }

        let table = self.lookup_table()?;
        let query = format!(
            "SELECT {} FROM {table} WHERE {}",
            DBTxKey::FIELDS,
            self.sql_where()?
        );
        let key = match self {
            TxKeyLookup::Hash(hash) => scylla.select_one(query, (hash,), table).await?,
            TxKeyLookup::AddressLT { address, lt } => {
                scylla
                    .select_one(query, (address, *lt as i64), table)
                    .await?
            }
            TxKeyLookup::AddressLatest {
                address,
                to_mc_seqno,
            } => {
                let to_mc_seqno = *to_mc_seqno as i32;
                scylla
                    .select_one(query, (address, to_mc_seqno), table)
                    .await?
            }
            TxKeyLookup::InMsgHash(hash) => scylla.select_one(query, (hash,), table).await?,
            TxKeyLookup::InMsgHashNormalized(hash) => {
                scylla.select_one(query, (hash,), table).await?
            }
            rest => bail!("use 'find_keys' method for filter {rest:?}"),
        };
        Ok(key)
    }

    pub(crate) async fn find_keys(&self, scylla: &ScyllaClient) -> anyhow::Result<Vec<DBTxKey>> {
        let table = self.lookup_table()?;
        let query = format!(
            "SELECT {} FROM {table} WHERE {}",
            DBTxKey::FIELDS,
            self.sql_where()?
        );

        match self {
            TxKeyLookup::AddressLTRange {
                address,
                from_lt,
                to_lt,
                order_str,
                limit,
            } => {
                let final_query = format!("{query} ORDER BY tx_lt {order_str} LIMIT {limit}");
                scylla
                    .select(
                        final_query,
                        (address, *from_lt as i64, *to_lt as i64),
                        table,
                    )
                    .await
                    .map_err(anyhow::Error::from)
            }
            TxKeyLookup::AddressUtimeRange {
                address,
                from_utime,
                to_utime,
                order_str,
                limit,
            } => {
                let final_query = format!("{query} ORDER BY tx_utime {order_str} LIMIT {limit}");
                scylla
                    .select(
                        final_query,
                        (address, *from_utime as i64, *to_utime as i64),
                        table,
                    )
                    .await
                    .map_err(anyhow::Error::from)
            }
            TxKeyLookup::AddressMcSeqnoRange {
                address,
                from_mc_seqno,
                to_mc_seqno,
                order_str,
                limit,
            } => {
                let final_query = format!("{query} ORDER BY mc_seqno {order_str} LIMIT {limit}");
                let from_mc_seqno = *from_mc_seqno as i32;
                let to_mc_seqno = *to_mc_seqno as i32;
                scylla
                    .select(final_query, (address, from_mc_seqno, to_mc_seqno), table)
                    .await
                    .map_err(anyhow::Error::from)
            }
            TxKeyLookup::AddressQueryId {
                address,
                query_id,
                from_utime,
            } => scylla
                .select::<DBTxKey>(
                    query,
                    (address, *query_id as i64, *from_utime as i64),
                    table,
                )
                .await
                .map_err(anyhow::Error::from),
            TxKeyLookup::SeqnosCodeHashes {
                mc_seqnos,
                code_hashes,
            } => {
                let mc_seqnos_i32: Vec<i32> = mc_seqnos.iter().map(|seqno| *seqno as i32).collect();
                let mut tx_keys = scylla
                    .select::<DBTxKey>(query, (code_hashes, &mc_seqnos_i32), table)
                    .await?;
                tx_keys.sort();
                Ok(tx_keys)
            }
            TxKeyLookup::SeqnosLastCodeHashes {
                mc_seqnos,
                last_code_hashes,
            } => {
                let mc_seqnos_i32: Vec<i32> = mc_seqnos.iter().map(|seqno| *seqno as i32).collect();
                let mut tx_keys = scylla
                    .select::<DBTxKey>(query, (last_code_hashes, &mc_seqnos_i32), table)
                    .await?;
                tx_keys.sort();
                Ok(tx_keys)
            }
            rest => bail!("use 'find_key' method for filter {rest:?}"),
        }
    }

    fn lookup_table(&self) -> anyhow::Result<&'static str> {
        let table = match self {
            TxKeyLookup::Hash { .. } => "tx_by_hash",
            TxKeyLookup::AddressLT { .. } => "tx_by_address_lt",
            TxKeyLookup::AddressLatest { .. } => TX_TABLE_NAME_BY_ADDRESS_MC_SEQNO,
            TxKeyLookup::AddressLTRange { .. } => "tx_by_address_lt",
            TxKeyLookup::AddressUtimeRange { .. } => "tx_by_address_utime_order_desc",
            TxKeyLookup::AddressMcSeqnoRange { .. } => TX_TABLE_NAME_BY_ADDRESS_MC_SEQNO,
            TxKeyLookup::AddressQueryId { .. } => "tx_by_out_msg_query_id",
            TxKeyLookup::InMsgHash { .. } => "tx_by_in_msg",
            TxKeyLookup::InMsgHashNormalized { .. } => "tx_by_in_msg_normalized",
            TxKeyLookup::SeqnosCodeHashes { .. } => "account_by_code_hash",
            TxKeyLookup::SeqnosLastCodeHashes { .. } => "account_by_last_code_hash",
            TxKeyLookup::AnyHash(_) => bail!("AnyHash should be handled separately"),
        };
        Ok(table)
    }

    // Keep the lookup variants aligned with their CQL predicates for schema review.
    #[rustfmt::skip]
    fn sql_where(&self) -> anyhow::Result<&'static str> {
        let sql_where = match self {
            TxKeyLookup::Hash { .. } => "tx_hash = ?",
            TxKeyLookup::AddressLT { .. } => "address = ? AND tx_lt = ?",
            TxKeyLookup::AddressLatest { .. } => "address = ? AND mc_seqno <= ? ORDER BY mc_seqno DESC, tx_lt DESC LIMIT 1",
            TxKeyLookup::AddressLTRange { .. } => "address = ? AND tx_lt >= ? AND tx_lt <= ?",
            TxKeyLookup::AddressUtimeRange { .. } => "address = ? AND tx_utime >= ? and tx_utime <= ?",
            TxKeyLookup::AddressMcSeqnoRange { .. } => "address = ? AND mc_seqno >= ? and mc_seqno <= ?",
            TxKeyLookup::AddressQueryId { .. } => "address = ? AND query_id = ? AND tx_utime >= ?",
            TxKeyLookup::InMsgHash { .. } => "in_msg_hash = ? ORDER BY mc_seqno DESC LIMIT 1",
            TxKeyLookup::InMsgHashNormalized { .. } => "in_msg_hash_normalized = ? ORDER BY mc_seqno DESC LIMIT 1",
            TxKeyLookup::SeqnosCodeHashes { .. } => "code_hash IN ? AND mc_seqno IN ?",
            TxKeyLookup::SeqnosLastCodeHashes { .. } => "last_code_hash IN ? AND mc_seqno IN ?",
            TxKeyLookup::AnyHash(_) => bail!("AnyHash should be handled separately"),
        };
        Ok(sql_where)
    }
}
