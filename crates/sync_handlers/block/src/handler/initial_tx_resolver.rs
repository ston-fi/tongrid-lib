use anyhow::anyhow;
use anyhow::bail;
use futures::future::try_join_all;
use moka::future::Cache;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;
use ton::block_tlb::Msg;
use ton::ton_core::cell::TonCell;
use ton::ton_core::traits::tlb::TLB;
use tongrid_db::entity::scylla_types::DBMsgKey;
use tongrid_db::entity::tx::tx_key::{DBTxKey, DB_TX_KEY_ZERO};
use tongrid_db::entity::DBTx;
use tongrid_tda::ton_db_adapter::TonDBAdapter;

use super::metered_cache::MeteredCache;

const SYSTEM_CONTRACT_ADDRESS: &str = "Ef8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADAU";
const INITIAL_TX_CACHE_SIZE: u64 = 10_000;

pub struct InititalTxResolver {
    initial_tx_cache: MeteredCache<DBMsgKey, Arc<DBTxKey>>,
}

struct TxResolverCollections {
    pub known_initial_txs: HashMap<DBMsgKey, Arc<DBTxKey>>,
    pub unknown_initial_txs: HashMap<DBMsgKey, Arc<DBMsgKey>>,
    pub txs_to_fetch: HashSet<Arc<DBMsgKey>>,
}

impl InititalTxResolver {
    pub(super) fn new() -> InititalTxResolver {
        InititalTxResolver {
            initial_tx_cache: MeteredCache::new(
                "initial_tx",
                Cache::builder().max_capacity(INITIAL_TX_CACHE_SIZE).build(),
            ),
        }
    }

    pub async fn fill_initial_txs(
        &self,
        txs: &mut [DBTx],
        tda: &TonDBAdapter,
    ) -> anyhow::Result<()> {
        let mut tx_resolver_collections = TxResolverCollections {
            known_initial_txs: HashMap::new(),
            unknown_initial_txs: HashMap::new(),
            txs_to_fetch: HashSet::new(),
        };

        for tx in txs.iter_mut() {
            self.fill_initial_mapping_or_initial_tx(tx, &mut tx_resolver_collections)
                .await?;
        }
        let futs = tx_resolver_collections
            .txs_to_fetch
            .into_iter()
            .map(|tx_in_msg| async move {
                let init_tx = self.get_initial_tx_from_db(tda, &tx_in_msg).await?;
                Ok::<_, anyhow::Error>((tx_in_msg, init_tx))
            });
        let resolved: HashMap<_, _> = try_join_all(futs).await?.into_iter().collect();

        for tx in txs.iter_mut() {
            if !tx.initial_tx.is_zero() {
                continue;
            }

            let msg = tx
                .in_msg
                .as_ref()
                .ok_or_else(|| anyhow!("in_msg should be set for non-initial tx"))?;
            let msg_key = msg.key();

            if let Some(original_in_msg) = tx_resolver_collections.unknown_initial_txs.get(&msg_key)
            {
                if let Some(initial_tx) = resolved.get(original_in_msg) {
                    tx.initial_tx = initial_tx.clone();
                } else {
                    log::warn!("Couldn't get initial tx from db with msg_key={:?}", msg_key);
                }
            } else {
                log::warn!("Initial tx not found by msg_key={:?}", msg_key);
            }
            let initial_tx = Arc::new(tx.initial_tx.clone());
            for out_msg in &tx.out_msgs {
                self.initial_tx_cache
                    .insert(out_msg.key(), initial_tx.clone())
                    .await;
            }
        }

        Ok(())
    }

    async fn fill_initial_mapping_or_initial_tx(
        &self,
        tx: &mut DBTx,
        tx_resolver_collections: &mut TxResolverCollections,
    ) -> anyhow::Result<()> {
        if is_initial_tx(tx)? {
            let tx_key = DBTxKey {
                mc_seqno: tx.mc_seqno,
                address: tx.address.clone(),
                tx_lt: tx.tx_lt,
            };
            tx.initial_tx = tx_key.clone();
            let tx_key_arc = Arc::new(tx_key);
            for out_msg in &tx.out_msgs {
                tx_resolver_collections
                    .known_initial_txs
                    .insert(out_msg.key(), tx_key_arc.clone());
            }
            return Ok(());
        }

        let msg = tx
            .in_msg
            .as_ref()
            .ok_or_else(|| anyhow!("in_msg should be set for non-initial tx"))?;
        let msg_key = msg.key();

        // First, we check "in_block" cache
        if let Some(known_tx_key) = tx_resolver_collections
            .known_initial_txs
            .get(&msg_key)
            .cloned()
        {
            tx.initial_tx = known_tx_key.deref().clone();

            for out_msg in &tx.out_msgs {
                tx_resolver_collections
                    .known_initial_txs
                    .insert(out_msg.key(), known_tx_key.clone());
            }

            return Ok(());
        }

        // Second, we check "all_blocks" cache
        if let Some(known_tx_key) = self.initial_tx_cache.get(&msg_key).await {
            tx.initial_tx = known_tx_key.deref().clone();

            for out_msg in &tx.out_msgs {
                tx_resolver_collections
                    .known_initial_txs
                    .insert(out_msg.key(), known_tx_key.clone());
            }

            return Ok(());
        }

        // Then, adding to queue to retrieve it after from db
        if let Some(demanded_init_tx) = tx_resolver_collections
            .unknown_initial_txs
            .get(&msg_key)
            .cloned()
        {
            for out_msg in &tx.out_msgs {
                tx_resolver_collections
                    .unknown_initial_txs
                    .insert(out_msg.key(), demanded_init_tx.clone());
            }

            return Ok(());
        }

        let msg_key_arc = Arc::new(msg_key.clone());
        for out_msg in &tx.out_msgs {
            tx_resolver_collections
                .unknown_initial_txs
                .insert(out_msg.key(), msg_key_arc.clone());
        }
        tx_resolver_collections
            .txs_to_fetch
            .insert(msg_key_arc.clone());
        tx_resolver_collections
            .unknown_initial_txs
            .insert(msg_key, msg_key_arc); // to unify initial_tx insertion algorithm

        Ok(())
    }

    pub async fn get_initial_tx_from_db(
        &self,
        tda: &TonDBAdapter,
        in_msg_key: &DBMsgKey,
    ) -> anyhow::Result<DBTxKey> {
        let prev_tx_key = match tda
            .db()
            .msg_txs()
            .get(&in_msg_key.src, &in_msg_key.dst, in_msg_key.created_lt)
            .await?
            .and_then(|msg_tx| msg_tx.src_tx_key())
        {
            Some(t) => t,
            None => {
                let initial_seqno = tda.db().progress().load_initial_height().await?;
                let warn_msg = format!(
                    "tx not found for msg with src={} dst={} lt={}",
                    in_msg_key.src, in_msg_key.dst, in_msg_key.created_lt,
                );
                if initial_seqno.is_some_and(|seq| seq == 0) {
                    // With initial height zero, a missing predecessor is an error rather than an unknown root.
                    bail!(warn_msg);
                }

                log::warn!("{warn_msg}");
                return Ok(DB_TX_KEY_ZERO.clone());
            }
        };

        let prev_tx = tda.db().tx_lite().get(&prev_tx_key).await?;

        // A NULL initial_tx in the lite row maps to the unknown-root sentinel, not to the predecessor itself.
        // TODO: remove this fallback only after stored NULL roots are backfilled and DBTxLite is non-optional.
        match prev_tx.initial_tx {
            None => Ok(DB_TX_KEY_ZERO.clone()),
            Some(tx_key) => Ok(tx_key),
        }
    }
}

pub fn is_initial_tx(tx: &DBTx) -> anyhow::Result<bool> {
    let in_msg: Msg<TonCell> = match &tx.in_msg {
        Some(msg) => Msg::from_boc(msg.boc_stripped.to_vec())?,
        None => return Ok(true),
    };

    if in_msg.info.as_ext_in().is_some() {
        return Ok(true);
    }

    // Unbounced system-contract messages at the protocol LT boundary begin a transaction tree.
    if in_msg.info.as_int().is_some_and(|x| !x.bounced)
        && tx
            .in_msg
            .as_ref()
            .is_some_and(|msg| msg.src == SYSTEM_CONTRACT_ADDRESS)
        && tx
            .in_msg
            .as_ref()
            .is_some_and(|msg| msg.created_lt.is_multiple_of(1_000_000))
    {
        return Ok(true);
    }

    Ok(false)
}
