use std::marker::PhantomData;

use crate::db::DB;
use crate::entity::tx::tx_key_lookup::MAX_LIMIT;
use crate::entity::TxKeyLookup;
use anyhow::{bail, Result};
use async_trait::async_trait;
use tokio::try_join;
use tongrid_proto::v3::{tx_pagination_params, LtRange, TxPaginationParams};

#[async_trait]
pub trait LTPageLoader: Send + Sync {
    type EntityWithLT;
    /// result must be sorted by LT according to page.order_desc param
    async fn load_page_sorted(
        db: &DB,
        address: &str,
        page: &LTPage,
    ) -> Result<Vec<Self::EntityWithLT>>;
    fn read_lt(entity: &Self::EntityWithLT) -> u64;
}

pub struct LTPaginator<T: LTPageLoader> {
    // None if provided in PaginationParams Range doesn't contain any txs
    cur_page: Option<LTPage>,
    next_page: Option<LTPage>,
    db: DB,
    address: String,
    phantom_data: PhantomData<T>,
}

/// borders included
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct LTPage {
    pub from_lt: u64,
    pub to_lt: u64,
    pub limit: i64,
    pub order_desc: bool,
}

impl Default for LTPage {
    fn default() -> Self {
        LTPage {
            from_lt: 0,
            to_lt: i64::MAX as u64,
            limit: MAX_LIMIT,
            order_desc: false,
        }
    }
}

impl LTPage {
    pub fn to_pagination_params(self) -> TxPaginationParams {
        TxPaginationParams {
            limit: self.limit as u64,
            order_desc: Some(self.order_desc),
            range: Some(tx_pagination_params::Range::LtRange(LtRange {
                from_lt: self.from_lt,
                to_lt: self.to_lt,
            })),
        }
    }
}

fn normalize_page_limit(limit: u64) -> i64 {
    limit.clamp(1, MAX_LIMIT as u64) as i64
}

impl<T: LTPageLoader> LTPaginator<T> {
    pub async fn new(
        db: DB,
        address: String,
        wait_mc_seqno: u32,
        proto_page_params: Option<&TxPaginationParams>,
    ) -> Result<Self> {
        let Some(proto_page_params) = proto_page_params else {
            return Ok(Self {
                cur_page: Some(LTPage::default()),
                next_page: Some(LTPage::default()),
                db,
                address,
                phantom_data: Default::default(),
            });
        };

        let wait_mc_seqno_lookup = TxKeyLookup::address_latest(&address, wait_mc_seqno);
        let wait_mc_seqno_tx = db.tx_lite().lookup(wait_mc_seqno_lookup).await?;

        let range = proto_page_params
            .range
            .unwrap_or(tx_pagination_params::Range::LtRange(LtRange {
                from_lt: 0,
                to_lt: i64::MAX as u64,
            }));

        let maybe_lt_range = match range {
            tx_pagination_params::Range::LtRange(lt_range) => {
                let (from_lt, to_lt): (i64, i64) = lt_range.into();
                Some((from_lt as u64, (to_lt as u64).min(wait_mc_seqno_tx.tx_lt)))
            }
            tx_pagination_params::Range::UtimeRange(utime_range) => {
                let from = utime_range.from_utime;
                let to = utime_range.to_utime;
                let to_checked = to.min(wait_mc_seqno_tx.tx_utime);
                let lookup_first =
                    TxKeyLookup::address_utime_range(&address, from, to_checked, false, 1);
                let lookup_last =
                    TxKeyLookup::address_utime_range(&address, from, to_checked, true, 1);
                resolve_lt_range(&db, lookup_first, lookup_last).await?
            }
            tx_pagination_params::Range::McSeqnoRange(mc_seqno_range) => {
                let (from, to) = mc_seqno_range.clamp_to_i32_supported_range();
                let to_checked = to.min(wait_mc_seqno_tx.mc_seqno);
                let lookup_first =
                    TxKeyLookup::address_mc_seqno_range(&address, from, to_checked, false, 1);
                let lookup_last =
                    TxKeyLookup::address_mc_seqno_range(&address, from, to_checked, true, 1);
                resolve_lt_range(&db, lookup_first, lookup_last).await?
            }
        };
        let cur_page = maybe_lt_range.map(|(from_lt, to_lt)| LTPage {
            from_lt,
            to_lt,
            limit: normalize_page_limit(proto_page_params.limit),
            order_desc: proto_page_params.order_desc.unwrap_or_default(),
        });

        Ok(Self {
            db,
            address,
            cur_page,
            next_page: cur_page,
            phantom_data: Default::default(),
        })
    }

    /// Second call will load the same page again
    /// Results are sorted by lt according to page.order_desc param
    pub async fn load_page(&mut self) -> Result<Vec<T::EntityWithLT>> {
        let Some(cur_page) = &self.cur_page else {
            return Ok(vec![]);
        };

        let result = T::load_page_sorted(&self.db, &self.address, cur_page).await?;
        if result.is_empty() {
            self.next_page = None;
            return Ok(result);
        }

        let Some(last) = result.last() else {
            self.next_page = None;
            return Ok(result);
        };
        let last_lt = T::read_lt(last);

        let mut next_page = *cur_page;

        if cur_page.order_desc {
            let Some(next_to) = last_lt.checked_sub(1) else {
                self.next_page = None;
                return Ok(result);
            };
            next_page.to_lt = next_to;
        } else {
            let Some(next_from) = last_lt.checked_add(1) else {
                self.next_page = None;
                return Ok(result);
            };
            next_page.from_lt = next_from;
        }
        self.next_page = if next_page.from_lt > next_page.to_lt {
            None
        } else {
            Some(next_page)
        };

        Ok(result)
    }

    pub fn next_page_proto(&self) -> Option<TxPaginationParams> {
        self.next_page.map(|x| x.to_pagination_params())
    }
}

async fn resolve_lt_range<'a>(
    db: &DB,
    from: TxKeyLookup<'a>,
    to: TxKeyLookup<'a>,
) -> Result<Option<(u64, u64)>> {
    let (from_txs, to_txs) =
        try_join!(db.tx_lite().lookup_keys(from), db.tx_lite().lookup_keys(to),)?;

    if from_txs.is_empty() && to_txs.is_empty() {
        return Ok(None); // No txs in this range
    }
    if !from_txs.is_empty() && !to_txs.is_empty() {
        return Ok(Some((from_txs[0].tx_lt, to_txs[0].tx_lt)));
    }
    bail!(
        "[resolve_lt] from_txs.len() = {}, to_txs.len() = {}. Inconsistent state!",
        from_txs.len(),
        to_txs.len()
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use tongrid_proto::v3::{tx_pagination_params, LtRange, McSeqnoRange, TxPaginationParams};

    #[test]
    fn lt_page_serializes_as_v3_pagination() {
        let page = LTPage {
            from_lt: 10,
            to_lt: 20,
            limit: 5,
            order_desc: true,
        };

        assert_eq!(
            page.to_pagination_params(),
            TxPaginationParams {
                range: Some(tx_pagination_params::Range::LtRange(LtRange {
                    from_lt: 10,
                    to_lt: 20
                })),
                limit: 5,
                order_desc: Some(true),
            }
        );
    }

    #[test]
    fn default_page_preserves_existing_bounds() {
        assert_eq!(
            LTPage::default().to_pagination_params(),
            TxPaginationParams {
                range: Some(tx_pagination_params::Range::LtRange(LtRange {
                    from_lt: 0,
                    to_lt: i64::MAX as u64,
                })),
                limit: MAX_LIMIT as u64,
                order_desc: Some(false),
            }
        );

        let (from_lt, to_lt): (i64, i64) = LtRange {
            from_lt: u64::MAX,
            to_lt: u64::MAX,
        }
        .into();
        assert_eq!((from_lt, to_lt), (i64::MAX, i64::MAX));

        let mc_range = McSeqnoRange {
            from_mc_seqno: u32::MAX,
            to_mc_seqno: u32::MAX,
        };
        assert_eq!(
            mc_range.clamp_to_i32_supported_range(),
            (i32::MAX as u32, i32::MAX as u32)
        );
    }

    #[test]
    fn provided_page_limit_keeps_existing_clamp() {
        assert_eq!(normalize_page_limit(0), 1);
        assert_eq!(normalize_page_limit(MAX_LIMIT as u64), MAX_LIMIT);
        assert_eq!(normalize_page_limit(MAX_LIMIT as u64 + 1), MAX_LIMIT);
    }
}
