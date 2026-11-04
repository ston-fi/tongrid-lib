use crate::metered_cache::MeteredCache;
use moka::future::Cache;
use moka::policy::EvictionPolicy;
use std::sync::Arc;
use std::time::Duration;

pub struct DBAccountCache<T> {
    pub(super) cache: MeteredCache<DBAccountCacheKey, Arc<T>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DBAccountCacheKey {
    pub mc_seqno: u32,
    pub address: String,
}

impl DBAccountCacheKey {
    pub fn new<T: ToString>(mc_seqno: u32, address: T) -> Self {
        Self {
            mc_seqno,
            address: address.to_string(),
        }
    }
}

impl<T> DBAccountCache<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub fn new_with_name(capacity: u64, ttl: Duration, name: &'static str) -> Self {
        let cache_impl = Cache::builder()
            .max_capacity(capacity)
            .time_to_live(ttl)
            .eviction_policy(EvictionPolicy::lru())
            .build();
        Self {
            cache: MeteredCache::new(name, cache_impl),
        }
    }

    pub fn new(capacity: u64, ttl: Duration) -> Self {
        Self::new_with_name(capacity, ttl, "DBAccountCache")
    }
}
