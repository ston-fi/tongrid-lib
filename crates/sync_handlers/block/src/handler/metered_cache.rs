use moka::future::Cache;
use std::future::Future;
use std::hash::Hash;
use stonfi_metrics::CacheStatsMetric;

pub(super) struct MeteredCache<K, V> {
    cache_name: &'static str,
    inner: Cache<K, V>,
}

impl<K, V> MeteredCache<K, V>
where
    K: Hash + Eq + Send + Sync + Clone + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub(super) fn new(cache_name: &'static str, inner: Cache<K, V>) -> Self {
        Self { cache_name, inner }
    }

    pub(super) async fn get(&self, key: &K) -> Option<V> {
        CacheStatsMetric::inc_request(self.cache_name);

        let value = self.inner.get(key).await;
        if value.is_none() {
            CacheStatsMetric::inc_miss(self.cache_name);
        }
        value
    }

    pub(super) async fn get_or_init_option<F>(&self, key: &K, init: F) -> anyhow::Result<Option<V>>
    where
        F: Future<Output = anyhow::Result<Option<V>>>,
    {
        CacheStatsMetric::inc_request(self.cache_name);

        if let Some(value) = self.inner.get(key).await {
            return Ok(Some(value));
        }

        CacheStatsMetric::inc_miss(self.cache_name);
        match init.await? {
            None => Ok(None),
            Some(value) => {
                self.inner.insert(key.clone(), value.clone()).await;
                Ok(Some(value))
            }
        }
    }

    pub(super) async fn insert(&self, key: K, value: V) {
        self.inner.insert(key, value).await;
    }
}
