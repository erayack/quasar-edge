use std::sync::Arc;

use dashmap::{DashMap, DashSet};
use tokio::sync::{Mutex, RwLock};

use crate::types::{CacheEntry, QueryCacheKey};

#[derive(Clone)]
pub struct AuthScopedCache {
    primary: Arc<DashMap<QueryCacheKey, Arc<RwLock<CacheEntry>>>>,
    query_index: Arc<DashMap<Arc<str>, DashSet<QueryCacheKey>>>,
    refresh_locks: Arc<DashMap<QueryCacheKey, Arc<Mutex<()>>>>,
}

impl Default for AuthScopedCache {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthScopedCache {
    pub fn new() -> Self {
        Self {
            primary: Arc::new(DashMap::new()),
            query_index: Arc::new(DashMap::new()),
            refresh_locks: Arc::new(DashMap::new()),
        }
    }

    pub async fn get(&self, key: &QueryCacheKey) -> Option<CacheEntry> {
        let entry = self.primary.get(key)?;
        let guard = entry.value().read().await;
        Some(guard.clone())
    }

    pub async fn put_atomic(&self, key: QueryCacheKey, entry: CacheEntry) {
        match self.primary.get(&key) {
            Some(existing) => {
                let mut guard = existing.value().write().await;
                *guard = entry;
            }
            None => {
                self.primary
                    .insert(key.clone(), Arc::new(RwLock::new(entry)));
                self.query_index
                    .entry(key.query_id.clone())
                    .or_default()
                    .insert(key);
            }
        }
    }

    pub fn mark_stale_by_query_id(&self, query_id: &str) -> Vec<QueryCacheKey> {
        let keys_ref = match self.query_index.get(query_id) {
            Some(r) => r,
            None => return Vec::new(),
        };

        let mut stale_keys = Vec::with_capacity(keys_ref.len());

        for key_ref in keys_ref.iter() {
            let key = key_ref.key().clone();
            if let Some(entry) = self.primary.get(&key) {
                if let Ok(mut guard) = entry.value().try_write() {
                    guard.stale = true;
                }
            }
            stale_keys.push(key);
        }

        stale_keys
    }

    pub async fn with_refresh_lock<F, Fut, T, E>(
        &self,
        key: &QueryCacheKey,
        f: F,
    ) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        let lock = self
            .refresh_locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .value()
            .clone();

        let _guard = lock.lock().await;
        f().await
    }

    pub fn remove(&self, key: &QueryCacheKey) {
        self.primary.remove(key);
        if let Some(set) = self.query_index.get(&key.query_id) {
            set.remove(key);
            if set.is_empty() {
                drop(set);
                self.query_index.remove(&key.query_id);
            }
        }
        self.refresh_locks.remove(key);
    }

    pub fn subscribed_keys(&self) -> Vec<QueryCacheKey> {
        self.primary.iter().map(|r| r.key().clone()).collect()
    }
}
