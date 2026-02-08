use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type AuthContextKey = Arc<str>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryCacheKey {
    pub query_id: Arc<str>,
    pub args_canonical: Arc<str>,
    pub auth: AuthContextKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvalidationEvent {
    pub version: u64,
    pub affected_query_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    pub result: Value,
    pub last_mutation_version: u64,
    pub stale: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMsg {
    Subscribe { query_id: String, args: Value },
    Unsubscribe { query_id: String, args: Value },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMsg {
    Snapshot { query_id: String, payload: Value },
    Error { message: String },
}
