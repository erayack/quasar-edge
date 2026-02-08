use std::sync::Arc;

use dashmap::{DashMap, DashSet};
use tokio::sync::mpsc;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::types::{QueryCacheKey, ServerMsg};

pub type ConnectionId = Uuid;

#[derive(Clone)]
pub struct Subscriptions {
    senders: Arc<DashMap<ConnectionId, mpsc::Sender<ServerMsg>>>,
    key_to_connections: Arc<DashMap<QueryCacheKey, DashSet<ConnectionId>>>,
    connection_to_keys: Arc<DashMap<ConnectionId, DashSet<QueryCacheKey>>>,
}

impl Default for Subscriptions {
    fn default() -> Self {
        Self::new()
    }
}

impl Subscriptions {
    pub fn new() -> Self {
        Self {
            senders: Arc::new(DashMap::new()),
            key_to_connections: Arc::new(DashMap::new()),
            connection_to_keys: Arc::new(DashMap::new()),
        }
    }

    pub fn register_connection(
        &self,
        connection_id: ConnectionId,
        sender: mpsc::Sender<ServerMsg>,
    ) {
        self.senders.insert(connection_id, sender);
        self.connection_to_keys
            .insert(connection_id, DashSet::new());
        debug!(%connection_id, "connection registered");
    }

    pub fn unregister_connection(&self, connection_id: &ConnectionId) -> Vec<QueryCacheKey> {
        self.senders.remove(connection_id);
        let mut orphaned_keys = Vec::new();

        if let Some((_, keys)) = self.connection_to_keys.remove(connection_id) {
            for key in keys.iter() {
                if let Some(conns) = self.key_to_connections.get(&*key) {
                    conns.remove(connection_id);
                    if conns.is_empty() {
                        drop(conns);
                        self.key_to_connections
                            .remove_if(&*key, |_, v| v.is_empty());
                        orphaned_keys.push(key.clone());
                    }
                }
            }
        }

        debug!(%connection_id, "connection unregistered");
        orphaned_keys
    }

    pub fn subscribe(&self, connection_id: ConnectionId, key: QueryCacheKey) {
        self.key_to_connections
            .entry(key.clone())
            .or_default()
            .insert(connection_id);

        if let Some(keys) = self.connection_to_keys.get(&connection_id) {
            keys.insert(key.clone());
        }

        debug!(%connection_id, query_id = %key.query_id, "subscribed");
    }

    pub fn unsubscribe(&self, connection_id: &ConnectionId, key: &QueryCacheKey) -> bool {
        let mut orphaned = false;

        if let Some(conns) = self.key_to_connections.get(key) {
            conns.remove(connection_id);
            if conns.is_empty() {
                drop(conns);
                self.key_to_connections.remove_if(key, |_, v| v.is_empty());
                orphaned = true;
            }
        }

        if let Some(keys) = self.connection_to_keys.get(connection_id) {
            keys.remove(key);
        }

        debug!(%connection_id, query_id = %key.query_id, "unsubscribed");
        orphaned
    }

    pub fn fan_out_snapshot(&self, key: &QueryCacheKey, msg: &ServerMsg) -> Vec<QueryCacheKey> {
        let conns = match self.key_to_connections.get(key) {
            Some(c) => c,
            None => return Vec::new(),
        };

        let connection_ids: Vec<ConnectionId> = conns.iter().map(|r| *r).collect();
        drop(conns);
        let mut orphaned_keys = Vec::new();

        for conn_id in connection_ids {
            if let Some(sender) = self.senders.get(&conn_id) {
                match sender.try_send(msg.clone()) {
                    Ok(_) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        warn!(%conn_id, "send buffer full, disconnecting");
                        drop(sender);
                        orphaned_keys.extend(self.unregister_connection(&conn_id));
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        debug!(%conn_id, "channel closed, cleaning up");
                        orphaned_keys.extend(self.unregister_connection(&conn_id));
                    }
                }
            }
        }

        orphaned_keys
    }

    pub fn subscribed_keys(&self) -> Vec<QueryCacheKey> {
        self.key_to_connections
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::mpsc;
    use uuid::Uuid;

    use super::Subscriptions;
    use crate::types::{QueryCacheKey, ServerMsg};

    fn key(query_id: &str, auth: &str) -> QueryCacheKey {
        QueryCacheKey {
            query_id: Arc::from(query_id),
            args_canonical: Arc::from("{\"a\":1}"),
            auth: Arc::from(auth),
        }
    }

    #[test]
    fn unsubscribe_reports_orphaned_key() {
        let subs = Subscriptions::new();
        let connection_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::channel::<ServerMsg>(4);
        let cache_key = key("feed", "user-1");

        subs.register_connection(connection_id, tx);
        subs.subscribe(connection_id, cache_key.clone());

        assert!(subs.unsubscribe(&connection_id, &cache_key));
    }

    #[test]
    fn unregister_returns_all_orphaned_keys() {
        let subs = Subscriptions::new();
        let connection_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::channel::<ServerMsg>(4);
        let key_a = key("feed", "user-1");
        let key_b = key("alerts", "user-1");

        subs.register_connection(connection_id, tx);
        subs.subscribe(connection_id, key_a.clone());
        subs.subscribe(connection_id, key_b.clone());

        let orphaned = subs.unregister_connection(&connection_id);
        assert_eq!(orphaned.len(), 2);
        assert!(orphaned.contains(&key_a));
        assert!(orphaned.contains(&key_b));
    }

    #[test]
    fn unsubscribe_not_orphaned_when_other_connections_remain() {
        let subs = Subscriptions::new();
        let connection_a = Uuid::new_v4();
        let connection_b = Uuid::new_v4();
        let (tx_a, _rx_a) = mpsc::channel::<ServerMsg>(4);
        let (tx_b, _rx_b) = mpsc::channel::<ServerMsg>(4);
        let cache_key = key("feed", "user-1");

        subs.register_connection(connection_a, tx_a);
        subs.register_connection(connection_b, tx_b);
        subs.subscribe(connection_a, cache_key.clone());
        subs.subscribe(connection_b, cache_key.clone());

        assert!(!subs.unsubscribe(&connection_a, &cache_key));
    }
}
