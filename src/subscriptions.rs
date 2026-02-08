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

    pub fn unregister_connection(&self, connection_id: &ConnectionId) {
        self.senders.remove(connection_id);

        if let Some((_, keys)) = self.connection_to_keys.remove(connection_id) {
            for key in keys.iter() {
                if let Some(conns) = self.key_to_connections.get(&*key) {
                    conns.remove(connection_id);
                    if conns.is_empty() {
                        drop(conns);
                        self.key_to_connections
                            .remove_if(&*key, |_, v| v.is_empty());
                    }
                }
            }
        }

        debug!(%connection_id, "connection unregistered");
    }

    pub fn subscribe(&self, connection_id: ConnectionId, key: QueryCacheKey) {
        self.key_to_connections
            .entry(key.clone())
            .or_insert_with(DashSet::new)
            .insert(connection_id);

        if let Some(keys) = self.connection_to_keys.get(&connection_id) {
            keys.insert(key.clone());
        }

        debug!(%connection_id, query_id = %key.query_id, "subscribed");
    }

    pub fn unsubscribe(&self, connection_id: &ConnectionId, key: &QueryCacheKey) {
        if let Some(conns) = self.key_to_connections.get(key) {
            conns.remove(connection_id);
            if conns.is_empty() {
                drop(conns);
                self.key_to_connections.remove_if(key, |_, v| v.is_empty());
            }
        }

        if let Some(keys) = self.connection_to_keys.get(connection_id) {
            keys.remove(key);
        }

        debug!(%connection_id, query_id = %key.query_id, "unsubscribed");
    }

    pub fn fan_out_snapshot(&self, key: &QueryCacheKey, msg: &ServerMsg) {
        let conns = match self.key_to_connections.get(key) {
            Some(c) => c,
            None => return,
        };

        let connection_ids: Vec<ConnectionId> = conns.iter().map(|r| *r).collect();
        drop(conns);

        for conn_id in connection_ids {
            if let Some(sender) = self.senders.get(&conn_id) {
                match sender.try_send(msg.clone()) {
                    Ok(_) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        warn!(%conn_id, "send buffer full, disconnecting");
                        drop(sender);
                        self.unregister_connection(&conn_id);
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        debug!(%conn_id, "channel closed, cleaning up");
                        self.unregister_connection(&conn_id);
                    }
                }
            }
        }
    }

    pub fn subscribed_keys(&self) -> Vec<QueryCacheKey> {
        self.key_to_connections
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }
}
