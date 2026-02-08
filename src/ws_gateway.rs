use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::IntoResponse,
};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    auth,
    core_client::CoreError,
    subscriptions::ConnectionId,
    types::{AuthContextKey, CacheEntry, ClientInitMsg, ClientMsg, QueryCacheKey, ServerMsg},
    AppState,
};

#[derive(Debug, thiserror::Error)]
enum WsGatewayError {
    #[error("invalid init message: {0}")]
    InvalidInitMessage(#[from] serde_json::Error),
    #[error("invalid token value")]
    InvalidTokenValue,
    #[error("auth failed: {0}")]
    Auth(#[from] auth::AuthError),
    #[error("args serialization failed: {0}")]
    ArgsSerialization(serde_json::Error),
    #[error("invalid canonical args: {0}")]
    InvalidCanonicalArgs(serde_json::Error),
    #[error("semaphore closed: {0}")]
    SemaphoreClosed(String),
    #[error("core fetch error: {0}")]
    CoreFetch(#[from] CoreError),
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_connection(socket, state))
}

async fn handle_connection(socket: WebSocket, state: Arc<AppState>) {
    let (mut ws_sink, mut ws_stream) = socket.split();

    let first_msg = match ws_stream.next().await {
        Some(Ok(Message::Text(text))) => text,
        _ => {
            let text = serialize_server_msg(&ServerMsg::Error {
                message: "expected init message".into(),
            });
            let _ = ws_sink.send(Message::Text(text)).await;
            return;
        }
    };

    let auth_key =
        match extract_auth_from_init(&first_msg, &state.auth_secret, state.auth_validate_exp) {
            Ok(key) => key,
            Err(e) => {
                let text = serialize_server_msg(&ServerMsg::Error {
                    message: e.to_string(),
                });
                let _ = ws_sink.send(Message::Text(text)).await;
                return;
            }
        };

    let connection_id: ConnectionId = Uuid::new_v4();
    let (tx, mut rx) = mpsc::channel::<ServerMsg>(state.ws_send_buffer);

    state
        .subscriptions
        .register_connection(connection_id, tx.clone());

    info!(%connection_id, "ws connected");

    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let text = match serde_json::to_string(&msg) {
                Ok(t) => t,
                Err(e) => {
                    error!("failed to serialize server msg: {e}");
                    continue;
                }
            };
            if ws_sink.send(Message::Text(text)).await.is_err() {
                break;
            }
        }
    });

    while let Some(Ok(msg)) = ws_stream.next().await {
        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => break,
            _ => continue,
        };

        let client_msg: ClientMsg = match serde_json::from_str(&text) {
            Ok(m) => m,
            Err(e) => {
                let _ = tx
                    .send(ServerMsg::Error {
                        message: format!("invalid message: {e}"),
                    })
                    .await;
                continue;
            }
        };

        match client_msg {
            ClientMsg::Subscribe { query_id, args } => {
                handle_subscribe(&state, connection_id, &auth_key, &query_id, &args, &tx).await;
            }
            ClientMsg::Unsubscribe { query_id, args } => {
                match build_cache_key(&query_id, &args, &auth_key) {
                    Ok(key) => {
                        if state.subscriptions.unsubscribe(&connection_id, &key) {
                            state.cache.remove(&key);
                        }
                    }
                    Err(e) => {
                        let _ = tx
                            .send(ServerMsg::Error {
                                message: format!("invalid unsubscribe args: {e}"),
                            })
                            .await;
                    }
                }
            }
        }
    }

    let orphaned = state.subscriptions.unregister_connection(&connection_id);
    for key in orphaned {
        state.cache.remove(&key);
    }
    send_task.abort();
    info!(%connection_id, "ws disconnected");
}

fn extract_auth_from_init(
    text: &str,
    secret: &str,
    validate_exp: bool,
) -> Result<AuthContextKey, WsGatewayError> {
    let init: ClientInitMsg = serde_json::from_str(text)?;

    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "authorization",
        format!("Bearer {}", init.token)
            .parse()
            .map_err(|_| WsGatewayError::InvalidTokenValue)?,
    );

    Ok(auth::auth_context_key(&headers, secret, validate_exp)?)
}

async fn handle_subscribe(
    state: &Arc<AppState>,
    connection_id: ConnectionId,
    auth_key: &AuthContextKey,
    query_id: &str,
    args: &Value,
    tx: &mpsc::Sender<ServerMsg>,
) {
    let key = match build_cache_key(query_id, args, auth_key) {
        Ok(key) => key,
        Err(e) => {
            let _ = tx
                .send(ServerMsg::Error {
                    message: format!("invalid subscribe args: {e}"),
                })
                .await;
            return;
        }
    };

    state.subscriptions.subscribe(connection_id, key.clone());

    match resolve_snapshot(state, &key).await {
        Ok(entry) => {
            let msg = ServerMsg::Snapshot {
                query_id: query_id.to_string(),
                payload: entry.result,
            };
            if tx.send(msg).await.is_err() {
                warn!(%connection_id, "channel closed during snapshot send");
            }
        }
        Err(e) => {
            if state.subscriptions.unsubscribe(&connection_id, &key) {
                state.cache.remove(&key);
            }
            let _ = tx
                .send(ServerMsg::Error {
                    message: format!("fetch failed: {e}"),
                })
                .await;
        }
    }
}

async fn resolve_snapshot(
    state: &Arc<AppState>,
    key: &QueryCacheKey,
) -> Result<CacheEntry, WsGatewayError> {
    if let Some(entry) = state.cache.get(key).await {
        if !entry.stale {
            debug!(query_id = %key.query_id, "serving from cache");
            return Ok(entry);
        }
    }

    fetch_and_cache(state, key).await
}

async fn fetch_and_cache(
    state: &Arc<AppState>,
    key: &QueryCacheKey,
) -> Result<CacheEntry, WsGatewayError> {
    let state = state.clone();
    let key = key.clone();

    state
        .cache
        .with_refresh_lock(&key, || {
            let state = state.clone();
            let key = key.clone();
            async move {
                if let Some(entry) = state.cache.get(&key).await {
                    if !entry.stale {
                        return Ok(entry);
                    }
                }

                let _permit = state
                    .refetch_semaphore
                    .acquire()
                    .await
                    .map_err(|e| WsGatewayError::SemaphoreClosed(e.to_string()))?;

                let args: Value = serde_json::from_str(&key.args_canonical)
                    .map_err(WsGatewayError::InvalidCanonicalArgs)?;

                let (version, result) = state
                    .core_client
                    .fetch_query(&key.auth, &key.query_id, &args)
                    .await
                    .map_err(WsGatewayError::CoreFetch)?;

                let entry = CacheEntry {
                    result,
                    last_mutation_version: version,
                    stale: false,
                };

                state.cache.put_atomic(key, entry.clone()).await;
                Ok(entry)
            }
        })
        .await
}

fn build_cache_key(
    query_id: &str,
    args: &Value,
    auth_key: &AuthContextKey,
) -> Result<QueryCacheKey, WsGatewayError> {
    let args_canonical = serde_jcs::to_string(args)
        .or_else(|_| serde_json::to_string(args))
        .map_err(WsGatewayError::ArgsSerialization)?;

    Ok(QueryCacheKey {
        query_id: Arc::from(query_id),
        args_canonical: Arc::from(args_canonical.as_str()),
        auth: auth_key.clone(),
    })
}

fn serialize_server_msg(msg: &ServerMsg) -> String {
    serde_json::to_string(msg).unwrap_or_else(|e| {
        serde_json::json!({
            "Error": {
                "message": format!("server serialization error: {e}")
            }
        })
        .to_string()
    })
}
