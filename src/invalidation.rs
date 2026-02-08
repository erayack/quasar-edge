use std::sync::Arc;

use futures::StreamExt;
use serde_json::Value;
use tracing::{debug, error, info, warn};

use crate::core_client::{CoreClient, CoreError};
use crate::ordering::ApplyDecision;
use crate::types::{CacheEntry, QueryCacheKey, ServerMsg};
use crate::AppState;

pub async fn run_invalidation_loop(state: Arc<AppState>, core: Arc<dyn CoreClient>) {
    let mut stream = core.invalidation_stream();

    while let Some(result) = stream.next().await {
        let event = match result {
            Ok(ev) => ev,
            Err(e) => {
                error!(error = %e, "invalidation stream error");
                continue;
            }
        };

        let decision = state.ordering.classify(event.version);
        debug!(version = event.version, ?decision, "classified invalidation event");

        match decision {
            ApplyDecision::DropOld => {
                debug!(version = event.version, "dropping old event");
            }
            ApplyDecision::ApplyInOrder => {
                apply_invalidation(&state, &core, &event.affected_query_ids).await;
            }
            ApplyDecision::GapDetected { expected, got } => {
                warn!(expected, got, "gap detected, performing full recovery");
                handle_gap_recovery(&state, &core, got).await;
            }
        }
    }

    warn!("invalidation stream ended");
}

async fn apply_invalidation(
    state: &Arc<AppState>,
    core: &Arc<dyn CoreClient>,
    affected_query_ids: &[String],
) {
    for query_id in affected_query_ids {
        let stale_keys = state.cache.mark_stale_by_query_id(query_id);
        refetch_keys(state, core, &stale_keys).await;
    }
}

async fn handle_gap_recovery(
    state: &Arc<AppState>,
    core: &Arc<dyn CoreClient>,
    new_watermark: u64,
) {
    let all_keys = state.subscriptions.subscribed_keys();
    info!(key_count = all_keys.len(), "gap recovery: refetching all subscribed keys");

    for key in &all_keys {
        state.cache.mark_stale_by_query_id(&key.query_id);
    }

    refetch_keys(state, core, &all_keys).await;

    state.ordering.reset();
    state.ordering.classify(new_watermark);
}

async fn refetch_keys(
    state: &Arc<AppState>,
    core: &Arc<dyn CoreClient>,
    keys: &[QueryCacheKey],
) {
    let mut handles = Vec::with_capacity(keys.len());

    for key in keys {
        let state = state.clone();
        let core = core.clone();
        let key = key.clone();

        let handle = tokio::spawn(async move {
            let _permit = state.refetch_semaphore.acquire().await;
            refetch_single(&state, &core, &key).await;
        });

        handles.push(handle);
    }

    for handle in handles {
        if let Err(e) = handle.await {
            error!(error = %e, "refetch task panicked");
        }
    }
}

async fn refetch_single(
    state: &AppState,
    core: &Arc<dyn CoreClient>,
    key: &QueryCacheKey,
) {
    let result: Result<(), CoreError> = state
        .cache
        .with_refresh_lock(key, || async {
            let args: Value =
                serde_json::from_str(&key.args_canonical).unwrap_or(Value::Null);

            let (version, payload) = core
                .fetch_query(&key.auth, &key.query_id, &args)
                .await?;

            let entry = CacheEntry {
                result: payload.clone(),
                last_mutation_version: version,
                stale: false,
            };

            state.cache.put_atomic(key.clone(), entry).await;

            let msg = ServerMsg::Snapshot {
                query_id: key.query_id.to_string(),
                payload,
            };
            state.subscriptions.fan_out_snapshot(key, &msg);

            Ok(())
        })
        .await;

    if let Err(e) = result {
        warn!(query_id = %key.query_id, error = %e, "failed to refetch query");
    }
}
