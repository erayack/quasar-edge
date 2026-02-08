use std::sync::Arc;

use futures::{stream, StreamExt};
use serde_json::Value;
use tracing::{debug, error, info, warn};

use crate::core_client::{CoreClient, CoreError};
use crate::ordering::ApplyDecision;
use crate::types::{CacheEntry, QueryCacheKey, ServerMsg};
use crate::AppState;

pub async fn run_invalidation_loop(state: Arc<AppState>, core: Arc<dyn CoreClient>) {
    loop {
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
            debug!(
                version = event.version,
                ?decision,
                "classified invalidation event"
            );

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

        warn!("invalidation stream ended; retrying after backoff");
        tokio::time::sleep(std::time::Duration::from_millis(
            state.invalidation_retry_backoff_ms,
        ))
        .await;
    }
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
    info!(
        key_count = all_keys.len(),
        "gap recovery: refetching all subscribed keys"
    );

    for key in &all_keys {
        state.cache.mark_stale_by_query_id(&key.query_id);
    }

    let failures = refetch_keys(state, core, &all_keys).await;
    if failures > 0 {
        warn!(failures, "gap recovery finished with refetch failures");
    }
    state.ordering.set(new_watermark);
}

async fn refetch_keys(
    state: &Arc<AppState>,
    core: &Arc<dyn CoreClient>,
    keys: &[QueryCacheKey],
) -> usize {
    let concurrency = state.refetch_semaphore.available_permits().max(1);
    stream::iter(keys.iter().cloned())
        .map(|key| {
            let state = state.clone();
            let core = core.clone();
            async move { refetch_single(&state, &core, &key).await }
        })
        .buffer_unordered(concurrency)
        .filter(|ok| futures::future::ready(!ok))
        .count()
        .await
}

async fn refetch_single(state: &AppState, core: &Arc<dyn CoreClient>, key: &QueryCacheKey) -> bool {
    let result: Result<(), CoreError> = state
        .cache
        .with_refresh_lock(key, || async {
            let _permit = state
                .refetch_semaphore
                .acquire()
                .await
                .map_err(|e| CoreError::Stream(format!("semaphore closed: {e}")))?;

            let args = decode_args(&key.args_canonical)?;

            let (version, payload) = core.fetch_query(&key.auth, &key.query_id, &args).await?;

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
            let orphaned = state.subscriptions.fan_out_snapshot(key, &msg);
            for orphan in orphaned {
                state.cache.remove(&orphan);
            }

            Ok(())
        })
        .await;

    if let Err(e) = result {
        warn!(query_id = %key.query_id, error = %e, "failed to refetch query");
        return false;
    }
    true
}

fn decode_args(args_canonical: &str) -> Result<Value, CoreError> {
    serde_json::from_str(args_canonical).map_err(CoreError::Deserialization)
}
