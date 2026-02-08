use std::{net::SocketAddr, sync::Arc};

use axum::{routing::get, Router};
use tokio::sync::Semaphore;
use tracing::{error, info};

mod auth;
mod cache;
mod config;
mod core_client;
mod invalidation;
mod ordering;
mod subscriptions;
mod types;
mod ws_gateway;

pub struct AppState {
    pub cache: Arc<cache::AuthScopedCache>,
    pub subscriptions: Arc<subscriptions::Subscriptions>,
    pub ordering: ordering::OrderingGuard,
    pub core_client: Arc<dyn core_client::CoreClient>,
    pub refetch_semaphore: Arc<Semaphore>,
    pub ws_send_buffer: usize,
    pub auth_secret: Arc<str>,
    pub auth_validate_exp: bool,
    pub invalidation_retry_backoff_ms: u64,
}

impl AppState {
    pub async fn initialize(config: config::Config) -> anyhow::Result<Arc<Self>> {
        let cache = Arc::new(cache::AuthScopedCache::new());
        let subscriptions = Arc::new(subscriptions::Subscriptions::new());
        let ordering = ordering::OrderingGuard::new();
        let refetch_semaphore = Arc::new(Semaphore::new(config.refetch_concurrency));
        let ws_send_buffer = config.ws_send_buffer;
        let auth_secret: Arc<str> = Arc::from(config.auth_secret.as_str());
        let auth_validate_exp = config.auth_validate_exp;
        let invalidation_retry_backoff_ms = config.invalidation_retry_backoff_ms;
        let core_client = Arc::new(core_client::HttpCoreClient::new(config)?);

        Ok(Arc::new(Self {
            cache,
            subscriptions,
            ordering,
            core_client,
            refetch_semaphore,
            ws_send_buffer,
            auth_secret,
            auth_validate_exp,
            invalidation_retry_backoff_ms,
        }))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = config::Config::from_env()?;
    let bind_addr = config.bind_addr.clone();
    let state = AppState::initialize(config).await?;

    // start invalidation worker
    let inv_state = state.clone();
    let inv_core = state.core_client.clone();
    tokio::spawn(async move {
        invalidation::run_invalidation_loop(inv_state, inv_core).await;
    });

    let router = Router::new()
        .route("/ws", get(ws_gateway::ws_handler))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state.clone());

    let addr: SocketAddr = bind_addr.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("listening on {}", addr);

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| {
            error!(error = %e, "server failed");
            e
        })?;

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("shutdown signal received");
}
