use anyhow::Context;

#[derive(Clone, Debug)]
pub struct Config {
    pub bind_addr: String,
    pub core_base_url: String,
    pub invalidation_stream_url: String,
    pub auth_secret: String,
    pub auth_validate_exp: bool,
    pub refetch_concurrency: usize,
    pub ws_send_buffer: usize,
    pub invalidation_retry_backoff_ms: u64,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let bind_addr = std::env::var("EDGE_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".into());
        let core_base_url =
            std::env::var("CORE_BASE_URL").unwrap_or_else(|_| "http://localhost:8080".into());
        let invalidation_stream_url = std::env::var("CORE_INVALIDATION_STREAM_URL")
            .unwrap_or_else(|_| "http://localhost:8080/invalidation".into());
        let auth_secret =
            std::env::var("AUTH_JWT_SECRET").context("AUTH_JWT_SECRET must be set")?;
        let auth_validate_exp = std::env::var("AUTH_VALIDATE_EXP")
            .unwrap_or_else(|_| "true".into())
            .parse()
            .context("AUTH_VALIDATE_EXP must be true or false")?;
        let refetch_concurrency = std::env::var("REFETCH_CONCURRENCY")
            .unwrap_or_else(|_| "8".into())
            .parse()
            .context("REFETCH_CONCURRENCY must be a number")?;
        let ws_send_buffer = std::env::var("WS_SEND_BUFFER")
            .unwrap_or_else(|_| "64".into())
            .parse()
            .context("WS_SEND_BUFFER must be a number")?;
        let invalidation_retry_backoff_ms = std::env::var("INVALIDATION_RETRY_BACKOFF_MS")
            .unwrap_or_else(|_| "3000".into())
            .parse()
            .context("INVALIDATION_RETRY_BACKOFF_MS must be a number")?;

        Ok(Self {
            bind_addr,
            core_base_url,
            invalidation_stream_url,
            auth_secret,
            auth_validate_exp,
            refetch_concurrency,
            ws_send_buffer,
            invalidation_retry_backoff_ms,
        })
    }
}
