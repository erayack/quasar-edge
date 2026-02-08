use anyhow::Context;

#[derive(Clone, Debug)]
pub struct Config {
    pub bind_addr: String,
    pub core_base_url: String,
    pub invalidation_stream_url: String,
    pub auth_secret: String,
    pub refetch_concurrency: usize,
    pub ws_send_buffer: usize,
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
        let refetch_concurrency = std::env::var("REFETCH_CONCURRENCY")
            .unwrap_or_else(|_| "8".into())
            .parse()
            .context("REFETCH_CONCURRENCY must be a number")?;
        let ws_send_buffer = std::env::var("WS_SEND_BUFFER")
            .unwrap_or_else(|_| "64".into())
            .parse()
            .context("WS_SEND_BUFFER must be a number")?;

        Ok(Self {
            bind_addr,
            core_base_url,
            invalidation_stream_url,
            auth_secret,
            refetch_concurrency,
            ws_send_buffer,
        })
    }
}
