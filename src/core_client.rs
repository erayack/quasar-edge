use std::pin::Pin;

use async_trait::async_trait;
use futures::StreamExt;
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use tokio_stream::wrappers::LinesStream;
use tracing::warn;

use crate::config::Config;
use crate::types::{AuthContextKey, InvalidationEvent};

#[derive(thiserror::Error, Debug)]
pub enum CoreError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
    #[error("stream error: {0}")]
    Stream(String),
}

#[derive(Debug, Deserialize)]
struct CoreQueryResponse {
    version: u64,
    result: Value,
}

#[async_trait]
pub trait CoreClient: Send + Sync {
    async fn fetch_query(
        &self,
        auth: &AuthContextKey,
        query_id: &str,
        args: &Value,
    ) -> Result<(u64, Value), CoreError>;

    fn invalidation_stream(
        &self,
    ) -> Pin<Box<dyn futures::Stream<Item = Result<InvalidationEvent, CoreError>> + Send>>;
}

pub struct HttpCoreClient {
    client: Client,
    config: Config,
}

impl HttpCoreClient {
    pub fn new(config: Config) -> Result<Self, CoreError> {
        let client = Client::new();
        Ok(Self { client, config })
    }
}

#[async_trait]
impl CoreClient for HttpCoreClient {
    async fn fetch_query(
        &self,
        auth: &AuthContextKey,
        query_id: &str,
        args: &Value,
    ) -> Result<(u64, Value), CoreError> {
        let url = format!("{}/query/{}", self.config.core_base_url, query_id);
        let resp = self
            .client
            .post(&url)
            .header("X-Auth-Context", auth.as_ref())
            .json(args)
            .send()
            .await?
            .error_for_status()?;

        let body: CoreQueryResponse = resp.json().await?;
        Ok((body.version, body.result))
    }

    fn invalidation_stream(
        &self,
    ) -> Pin<Box<dyn futures::Stream<Item = Result<InvalidationEvent, CoreError>> + Send>> {
        let url = self.config.invalidation_stream_url.clone();
        let client = self.client.clone();

        let stream = async_stream(client, url);
        Box::pin(stream)
    }
}

fn async_stream(
    client: Client,
    url: String,
) -> Pin<Box<dyn futures::Stream<Item = Result<InvalidationEvent, CoreError>> + Send>> {
    use tokio::io::AsyncBufReadExt;

    let stream = futures::stream::once(async move {
        let resp = client.get(&url).send().await.map_err(CoreError::Http)?;
        let byte_stream = resp.bytes_stream();

        let reader = tokio_util::io::StreamReader::new(
            byte_stream.map(|r| r.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))),
        );
        let lines = tokio::io::BufReader::new(reader).lines();
        Ok::<_, CoreError>(LinesStream::new(lines))
    })
    .filter_map(|res| async {
        match res {
            Ok(line_stream) => Some(line_stream),
            Err(e) => {
                warn!("Failed to open invalidation stream: {e}");
                None
            }
        }
    })
    .flatten()
    .filter_map(|line_result| async {
        match line_result {
            Ok(line) if line.trim().is_empty() => None,
            Ok(line) => Some(
                serde_json::from_str::<InvalidationEvent>(&line)
                    .map_err(CoreError::Deserialization),
            ),
            Err(e) => Some(Err(CoreError::Stream(e.to_string()))),
        }
    });

    Box::pin(stream)
}
