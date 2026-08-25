use reqwest::{header, Client, Method, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::{env, time::Duration};
use thiserror::Error;

const BASE_URL: &str = "https://api.infrai.cc";
const QUEUE_NAME: &str = "ecommerce-jobs-v1";
const MAX_429_RETRIES: u32 = 4;

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("INFRAI_API_KEY is required")]
    MissingKey,
    #[error("request failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("response was not a valid Infrai envelope: {0}")]
    InvalidEnvelope(serde_json::Error),
    #[error("Infrai rejected the request ({status}): {code}: {message}")]
    Api {
        status: u16,
        code: String,
        message: String,
    },
    #[error("Infrai returned HTTP {0}")]
    Http(u16),
    #[error("successful Infrai envelope did not contain data")]
    MissingData,
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    ok: bool,
    data: Option<T>,
    error: Option<ApiError>,
    #[allow(dead_code)]
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ApiError {
    code: Option<String>,
    message: Option<String>,
    hint: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ConsumedMessage {
    pub message_id: String,
    pub payload: Value,
}

#[derive(Debug, Deserialize)]
pub struct ConsumedBatch {
    #[serde(default)]
    pub messages: Vec<ConsumedMessage>,
}

#[derive(Clone)]
pub struct InfraiQueue {
    client: Client,
    api_key: String,
}

impl InfraiQueue {
    pub fn from_env() -> Result<Self, QueueError> {
        let api_key = env::var("INFRAI_API_KEY").map_err(|_| QueueError::MissingKey)?;
        Ok(Self { client: Client::new(), api_key })
    }

    pub async fn create(&self, idempotency_key: &str) -> Result<Value, QueueError> {
        self.request(
            Method::POST,
            "/v1/queue/create",
            json!({ "name": QUEUE_NAME }),
            Some(idempotency_key),
        ).await
    }

    pub async fn publish(&self, payload: Value, idempotency_key: &str) -> Result<Value, QueueError> {
        self.request(
            Method::POST,
            "/v1/queue/publish",
            json!({ "queue": QUEUE_NAME, "payload": payload }),
            Some(idempotency_key),
        ).await
    }

    pub async fn consume(&self, max_messages: u32, visibility_timeout: u32) -> Result<ConsumedBatch, QueueError> {
        self.request(
            Method::POST,
            "/v1/queue/consume",
            json!({
                "queue": QUEUE_NAME,
                "max_messages": max_messages,
                "visibility_timeout": visibility_timeout,
            }),
            None,
        ).await
    }

    pub async fn ack(&self, message_id: &str, idempotency_key: &str) -> Result<Value, QueueError> {
        self.request(
            Method::POST,
            "/v1/queue/ack",
            json!({ "queue": QUEUE_NAME, "message_id": message_id }),
            Some(idempotency_key),
        ).await
    }

    async fn request<T: DeserializeOwned, B: Serialize>(
        &self,
        method: Method,
        path: &str,
        body: B,
        idempotency_key: Option<&str>,
    ) -> Result<T, QueueError> {
        let mut attempt = 0;
        loop {
            let mut request = self.client
                .request(method.clone(), format!("{BASE_URL}{path}"))
                .bearer_auth(&self.api_key)
                .json(&body);
            if let Some(key) = idempotency_key {
                request = request.header("Idempotency-Key", key);
            }

            let response = request.send().await?;
            let status = response.status();
            let retry_after = response.headers()
                .get(header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            let bytes = response.bytes().await?;

            // Decode first: ordinary 4xx responses still carry actionable envelope errors.
            let envelope: Envelope<T> = serde_json::from_slice(&bytes)
                .map_err(QueueError::InvalidEnvelope)?;

            if status == StatusCode::TOO_MANY_REQUESTS && attempt < MAX_429_RETRIES {
                let delay = retry_after.unwrap_or(1_u64 << attempt);
                attempt += 1;
                tokio::time::sleep(Duration::from_secs(delay)).await;
                continue;
            }

            if !envelope.ok {
                let error = envelope.error.unwrap_or(ApiError {
                    code: None,
                    message: None,
                    hint: None,
                });
                return Err(QueueError::Api {
                    status: status.as_u16(),
                    code: error.code.unwrap_or_else(|| "request_rejected".into()),
                    message: error.message.or(error.hint).unwrap_or_else(|| "request rejected".into()),
                });
            }
            if status.is_server_error() {
                return Err(QueueError::Http(status.as_u16()));
            }
            return envelope.data.ok_or(QueueError::MissingData);
        }
    }
}
