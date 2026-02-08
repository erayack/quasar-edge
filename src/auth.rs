use axum::http::HeaderMap;
use thiserror::Error;

use crate::types::AuthContextKey;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("missing authorization header")]
    MissingHeader,
    #[error("invalid authorization header: {0}")]
    Invalid(String),
}

pub fn auth_context_key(headers: &HeaderMap) -> Result<AuthContextKey, AuthError> {
    let value = headers
        .get("authorization")
        .ok_or(AuthError::MissingHeader)?
        .to_str()
        .map_err(|e| AuthError::Invalid(e.to_string()))?;

    let token = value
        .strip_prefix("Bearer ")
        .ok_or_else(|| AuthError::Invalid("expected Bearer scheme".into()))?;

    let user_id = decode_user_id(token)?;

    if user_id.is_empty() {
        return Err(AuthError::Invalid("empty user_id".into()));
    }

    Ok(AuthContextKey::from(user_id))
}

fn decode_user_id(token: &str) -> Result<String, AuthError> {
    let parts: Vec<&str> = token.splitn(3, '.').collect();
    if parts.len() != 3 {
        return Err(AuthError::Invalid("malformed JWT".into()));
    }

    let payload_bytes = base64_url_decode(parts[1])
        .map_err(|e| AuthError::Invalid(format!("base64 decode error: {e}")))?;

    let payload: serde_json::Value = serde_json::from_slice(&payload_bytes)
        .map_err(|e| AuthError::Invalid(format!("invalid JSON payload: {e}")))?;

    payload["sub"]
        .as_str()
        .map(String::from)
        .ok_or_else(|| AuthError::Invalid("missing 'sub' claim".into()))
}

fn base64_url_decode(input: &str) -> Result<Vec<u8>, String> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    URL_SAFE_NO_PAD
        .decode(input)
        .map_err(|e| e.to_string())
}
