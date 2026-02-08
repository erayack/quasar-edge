use axum::http::HeaderMap;
use thiserror::Error;

use crate::types::AuthContextKey;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("missing authorization header")]
    MissingHeader,
    #[error("invalid authorization header: {0}")]
    Invalid(String),
    #[error("token verification failed: {0}")]
    Verification(String),
}

pub fn auth_context_key(
    headers: &HeaderMap,
    secret: &str,
    validate_exp: bool,
) -> Result<AuthContextKey, AuthError> {
    let value = headers
        .get("authorization")
        .ok_or(AuthError::MissingHeader)?
        .to_str()
        .map_err(|e| AuthError::Invalid(e.to_string()))?;

    let token = value
        .strip_prefix("Bearer ")
        .ok_or_else(|| AuthError::Invalid("expected Bearer scheme".into()))?;

    let user_id = decode_user_id(token, secret, validate_exp)?;

    if user_id.is_empty() {
        return Err(AuthError::Invalid("empty user_id".into()));
    }

    Ok(AuthContextKey::from(user_id))
}

#[derive(serde::Deserialize)]
struct Claims {
    sub: String,
}

fn decode_user_id(token: &str, secret: &str, validate_exp: bool) -> Result<String, AuthError> {
    use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};

    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = validate_exp;
    let data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )
    .map_err(|e| AuthError::Verification(e.to_string()))?;

    Ok(data.claims.sub)
}
