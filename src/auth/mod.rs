use async_trait::async_trait;
use thiserror::Error;
use tonic::Request;

pub mod file_auth;

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub access_key: String,
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("missing credentials")]
    MissingCredentials,
    #[error("invalid credentials")]
    InvalidCredentials,
    #[error("internal auth error: {0}")]
    Internal(String),
}

#[async_trait]
pub trait Authenticator: Send + Sync + 'static {
    async fn authenticate(&self, req: &Request<()>) -> Result<AuthContext, AuthError>;
}

/// Helper: read (x-access-key, x-secret-key) from gRPC metadata.
pub fn metadata_creds(req: &Request<()>) -> Option<(String, String)> {
    let ak = req.metadata().get("x-access-key")?.to_str().ok()?.to_string();
    let sk = req.metadata().get("x-secret-key")?.to_str().ok()?.to_string();
    Some((ak, sk))
}
