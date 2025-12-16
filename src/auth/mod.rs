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

    /// Optional: retrieve the secret for a given access key (used for SigV4 verification).
    /// Default implementation returns InvalidCredentials so existing authenticators continue to work.
    async fn secret_for(&self, access_key: &str) -> Result<String, AuthError> {
        let _ = access_key;
        Err(AuthError::InvalidCredentials)
    }
}

/// Helper: read (x-access-key, x-secret-key) from gRPC metadata.
pub fn metadata_creds(req: &Request<()>) -> Option<(String, String)> {
    let ak = req
        .metadata()
        .get("x-access-key")?
        .to_str()
        .ok()?
        .to_string();
    let sk = req
        .metadata()
        .get("x-secret-key")?
        .to_str()
        .ok()?
        .to_string();
    Some((ak, sk))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_creds_success() {
        let mut req = Request::new(());
        req.metadata_mut()
            .insert("x-access-key", "access123".parse().unwrap());
        req.metadata_mut()
            .insert("x-secret-key", "secret456".parse().unwrap());

        let result = metadata_creds(&req);
        assert_eq!(
            result,
            Some(("access123".to_string(), "secret456".to_string()))
        );
    }

    #[test]
    fn test_metadata_creds_missing_access_key() {
        let mut req = Request::new(());
        req.metadata_mut()
            .insert("x-secret-key", "secret456".parse().unwrap());

        let result = metadata_creds(&req);
        assert_eq!(result, None);
    }

    #[test]
    fn test_metadata_creds_missing_secret_key() {
        let mut req = Request::new(());
        req.metadata_mut()
            .insert("x-access-key", "access123".parse().unwrap());

        let result = metadata_creds(&req);
        assert_eq!(result, None);
    }

    #[test]
    fn test_metadata_creds_missing_both() {
        let req = Request::new(());
        let result = metadata_creds(&req);
        assert_eq!(result, None);
    }
}
