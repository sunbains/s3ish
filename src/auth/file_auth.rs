use crate::auth::{metadata_creds, AuthContext, AuthError, Authenticator};
use async_trait::async_trait;
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::sync::RwLock;
use tonic::Request;

/// Simple file-based authentication.
///
/// File format (one per line):
///   access_key:secret_key
/// Lines starting with '#' are comments. Blank lines are ignored.
///
/// Notes:
/// - Credentials are loaded on startup.
/// - You can call `reload()` to re-read the file (e.g. if you add an admin endpoint later).
#[derive(Debug, Clone)]
pub struct FileAuthenticator {
    path: PathBuf,
    creds: Arc<RwLock<HashMap<String, String>>>,
}

impl FileAuthenticator {
    pub async fn new(path: impl Into<PathBuf>) -> Result<Self, AuthError> {
        let this = Self {
            path: path.into(),
            creds: Arc::new(RwLock::new(HashMap::new())),
        };
        this.reload().await?;
        Ok(this)
    }

    pub async fn reload(&self) -> Result<(), AuthError> {
        let raw = tokio::fs::read_to_string(&self.path)
            .await
            .map_err(|e| AuthError::Internal(format!("read {}: {e}", self.path.display())))?;

        let mut map = HashMap::new();
        for (idx, line) in raw.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (ak, sk) = line.split_once(':').ok_or_else(|| {
                AuthError::Internal(format!(
                    "invalid creds file format at line {} (expected access:secret)",
                    idx + 1
                ))
            })?;
            map.insert(ak.trim().to_string(), sk.trim().to_string());
        }

        let mut guard = self.creds.write().await;
        *guard = map;
        Ok(())
    }
}

#[async_trait]
impl Authenticator for FileAuthenticator {
    async fn authenticate(&self, req: &Request<()>) -> Result<AuthContext, AuthError> {
        let (ak, sk) = metadata_creds(req).ok_or(AuthError::MissingCredentials)?;

        let guard = self.creds.read().await;
        match guard.get(&ak) {
            Some(expected) if expected == &sk => Ok(AuthContext { access_key: ak }),
            _ => Err(AuthError::InvalidCredentials),
        }
    }
}
