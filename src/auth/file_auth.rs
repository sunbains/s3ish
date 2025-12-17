// Copyright PingCAP Inc. 2025.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; version 2 of the License.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

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

    async fn secret_for(&self, access_key: &str) -> Result<String, AuthError> {
        let guard = self.creds.read().await;
        guard
            .get(access_key)
            .cloned()
            .ok_or(AuthError::InvalidCredentials)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    async fn create_auth_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file.flush().unwrap();
        file
    }

    #[tokio::test]
    async fn test_file_auth_valid_credentials() {
        let file = create_auth_file("user1:pass1\nuser2:pass2\n").await;
        let auth = FileAuthenticator::new(file.path()).await.unwrap();

        let mut req = Request::new(());
        req.metadata_mut()
            .insert("x-access-key", "user1".parse().unwrap());
        req.metadata_mut()
            .insert("x-secret-key", "pass1".parse().unwrap());

        let ctx = auth.authenticate(&req).await.unwrap();
        assert_eq!(ctx.access_key, "user1");
    }

    #[tokio::test]
    async fn test_file_auth_invalid_secret() {
        let file = create_auth_file("user1:pass1\n").await;
        let auth = FileAuthenticator::new(file.path()).await.unwrap();

        let mut req = Request::new(());
        req.metadata_mut()
            .insert("x-access-key", "user1".parse().unwrap());
        req.metadata_mut()
            .insert("x-secret-key", "wrongpass".parse().unwrap());

        let result = auth.authenticate(&req).await;
        assert!(matches!(result, Err(AuthError::InvalidCredentials)));
    }

    #[tokio::test]
    async fn test_file_auth_unknown_user() {
        let file = create_auth_file("user1:pass1\n").await;
        let auth = FileAuthenticator::new(file.path()).await.unwrap();

        let mut req = Request::new(());
        req.metadata_mut()
            .insert("x-access-key", "unknown".parse().unwrap());
        req.metadata_mut()
            .insert("x-secret-key", "pass1".parse().unwrap());

        let result = auth.authenticate(&req).await;
        assert!(matches!(result, Err(AuthError::InvalidCredentials)));
    }

    #[tokio::test]
    async fn test_file_auth_missing_credentials() {
        let file = create_auth_file("user1:pass1\n").await;
        let auth = FileAuthenticator::new(file.path()).await.unwrap();

        let req = Request::new(());
        let result = auth.authenticate(&req).await;
        assert!(matches!(result, Err(AuthError::MissingCredentials)));
    }

    #[tokio::test]
    async fn test_file_auth_comments_and_blank_lines() {
        let file = create_auth_file(
            "# This is a comment\nuser1:pass1\n\n# Another comment\nuser2:pass2\n\n",
        )
        .await;
        let auth = FileAuthenticator::new(file.path()).await.unwrap();

        let mut req = Request::new(());
        req.metadata_mut()
            .insert("x-access-key", "user2".parse().unwrap());
        req.metadata_mut()
            .insert("x-secret-key", "pass2".parse().unwrap());

        let ctx = auth.authenticate(&req).await.unwrap();
        assert_eq!(ctx.access_key, "user2");
    }

    #[tokio::test]
    async fn test_file_auth_whitespace_trimming() {
        let file = create_auth_file("  user1  :  pass1  \n").await;
        let auth = FileAuthenticator::new(file.path()).await.unwrap();

        let mut req = Request::new(());
        req.metadata_mut()
            .insert("x-access-key", "user1".parse().unwrap());
        req.metadata_mut()
            .insert("x-secret-key", "pass1".parse().unwrap());

        let ctx = auth.authenticate(&req).await.unwrap();
        assert_eq!(ctx.access_key, "user1");
    }

    #[tokio::test]
    async fn test_file_auth_invalid_format() {
        let file = create_auth_file("user1:pass1\ninvalidline\n").await;
        let result = FileAuthenticator::new(file.path()).await;
        assert!(matches!(result, Err(AuthError::Internal(_))));
    }

    #[tokio::test]
    async fn test_file_auth_reload() {
        use std::io::{Seek, SeekFrom};

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"user1:pass1\n").unwrap();
        file.flush().unwrap();

        let auth = FileAuthenticator::new(file.path()).await.unwrap();

        // Test with initial credentials
        let mut req = Request::new(());
        req.metadata_mut()
            .insert("x-access-key", "user1".parse().unwrap());
        req.metadata_mut()
            .insert("x-secret-key", "pass1".parse().unwrap());
        auth.authenticate(&req).await.unwrap();

        // Update the file - truncate and rewrite
        file.seek(SeekFrom::Start(0)).unwrap();
        file.as_file_mut().set_len(0).unwrap();
        file.write_all(b"user2:pass2\n").unwrap();
        file.flush().unwrap();

        // Reload and test with new credentials
        auth.reload().await.unwrap();

        let mut req2 = Request::new(());
        req2.metadata_mut()
            .insert("x-access-key", "user2".parse().unwrap());
        req2.metadata_mut()
            .insert("x-secret-key", "pass2".parse().unwrap());
        let ctx = auth.authenticate(&req2).await.unwrap();
        assert_eq!(ctx.access_key, "user2");

        // Old credentials should no longer work
        let result = auth.authenticate(&req).await;
        assert!(matches!(result, Err(AuthError::InvalidCredentials)));
    }

    #[tokio::test]
    async fn test_file_auth_nonexistent_file() {
        let result = FileAuthenticator::new("/nonexistent/path/to/file.txt").await;
        assert!(matches!(result, Err(AuthError::Internal(_))));
    }
}
