use async_trait::async_trait;
use thiserror::Error;

pub mod in_memory;

#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    pub content_type: String,
    pub etag: String,
    pub size: u64,
    pub last_modified_unix_secs: i64,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("bucket not found: {0}")]
    BucketNotFound(String),
    #[error("bucket not empty: {0}")]
    BucketNotEmpty(String),
    #[error("object not found: {bucket}/{key}")]
    ObjectNotFound { bucket: String, key: String },
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("internal storage error: {0}")]
    Internal(String),
}

#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    async fn create_bucket(&self, bucket: &str) -> Result<bool, StorageError>;
    async fn delete_bucket(&self, bucket: &str) -> Result<bool, StorageError>;

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: bytes::Bytes,
        content_type: &str,
    ) -> Result<ObjectMetadata, StorageError>;

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(bytes::Bytes, ObjectMetadata), StorageError>;

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<bool, StorageError>;

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, ObjectMetadata)>, StorageError>;
}
