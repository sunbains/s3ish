use crate::storage::StorageError;
use bytes::Bytes;

pub fn validate_bucket(bucket: &str) -> Result<(), StorageError> {
    if bucket.is_empty() {
        return Err(StorageError::InvalidInput(
            "bucket must be non-empty".into(),
        ));
    }
    Ok(())
}

pub fn validate_key(key: &str) -> Result<(), StorageError> {
    if key.is_empty() {
        return Err(StorageError::InvalidInput("key must be non-empty".into()));
    }
    if key.contains("..") {
        return Err(StorageError::InvalidInput("key cannot contain ..".into()));
    }
    Ok(())
}

pub fn compute_etag(data: &Bytes) -> String {
    format!("{:x}", md5::compute(data))
}
