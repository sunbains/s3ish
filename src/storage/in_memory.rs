use crate::storage::{ObjectMetadata, StorageBackend, StorageError};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
struct StoredObject {
    data: Bytes,
    meta: ObjectMetadata,
}

/// In-memory storage implementation.
///
/// Data structures:
/// - `buckets`: a set of bucket names
/// - `objects`: a BTreeMap keyed by (bucket, key) => StoredObject
///
/// BTreeMap gives deterministic iteration order (useful for tests and predictable listing).
#[derive(Debug, Clone)]
pub struct InMemoryStorage {
    buckets: Arc<RwLock<BTreeSet<String>>>,
    objects: Arc<RwLock<BTreeMap<(String, String), StoredObject>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            buckets: Arc::new(RwLock::new(BTreeSet::new())),
            objects: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

fn validate_bucket(bucket: &str) -> Result<(), StorageError> {
    if bucket.is_empty() {
        return Err(StorageError::InvalidInput("bucket must be non-empty".into()));
    }
    Ok(())
}

fn validate_key(key: &str) -> Result<(), StorageError> {
    if key.is_empty() {
        return Err(StorageError::InvalidInput("key must be non-empty".into()));
    }
    Ok(())
}

#[async_trait]
impl StorageBackend for InMemoryStorage {
    async fn create_bucket(&self, bucket: &str) -> Result<bool, StorageError> {
        validate_bucket(bucket)?;
        let mut b = self.buckets.write().await;
        Ok(b.insert(bucket.to_string()))
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<bool, StorageError> {
        validate_bucket(bucket)?;
        // check emptiness
        {
            let objs = self.objects.read().await;
            if objs.keys().any(|(b, _k)| b == bucket) {
                return Err(StorageError::BucketNotEmpty(bucket.to_string()));
            }
        }
        let mut b = self.buckets.write().await;
        Ok(b.remove(bucket))
    }

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        content_type: &str,
    ) -> Result<ObjectMetadata, StorageError> {
        validate_bucket(bucket)?;
        validate_key(key)?;
        if content_type.is_empty() {
            return Err(StorageError::InvalidInput("content_type must be non-empty".into()));
        }

        // bucket must exist
        {
            let b = self.buckets.read().await;
            if !b.contains(bucket) {
                return Err(StorageError::BucketNotFound(bucket.to_string()));
            }
        }

        let size = data.len() as u64;
        let digest = md5::compute(&data);
        let etag = format!("{:x}", digest);

        let meta = ObjectMetadata {
            content_type: content_type.to_string(),
            etag,
            size,
            last_modified_unix_secs: Utc::now().timestamp(),
        };

        let mut objs = self.objects.write().await;
        objs.insert(
            (bucket.to_string(), key.to_string()),
            StoredObject {
                data,
                meta: meta.clone(),
            },
        );
        Ok(meta)
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(Bytes, ObjectMetadata), StorageError> {
        validate_bucket(bucket)?;
        validate_key(key)?;
        let objs = self.objects.read().await;
        let obj = objs
            .get(&(bucket.to_string(), key.to_string()))
            .ok_or_else(|| StorageError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;
        Ok((obj.data.clone(), obj.meta.clone()))
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<bool, StorageError> {
        validate_bucket(bucket)?;
        validate_key(key)?;
        let mut objs = self.objects.write().await;
        Ok(objs.remove(&(bucket.to_string(), key.to_string())).is_some())
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, ObjectMetadata)>, StorageError> {
        validate_bucket(bucket)?;
        {
            let b = self.buckets.read().await;
            if !b.contains(bucket) {
                return Err(StorageError::BucketNotFound(bucket.to_string()));
            }
        }

        let limit = if limit == 0 { 1000 } else { limit.min(10_000) };

        let objs = self.objects.read().await;
        let mut out = Vec::new();

        // Because keys are (bucket, key) sorted lexicographically, we can range-scan.
        let start = (bucket.to_string(), prefix.to_string());
        for ((b, k), v) in objs.range(start..) {
            if b != bucket {
                break;
            }
            if !k.starts_with(prefix) {
                break;
            }
            out.push((k.clone(), v.meta.clone()));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }
}
