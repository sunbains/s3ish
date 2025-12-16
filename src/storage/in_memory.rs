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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_bucket_success() {
        let storage = InMemoryStorage::new();
        let created = storage.create_bucket("bucket1").await.unwrap();
        assert!(created);
    }

    #[tokio::test]
    async fn test_create_bucket_duplicate() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        let created = storage.create_bucket("bucket1").await.unwrap();
        assert!(!created);
    }

    #[tokio::test]
    async fn test_create_bucket_empty_name() {
        let storage = InMemoryStorage::new();
        let result = storage.create_bucket("").await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    #[tokio::test]
    async fn test_delete_bucket_success() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        let deleted = storage.delete_bucket("bucket1").await.unwrap();
        assert!(deleted);
    }

    #[tokio::test]
    async fn test_delete_bucket_nonexistent() {
        let storage = InMemoryStorage::new();
        let deleted = storage.delete_bucket("nonexistent").await.unwrap();
        assert!(!deleted);
    }

    #[tokio::test]
    async fn test_delete_bucket_not_empty() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        storage
            .put_object("bucket1", "key1", Bytes::from("data"), "text/plain")
            .await
            .unwrap();

        let result = storage.delete_bucket("bucket1").await;
        assert!(matches!(result, Err(StorageError::BucketNotEmpty(_))));
    }

    #[tokio::test]
    async fn test_put_object_success() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();

        let data = Bytes::from("hello world");
        let meta = storage
            .put_object("bucket1", "key1", data.clone(), "text/plain")
            .await
            .unwrap();

        assert_eq!(meta.size, 11);
        assert_eq!(meta.content_type, "text/plain");
        assert!(!meta.etag.is_empty());
    }

    #[tokio::test]
    async fn test_put_object_bucket_not_found() {
        let storage = InMemoryStorage::new();
        let result = storage
            .put_object("nonexistent", "key1", Bytes::from("data"), "text/plain")
            .await;
        assert!(matches!(result, Err(StorageError::BucketNotFound(_))));
    }

    #[tokio::test]
    async fn test_put_object_empty_key() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        let result = storage
            .put_object("bucket1", "", Bytes::from("data"), "text/plain")
            .await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    #[tokio::test]
    async fn test_put_object_empty_content_type() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        let result = storage
            .put_object("bucket1", "key1", Bytes::from("data"), "")
            .await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    #[tokio::test]
    async fn test_put_object_overwrite() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();

        let meta1 = storage
            .put_object("bucket1", "key1", Bytes::from("data1"), "text/plain")
            .await
            .unwrap();
        let meta2 = storage
            .put_object("bucket1", "key1", Bytes::from("data2"), "text/html")
            .await
            .unwrap();

        assert_ne!(meta1.etag, meta2.etag);
        assert_eq!(meta2.content_type, "text/html");

        let (data, _) = storage.get_object("bucket1", "key1").await.unwrap();
        assert_eq!(&data[..], b"data2");
    }

    #[tokio::test]
    async fn test_get_object_success() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        let original_data = Bytes::from("test data");
        storage
            .put_object("bucket1", "key1", original_data.clone(), "text/plain")
            .await
            .unwrap();

        let (data, meta) = storage.get_object("bucket1", "key1").await.unwrap();
        assert_eq!(data, original_data);
        assert_eq!(meta.size, 9);
        assert_eq!(meta.content_type, "text/plain");
    }

    #[tokio::test]
    async fn test_get_object_not_found() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();

        let result = storage.get_object("bucket1", "nonexistent").await;
        assert!(matches!(result, Err(StorageError::ObjectNotFound { .. })));
    }

    #[tokio::test]
    async fn test_get_object_empty_key() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        let result = storage.get_object("bucket1", "").await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    #[tokio::test]
    async fn test_delete_object_success() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        storage
            .put_object("bucket1", "key1", Bytes::from("data"), "text/plain")
            .await
            .unwrap();

        let deleted = storage.delete_object("bucket1", "key1").await.unwrap();
        assert!(deleted);

        let result = storage.get_object("bucket1", "key1").await;
        assert!(matches!(result, Err(StorageError::ObjectNotFound { .. })));
    }

    #[tokio::test]
    async fn test_delete_object_nonexistent() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();

        let deleted = storage.delete_object("bucket1", "nonexistent").await.unwrap();
        assert!(!deleted);
    }

    #[tokio::test]
    async fn test_list_objects_empty() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();

        let objects = storage.list_objects("bucket1", "", 100).await.unwrap();
        assert!(objects.is_empty());
    }

    #[tokio::test]
    async fn test_list_objects_all() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        storage
            .put_object("bucket1", "key1", Bytes::from("data1"), "text/plain")
            .await
            .unwrap();
        storage
            .put_object("bucket1", "key2", Bytes::from("data2"), "text/plain")
            .await
            .unwrap();
        storage
            .put_object("bucket1", "key3", Bytes::from("data3"), "text/plain")
            .await
            .unwrap();

        let objects = storage.list_objects("bucket1", "", 100).await.unwrap();
        assert_eq!(objects.len(), 3);
        assert_eq!(objects[0].0, "key1");
        assert_eq!(objects[1].0, "key2");
        assert_eq!(objects[2].0, "key3");
    }

    #[tokio::test]
    async fn test_list_objects_with_prefix() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        storage
            .put_object("bucket1", "photos/2023/1.jpg", Bytes::from("data"), "image/jpeg")
            .await
            .unwrap();
        storage
            .put_object("bucket1", "photos/2024/1.jpg", Bytes::from("data"), "image/jpeg")
            .await
            .unwrap();
        storage
            .put_object("bucket1", "docs/readme.txt", Bytes::from("data"), "text/plain")
            .await
            .unwrap();

        let objects = storage.list_objects("bucket1", "photos/", 100).await.unwrap();
        assert_eq!(objects.len(), 2);
        assert!(objects[0].0.starts_with("photos/"));
        assert!(objects[1].0.starts_with("photos/"));
    }

    #[tokio::test]
    async fn test_list_objects_with_limit() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        for i in 1..=10 {
            storage
                .put_object(
                    "bucket1",
                    &format!("key{}", i),
                    Bytes::from("data"),
                    "text/plain",
                )
                .await
                .unwrap();
        }

        let objects = storage.list_objects("bucket1", "", 5).await.unwrap();
        assert_eq!(objects.len(), 5);
    }

    #[tokio::test]
    async fn test_list_objects_default_limit() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        storage
            .put_object("bucket1", "key1", Bytes::from("data"), "text/plain")
            .await
            .unwrap();

        // Limit of 0 should use default of 1000
        let objects = storage.list_objects("bucket1", "", 0).await.unwrap();
        assert_eq!(objects.len(), 1);
    }

    #[tokio::test]
    async fn test_list_objects_bucket_not_found() {
        let storage = InMemoryStorage::new();
        let result = storage.list_objects("nonexistent", "", 100).await;
        assert!(matches!(result, Err(StorageError::BucketNotFound(_))));
    }

    #[tokio::test]
    async fn test_list_objects_sorted() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();

        // Insert in non-alphabetical order
        storage.put_object("bucket1", "zebra", Bytes::from("data"), "text/plain").await.unwrap();
        storage.put_object("bucket1", "apple", Bytes::from("data"), "text/plain").await.unwrap();
        storage.put_object("bucket1", "middle", Bytes::from("data"), "text/plain").await.unwrap();

        let objects = storage.list_objects("bucket1", "", 100).await.unwrap();
        assert_eq!(objects[0].0, "apple");
        assert_eq!(objects[1].0, "middle");
        assert_eq!(objects[2].0, "zebra");
    }

    #[tokio::test]
    async fn test_etag_consistency() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();

        let data = Bytes::from("test data");
        let meta1 = storage
            .put_object("bucket1", "key1", data.clone(), "text/plain")
            .await
            .unwrap();

        let (_, meta2) = storage.get_object("bucket1", "key1").await.unwrap();
        assert_eq!(meta1.etag, meta2.etag);
    }

    #[tokio::test]
    async fn test_multiple_buckets_isolation() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        storage.create_bucket("bucket2").await.unwrap();

        storage
            .put_object("bucket1", "shared-key", Bytes::from("data1"), "text/plain")
            .await
            .unwrap();
        storage
            .put_object("bucket2", "shared-key", Bytes::from("data2"), "text/plain")
            .await
            .unwrap();

        let (data1, _) = storage.get_object("bucket1", "shared-key").await.unwrap();
        let (data2, _) = storage.get_object("bucket2", "shared-key").await.unwrap();

        assert_eq!(&data1[..], b"data1");
        assert_eq!(&data2[..], b"data2");
    }
}
