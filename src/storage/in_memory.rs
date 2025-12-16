use crate::observability::metrics;
use crate::storage::common::{compute_etag, validate_bucket, validate_key};
use crate::storage::erasure::Erasure;
use crate::storage::multipart::{InMemoryUpload, MultipartManager, MultipartStorage, PartData, UploadMetadata};
use crate::storage::versioning::{VersionManager, VersionMetadata, VersionStorage};
use crate::storage::{ObjectMetadata, StorageBackend, StorageError, VersioningStatus};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use tokio::sync::RwLock;

const ERASURE_CHUNK_SIZE: usize = 1024 * 1024; // 1MiB chunks for simulated erasure coding

#[derive(Debug, Clone)]
struct StoredObject {
    shards: Vec<Bytes>,
    orig_len: usize,
    meta: ObjectMetadata,
}

/// In-memory storage implementation with simulated erasure coding and versioning support.
///
/// Data structures:
/// - `buckets`: a set of bucket names
/// - `objects`: a BTreeMap keyed by (bucket, key) => Vec<StoredObject> (multiple versions)
/// - `bucket_versioning`: versioning status per bucket
///
/// BTreeMap gives deterministic iteration order (useful for tests and predictable listing).
type ObjectStore = Arc<RwLock<BTreeMap<(String, String), Vec<StoredObject>>>>;

#[derive(Debug, Clone)]
pub struct InMemoryStorage {
    buckets: Arc<RwLock<BTreeSet<String>>>,
    objects: ObjectStore,
    bucket_versioning: Arc<RwLock<HashMap<String, VersioningStatus>>>,
    multipart_uploads: Arc<RwLock<HashMap<String, InMemoryUpload>>>,
    lifecycle_policies: Arc<RwLock<HashMap<String, crate::storage::lifecycle::LifecyclePolicy>>>,
    erasure: Erasure,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            buckets: Arc::new(RwLock::new(BTreeSet::new())),
            objects: Arc::new(RwLock::new(BTreeMap::new())),
            bucket_versioning: Arc::new(RwLock::new(HashMap::new())),
            multipart_uploads: Arc::new(RwLock::new(HashMap::new())),
            lifecycle_policies: Arc::new(RwLock::new(HashMap::new())),
            erasure: Erasure::new(4, 2, ERASURE_CHUNK_SIZE).expect("erasure init"),
        }
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

// Implement VersionStorage trait for versioning support
#[async_trait]
impl VersionStorage for InMemoryStorage {
    async fn read_versioning_status(
        &self,
        bucket: &str,
    ) -> Result<VersioningStatus, StorageError> {
        let vers = self.bucket_versioning.read().await;
        Ok(*vers.get(bucket).unwrap_or(&VersioningStatus::Unversioned))
    }

    async fn write_versioning_status(
        &self,
        bucket: &str,
        status: VersioningStatus,
    ) -> Result<(), StorageError> {
        let mut vers = self.bucket_versioning.write().await;
        vers.insert(bucket.to_string(), status);
        Ok(())
    }

    async fn store_version_metadata(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        metadata: VersionMetadata,
    ) -> Result<(), StorageError> {
        let mut objs = self.objects.write().await;
        let obj_key = (bucket.to_string(), key.to_string());

        // Create a StoredObject from the version metadata
        // For in-memory storage, we store the metadata in the StoredObject structure
        let stored_obj = StoredObject {
            shards: vec![], // Data shards are stored separately via put_object
            orig_len: metadata.size as usize,
            meta: metadata.to_object_metadata(metadata.created_at),
        };

        if let Some(versions) = objs.get_mut(&obj_key) {
            // Check if this version already exists and update it
            if let Some(existing) = versions.iter_mut().find(|v| {
                v.meta.version_id.as_ref() == Some(&version_id.to_string())
            }) {
                existing.meta = stored_obj.meta;
            } else {
                versions.push(stored_obj);
            }
        } else {
            objs.insert(obj_key, vec![stored_obj]);
        }

        Ok(())
    }

    async fn get_version_metadata(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<VersionMetadata, StorageError> {
        let objs = self.objects.read().await;
        let obj_key = (bucket.to_string(), key.to_string());

        let versions = objs.get(&obj_key).ok_or_else(|| StorageError::ObjectNotFound {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })?;

        let stored_obj = versions
            .iter()
            .find(|v| v.meta.version_id.as_ref() == Some(&version_id.to_string()))
            .ok_or_else(|| StorageError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;

        Ok(VersionMetadata::from_object_metadata(
            version_id.to_string(),
            &stored_obj.meta,
        ))
    }

    async fn list_version_ids(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<String>, StorageError> {
        let objs = self.objects.read().await;
        let obj_key = (bucket.to_string(), key.to_string());

        if let Some(versions) = objs.get(&obj_key) {
            let mut version_ids: Vec<String> = versions
                .iter()
                .filter_map(|v| v.meta.version_id.clone())
                .collect();

            // Sort in reverse chronological order (newest first)
            version_ids.sort_by(|a, b| b.cmp(a));

            Ok(version_ids)
        } else {
            Ok(Vec::new())
        }
    }

    async fn delete_version_metadata(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<bool, StorageError> {
        let mut objs = self.objects.write().await;
        let obj_key = (bucket.to_string(), key.to_string());

        if let Some(versions) = objs.get_mut(&obj_key) {
            if let Some(pos) = versions
                .iter()
                .position(|v| v.meta.version_id.as_ref() == Some(&version_id.to_string()))
            {
                versions.remove(pos);

                // If no versions left, remove the key entirely
                if versions.is_empty() {
                    objs.remove(&obj_key);
                }

                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn get_version_data(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Bytes, StorageError> {
        let objs = self.objects.read().await;
        let obj_key = (bucket.to_string(), key.to_string());

        let versions = objs.get(&obj_key).ok_or_else(|| StorageError::ObjectNotFound {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })?;

        let stored_obj = versions
            .iter()
            .find(|v| v.meta.version_id.as_ref() == Some(&version_id.to_string()))
            .ok_or_else(|| StorageError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;

        // Decode the erasure-coded shards
        // Convert Vec<Bytes> to Vec<Option<Vec<u8>>> format expected by decode
        let mut shard_options: Vec<Option<Vec<u8>>> = stored_obj.shards
            .iter()
            .map(|b| Some(b.to_vec()))
            .collect();
        let data = self.erasure.decode(&mut shard_options, stored_obj.orig_len)?;

        Ok(Bytes::from(data))
    }
}

#[async_trait]
impl StorageBackend for InMemoryStorage {
    async fn list_buckets(&self) -> Result<Vec<String>, StorageError> {
        let b = self.buckets.read().await;
        Ok(b.iter().cloned().collect())
    }

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

    #[tracing::instrument(skip(self, data, metadata), fields(bucket = %bucket, key = %key, size = data.len()))]
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        content_type: &str,
        metadata: HashMap<String, String>,
        storage_class: Option<String>,
        server_side_encryption: Option<String>,
    ) -> Result<ObjectMetadata, StorageError> {
        let start_time = std::time::Instant::now();

        validate_bucket(bucket)?;
        validate_key(key)?;
        if content_type.is_empty() {
            return Err(StorageError::InvalidInput(
                "content_type must be non-empty".into(),
            ));
        }

        // bucket must exist - measure lock wait
        {
            let lock_start = std::time::Instant::now();
            let b = self.buckets.read().await;
            metrics::record_lock_wait("bucket_read", lock_start.elapsed().as_secs_f64());

            if !b.contains(bucket) {
                return Err(StorageError::BucketNotFound(bucket.to_string()));
            }
        }

        let size = data.len() as u64;
        let etag = compute_etag(&data);
        let (shards, orig_len) = self.erasure.encode(&data)?;
        let shards: Vec<Bytes> = shards.into_iter().map(Bytes::from).collect();

        // Check if versioning is enabled for this bucket using VersionManager
        let manager = VersionManager::new(self);
        let versioning_status = manager.get_bucket_versioning(bucket).await?;

        let version_id = if versioning_status == VersioningStatus::Enabled {
            Some(VersionManager::<InMemoryStorage>::generate_version_id())
        } else {
            None
        };

        let meta = ObjectMetadata {
            content_type: content_type.to_string(),
            etag,
            size,
            last_modified_unix_secs: Utc::now().timestamp(),
            metadata,
            storage_class,
            server_side_encryption,
            version_id,
            is_latest: true,
            is_delete_marker: false,
        };

        // measure lock wait for write
        let lock_start = std::time::Instant::now();
        let mut objs = self.objects.write().await;
        metrics::record_lock_wait("object_write", lock_start.elapsed().as_secs_f64());

        let stored_obj = StoredObject {
            shards,
            orig_len,
            meta: meta.clone(),
        };

        let obj_key = (bucket.to_string(), key.to_string());

        if versioning_status == VersioningStatus::Enabled {
            // Mark all previous versions as not latest using VersionManager
            drop(objs); // Release write lock temporarily
            manager.mark_previous_versions_not_latest(bucket, key).await?;

            // Re-acquire write lock to insert new version
            let mut objs = self.objects.write().await;
            if let Some(versions) = objs.get_mut(&obj_key) {
                versions.push(stored_obj);
            } else {
                objs.insert(obj_key, vec![stored_obj]);
            }
        } else {
            // Replace the single version
            objs.insert(obj_key, vec![stored_obj]);
        }

        // Record overall operation metrics
        let duration = start_time.elapsed().as_secs_f64();
        metrics::record_storage_op("put", "memory", duration);

        tracing::debug!(
            bucket = %bucket,
            key = %key,
            size = size,
            duration_ms = duration * 1000.0,
            "Put object completed"
        );

        Ok(meta)
    }

    #[tracing::instrument(skip(self), fields(bucket = %bucket, key = %key))]
    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(Bytes, ObjectMetadata), StorageError> {
        let start_time = std::time::Instant::now();

        validate_bucket(bucket)?;
        validate_key(key)?;

        // Measure lock wait for read
        let lock_start = std::time::Instant::now();
        let objs = self.objects.read().await;
        metrics::record_lock_wait("object_read", lock_start.elapsed().as_secs_f64());

        let versions = objs
            .get(&(bucket.to_string(), key.to_string()))
            .ok_or_else(|| StorageError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;

        // Get the latest version (handles both versioned and unversioned objects)
        let obj = versions
            .iter()
            .rev()
            .find(|v| v.meta.is_latest && !v.meta.is_delete_marker)
            .ok_or_else(|| StorageError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;

        let mut shards: Vec<Option<Vec<u8>>> =
            obj.shards.iter().map(|c| Some(c.to_vec())).collect();
        let data = self.erasure.decode(&mut shards, obj.orig_len)?;

        // Record overall operation metrics
        let duration = start_time.elapsed().as_secs_f64();
        metrics::record_storage_op("get", "memory", duration);

        tracing::debug!(
            bucket = %bucket,
            key = %key,
            size = data.len(),
            duration_ms = duration * 1000.0,
            "Get object completed"
        );

        Ok((Bytes::from(data), obj.meta.clone()))
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMetadata, StorageError> {
        validate_bucket(bucket)?;
        validate_key(key)?;
        {
            let b = self.buckets.read().await;
            if !b.contains(bucket) {
                return Err(StorageError::BucketNotFound(bucket.to_string()));
            }
        }
        let objs = self.objects.read().await;
        let versions = objs
            .get(&(bucket.to_string(), key.to_string()))
            .ok_or_else(|| StorageError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;

        // Get the latest non-delete-marker version (handles both versioned and unversioned)
        let obj = versions
            .iter()
            .rev()
            .find(|v| v.meta.is_latest && !v.meta.is_delete_marker)
            .ok_or_else(|| StorageError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;

        Ok(obj.meta.clone())
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<bool, StorageError> {
        validate_bucket(bucket)?;
        validate_key(key)?;

        // Check versioning status using VersionManager
        let manager = VersionManager::new(self);
        let versioning_status = manager.get_bucket_versioning(bucket).await?;

        if versioning_status == VersioningStatus::Enabled {
            // Use VersionManager to create a delete marker
            manager.create_delete_marker(bucket, key).await?;
            Ok(true)
        } else {
            // Permanently delete (unversioned behavior)
            let mut objs = self.objects.write().await;
            let obj_key = (bucket.to_string(), key.to_string());
            Ok(objs.remove(&obj_key).is_some())
        }
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
        for ((b, k), versions) in objs.range(start..) {
            if b != bucket {
                break;
            }
            if !k.starts_with(prefix) {
                break;
            }

            // Only include the latest non-delete-marker version
            if let Some(latest) = versions
                .iter()
                .rev()
                .find(|v| v.meta.is_latest && !v.meta.is_delete_marker)
            {
                out.push((k.clone(), latest.meta.clone()));
            }

            if out.len() >= limit {
                break;
            }
        }

        Ok(out)
    }

    async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dest_bucket: &str,
        dest_key: &str,
        content_type: &str,
        metadata: HashMap<String, String>,
        storage_class: Option<String>,
        server_side_encryption: Option<String>,
    ) -> Result<ObjectMetadata, StorageError> {
        validate_bucket(src_bucket)?;
        validate_bucket(dest_bucket)?;
        validate_key(src_key)?;
        validate_key(dest_key)?;

        let (data, _meta) = self.get_object(src_bucket, src_key).await?;
        let etag = compute_etag(&data);
        let (shards, orig_len) = self.erasure.encode(&data)?;
        let shards: Vec<Bytes> = shards.into_iter().map(Bytes::from).collect();

        // Check if versioning is enabled for destination bucket
        let versioning_status = {
            let vers = self.bucket_versioning.read().await;
            *vers.get(dest_bucket).unwrap_or(&VersioningStatus::Unversioned)
        };

        let version_id = if versioning_status == VersioningStatus::Enabled {
            Some(VersionManager::<InMemoryStorage>::generate_version_id())
        } else {
            None
        };

        let meta = ObjectMetadata {
            content_type: content_type.to_string(),
            etag,
            size: data.len() as u64,
            last_modified_unix_secs: Utc::now().timestamp(),
            metadata,
            storage_class,
            server_side_encryption,
            version_id,
            is_latest: true,
            is_delete_marker: false,
        };

        let mut objs = self.objects.write().await;
        let stored_obj = StoredObject {
            shards,
            orig_len,
            meta: meta.clone(),
        };

        let obj_key = (dest_bucket.to_string(), dest_key.to_string());

        if versioning_status == VersioningStatus::Enabled {
            // Mark all previous versions as not latest
            if let Some(versions) = objs.get_mut(&obj_key) {
                for v in versions.iter_mut() {
                    v.meta.is_latest = false;
                }
                versions.push(stored_obj);
            } else {
                objs.insert(obj_key, vec![stored_obj]);
            }
        } else {
            // Replace the single version
            objs.insert(obj_key, vec![stored_obj]);
        }

        Ok(meta)
    }

    async fn initiate_multipart(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<String, StorageError> {
        validate_bucket(bucket)?;
        validate_key(key)?;

        // Check bucket exists
        {
            let b = self.buckets.read().await;
            if !b.contains(bucket) {
                return Err(StorageError::BucketNotFound(bucket.to_string()));
            }
        }

        let manager = MultipartManager::new(self);
        let upload_id = manager.initiate_multipart(bucket, key).await?;

        tracing::debug!(
            bucket = %bucket,
            key = %key,
            upload_id = %upload_id,
            "Multipart upload initiated"
        );

        Ok(upload_id)
    }

    async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: Bytes,
    ) -> Result<String, StorageError> {
        let manager = MultipartManager::new(self);
        let etag = manager
            .upload_part(bucket, key, upload_id, part_number, data)
            .await?;

        tracing::debug!(
            bucket = %bucket,
            key = %key,
            upload_id = %upload_id,
            part_number = part_number,
            "Multipart part uploaded"
        );

        Ok(etag)
    }

    async fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<(u32, String)>,
    ) -> Result<ObjectMetadata, StorageError> {
        let manager = MultipartManager::new(self);
        let final_data = manager
            .prepare_complete_multipart(bucket, key, upload_id, parts)
            .await?;

        let result = self
            .put_object(
                bucket,
                key,
                final_data,
                "application/octet-stream",
                HashMap::new(),
                None,
                None,
            )
            .await?;

        manager.cleanup_multipart(upload_id).await?;

        tracing::debug!(
            bucket = %bucket,
            key = %key,
            upload_id = %upload_id,
            "Multipart upload completed"
        );

        Ok(result)
    }

    async fn abort_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), StorageError> {
        let manager = MultipartManager::new(self);
        manager.abort_multipart(bucket, key, upload_id).await?;

        tracing::debug!(
            bucket = %bucket,
            key = %key,
            upload_id = %upload_id,
            "Multipart upload aborted"
        );

        Ok(())
    }

    async fn get_bucket_versioning(&self, bucket: &str) -> Result<VersioningStatus, StorageError> {
        validate_bucket(bucket)?;

        // Check bucket exists
        {
            let b = self.buckets.read().await;
            if !b.contains(bucket) {
                return Err(StorageError::BucketNotFound(bucket.to_string()));
            }
        }

        // Delegate to VersionManager
        let manager = VersionManager::new(self);
        manager.get_bucket_versioning(bucket).await
    }

    async fn put_bucket_versioning(
        &self,
        bucket: &str,
        status: VersioningStatus,
    ) -> Result<(), StorageError> {
        validate_bucket(bucket)?;

        // Check bucket exists
        {
            let b = self.buckets.read().await;
            if !b.contains(bucket) {
                return Err(StorageError::BucketNotFound(bucket.to_string()));
            }
        }

        // Delegate to VersionManager
        let manager = VersionManager::new(self);
        manager.put_bucket_versioning(bucket, status).await
    }

    async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(Bytes, ObjectMetadata), StorageError> {
        validate_bucket(bucket)?;
        validate_key(key)?;

        // Delegate to VersionManager
        let manager = VersionManager::new(self);
        manager.get_object_version(bucket, key, version_id).await
    }

    async fn head_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<ObjectMetadata, StorageError> {
        validate_bucket(bucket)?;
        validate_key(key)?;

        {
            let b = self.buckets.read().await;
            if !b.contains(bucket) {
                return Err(StorageError::BucketNotFound(bucket.to_string()));
            }
        }

        // Delegate to VersionManager
        let manager = VersionManager::new(self);
        manager.head_object_version(bucket, key, version_id).await
    }

    async fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<bool, StorageError> {
        validate_bucket(bucket)?;
        validate_key(key)?;

        // Delegate to VersionManager
        let manager = VersionManager::new(self);
        manager.delete_object_version(bucket, key, version_id).await
    }

    async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<ObjectMetadata>, StorageError> {
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

        // Range-scan for keys matching the bucket and prefix
        let start = (bucket.to_string(), prefix.to_string());
        for ((b, k), versions) in objs.range(start..) {
            if b != bucket {
                break;
            }
            if !k.starts_with(prefix) {
                break;
            }

            // Include ALL versions (including delete markers) in reverse chronological order
            for version in versions.iter().rev() {
                out.push(version.meta.clone());
                if out.len() >= limit {
                    return Ok(out);
                }
            }
        }

        Ok(out)
    }

    async fn get_bucket_lifecycle(
        &self,
        bucket: &str,
    ) -> Result<Option<crate::storage::lifecycle::LifecyclePolicy>, StorageError> {
        use crate::storage::lifecycle::{LifecycleManager, LifecycleStorage};
        let manager = LifecycleManager::new(self as &dyn LifecycleStorage);
        manager.get_policy(bucket).await
    }

    async fn put_bucket_lifecycle(
        &self,
        bucket: &str,
        policy: crate::storage::lifecycle::LifecyclePolicy,
    ) -> Result<(), StorageError> {
        use crate::storage::lifecycle::{LifecycleManager, LifecycleStorage};
        let manager = LifecycleManager::new(self as &dyn LifecycleStorage);
        manager.put_policy(bucket, policy).await
    }

    async fn delete_bucket_lifecycle(&self, bucket: &str) -> Result<bool, StorageError> {
        use crate::storage::lifecycle::{LifecycleManager, LifecycleStorage};
        let manager = LifecycleManager::new(self as &dyn LifecycleStorage);
        manager.delete_policy(bucket).await
    }
}

// Implement MultipartStorage trait for low-level multipart operations
#[async_trait]
impl MultipartStorage for InMemoryStorage {
    async fn store_upload_metadata(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
    ) -> Result<(), StorageError> {
        let mut uploads = self.multipart_uploads.write().await;
        uploads.insert(
            upload_id.to_string(),
            InMemoryUpload {
                metadata: UploadMetadata {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                },
                parts: HashMap::new(),
            },
        );
        Ok(())
    }

    async fn get_upload_metadata(&self, upload_id: &str) -> Result<(String, String), StorageError> {
        let uploads = self.multipart_uploads.read().await;
        uploads
            .get(upload_id)
            .map(|u| (u.metadata.bucket.clone(), u.metadata.key.clone()))
            .ok_or_else(|| StorageError::Internal("Upload not found".to_string()))
    }

    async fn store_part(
        &self,
        upload_id: &str,
        part_number: u32,
        data: Bytes,
    ) -> Result<String, StorageError> {
        let mut uploads = self.multipart_uploads.write().await;
        let upload = uploads
            .get_mut(upload_id)
            .ok_or_else(|| StorageError::Internal("Upload not found".to_string()))?;

        let etag = compute_etag(&data);
        upload.parts.insert(
            part_number,
            PartData {
                etag: etag.clone(),
                data,
            },
        );

        Ok(etag)
    }

    async fn get_part(
        &self,
        upload_id: &str,
        part_number: u32,
    ) -> Result<(Bytes, String), StorageError> {
        let uploads = self.multipart_uploads.read().await;
        let upload = uploads
            .get(upload_id)
            .ok_or_else(|| StorageError::Internal("Upload not found".to_string()))?;

        upload
            .parts
            .get(&part_number)
            .map(|p| (p.data.clone(), p.etag.clone()))
            .ok_or_else(|| StorageError::Internal(format!("Part {} not found", part_number)))
    }

    async fn list_parts(&self, upload_id: &str) -> Result<Vec<(u32, String)>, StorageError> {
        let uploads = self.multipart_uploads.read().await;
        let upload = uploads
            .get(upload_id)
            .ok_or_else(|| StorageError::Internal("Upload not found".to_string()))?;

        Ok(upload
            .parts
            .iter()
            .map(|(num, part)| (*num, part.etag.clone()))
            .collect())
    }

    async fn remove_upload(&self, upload_id: &str) -> Result<(), StorageError> {
        let mut uploads = self.multipart_uploads.write().await;
        uploads.remove(upload_id);
        Ok(())
    }

    async fn upload_exists(&self, upload_id: &str) -> bool {
        let uploads = self.multipart_uploads.read().await;
        uploads.contains_key(upload_id)
    }
}

// ============================================================================
// LifecycleStorage Implementation
// ============================================================================

#[async_trait]
impl crate::storage::lifecycle::LifecycleStorage for InMemoryStorage {
    async fn read_lifecycle_policy(
        &self,
        bucket: &str,
    ) -> Result<Option<crate::storage::lifecycle::LifecyclePolicy>, StorageError> {
        let policies = self.lifecycle_policies.read().await;
        Ok(policies.get(bucket).cloned())
    }

    async fn write_lifecycle_policy(
        &self,
        bucket: &str,
        policy: crate::storage::lifecycle::LifecyclePolicy,
    ) -> Result<(), StorageError> {
        // Validate bucket exists
        let buckets = self.buckets.read().await;
        if !buckets.contains(bucket) {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        drop(buckets);

        let mut policies = self.lifecycle_policies.write().await;
        policies.insert(bucket.to_string(), policy);
        Ok(())
    }

    async fn delete_lifecycle_policy(&self, bucket: &str) -> Result<bool, StorageError> {
        let mut policies = self.lifecycle_policies.write().await;
        Ok(policies.remove(bucket).is_some())
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
    async fn test_list_buckets() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("b1").await.unwrap();
        storage.create_bucket("b2").await.unwrap();

        let buckets = storage.list_buckets().await.unwrap();
        assert_eq!(buckets, vec!["b1".to_string(), "b2".to_string()]);
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
            .put_object(
                "bucket1",
                "key1",
                Bytes::from("data"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
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
            .put_object(
                "bucket1",
                "key1",
                data.clone(),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
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
            .put_object(
                "nonexistent",
                "key1",
                Bytes::from("data"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await;
        assert!(matches!(result, Err(StorageError::BucketNotFound(_))));
    }

    #[tokio::test]
    async fn test_put_object_empty_key() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        let result = storage
            .put_object(
                "bucket1",
                "",
                Bytes::from("data"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    #[tokio::test]
    async fn test_put_object_empty_content_type() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        let result = storage
            .put_object("bucket1", "key1", Bytes::from("data"), "", HashMap::new(), None, None)
            .await;
        assert!(matches!(result, Err(StorageError::InvalidInput(_))));
    }

    #[tokio::test]
    async fn test_put_object_overwrite() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();

        let meta1 = storage
            .put_object(
                "bucket1",
                "key1",
                Bytes::from("data1"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();
        let meta2 = storage
            .put_object(
                "bucket1",
                "key1",
                Bytes::from("data2"),
                "text/html",
                HashMap::new(),
                None,
                None,
            )
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
            .put_object(
                "bucket1",
                "key1",
                original_data.clone(),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
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
            .put_object(
                "bucket1",
                "key1",
                Bytes::from("data"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
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

        let deleted = storage
            .delete_object("bucket1", "nonexistent")
            .await
            .unwrap();
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
            .put_object(
                "bucket1",
                "key1",
                Bytes::from("data1"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();
        storage
            .put_object(
                "bucket1",
                "key2",
                Bytes::from("data2"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();
        storage
            .put_object(
                "bucket1",
                "key3",
                Bytes::from("data3"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
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
            .put_object(
                "bucket1",
                "photos/2023/1.jpg",
                Bytes::from("data"),
                "image/jpeg",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();
        storage
            .put_object(
                "bucket1",
                "photos/2024/1.jpg",
                Bytes::from("data"),
                "image/jpeg",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();
        storage
            .put_object(
                "bucket1",
                "docs/readme.txt",
                Bytes::from("data"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();

        let objects = storage
            .list_objects("bucket1", "photos/", 100)
            .await
            .unwrap();
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
                    HashMap::new(),
                    None,
                    None,
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
            .put_object(
                "bucket1",
                "key1",
                Bytes::from("data"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
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
        storage
            .put_object(
                "bucket1",
                "zebra",
                Bytes::from("data"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();
        storage
            .put_object(
                "bucket1",
                "apple",
                Bytes::from("data"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();
        storage
            .put_object(
                "bucket1",
                "middle",
                Bytes::from("data"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();

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
            .put_object(
                "bucket1",
                "key1",
                data.clone(),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
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
            .put_object(
                "bucket1",
                "shared-key",
                Bytes::from("data1"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();
        storage
            .put_object(
                "bucket2",
                "shared-key",
                Bytes::from("data2"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();

        let (data1, _) = storage.get_object("bucket1", "shared-key").await.unwrap();
        let (data2, _) = storage.get_object("bucket2", "shared-key").await.unwrap();

        assert_eq!(&data1[..], b"data1");
        assert_eq!(&data2[..], b"data2");
    }

    #[tokio::test]
    async fn test_erasure_encode_decode_roundtrip() {
        let erasure = Erasure::new(4, 1, ERASURE_CHUNK_SIZE).unwrap();
        let data = Bytes::from(&b"erasure-coded-data"[..]);
        let (mut shards, orig_len) = erasure.encode(&data).unwrap();
        let mut opts: Vec<Option<Vec<u8>>> = shards.drain(..).map(Some).collect();
        let recovered = erasure.decode(&mut opts, orig_len).unwrap();
        assert_eq!(Bytes::from(recovered), data);
    }

    #[tokio::test]
    async fn test_erasure_recover_single_missing_chunk() {
        let erasure = Erasure::new(4, 1, ERASURE_CHUNK_SIZE).unwrap();
        let data = Bytes::from(vec![1u8; ERASURE_CHUNK_SIZE * 2 + 10]);
        let (shards, orig_len) = erasure.encode(&data).unwrap();
        let mut opt_shards: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        opt_shards[1] = None;
        let recovered = erasure.decode(&mut opt_shards, orig_len).unwrap();
        assert_eq!(Bytes::from(recovered), data);
    }

    // Versioning tests
    #[tokio::test]
    async fn test_versioning_unversioned_by_default() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();

        let status = storage.get_bucket_versioning("bucket1").await.unwrap();
        assert_eq!(status, VersioningStatus::Unversioned);
    }

    #[tokio::test]
    async fn test_versioning_enable() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();

        storage.put_bucket_versioning("bucket1", VersioningStatus::Enabled).await.unwrap();
        let status = storage.get_bucket_versioning("bucket1").await.unwrap();
        assert_eq!(status, VersioningStatus::Enabled);
    }

    #[tokio::test]
    async fn test_versioning_creates_versions() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        storage.put_bucket_versioning("bucket1", VersioningStatus::Enabled).await.unwrap();

        // Put object twice
        let meta1 = storage
            .put_object(
                "bucket1",
                "key1",
                Bytes::from("version1"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();

        let meta2 = storage
            .put_object(
                "bucket1",
                "key1",
                Bytes::from("version2"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();

        // Both should have version IDs
        assert!(meta1.version_id.is_some());
        assert!(meta2.version_id.is_some());
        assert_ne!(meta1.version_id, meta2.version_id);

        // Latest should be is_latest
        assert!(meta2.is_latest);

        // Getting the object should return version2
        let (data, _) = storage.get_object("bucket1", "key1").await.unwrap();
        assert_eq!(&data[..], b"version2");
    }

    #[tokio::test]
    async fn test_versioning_get_specific_version() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        storage.put_bucket_versioning("bucket1", VersioningStatus::Enabled).await.unwrap();

        let meta1 = storage
            .put_object(
                "bucket1",
                "key1",
                Bytes::from("version1"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();

        storage
            .put_object(
                "bucket1",
                "key1",
                Bytes::from("version2"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();

        // Get the first version specifically
        let version_id = meta1.version_id.unwrap();
        let (data, meta) = storage
            .get_object_version("bucket1", "key1", &version_id)
            .await
            .unwrap();

        assert_eq!(&data[..], b"version1");
        assert_eq!(meta.version_id.as_ref(), Some(&version_id));
        assert!(!meta.is_latest);
    }

    #[tokio::test]
    async fn test_versioning_delete_marker() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        storage.put_bucket_versioning("bucket1", VersioningStatus::Enabled).await.unwrap();

        storage
            .put_object(
                "bucket1",
                "key1",
                Bytes::from("data"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();

        // Delete should create a delete marker
        let deleted = storage.delete_object("bucket1", "key1").await.unwrap();
        assert!(deleted);

        // Getting the object should now return NotFound
        let result = storage.get_object("bucket1", "key1").await;
        assert!(matches!(result, Err(StorageError::ObjectNotFound { .. })));

        // List versions should show both the object and delete marker
        let versions = storage
            .list_object_versions("bucket1", "", 100)
            .await
            .unwrap();
        assert_eq!(versions.len(), 2);
        assert!(versions[0].is_delete_marker);
        assert!(!versions[1].is_delete_marker);
    }

    #[tokio::test]
    async fn test_versioning_delete_specific_version() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        storage.put_bucket_versioning("bucket1", VersioningStatus::Enabled).await.unwrap();

        let meta1 = storage
            .put_object(
                "bucket1",
                "key1",
                Bytes::from("version1"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();

        storage
            .put_object(
                "bucket1",
                "key1",
                Bytes::from("version2"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();

        // Delete specific version
        let version_id = meta1.version_id.unwrap();
        let deleted = storage
            .delete_object_version("bucket1", "key1", &version_id)
            .await
            .unwrap();
        assert!(deleted);

        // Try to get that version - should fail
        let result = storage
            .get_object_version("bucket1", "key1", &version_id)
            .await;
        assert!(matches!(result, Err(StorageError::ObjectNotFound { .. })));

        // Latest version should still be available
        let (data, _) = storage.get_object("bucket1", "key1").await.unwrap();
        assert_eq!(&data[..], b"version2");
    }

    #[tokio::test]
    async fn test_versioning_list_all_versions() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        storage.put_bucket_versioning("bucket1", VersioningStatus::Enabled).await.unwrap();

        // Create multiple versions
        for i in 1..=3 {
            storage
                .put_object(
                    "bucket1",
                    "key1",
                    Bytes::from(format!("version{}", i)),
                    "text/plain",
                    HashMap::new(),
                    None,
                    None,
                )
                .await
                .unwrap();
        }

        let versions = storage
            .list_object_versions("bucket1", "", 100)
            .await
            .unwrap();

        assert_eq!(versions.len(), 3);
        // Should be in reverse chronological order (newest first)
        assert!(versions[0].is_latest);
        assert!(!versions[1].is_latest);
        assert!(!versions[2].is_latest);
    }

    #[tokio::test]
    async fn test_versioning_unversioned_overwrites() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        // Don't enable versioning

        let meta1 = storage
            .put_object(
                "bucket1",
                "key1",
                Bytes::from("version1"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();

        let meta2 = storage
            .put_object(
                "bucket1",
                "key1",
                Bytes::from("version2"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();

        // Neither should have version IDs
        assert!(meta1.version_id.is_none());
        assert!(meta2.version_id.is_none());

        // Only one version should exist
        let versions = storage
            .list_object_versions("bucket1", "", 100)
            .await
            .unwrap();
        assert_eq!(versions.len(), 1);

        // Get should return version2
        let (data, _) = storage.get_object("bucket1", "key1").await.unwrap();
        assert_eq!(&data[..], b"version2");
    }

    #[tokio::test]
    async fn test_versioning_head_object_version() {
        let storage = InMemoryStorage::new();
        storage.create_bucket("bucket1").await.unwrap();
        storage.put_bucket_versioning("bucket1", VersioningStatus::Enabled).await.unwrap();

        let meta1 = storage
            .put_object(
                "bucket1",
                "key1",
                Bytes::from("version1"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();

        let version_id = meta1.version_id.as_ref().unwrap();
        let head_meta = storage
            .head_object_version("bucket1", "key1", version_id)
            .await
            .unwrap();

        assert_eq!(head_meta.version_id.as_ref(), Some(version_id));
        assert_eq!(head_meta.size, 8); // "version1" length
    }
}
