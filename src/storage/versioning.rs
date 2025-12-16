//! Versioning module for object version management.
//!
//! This module provides a trait-based abstraction for implementing S3-compatible
//! object versioning across different storage backends. It follows the manager pattern
//! similar to the multipart module.
//!
//! # Architecture
//!
//! - `VersionStorage` trait: Low-level storage operations that backends must implement
//! - `VersionManager`: High-level orchestration and business logic
//! - `VersionMetadata`: Shared data structure for version information
//!
//! # Example
//!
//! ```rust,ignore
//! use crate::storage::versioning::{VersionManager, VersionStorage};
//!
//! // Create a version manager for a storage backend
//! let manager = VersionManager::new(&my_storage);
//!
//! // Check if versioning is enabled
//! let status = manager.get_bucket_versioning("my-bucket").await?;
//!
//! // Generate a new version ID
//! let version_id = VersionManager::generate_version_id();
//! ```

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{ObjectMetadata, StorageError, VersioningStatus};

/// Version metadata structure used internally by the versioning system.
///
/// This structure contains all information needed to track a specific version
/// of an object, including whether it's a delete marker and metadata fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionMetadata {
    /// Unique version identifier (timestamp-based)
    pub version_id: String,

    /// Whether this is the latest version of the object
    pub is_latest: bool,

    /// Whether this version is a delete marker
    pub is_delete_marker: bool,

    /// When this version was created (Unix timestamp)
    pub created_at: i64,

    /// Object content type (empty for delete markers)
    pub content_type: String,

    /// Object ETag (empty for delete markers)
    pub etag: String,

    /// Object size in bytes (0 for delete markers)
    pub size: u64,

    /// User-defined metadata (empty for delete markers)
    pub metadata: HashMap<String, String>,

    /// Storage class (optional)
    pub storage_class: Option<String>,

    /// Server-side encryption (optional)
    pub server_side_encryption: Option<String>,
}

impl VersionMetadata {
    /// Convert version metadata to object metadata.
    ///
    /// # Arguments
    ///
    /// * `last_modified` - The last modified timestamp to use (usually created_at)
    pub fn to_object_metadata(&self, last_modified: i64) -> ObjectMetadata {
        ObjectMetadata {
            content_type: self.content_type.clone(),
            etag: self.etag.clone(),
            size: self.size,
            last_modified_unix_secs: last_modified,
            metadata: self.metadata.clone(),
            storage_class: self.storage_class.clone(),
            server_side_encryption: self.server_side_encryption.clone(),
            version_id: Some(self.version_id.clone()),
            is_latest: self.is_latest,
            is_delete_marker: self.is_delete_marker,
        }
    }

    /// Create a delete marker version metadata.
    ///
    /// Delete markers are special versions that indicate the object has been deleted
    /// in a versioned bucket. They have no content and are marked with `is_delete_marker: true`.
    ///
    /// # Arguments
    ///
    /// * `version_id` - The version ID for this delete marker
    pub fn create_delete_marker(version_id: String) -> Self {
        Self {
            version_id,
            is_latest: true,
            is_delete_marker: true,
            created_at: Utc::now().timestamp(),
            content_type: String::new(),
            etag: String::new(),
            size: 0,
            metadata: HashMap::new(),
            storage_class: None,
            server_side_encryption: None,
        }
    }

    /// Create version metadata from object metadata.
    ///
    /// # Arguments
    ///
    /// * `version_id` - The version ID for this version
    /// * `obj_meta` - The object metadata to convert from
    pub fn from_object_metadata(version_id: String, obj_meta: &ObjectMetadata) -> Self {
        Self {
            version_id,
            is_latest: obj_meta.is_latest,
            is_delete_marker: obj_meta.is_delete_marker,
            created_at: obj_meta.last_modified_unix_secs,
            content_type: obj_meta.content_type.clone(),
            etag: obj_meta.etag.clone(),
            size: obj_meta.size,
            metadata: obj_meta.metadata.clone(),
            storage_class: obj_meta.storage_class.clone(),
            server_side_encryption: obj_meta.server_side_encryption.clone(),
        }
    }
}

/// Low-level trait for version storage operations.
///
/// Storage backends must implement this trait to provide versioning support.
/// The trait focuses on version metadata management; actual object data storage
/// is handled by the backend's existing put_object/get_object methods.
///
/// # Implementation Notes
///
/// - `read_versioning_status` and `write_versioning_status` manage bucket-level versioning state
/// - Version metadata operations store/retrieve version information without the actual data
/// - Version IDs should be sortable (timestamp-based recommended)
/// - Implementations should handle concurrent access appropriately
#[async_trait]
pub trait VersionStorage: Send + Sync {
    /// Read the versioning status for a bucket.
    ///
    /// Returns `Unversioned` if versioning has never been configured for the bucket.
    async fn read_versioning_status(
        &self,
        bucket: &str,
    ) -> Result<VersioningStatus, StorageError>;

    /// Write the versioning status for a bucket.
    ///
    /// This enables, suspends, or explicitly sets versioning to unversioned for a bucket.
    async fn write_versioning_status(
        &self,
        bucket: &str,
        status: VersioningStatus,
    ) -> Result<(), StorageError>;

    /// Store metadata for a specific version.
    ///
    /// This stores version metadata without the actual object data.
    /// The data itself should be stored through the backend's normal put_object flow.
    async fn store_version_metadata(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        metadata: VersionMetadata,
    ) -> Result<(), StorageError>;

    /// Get metadata for a specific version.
    ///
    /// Returns an error if the version doesn't exist.
    async fn get_version_metadata(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<VersionMetadata, StorageError>;

    /// List all version IDs for an object.
    ///
    /// Version IDs should be returned in reverse chronological order (newest first).
    /// Returns an empty vector if the object has no versions.
    async fn list_version_ids(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<String>, StorageError>;

    /// Delete metadata for a specific version.
    ///
    /// This removes the version metadata. The actual data cleanup is backend-specific.
    /// Returns `true` if the version was deleted, `false` if it didn't exist.
    async fn delete_version_metadata(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<bool, StorageError>;

    /// Get the data for a specific version.
    ///
    /// This delegates to the backend's existing data retrieval mechanism.
    /// Returns the raw bytes for the version.
    async fn get_version_data(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Bytes, StorageError>;
}

/// High-level version manager for orchestrating versioning operations.
///
/// The manager provides business logic and coordination for versioning operations,
/// delegating low-level storage operations to the underlying storage backend through
/// the `VersionStorage` trait.
///
/// # Type Parameters
///
/// * `S` - Storage backend implementing `VersionStorage`
///
/// # Example
///
/// ```rust,ignore
/// let manager = VersionManager::new(&storage);
/// let version_id = VersionManager::generate_version_id();
/// manager.create_delete_marker("bucket", "key").await?;
/// ```
pub struct VersionManager<'a, S> {
    storage: &'a S,
}

impl<'a, S: VersionStorage> VersionManager<'a, S> {
    /// Create a new version manager with a reference to a storage backend.
    ///
    /// # Arguments
    ///
    /// * `storage` - Reference to a storage backend implementing VersionStorage
    pub fn new(storage: &'a S) -> Self {
        Self { storage }
    }

    /// Generate a unique version ID.
    ///
    /// Version IDs are timestamp-based with nanosecond precision to ensure uniqueness
    /// and provide chronological ordering.
    ///
    /// Format: `{unix_timestamp}{nanoseconds:09}`
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let version_id = VersionManager::<MyStorage>::generate_version_id();
    /// // Returns something like: "1734364800123456789"
    /// ```
    pub fn generate_version_id() -> String {
        let now = Utc::now();
        format!("{}{:09}", now.timestamp(), now.timestamp_subsec_nanos())
    }

    /// Get the versioning status for a bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    ///
    /// # Returns
    ///
    /// The versioning status (Unversioned, Enabled, or Suspended)
    pub async fn get_bucket_versioning(
        &self,
        bucket: &str,
    ) -> Result<VersioningStatus, StorageError> {
        self.storage.read_versioning_status(bucket).await
    }

    /// Set the versioning status for a bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `status` - New versioning status
    pub async fn put_bucket_versioning(
        &self,
        bucket: &str,
        status: VersioningStatus,
    ) -> Result<(), StorageError> {
        self.storage.write_versioning_status(bucket, status).await
    }

    /// Create a delete marker for an object.
    ///
    /// Delete markers are special versions that indicate an object has been deleted
    /// in a versioned bucket. They have no actual data.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    ///
    /// # Returns
    ///
    /// The version ID of the created delete marker
    pub async fn create_delete_marker(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<String, StorageError> {
        let version_id = Self::generate_version_id();
        let delete_marker = VersionMetadata::create_delete_marker(version_id.clone());

        // Mark previous versions as not latest
        self.mark_previous_versions_not_latest(bucket, key).await?;

        // Store the delete marker metadata
        self.storage
            .store_version_metadata(bucket, key, &version_id, delete_marker)
            .await?;

        Ok(version_id)
    }

    /// Find the latest non-delete-marker version of an object.
    ///
    /// This searches through all versions in reverse chronological order and returns
    /// the first non-delete-marker version found.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    ///
    /// # Returns
    ///
    /// - `Ok(Some(metadata))` if a non-delete-marker version is found
    /// - `Ok(None)` if only delete markers exist or no versions exist
    pub async fn find_latest_version(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<VersionMetadata>, StorageError> {
        let version_ids = self.storage.list_version_ids(bucket, key).await?;

        for version_id in version_ids {
            match self.storage.get_version_metadata(bucket, key, &version_id).await {
                Ok(metadata) => {
                    if !metadata.is_delete_marker {
                        return Ok(Some(metadata));
                    }
                }
                Err(StorageError::ObjectNotFound { .. }) => {
                    // Version metadata not found, skip to next
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(None)
    }

    /// Get a specific version of an object (data + metadata).
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `version_id` - Version ID to retrieve
    ///
    /// # Returns
    ///
    /// A tuple of (data, metadata) for the version
    ///
    /// # Errors
    ///
    /// Returns `ObjectNotFound` if the version doesn't exist or is a delete marker
    pub async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(Bytes, ObjectMetadata), StorageError> {
        let metadata = self.storage.get_version_metadata(bucket, key, version_id).await?;

        // Don't return delete markers
        if metadata.is_delete_marker {
            return Err(StorageError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        let data = self.storage.get_version_data(bucket, key, version_id).await?;
        let obj_metadata = metadata.to_object_metadata(metadata.created_at);

        Ok((data, obj_metadata))
    }

    /// Mark all previous versions of an object as not latest.
    ///
    /// This is called before creating a new version to ensure only one version
    /// has `is_latest: true`.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    pub async fn mark_previous_versions_not_latest(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(), StorageError> {
        let version_ids = self.storage.list_version_ids(bucket, key).await?;

        for version_id in version_ids {
            if let Ok(mut metadata) = self.storage.get_version_metadata(bucket, key, &version_id).await {
                if metadata.is_latest {
                    metadata.is_latest = false;
                    self.storage
                        .store_version_metadata(bucket, key, &version_id, metadata)
                        .await?;
                }
            }
        }

        Ok(())
    }

    /// List all versions for objects with a given prefix.
    ///
    /// This is a high-level operation that aggregates versions across multiple objects.
    /// It's typically used for ListObjectVersions API operations.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `prefix` - Key prefix to filter objects
    /// * `limit` - Maximum number of versions to return
    ///
    /// # Returns
    ///
    /// A vector of ObjectMetadata for all matching versions
    /// Get metadata for a specific object version
    pub async fn head_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<ObjectMetadata, StorageError> {
        let version_meta = self.storage.get_version_metadata(bucket, key, version_id).await?;
        Ok(version_meta.to_object_metadata(version_meta.created_at))
    }

    /// Permanently delete a specific object version
    pub async fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<bool, StorageError> {
        self.storage.delete_version_metadata(bucket, key, version_id).await
    }

    pub async fn list_all_versions(
        &self,
        bucket: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<ObjectMetadata>, StorageError> {
        // This method is implemented by backends since they need to traverse
        // their object namespace. This is a placeholder showing the interface.
        // Backends will implement this in their list_object_versions method.
        let _ = (bucket, prefix, limit);
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_generate_version_id_format() {
        let version_id = VersionManager::<MockStorage>::generate_version_id();

        // Should be a string of digits
        assert!(version_id.chars().all(|c| c.is_ascii_digit()));

        // Should be reasonable length (timestamp + nanoseconds = ~19 digits)
        assert!(version_id.len() >= 19 && version_id.len() <= 20);
    }

    #[test]
    fn test_generate_version_id_unique() {
        let id1 = VersionManager::<MockStorage>::generate_version_id();
        thread::sleep(Duration::from_nanos(100));
        let id2 = VersionManager::<MockStorage>::generate_version_id();

        assert_ne!(id1, id2, "Version IDs should be unique");
    }

    #[test]
    fn test_generate_version_id_ordered() {
        let id1 = VersionManager::<MockStorage>::generate_version_id();
        thread::sleep(Duration::from_millis(1));
        let id2 = VersionManager::<MockStorage>::generate_version_id();

        // Later IDs should be lexicographically greater (for sorting)
        assert!(id2 > id1, "Later version IDs should sort after earlier ones");
    }

    #[test]
    fn test_delete_marker_creation() {
        let version_id = "test-version-123".to_string();
        let marker = VersionMetadata::create_delete_marker(version_id.clone());

        assert_eq!(marker.version_id, version_id);
        assert!(marker.is_delete_marker);
        assert!(marker.is_latest);
        assert_eq!(marker.size, 0);
        assert_eq!(marker.content_type, "");
        assert_eq!(marker.etag, "");
        assert!(marker.metadata.is_empty());
    }

    #[test]
    fn test_version_metadata_to_object_metadata() {
        let mut metadata_map = HashMap::new();
        metadata_map.insert("key1".to_string(), "value1".to_string());

        let version_meta = VersionMetadata {
            version_id: "v123".to_string(),
            is_latest: true,
            is_delete_marker: false,
            created_at: 1234567890,
            content_type: "text/plain".to_string(),
            etag: "abc123".to_string(),
            size: 1024,
            metadata: metadata_map.clone(),
            storage_class: Some("STANDARD".to_string()),
            server_side_encryption: None,
        };

        let obj_meta = version_meta.to_object_metadata(1234567890);

        assert_eq!(obj_meta.version_id, Some("v123".to_string()));
        assert_eq!(obj_meta.is_latest, true);
        assert_eq!(obj_meta.is_delete_marker, false);
        assert_eq!(obj_meta.content_type, "text/plain");
        assert_eq!(obj_meta.etag, "abc123");
        assert_eq!(obj_meta.size, 1024);
        assert_eq!(obj_meta.last_modified_unix_secs, 1234567890);
        assert_eq!(obj_meta.metadata, metadata_map);
        assert_eq!(obj_meta.storage_class, Some("STANDARD".to_string()));
    }

    #[test]
    fn test_from_object_metadata() {
        let mut metadata_map = HashMap::new();
        metadata_map.insert("key1".to_string(), "value1".to_string());

        let obj_meta = ObjectMetadata {
            content_type: "application/json".to_string(),
            etag: "def456".to_string(),
            size: 2048,
            last_modified_unix_secs: 9876543210,
            metadata: metadata_map.clone(),
            storage_class: Some("GLACIER".to_string()),
            server_side_encryption: Some("AES256".to_string()),
            version_id: Some("old-version".to_string()),
            is_latest: false,
            is_delete_marker: false,
        };

        let version_id = "new-version".to_string();
        let version_meta = VersionMetadata::from_object_metadata(version_id.clone(), &obj_meta);

        assert_eq!(version_meta.version_id, version_id);
        assert_eq!(version_meta.is_latest, false);  // Preserves is_latest from obj_meta
        assert_eq!(version_meta.is_delete_marker, false);
        assert_eq!(version_meta.content_type, "application/json");
        assert_eq!(version_meta.etag, "def456");
        assert_eq!(version_meta.size, 2048);
        assert_eq!(version_meta.metadata, metadata_map);
        assert_eq!(version_meta.storage_class, Some("GLACIER".to_string()));
        assert_eq!(version_meta.server_side_encryption, Some("AES256".to_string()));
    }

    // Mock storage for testing
    struct MockStorage;

    #[async_trait]
    impl VersionStorage for MockStorage {
        async fn read_versioning_status(&self, _bucket: &str) -> Result<VersioningStatus, StorageError> {
            Ok(VersioningStatus::Unversioned)
        }

        async fn write_versioning_status(&self, _bucket: &str, _status: VersioningStatus) -> Result<(), StorageError> {
            Ok(())
        }

        async fn store_version_metadata(&self, _bucket: &str, _key: &str, _version_id: &str, _metadata: VersionMetadata) -> Result<(), StorageError> {
            Ok(())
        }

        async fn get_version_metadata(&self, _bucket: &str, _key: &str, _version_id: &str) -> Result<VersionMetadata, StorageError> {
            Err(StorageError::ObjectNotFound {
                bucket: "test".to_string(),
                key: "test".to_string(),
            })
        }

        async fn list_version_ids(&self, _bucket: &str, _key: &str) -> Result<Vec<String>, StorageError> {
            Ok(Vec::new())
        }

        async fn delete_version_metadata(&self, _bucket: &str, _key: &str, _version_id: &str) -> Result<bool, StorageError> {
            Ok(false)
        }

        async fn get_version_data(&self, _bucket: &str, _key: &str, _version_id: &str) -> Result<Bytes, StorageError> {
            Ok(Bytes::new())
        }
    }
}
