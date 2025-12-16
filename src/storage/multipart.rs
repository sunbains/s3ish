use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

use super::StorageError;

/// Low-level trait for multipart upload storage operations.
/// Implementations handle the storage-specific details of storing
/// multipart upload metadata and parts.
#[async_trait]
pub trait MultipartStorage: Send + Sync {
    /// Store metadata for a new multipart upload
    async fn store_upload_metadata(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
    ) -> Result<(), StorageError>;

    /// Get metadata for an existing multipart upload
    /// Returns (bucket, key)
    async fn get_upload_metadata(&self, upload_id: &str) -> Result<(String, String), StorageError>;

    /// Store a part and return its ETag
    async fn store_part(
        &self,
        upload_id: &str,
        part_number: u32,
        data: Bytes,
    ) -> Result<String, StorageError>;

    /// Get a part's data and ETag
    /// Returns (data, etag)
    async fn get_part(
        &self,
        upload_id: &str,
        part_number: u32,
    ) -> Result<(Bytes, String), StorageError>;

    /// List all parts for an upload
    /// Returns Vec of (part_number, etag)
    async fn list_parts(&self, upload_id: &str) -> Result<Vec<(u32, String)>, StorageError>;

    /// Remove an upload and all its parts
    async fn remove_upload(&self, upload_id: &str) -> Result<(), StorageError>;

    /// Check if an upload exists
    async fn upload_exists(&self, upload_id: &str) -> bool;
}

/// Common multipart upload orchestration logic.
/// Uses the MultipartStorage trait for storage operations and provides
/// the high-level multipart upload workflow.
pub struct MultipartManager<'a, S> {
    storage: &'a S,
}

impl<'a, S: MultipartStorage> MultipartManager<'a, S> {
    pub fn new(storage: &'a S) -> Self {
        Self { storage }
    }

    /// Initiate a new multipart upload
    pub async fn initiate_multipart(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<String, StorageError> {
        let upload_id = Uuid::new_v4().simple().to_string();
        self.storage
            .store_upload_metadata(&upload_id, bucket, key)
            .await?;
        Ok(upload_id)
    }

    /// Upload a part
    pub async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: Bytes,
    ) -> Result<String, StorageError> {
        // Verify upload exists and matches bucket/key
        let (stored_bucket, stored_key) = self
            .storage
            .get_upload_metadata(upload_id)
            .await
            .map_err(|_| StorageError::NoSuchUpload {
                bucket: bucket.to_string(),
                key: key.to_string(),
                upload_id: upload_id.to_string(),
            })?;

        if stored_bucket != bucket || stored_key != key {
            return Err(StorageError::NoSuchUpload {
                bucket: bucket.to_string(),
                key: key.to_string(),
                upload_id: upload_id.to_string(),
            });
        }

        self.storage.store_part(upload_id, part_number, data).await
    }

    /// Complete a multipart upload
    /// If parts is empty, all uploaded parts will be used
    /// Returns the concatenated data for the storage backend to finalize
    pub async fn prepare_complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<(u32, String)>,
    ) -> Result<Bytes, StorageError> {
        // Verify upload exists and matches bucket/key
        let (stored_bucket, stored_key) = self
            .storage
            .get_upload_metadata(upload_id)
            .await
            .map_err(|_| StorageError::NoSuchUpload {
                bucket: bucket.to_string(),
                key: key.to_string(),
                upload_id: upload_id.to_string(),
            })?;

        if stored_bucket != bucket || stored_key != key {
            return Err(StorageError::NoSuchUpload {
                bucket: bucket.to_string(),
                key: key.to_string(),
                upload_id: upload_id.to_string(),
            });
        }

        // Determine which parts to use
        let parts_to_use = if parts.is_empty() {
            let all_parts = self.storage.list_parts(upload_id).await?;
            if all_parts.is_empty() {
                return Err(StorageError::InvalidPart(
                    "No parts uploaded for completion".to_string(),
                ));
            }
            all_parts
        } else {
            parts.clone()
        };

        // Sort parts by number
        let mut sorted_parts = parts_to_use;
        sorted_parts.sort_by_key(|(n, _)| *n);

        // Validate parts and concatenate data
        let mut combined = BytesMut::new();
        let validate_etags = !parts.is_empty();

        for (part_num, expected_etag) in &sorted_parts {
            let (data, actual_etag) = self
                .storage
                .get_part(upload_id, *part_num)
                .await
                .map_err(|_| {
                    StorageError::InvalidPart(format!(
                        "Part {} not found for upload {}",
                        part_num, upload_id
                    ))
                })?;

            // Validate ETag if parts were explicitly specified
            if validate_etags
                && actual_etag.trim_matches('"') != expected_etag.trim_matches('"') {
                    return Err(StorageError::InvalidPart(format!(
                        "ETag mismatch for part {}: expected {}, got {}",
                        part_num, expected_etag, actual_etag
                    )));
                }

            combined.extend_from_slice(&data);
        }

        // Return the concatenated data
        Ok(combined.freeze())
    }

    /// Cleanup multipart upload after completion
    pub async fn cleanup_multipart(&self, upload_id: &str) -> Result<(), StorageError> {
        self.storage.remove_upload(upload_id).await
    }

    /// Abort a multipart upload
    pub async fn abort_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), StorageError> {
        // Verify upload exists and matches bucket/key
        let (stored_bucket, stored_key) = self
            .storage
            .get_upload_metadata(upload_id)
            .await
            .map_err(|_| StorageError::NoSuchUpload {
                bucket: bucket.to_string(),
                key: key.to_string(),
                upload_id: upload_id.to_string(),
            })?;

        if stored_bucket != bucket || stored_key != key {
            return Err(StorageError::NoSuchUpload {
                bucket: bucket.to_string(),
                key: key.to_string(),
                upload_id: upload_id.to_string(),
            });
        }

        self.storage.remove_upload(upload_id).await
    }
}

/// Common multipart upload metadata structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadMetadata {
    pub bucket: String,
    pub key: String,
}

/// Common multipart part structure
#[derive(Debug, Clone)]
pub struct PartData {
    pub etag: String,
    pub data: Bytes,
}

/// In-memory multipart upload state
#[derive(Debug, Clone)]
pub struct InMemoryUpload {
    pub metadata: UploadMetadata,
    pub parts: HashMap<u32, PartData>,
}
