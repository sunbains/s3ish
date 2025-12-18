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

use async_trait::async_trait;
use std::{collections::HashMap, any::Any};
use thiserror::Error;

pub mod bucket_policy;
pub mod common;
pub mod erasure;
pub mod file_storage;
pub mod in_memory;
pub mod lifecycle;
pub mod lifecycle_executor;
pub mod metadata;
pub mod multipart;
pub mod versioning;
pub mod writeback_cache;

#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    pub content_type: String,
    pub etag: String,
    pub size: u64,
    pub last_modified_unix_secs: i64,
    pub metadata: HashMap<String, String>,
    pub storage_class: Option<String>,
    pub server_side_encryption: Option<String>,
    pub version_id: Option<String>,
    pub is_latest: bool,
    pub is_delete_marker: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersioningStatus {
    Unversioned,
    Enabled,
    Suspended,
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
    #[error("multipart upload not found: {bucket}/{key} upload_id={upload_id}")]
    NoSuchUpload {
        bucket: String,
        key: String,
        upload_id: String,
    },
    #[error("invalid part: {0}")]
    InvalidPart(String),
}

#[async_trait]
#[allow(clippy::too_many_arguments)]
pub trait StorageBackend: Send + Sync + 'static {
    /// Downcast hook for optional extensions (e.g., sendfile support).
    fn as_any(&self) -> &(dyn Any + Send + Sync)
    where
        Self: Sized,
    {
        self
    }

    async fn list_buckets(&self) -> Result<Vec<String>, StorageError>;
    async fn create_bucket(&self, bucket: &str) -> Result<bool, StorageError>;
    async fn delete_bucket(&self, bucket: &str) -> Result<bool, StorageError>;

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: bytes::Bytes,
        content_type: &str,
        metadata: HashMap<String, String>,
        storage_class: Option<String>,
        server_side_encryption: Option<String>,
    ) -> Result<ObjectMetadata, StorageError>;

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(bytes::Bytes, ObjectMetadata), StorageError>;
    async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMetadata, StorageError>;

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<bool, StorageError>;

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, ObjectMetadata)>, StorageError>;

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
    ) -> Result<ObjectMetadata, StorageError>;

    // Multipart upload operations
    async fn initiate_multipart(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<String, StorageError>;

    async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: bytes::Bytes,
    ) -> Result<String, StorageError>; // Returns ETag

    async fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<(u32, String)>, // Vec of (part_number, etag)
    ) -> Result<ObjectMetadata, StorageError>;

    async fn abort_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), StorageError>;

    // Versioning operations
    async fn get_bucket_versioning(&self, bucket: &str) -> Result<VersioningStatus, StorageError>;

    async fn put_bucket_versioning(
        &self,
        bucket: &str,
        status: VersioningStatus,
    ) -> Result<(), StorageError>;

    async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(bytes::Bytes, ObjectMetadata), StorageError>;

    async fn head_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<ObjectMetadata, StorageError>;

    async fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<bool, StorageError>;

    async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<ObjectMetadata>, StorageError>;

    // Lifecycle operations
    async fn get_bucket_lifecycle(
        &self,
        bucket: &str,
    ) -> Result<Option<lifecycle::LifecyclePolicy>, StorageError>;

    async fn put_bucket_lifecycle(
        &self,
        bucket: &str,
        policy: lifecycle::LifecyclePolicy,
    ) -> Result<(), StorageError>;

    async fn delete_bucket_lifecycle(&self, bucket: &str) -> Result<bool, StorageError>;

    // Bucket policy operations
    async fn get_bucket_policy(
        &self,
        bucket: &str,
    ) -> Result<Option<bucket_policy::BucketPolicy>, StorageError>;

    async fn put_bucket_policy(
        &self,
        bucket: &str,
        policy: bucket_policy::BucketPolicy,
    ) -> Result<(), StorageError>;

    async fn delete_bucket_policy(&self, bucket: &str) -> Result<bool, StorageError>;

    /// Optional: direct file handle for zero-copy/streaming paths.
    /// Default returns None; implementations can override to enable sendfile-like handling.
    async fn open_for_sendfile(
        &self,
        _bucket: &str,
        _key: &str,
    ) -> Option<Result<SendfileObject, StorageError>> {
        None
    }
}

/// File handle + metadata for direct streaming/sendfile use.
#[derive(Debug)]
pub struct SendfileObject {
    pub path: std::path::PathBuf,
    pub metadata: ObjectMetadata,
}
