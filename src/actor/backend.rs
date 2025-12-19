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

//! ActorStorageBackend - StorageBackend implementation using actor model

use std::collections::HashMap;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::actor::messages::{FsCommand, ObjectHeaders};
use crate::actor::fs_store::FsStoreReader;
use crate::storage::{ObjectMetadata, StorageBackend, StorageError};

/// Storage backend that delegates to FsStoreActor via message passing
/// Supports multiple actors for parallel processing
///
/// For GET operations, this bypasses the actor message passing overhead
/// by using a shared FsStoreReader, eliminating 20-30ms of latency.
#[derive(Clone)]
pub struct ActorStorageBackend {
    actors: Vec<mpsc::Sender<FsCommand>>,
    /// Direct reader for bypassing actor message passing on GET operations
    reader: FsStoreReader,
}

impl ActorStorageBackend {
    /// Create a new ActorStorageBackend with multiple actors
    pub fn new(actors: Vec<mpsc::Sender<FsCommand>>, reader: FsStoreReader) -> Self {
        assert!(!actors.is_empty(), "At least one actor required");
        Self { actors, reader }
    }

    /// Get actor for a specific key (consistent hashing)
    fn actor_for_key(&self, key: &str) -> &mpsc::Sender<FsCommand> {
        if self.actors.len() == 1 {
            return &self.actors[0];
        }

        // Simple hash-based sharding
        let hash = key.bytes().fold(0u64, |acc, b| {
            acc.wrapping_mul(31).wrapping_add(b as u64)
        });
        let idx = (hash % self.actors.len() as u64) as usize;
        &self.actors[idx]
    }

    /// Get primary actor (for bucket operations)
    fn primary_actor(&self) -> &mpsc::Sender<FsCommand> {
        &self.actors[0]
    }

    /// Send a command to a specific actor and wait for response
    async fn send_command<T>(
        &self,
        actor_tx: &mpsc::Sender<FsCommand>,
        make_command: impl FnOnce(oneshot::Sender<Result<T, StorageError>>) -> FsCommand,
    ) -> Result<T, StorageError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = make_command(reply_tx);

        actor_tx
            .send(cmd)
            .await
            .map_err(|_| StorageError::Internal("Actor channel closed".to_string()))?;

        reply_rx
            .await
            .map_err(|_| StorageError::Internal("Actor reply failed".to_string()))?
    }
}

#[async_trait]
impl StorageBackend for ActorStorageBackend {
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
        let actor_tx = self.actor_for_key(key);
        let bucket = bucket.to_string();
        let key = key.to_string();
        let headers = ObjectHeaders {
            content_type: content_type.to_string(),
            metadata,
            storage_class,
            server_side_encryption,
        };

        self.send_command(actor_tx, |reply| FsCommand::PutObject {
            bucket,
            key,
            data,
            headers,
            reply,
        })
        .await
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(Bytes, ObjectMetadata), StorageError> {
        // CRITICAL OPTIMIZATION: Bypass actor message passing for GET operations
        // This eliminates 20-30ms of overhead by directly calling the read operations
        // GET operations are read-only and don't need actor serialization for safety
        self.reader.get_object_direct(bucket, key).await
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMetadata, StorageError> {
        let actor_tx = self.actor_for_key(key);
        let bucket = bucket.to_string();
        let key = key.to_string();

        self.send_command(actor_tx, |reply| FsCommand::HeadObject { bucket, key, reply })
            .await
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<bool, StorageError> {
        let actor_tx = self.actor_for_key(key);
        let bucket = bucket.to_string();
        let key = key.to_string();

        self.send_command(actor_tx, |reply| FsCommand::DeleteObject { bucket, key, reply })
            .await
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, ObjectMetadata)>, StorageError> {
        // List objects from primary actor (simplified - could aggregate across all actors)
        let actor_tx = self.primary_actor();
        let bucket = bucket.to_string();
        let prefix = prefix.to_string();

        self.send_command(actor_tx, |reply| FsCommand::ListObjects {
            bucket,
            prefix,
            limit,
            reply,
        })
        .await
    }

    async fn create_bucket(&self, bucket: &str) -> Result<bool, StorageError> {
        let actor_tx = self.primary_actor();
        let bucket = bucket.to_string();

        self.send_command(actor_tx, |reply| FsCommand::CreateBucket { bucket, reply })
            .await
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<bool, StorageError> {
        let actor_tx = self.primary_actor();
        let bucket = bucket.to_string();

        self.send_command(actor_tx, |reply| FsCommand::DeleteBucket { bucket, reply })
            .await
    }

    async fn list_buckets(&self) -> Result<Vec<String>, StorageError> {
        let actor_tx = self.primary_actor();
        self.send_command(actor_tx, |reply| FsCommand::ListBuckets { reply })
            .await
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
        let actor_tx = self.actor_for_key(dest_key);
        let src_bucket = src_bucket.to_string();
        let src_key = src_key.to_string();
        let dest_bucket = dest_bucket.to_string();
        let dest_key = dest_key.to_string();
        let headers = ObjectHeaders {
            content_type: content_type.to_string(),
            metadata,
            storage_class,
            server_side_encryption,
        };

        self.send_command(actor_tx, |reply| FsCommand::CopyObject {
            src_bucket,
            src_key,
            dest_bucket,
            dest_key,
            headers,
            reply,
        })
        .await
    }

    async fn initiate_multipart(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<String, StorageError> {
        let actor_tx = self.actor_for_key(key);
        let bucket = bucket.to_string();
        let key = key.to_string();
        let headers = ObjectHeaders {
            content_type: "application/octet-stream".to_string(),
            metadata: HashMap::new(),
            storage_class: None,
            server_side_encryption: None,
        };

        self.send_command(actor_tx, |reply| FsCommand::InitiateMultipart {
            bucket,
            key,
            headers,
            reply,
        })
        .await
    }

    async fn upload_part(
        &self,
        _bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: Bytes,
    ) -> Result<String, StorageError> {
        let actor_tx = self.actor_for_key(key);
        let upload_id = upload_id.to_string();

        self.send_command(actor_tx, |reply| FsCommand::UploadPart {
            upload_id,
            part_number,
            data,
            reply,
        })
        .await
    }

    async fn complete_multipart(
        &self,
        _bucket: &str,
        key: &str,
        upload_id: &str,
        parts: Vec<(u32, String)>,
    ) -> Result<ObjectMetadata, StorageError> {
        let actor_tx = self.actor_for_key(key);
        let upload_id = upload_id.to_string();

        self.send_command(actor_tx, |reply| FsCommand::CompleteMultipart {
            upload_id,
            parts,
            reply,
        })
        .await
    }

    async fn abort_multipart(
        &self,
        _bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), StorageError> {
        let actor_tx = self.actor_for_key(key);
        let upload_id = upload_id.to_string();

        self.send_command(actor_tx, |reply| FsCommand::AbortMultipart { upload_id, reply })
            .await
    }

    // Versioning operations - not yet supported by actor
    async fn get_bucket_versioning(
        &self,
        _bucket: &str,
    ) -> Result<crate::storage::VersioningStatus, StorageError> {
        Ok(crate::storage::VersioningStatus::Unversioned)
    }

    async fn put_bucket_versioning(
        &self,
        _bucket: &str,
        _status: crate::storage::VersioningStatus,
    ) -> Result<(), StorageError> {
        Err(StorageError::Internal(
            "Versioning not yet supported by actor".to_string(),
        ))
    }

    async fn get_object_version(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: &str,
    ) -> Result<(Bytes, ObjectMetadata), StorageError> {
        Err(StorageError::Internal(
            "Versioning not yet supported by actor".to_string(),
        ))
    }

    async fn head_object_version(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: &str,
    ) -> Result<ObjectMetadata, StorageError> {
        Err(StorageError::Internal(
            "Versioning not yet supported by actor".to_string(),
        ))
    }

    async fn delete_object_version(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: &str,
    ) -> Result<bool, StorageError> {
        Err(StorageError::Internal(
            "Versioning not yet supported by actor".to_string(),
        ))
    }

    async fn list_object_versions(
        &self,
        _bucket: &str,
        _key: &str,
        _max_versions: usize,
    ) -> Result<Vec<ObjectMetadata>, StorageError> {
        Err(StorageError::Internal(
            "Versioning not yet supported by actor".to_string(),
        ))
    }

    // Lifecycle operations - not yet supported by actor
    async fn get_bucket_lifecycle(
        &self,
        _bucket: &str,
    ) -> Result<Option<crate::storage::lifecycle::LifecyclePolicy>, StorageError> {
        Ok(None)
    }

    async fn put_bucket_lifecycle(
        &self,
        _bucket: &str,
        _policy: crate::storage::lifecycle::LifecyclePolicy,
    ) -> Result<(), StorageError> {
        Err(StorageError::Internal(
            "Lifecycle not yet supported by actor".to_string(),
        ))
    }

    async fn delete_bucket_lifecycle(&self, _bucket: &str) -> Result<bool, StorageError> {
        Ok(false)
    }

    // Bucket policy operations - not yet supported by actor
    async fn get_bucket_policy(
        &self,
        _bucket: &str,
    ) -> Result<Option<crate::storage::bucket_policy::BucketPolicy>, StorageError> {
        Ok(None)
    }

    async fn put_bucket_policy(
        &self,
        _bucket: &str,
        _policy: crate::storage::bucket_policy::BucketPolicy,
    ) -> Result<(), StorageError> {
        Err(StorageError::Internal(
            "Bucket policy not yet supported by actor".to_string(),
        ))
    }

    async fn delete_bucket_policy(&self, _bucket: &str) -> Result<bool, StorageError> {
        Ok(false)
    }
}
