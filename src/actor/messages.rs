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

//! Message types for actor communication
//!
//! All actor communication uses these message types with oneshot channels
//! for replies, ensuring no shared mutable state.

use bytes::Bytes;
use std::collections::HashMap;
use tokio::sync::oneshot;

use crate::storage::{ObjectMetadata, StorageError};

/// Commands sent to the FsStoreActor for filesystem operations
#[derive(Debug)]
pub enum FsCommand {
    /// Put an object into storage
    PutObject {
        bucket: String,
        key: String,
        data: Bytes,
        headers: ObjectHeaders,
        reply: oneshot::Sender<Result<ObjectMetadata, StorageError>>,
    },

    /// Get an object from storage
    GetObject {
        bucket: String,
        key: String,
        reply: oneshot::Sender<Result<(Bytes, ObjectMetadata), StorageError>>,
    },

    /// Get object metadata only (HEAD request)
    HeadObject {
        bucket: String,
        key: String,
        reply: oneshot::Sender<Result<ObjectMetadata, StorageError>>,
    },

    /// Delete an object
    DeleteObject {
        bucket: String,
        key: String,
        reply: oneshot::Sender<Result<bool, StorageError>>,
    },

    /// List objects in a bucket
    ListObjects {
        bucket: String,
        prefix: String,
        limit: usize,
        reply: oneshot::Sender<Result<Vec<(String, ObjectMetadata)>, StorageError>>,
    },

    /// Create a bucket
    CreateBucket {
        bucket: String,
        reply: oneshot::Sender<Result<bool, StorageError>>,
    },

    /// Delete a bucket
    DeleteBucket {
        bucket: String,
        reply: oneshot::Sender<Result<bool, StorageError>>,
    },

    /// List all buckets
    ListBuckets {
        reply: oneshot::Sender<Result<Vec<String>, StorageError>>,
    },

    /// Copy an object
    CopyObject {
        src_bucket: String,
        src_key: String,
        dest_bucket: String,
        dest_key: String,
        headers: ObjectHeaders,
        reply: oneshot::Sender<Result<ObjectMetadata, StorageError>>,
    },

    /// Initiate multipart upload
    InitiateMultipart {
        bucket: String,
        key: String,
        headers: ObjectHeaders,
        reply: oneshot::Sender<Result<String, StorageError>>,
    },

    /// Upload a part
    UploadPart {
        upload_id: String,
        part_number: u32,
        data: Bytes,
        reply: oneshot::Sender<Result<String, StorageError>>,
    },

    /// Complete multipart upload
    CompleteMultipart {
        upload_id: String,
        parts: Vec<(u32, String)>,
        reply: oneshot::Sender<Result<ObjectMetadata, StorageError>>,
    },

    /// Abort multipart upload
    AbortMultipart {
        upload_id: String,
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
}

/// Object headers extracted from HTTP request
#[derive(Debug, Clone)]
pub struct ObjectHeaders {
    pub content_type: String,
    pub metadata: HashMap<String, String>,
    pub storage_class: Option<String>,
    pub server_side_encryption: Option<String>,
}

impl Default for ObjectHeaders {
    fn default() -> Self {
        Self {
            content_type: "application/octet-stream".to_string(),
            metadata: HashMap::new(),
            storage_class: None,
            server_side_encryption: None,
        }
    }
}

impl ObjectHeaders {
    /// Create headers from content type and metadata
    pub fn new(content_type: impl Into<String>, metadata: HashMap<String, String>) -> Self {
        Self {
            content_type: content_type.into(),
            metadata,
            storage_class: None,
            server_side_encryption: None,
        }
    }

    /// Set storage class
    pub fn with_storage_class(mut self, storage_class: impl Into<String>) -> Self {
        self.storage_class = Some(storage_class.into());
        self
    }

    /// Set server-side encryption
    pub fn with_encryption(mut self, encryption: impl Into<String>) -> Self {
        self.server_side_encryption = Some(encryption.into());
        self
    }
}
