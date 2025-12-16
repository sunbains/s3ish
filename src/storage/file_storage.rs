use crate::observability::metrics;
use crate::storage::common::{validate_bucket, validate_key};
use crate::storage::erasure::Erasure;
use crate::storage::multipart::{MultipartManager, MultipartStorage, UploadMetadata};
use crate::storage::versioning::{VersionManager, VersionMetadata, VersionStorage};
use crate::storage::{ObjectMetadata, StorageBackend, StorageError, VersioningStatus};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::fs::remove_dir_all;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter, BufReader};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredMeta {
    content_type: String,
    etag: String,
    size: u64,
    last_modified_unix_secs: i64,
    metadata: HashMap<String, String>,
    #[serde(default)]
    data_blocks: Option<usize>,
    #[serde(default)]
    parity_blocks: Option<usize>,
    #[serde(default)]
    block_size: Option<usize>,
    #[serde(default)]
    storage_class: Option<String>,
    #[serde(default)]
    server_side_encryption: Option<String>,
    #[serde(default)]
    version_id: Option<String>,
    #[serde(default)]
    is_latest: bool,
    #[serde(default)]
    is_delete_marker: bool,
}

/// Simple filesystem-backed storage. Objects are stored under `drives[i]/bucket/key`.
/// Metadata is persisted alongside each object as `<key>.meta` JSON.
/// Supports multiple drives for spreading shards across physical disks.
#[derive(Debug, Clone)]
pub struct FileStorage {
    pub(crate) drives: Vec<PathBuf>,
    erasure: Erasure,
}

impl FileStorage {
    /// Create FileStorage with single drive and default erasure coding (4 data, 1 parity, 1MB blocks)
    pub async fn new<P: AsRef<Path>>(root: P) -> Result<Self, StorageError> {
        Self::new_multi_drive(vec![root.as_ref().to_path_buf()], 4, 1, 1024 * 1024).await
    }

    /// Create FileStorage with multiple drives and custom erasure coding parameters
    pub async fn new_multi_drive(
        drives: Vec<PathBuf>,
        data_blocks: usize,
        parity_blocks: usize,
        block_size: usize,
    ) -> Result<Self, StorageError> {
        if drives.is_empty() {
            return Err(StorageError::InvalidInput(
                "At least one drive path required".to_string(),
            ));
        }

        // Create all drive directories
        for drive in &drives {
            fs::create_dir_all(drive)
                .await
                .map_err(|e| StorageError::Internal(format!("init drive {:?}: {}", drive, e)))?;
        }

        Ok(Self {
            drives,
            erasure: Erasure::new(data_blocks, parity_blocks, block_size)?,
        })
    }

    /// Get the number of drives
    pub fn num_drives(&self) -> usize {
        self.drives.len()
    }

    /// Get the drive for a specific shard using modulo sharding
    fn drive_for_shard(&self, shard_idx: usize) -> &Path {
        let drive_idx = shard_idx % self.drives.len();
        &self.drives[drive_idx]
    }

    /// Get primary drive (first drive) for metadata and bucket operations
    fn primary_drive(&self) -> &Path {
        &self.drives[0]
    }

    fn bucket_path(&self, bucket: &str) -> PathBuf {
        self.primary_drive().join(bucket)
    }

    fn bucket_path_on_drive(&self, drive: &Path, bucket: &str) -> PathBuf {
        drive.join(bucket)
    }

    fn object_path(&self, bucket: &str, key: &str) -> PathBuf {
        let mut path = self.bucket_path(bucket);
        for comp in key.split('/') {
            path.push(comp);
        }
        path
    }

    fn object_path_on_drive(&self, drive: &Path, bucket: &str, key: &str) -> PathBuf {
        let mut path = self.bucket_path_on_drive(drive, bucket);
        for comp in key.split('/') {
            path.push(comp);
        }
        path
    }

    fn meta_path(&self, obj_path: &Path) -> PathBuf {
        let mut p = obj_path.to_path_buf();
        if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
            p.set_file_name(format!("{name}.meta"));
        } else {
            p.set_file_name("object.meta");
        }
        p
    }

    fn shard_dir(&self, obj_path: &Path) -> PathBuf {
        let mut p = obj_path.to_path_buf();
        let file_name = p
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("obj")
            .to_string();
        p.set_file_name(format!("{}.shards", file_name));
        p
    }

    /// Get shard directory for a specific shard on its designated drive
    fn shard_dir_on_drive(&self, drive: &Path, bucket: &str, key: &str) -> PathBuf {
        let obj_path = self.object_path_on_drive(drive, bucket, key);
        let mut p = obj_path;
        let file_name = p
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("obj")
            .to_string();
        p.set_file_name(format!("{}.shards", file_name));
        p
    }

    fn temp_root_on_drive(&self, drive: &Path) -> PathBuf {
        drive.join(".tmp")
    }

    fn temp_write_dir_on_drive(&self, drive: &Path, write_id: &str) -> PathBuf {
        self.temp_root_on_drive(drive).join(write_id)
    }

    fn multipart_root(&self) -> PathBuf {
        self.primary_drive().join(".multipart")
    }

    fn multipart_upload_dir(&self, upload_id: &str) -> PathBuf {
        self.multipart_root().join(upload_id)
    }

    fn multipart_part_path(&self, upload_id: &str, part_number: u32) -> PathBuf {
        self.multipart_upload_dir(upload_id)
            .join(format!("part-{}", part_number))
    }

    fn multipart_meta_path(&self, upload_id: &str) -> PathBuf {
        self.multipart_upload_dir(upload_id).join("upload.meta")
    }

    /// Generate a unique version ID (timestamp-based with nanoseconds)
    fn generate_version_id() -> String {
        let now = Utc::now();
        format!("{}{:09}", now.timestamp(), now.timestamp_subsec_nanos())
    }

    fn versioning_status_path(&self, bucket: &str) -> PathBuf {
        self.bucket_path(bucket).join(".versioning")
    }

    fn lifecycle_policy_path(&self, bucket: &str) -> PathBuf {
        self.bucket_path(bucket).join(".lifecycle")
    }

    fn bucket_policy_path(&self, bucket: &str) -> PathBuf {
        self.bucket_path(bucket).join(".policy")
    }

    /// Get the versions directory for an object (e.g., bucket/key/.versions/)
    fn versions_dir(&self, bucket: &str, key: &str) -> PathBuf {
        let obj_path = self.object_path(bucket, key);
        obj_path.with_extension("").join(".versions")
    }

    /// Get the metadata path for a specific version
    fn version_meta_path(&self, bucket: &str, key: &str, version_id: &str) -> PathBuf {
        self.versions_dir(bucket, key)
            .join(version_id)
            .join("meta.json")
    }

    /// Get the shard directory for a specific version
    fn version_shard_dir(&self, bucket: &str, key: &str, version_id: &str) -> PathBuf {
        self.versions_dir(bucket, key)
            .join(version_id)
            .join("data.shards")
    }

    /// List all version IDs for an object (sorted newest first)
    async fn list_version_ids(&self, bucket: &str, key: &str) -> Result<Vec<String>, StorageError> {
        let versions_dir = self.versions_dir(bucket, key);
        if fs::metadata(&versions_dir).await.is_err() {
            return Ok(Vec::new());
        }

        let mut version_ids = Vec::new();
        let mut rd = fs::read_dir(&versions_dir)
            .await
            .map_err(|e| StorageError::Internal(format!("list versions: {e}")))?;

        while let Some(entry) = rd
            .next_entry()
            .await
            .map_err(|e| StorageError::Internal(format!("list versions: {e}")))?
        {
            let meta = entry
                .metadata()
                .await
                .map_err(|e| StorageError::Internal(format!("version meta: {e}")))?;
            if meta.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    version_ids.push(name.to_string());
                }
            }
        }

        // Sort by version ID descending (newest first, since version IDs are timestamp-based)
        version_ids.sort_by(|a, b| b.cmp(a));
        Ok(version_ids)
    }

    /// Find the latest non-delete-marker version
    async fn find_latest_version(&self, bucket: &str, key: &str) -> Result<Option<StoredMeta>, StorageError> {
        let version_ids = self.list_version_ids(bucket, key).await?;

        for version_id in version_ids {
            let meta_path = self.version_meta_path(bucket, key, &version_id);
            if let Ok(stored) = read_stored_meta(&meta_path).await {
                if !stored.is_delete_marker {
                    return Ok(Some(stored));
                }
            }
        }

        Ok(None)
    }

    /// Read and decode shards from a directory
    /// Read shards from multiple drives (multi-drive mode)
    async fn read_shards_multi_drive(&self, bucket: &str, key: &str, db: usize, pb: usize, size: u64) -> Result<Vec<u8>, StorageError> {
        let total = db + pb.max(1);
        let mut readers = Vec::new();

        // Open all shard files from their designated drives with buffered readers
        for idx in 0..total {
            let drive = self.drive_for_shard(idx);
            let shard_dir = self.shard_dir_on_drive(drive, bucket, key);
            let shard_path = shard_dir.join(format!("{}", idx));

            match fs::File::open(&shard_path).await {
                Ok(f) => readers.push(Some(BufReader::with_capacity(65536, f))),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => readers.push(None),
                Err(e) => return Err(StorageError::Internal(format!("open shard {} on {:?}: {}", idx, drive, e))),
            }
        }

        self.decode_shards(readers, db, pb, size).await
    }

    async fn read_shards_from_dir(&self, shard_dir: &Path, db: usize, pb: usize, size: u64) -> Result<Vec<u8>, StorageError> {
        let total = db + pb.max(1);
        let mut readers = Vec::new();

        // Open all shard files with buffered readers for better performance
        for idx in 0..total {
            let shard_path = shard_dir.join(format!("{}", idx));
            match fs::File::open(&shard_path).await {
                Ok(f) => readers.push(Some(BufReader::with_capacity(65536, f))),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => readers.push(None),
                Err(e) => return Err(StorageError::Internal(format!("open shard {}: {}", idx, e))),
            }
        }

        self.decode_shards(readers, db, pb, size).await
    }

    /// Common shard decoding logic with parallel shard reading for maximum throughput
    async fn decode_shards(&self, mut readers: Vec<Option<BufReader<fs::File>>>, db: usize, pb: usize, size: u64) -> Result<Vec<u8>, StorageError> {
        let total = db + pb.max(1);
        let mut out = Vec::with_capacity(size as usize);
        let mut offset = 0usize;

        // Pre-allocate buffers outside the loop to reduce allocations
        let block_size = self.erasure.block_size;

        while offset < size as usize {
            // Read all data shards and parity shard in parallel for maximum throughput
            let mut read_handles = Vec::new();

            // Spawn parallel reads for data shards
            for (idx, reader_opt) in readers.iter_mut().enumerate().take(db) {
                if let Some(_reader) = reader_opt {
                    // Move reader temporarily to tokio task
                    let mut reader_moved = reader_opt.take().unwrap();
                    let handle = tokio::spawn(async move {
                        let mut buf = vec![0u8; block_size];
                        match reader_moved.read(&mut buf).await {
                            Ok(n) => {
                                buf.truncate(n);
                                Ok((Some(buf), reader_moved))
                            }
                            Err(e) => Err(StorageError::Internal(format!("read shard {}: {}", idx, e)))
                        }
                    });
                    read_handles.push((idx, Some(handle)));
                } else {
                    read_handles.push((idx, None));
                }
            }

            // Spawn parallel read for parity shard
            let parity_idx = total - 1;
            let parity_handle = if let Some(_parity_reader) = readers[parity_idx].as_mut() {
                let mut reader_moved = readers[parity_idx].take().unwrap();
                Some(tokio::spawn(async move {
                    let mut buf = vec![0u8; block_size];
                    match reader_moved.read(&mut buf).await {
                        Ok(n) => {
                            buf.truncate(n);
                            Ok((buf, reader_moved))
                        }
                        Err(e) => Err(StorageError::Internal(format!("read parity: {}", e)))
                    }
                }))
            } else {
                None
            };

            // Wait for all reads to complete
            let mut blocks: Vec<Option<Vec<u8>>> = vec![None; db];
            for (idx, handle_opt) in read_handles {
                if let Some(handle) = handle_opt {
                    match handle.await {
                        Ok(Ok((buf, reader_back))) => {
                            blocks[idx] = buf;
                            readers[idx] = Some(reader_back);
                        }
                        Ok(Err(e)) => return Err(e),
                        Err(e) => return Err(StorageError::Internal(format!("task join: {}", e))),
                    }
                }
            }

            // Wait for parity read
            let parity_block = if let Some(handle) = parity_handle {
                match handle.await {
                    Ok(Ok((buf, reader_back))) => {
                        readers[parity_idx] = Some(reader_back);
                        buf
                    }
                    Ok(Err(e)) => return Err(e),
                    Err(e) => return Err(StorageError::Internal(format!("parity task join: {}", e))),
                }
            } else {
                // Compute parity from data blocks if parity shard doesn't exist
                let mut parity = vec![0u8; block_size];
                for block_opt in &blocks {
                    if let Some(b) = block_opt {
                        for (i, byte) in b.iter().enumerate() {
                            parity[i] ^= byte;
                        }
                    }
                }
                parity
            };

            // Reconstruct missing shard if exactly one is missing
            let missing_idx = blocks.iter().enumerate()
                .find_map(|(i, b)| if b.is_none() { Some(i) } else { None });

            if let Some(miss) = missing_idx {
                if pb == 0 {
                    return Err(StorageError::Internal("missing shard with no parity".into()));
                }
                // XOR all other blocks with parity to recover missing block
                let mut rec = parity_block.clone();
                for (i, blk) in blocks.iter().enumerate() {
                    if i != miss {
                        if let Some(b) = blk {
                            for (j, byte) in b.iter().enumerate() {
                                rec[j] ^= byte;
                            }
                        }
                    }
                }
                blocks[miss] = Some(rec);
            }

            // Append blocks to output
            for mut b in blocks.into_iter().flatten() {
                if out.len() + b.len() > size as usize {
                    b.truncate(size as usize - out.len());
                }
                out.extend_from_slice(&b);
                if out.len() >= size as usize {
                    break;
                }
            }
            offset = out.len();
        }
        Ok(out)
    }

    async fn read_versioning_status(
        &self,
        bucket: &str,
    ) -> Result<crate::storage::VersioningStatus, StorageError> {
        use crate::storage::VersioningStatus;
        let path = self.versioning_status_path(bucket);
        match fs::read_to_string(&path).await {
            Ok(contents) => match contents.trim() {
                "Enabled" => Ok(VersioningStatus::Enabled),
                "Suspended" => Ok(VersioningStatus::Suspended),
                _ => Ok(VersioningStatus::Unversioned),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Ok(VersioningStatus::Unversioned)
            }
            Err(e) => Err(StorageError::Internal(format!(
                "read versioning status: {e}"
            ))),
        }
    }
}


fn to_object_metadata(meta: StoredMeta) -> ObjectMetadata {
    ObjectMetadata {
        content_type: meta.content_type,
        etag: meta.etag,
        size: meta.size,
        last_modified_unix_secs: meta.last_modified_unix_secs,
        metadata: meta.metadata,
        storage_class: meta.storage_class,
        server_side_encryption: meta.server_side_encryption,
        version_id: meta.version_id,
        is_latest: meta.is_latest,
        is_delete_marker: meta.is_delete_marker,
    }
}

async fn read_stored_meta(meta_path: &Path) -> Result<StoredMeta, StorageError> {
    let meta_bytes = fs::read(meta_path).await.map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound => StorageError::ObjectNotFound {
            bucket: "unknown".to_string(),
            key: "unknown".to_string(),
        },
        _ => StorageError::Internal(format!("read meta: {e}")),
    })?;
    serde_json::from_slice(&meta_bytes)
        .map_err(|e| StorageError::Internal(format!("parse meta: {e}")))
}

// Implement VersionStorage trait for low-level version operations
#[async_trait]
impl VersionStorage for FileStorage {
    async fn read_versioning_status(&self, bucket: &str) -> Result<VersioningStatus, StorageError> {
        let path = self.versioning_status_path(bucket);
        match fs::read_to_string(&path).await {
            Ok(s) => match s.trim() {
                "Enabled" => Ok(VersioningStatus::Enabled),
                "Suspended" => Ok(VersioningStatus::Suspended),
                _ => Ok(VersioningStatus::Unversioned),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Ok(VersioningStatus::Unversioned)
            }
            Err(e) => Err(StorageError::Internal(format!(
                "read versioning status: {e}"
            ))),
        }
    }

    async fn write_versioning_status(
        &self,
        bucket: &str,
        status: VersioningStatus,
    ) -> Result<(), StorageError> {
        let path = self.versioning_status_path(bucket);
        let status_str = match status {
            VersioningStatus::Enabled => "Enabled",
            VersioningStatus::Suspended => "Suspended",
            VersioningStatus::Unversioned => "Unversioned",
        };
        fs::write(&path, status_str)
            .await
            .map_err(|e| StorageError::Internal(format!("write versioning status: {e}")))
    }

    async fn store_version_metadata(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        metadata: VersionMetadata,
    ) -> Result<(), StorageError> {
        let meta_path = self.version_meta_path(bucket, key, version_id);

        // Ensure parent directory exists
        if let Some(parent) = meta_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| StorageError::Internal(format!("create version meta dir: {e}")))?;
        }

        let stored = StoredMeta {
            content_type: metadata.content_type,
            etag: metadata.etag,
            size: metadata.size,
            last_modified_unix_secs: metadata.created_at,
            metadata: metadata.metadata,
            data_blocks: Some(self.erasure.data_blocks),
            parity_blocks: Some(self.erasure.parity_blocks),
            block_size: Some(self.erasure.block_size),
            storage_class: metadata.storage_class,
            server_side_encryption: metadata.server_side_encryption,
            version_id: Some(metadata.version_id),
            is_latest: metadata.is_latest,
            is_delete_marker: metadata.is_delete_marker,
        };

        let json = serde_json::to_vec(&stored)
            .map_err(|e| StorageError::Internal(format!("serialize meta: {e}")))?;
        fs::write(&meta_path, json)
            .await
            .map_err(|e| StorageError::Internal(format!("write version meta: {e}")))?;

        Ok(())
    }

    async fn get_version_metadata(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<VersionMetadata, StorageError> {
        let meta_path = self.version_meta_path(bucket, key, version_id);
        let stored = read_stored_meta(&meta_path).await?;
        Ok(VersionMetadata::from_object_metadata(
            version_id.to_string(),
            &to_object_metadata(stored),
        ))
    }

    async fn list_version_ids(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<String>, StorageError> {
        let versions_dir = self.versions_dir(bucket, key);
        if !versions_dir.exists() {
            return Ok(Vec::new());
        }

        let mut entries = fs::read_dir(&versions_dir)
            .await
            .map_err(|e| StorageError::Internal(format!("read versions dir: {e}")))?;

        let mut version_ids = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| StorageError::Internal(format!("read version entry: {e}")))?
        {
            if let Some(name) = entry.file_name().to_str() {
                if entry.path().is_dir() {
                    version_ids.push(name.to_string());
                }
            }
        }

        // Sort in reverse chronological order (newest first)
        version_ids.sort_by(|a, b| b.cmp(a));
        Ok(version_ids)
    }

    async fn delete_version_metadata(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<bool, StorageError> {
        let meta_path = self.version_meta_path(bucket, key, version_id);
        let shard_dir = self.version_shard_dir(bucket, key, version_id);

        let mut deleted = false;

        // Delete metadata file
        if meta_path.exists() {
            fs::remove_file(&meta_path)
                .await
                .map_err(|e| StorageError::Internal(format!("delete version meta: {e}")))?;
            deleted = true;
        }

        // Delete shard directory
        if shard_dir.exists() {
            remove_dir_all(&shard_dir)
                .await
                .map_err(|e| StorageError::Internal(format!("delete version shards: {e}")))?;
        }

        Ok(deleted)
    }

    async fn get_version_data(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Bytes, StorageError> {
        let shard_dir = self.version_shard_dir(bucket, key, version_id);
        let meta_path = self.version_meta_path(bucket, key, version_id);

        let stored = read_stored_meta(&meta_path).await?;

        // If it's a delete marker, return error
        if stored.is_delete_marker {
            return Err(StorageError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        let db = stored.data_blocks.unwrap_or(self.erasure.data_blocks);
        let pb = stored.parity_blocks.unwrap_or(self.erasure.parity_blocks);

        let data = self.read_shards_from_dir(&shard_dir, db, pb, stored.size).await?;

        Ok(Bytes::from(data))
    }
}

// Implement LifecycleStorage trait for low-level lifecycle operations
#[async_trait]
impl crate::storage::lifecycle::LifecycleStorage for FileStorage {
    async fn read_lifecycle_policy(
        &self,
        bucket: &str,
    ) -> Result<Option<crate::storage::lifecycle::LifecyclePolicy>, StorageError> {
        let path = self.lifecycle_policy_path(bucket);
        match fs::read(&path).await {
            Ok(data) => {
                let policy: crate::storage::lifecycle::LifecyclePolicy =
                    serde_json::from_slice(&data)
                        .map_err(|e| StorageError::Internal(format!("deserialize lifecycle: {}", e)))?;
                Ok(Some(policy))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::Internal(format!("read lifecycle: {}", e))),
        }
    }

    async fn write_lifecycle_policy(
        &self,
        bucket: &str,
        policy: crate::storage::lifecycle::LifecyclePolicy,
    ) -> Result<(), StorageError> {
        // Verify bucket exists
        let bucket_path = self.bucket_path(bucket);
        if fs::metadata(&bucket_path).await.is_err() {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }

        let path = self.lifecycle_policy_path(bucket);
        let json = serde_json::to_vec_pretty(&policy)
            .map_err(|e| StorageError::Internal(format!("serialize lifecycle: {}", e)))?;

        fs::write(&path, json)
            .await
            .map_err(|e| StorageError::Internal(format!("write lifecycle: {}", e)))?;
        Ok(())
    }

    async fn delete_lifecycle_policy(&self, bucket: &str) -> Result<bool, StorageError> {
        let path = self.lifecycle_policy_path(bucket);
        match fs::remove_file(&path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(StorageError::Internal(format!("delete lifecycle: {}", e))),
        }
    }
}

#[async_trait]
impl StorageBackend for FileStorage {
    async fn list_buckets(&self) -> Result<Vec<String>, StorageError> {
        let mut out = Vec::new();
        let mut rd = fs::read_dir(self.primary_drive())
            .await
            .map_err(|e| StorageError::Internal(format!("list buckets: {e}")))?;
        while let Some(entry) = rd
            .next_entry()
            .await
            .map_err(|e| StorageError::Internal(format!("list buckets: {e}")))?
        {
            let meta = entry
                .metadata()
                .await
                .map_err(|e| StorageError::Internal(format!("bucket meta: {e}")))?;
            if meta.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    // Skip hidden directories like .tmp, .multipart
                    if !name.starts_with('.') {
                        out.push(name.to_string());
                    }
                }
            }
        }
        Ok(out)
    }

    async fn create_bucket(&self, bucket: &str) -> Result<bool, StorageError> {
        validate_bucket(bucket)?;
        let path = self.bucket_path(bucket);
        match fs::create_dir(&path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(e) => Err(StorageError::Internal(format!("create bucket: {e}"))),
        }
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<bool, StorageError> {
        validate_bucket(bucket)?;
        let path = self.bucket_path(bucket);
        match fs::read_dir(&path).await {
            Ok(mut rd) => {
                if rd
                    .next_entry()
                    .await
                    .map_err(|e| StorageError::Internal(format!("delete bucket: {e}")))?
                    .is_some()
                {
                    return Err(StorageError::BucketNotEmpty(bucket.to_string()));
                }
                fs::remove_dir(path)
                    .await
                    .map_err(|e| StorageError::Internal(format!("delete bucket: {e}")))?;
                Ok(true)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(StorageError::Internal(format!("delete bucket: {e}"))),
        }
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

        // Measure filesystem check latency
        let fs_check_start = std::time::Instant::now();
        let bucket_path = self.bucket_path(bucket);
        if fs::metadata(&bucket_path).await.is_err() {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }
        let fs_check_duration = fs_check_start.elapsed().as_secs_f64();
        tracing::debug!(fs_check_ms = fs_check_duration * 1000.0, "Bucket existence check");

        // Check versioning status
        let versioning_status = self.read_versioning_status(bucket).await?;
        let is_versioned = versioning_status == crate::storage::VersioningStatus::Enabled;

        let (version_id, _shard_dir, meta_path) = if is_versioned {
            // Versioning enabled: generate version ID and use versioned paths
            let vid = Self::generate_version_id();
            let shard_dir = self.version_shard_dir(bucket, key, &vid);
            let meta_path = self.version_meta_path(bucket, key, &vid);
            (Some(vid), shard_dir, meta_path)
        } else {
            // Unversioned: use traditional paths
            let obj_path = self.object_path(bucket, key);
            if let Some(parent) = obj_path.parent() {
                fs::create_dir_all(parent)
                    .await
                    .map_err(|e| StorageError::Internal(format!("create parents: {e}")))?;
            }
            let shard_dir = self.shard_dir(&obj_path);
            let meta_path = self.meta_path(&obj_path);
            (None, shard_dir, meta_path)
        };

        // Ensure parent directory exists for versioned paths
        if let Some(parent) = meta_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| StorageError::Internal(format!("create version parents: {e}")))?;
        }

        // ATOMIC WRITE PHASE 1: Write to temporary location with unique write ID
        // Each shard goes to its designated drive using modulo sharding
        let write_id = uuid::Uuid::new_v4().to_string();

        let parity_blocks = self.erasure.parity_blocks.max(1);
        let shard_count = self.erasure.data_blocks + parity_blocks;

        // Create temp directories on each drive that will be used
        let mut drives_to_init = std::collections::HashSet::new();
        for idx in 0..shard_count {
            let drive = self.drive_for_shard(idx);
            drives_to_init.insert(drive.to_path_buf());
        }

        for drive in &drives_to_init {
            let temp_dir = self.temp_write_dir_on_drive(drive, &write_id);
            fs::create_dir_all(&temp_dir)
                .await
                .map_err(|e| StorageError::Internal(format!("create temp dir on {:?}: {}", drive, e)))?;
        }

        // Open buffered writers for all shards, each on its designated drive
        let mut files = Vec::new();
        for idx in 0..shard_count {
            let drive = self.drive_for_shard(idx);
            let temp_dir = self.temp_write_dir_on_drive(drive, &write_id);
            let shard_path = temp_dir.join(format!("shard_{}", idx));

            let file = fs::File::create(&shard_path)
                .await
                .map_err(|e| StorageError::Internal(format!("create temp shard {} on {:?}: {}", idx, drive, e)))?;
            // Use large 256KB buffer for high throughput streaming writes
            files.push(BufWriter::with_capacity(262144, file));
        }

        // Compute BLAKE3 hash incrementally while writing shards (single pass through data)
        // BLAKE3 is 10-15x faster than MD5 and hardware-accelerated (SIMD, AVX-512)
        let mut hasher = blake3::Hasher::new();

        for stripe_start in
            (0..data.len()).step_by(self.erasure.block_size * self.erasure.data_blocks)
        {
            // build parity block
            let mut parity_block = vec![0u8; self.erasure.block_size];
            for data_idx in 0..self.erasure.data_blocks {
                let shard_idx = data_idx;
                let offset = stripe_start + data_idx * self.erasure.block_size;
                if offset >= data.len() {
                    break;
                }
                let end = std::cmp::min(offset + self.erasure.block_size, data.len());
                let slice = &data[offset..end];

                // Update BLAKE3 hash incrementally (hardware-accelerated)
                hasher.update(slice);

                files[shard_idx]
                    .write_all(slice)
                    .await
                    .map_err(|e| StorageError::Internal(format!("write shard: {e}")))?;
                for (i, b) in slice.iter().enumerate() {
                    parity_block[i] ^= b;
                }
            }
            // parity shards (identical XOR parity for each parity slot)
            for parity_file in files
                .iter_mut()
                .skip(self.erasure.data_blocks)
                .take(parity_blocks)
            {
                parity_file
                    .write_all(&parity_block)
                    .await
                    .map_err(|e| StorageError::Internal(format!("write parity: {e}")))?;
            }
        }

        // ATOMIC WRITE PHASE 2: Flush all shards in parallel
        let mut flush_handles = Vec::new();
        for (idx, mut f) in files.into_iter().enumerate() {
            let handle = tokio::spawn(async move {
                f.flush()
                    .await
                    .map_err(|e| StorageError::Internal(format!("flush temp shard {}: {}", idx, e)))?;
                drop(f); // Close file handle
                Ok::<(), StorageError>(())
            });
            flush_handles.push(handle);
        }

        // Wait for all flushes to complete
        let mut flush_errors = Vec::new();
        for (idx, handle) in flush_handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(())) => {},
                Ok(Err(e)) => flush_errors.push(format!("shard {}: {}", idx, e)),
                Err(e) => flush_errors.push(format!("shard {} task: {}", idx, e)),
            }
        }

        // If any flush failed, cleanup temp directories on all drives and return error
        if !flush_errors.is_empty() {
            for drive in &drives_to_init {
                let temp_dir = self.temp_write_dir_on_drive(drive, &write_id);
                let _ = fs::remove_dir_all(&temp_dir).await;
            }
            return Err(StorageError::Internal(format!(
                "Failed to flush shards: {}",
                flush_errors.join(", ")
            )));
        }

        // ATOMIC WRITE PHASE 3: Verify write quorum (all shards must be written successfully)
        // Check each shard on its designated drive
        let mut verified_shards = 0;
        for idx in 0..shard_count {
            let drive = self.drive_for_shard(idx);
            let temp_dir = self.temp_write_dir_on_drive(drive, &write_id);
            let shard_path = temp_dir.join(format!("shard_{}", idx));
            if fs::metadata(&shard_path).await.is_ok() {
                verified_shards += 1;
            }
        }

        if verified_shards < shard_count {
            for drive in &drives_to_init {
                let temp_dir = self.temp_write_dir_on_drive(drive, &write_id);
                let _ = fs::remove_dir_all(&temp_dir).await;
            }
            return Err(StorageError::Internal(format!(
                "Write quorum not met: only {} of {} shards written",
                verified_shards, shard_count
            )));
        }

        // ATOMIC WRITE PHASE 4: Commit - Move shards from temp to final location atomically
        // Create shard directories on each drive and move shards in parallel
        let mut commit_handles = Vec::new();

        for idx in 0..shard_count {
            let drive = self.drive_for_shard(idx).to_path_buf();
            let temp_dir = self.temp_write_dir_on_drive(&drive, &write_id);
            let temp_shard = temp_dir.join(format!("shard_{}", idx));

            let shard_dir = self.shard_dir_on_drive(&drive, bucket, key);
            let final_shard = shard_dir.join(format!("{}", idx));

            let handle = tokio::spawn(async move {
                // Create shard directory if needed
                if let Some(parent) = final_shard.parent() {
                    fs::create_dir_all(parent).await
                        .map_err(|e| StorageError::Internal(format!("create shard dir {}: {}", idx, e)))?;
                }

                // Atomic rename within same drive
                fs::rename(&temp_shard, &final_shard)
                    .await
                    .map_err(|e| StorageError::Internal(format!("commit shard {}: {}", idx, e)))?;

                Ok::<(), StorageError>(())
            });
            commit_handles.push(handle);
        }

        // Wait for all commits
        let mut commit_errors = Vec::new();
        for (idx, handle) in commit_handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(())) => {},
                Ok(Err(e)) => commit_errors.push(format!("shard {}: {}", idx, e)),
                Err(e) => commit_errors.push(format!("shard {} task: {}", idx, e)),
            }
        }

        // Cleanup temp directories on all drives
        for drive in &drives_to_init {
            let temp_dir = self.temp_write_dir_on_drive(drive, &write_id);
            let _ = fs::remove_dir_all(&temp_dir).await;
        }

        if !commit_errors.is_empty() {
            return Err(StorageError::Internal(format!(
                "Failed to commit shards: {}",
                commit_errors.join(", ")
            )));
        }

        let size = data.len() as u64;
        // Use the incrementally computed BLAKE3 hash (computed during erasure coding loop)
        // BLAKE3 produces 256-bit hashes vs MD5's 128-bit, providing better collision resistance
        let etag = hasher.finalize().to_hex().to_string();
        let stored_meta = StoredMeta {
            content_type: content_type.to_string(),
            etag: etag.clone(),
            size,
            last_modified_unix_secs: Utc::now().timestamp(),
            metadata,
            data_blocks: Some(self.erasure.data_blocks),
            parity_blocks: Some(self.erasure.parity_blocks),
            block_size: Some(self.erasure.block_size),
            storage_class,
            server_side_encryption,
            version_id: version_id.clone(),
            is_latest: true,
            is_delete_marker: false,
        };

        // Write metadata concurrently with the last shard operations for better performance
        let meta_bytes =
            serde_json::to_vec(&stored_meta).map_err(|e| StorageError::Internal(e.to_string()))?;
        let meta_write = tokio::spawn({
            let meta_path = meta_path.clone();
            async move {
                fs::write(&meta_path, meta_bytes)
                    .await
                    .map_err(|e| StorageError::Internal(format!("write meta: {}", e)))
            }
        });

        // Wait for metadata write to complete
        meta_write
            .await
            .map_err(|e| StorageError::Internal(format!("metadata write task failed: {}", e)))??;

        // Record overall operation metrics
        let duration = start_time.elapsed().as_secs_f64();
        metrics::record_storage_op("put", "file", duration);

        tracing::debug!(
            bucket = %bucket,
            key = %key,
            size = size,
            version_id = ?version_id,
            duration_ms = duration * 1000.0,
            "FileStorage put object completed"
        );

        Ok(to_object_metadata(stored_meta))
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

        // Try to find latest versioned object first
        if let Some(stored) = self.find_latest_version(bucket, key).await? {
            let version_id = stored.version_id.as_ref().ok_or_else(|| {
                StorageError::Internal("version found but no version_id".to_string())
            })?;

            // Read data from versioned path
            let data = if let (Some(db), Some(pb)) = (stored.data_blocks, stored.parity_blocks) {
                let shard_dir = self.version_shard_dir(bucket, key, version_id);
                self.read_shards_from_dir(&shard_dir, db, pb, stored.size).await?
            } else {
                // Fallback for objects without shards (shouldn't happen for versioned objects)
                return Err(StorageError::Internal("versioned object missing shards".to_string()));
            };

            let duration = start_time.elapsed().as_secs_f64();
            metrics::record_storage_op("get", "file", duration);
            tracing::debug!(
                bucket = %bucket,
                key = %key,
                version_id = %version_id,
                size = data.len(),
                duration_ms = duration * 1000.0,
                "FileStorage get versioned object completed"
            );
            return Ok((Bytes::from(data), to_object_metadata(stored)));
        }

        // Fall back to non-versioned path for backward compatibility
        let obj_path = self.object_path(bucket, key);
        let meta_path = self.meta_path(&obj_path);

        // Measure filesystem metadata read
        let meta_read_start = std::time::Instant::now();
        let stored = read_stored_meta(&meta_path).await.map_err(|e| match e {
            StorageError::ObjectNotFound { .. } => StorageError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            },
            other => other,
        })?;
        tracing::debug!(meta_read_ms = meta_read_start.elapsed().as_secs_f64() * 1000.0, "Metadata read");
        let data = if let (Some(db), Some(pb)) = (stored.data_blocks, stored.parity_blocks) {
            // Use multi-drive reading for sharded objects
            self.read_shards_multi_drive(bucket, key, db, pb, stored.size).await?
        } else {
            fs::read(&obj_path).await.map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => StorageError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                },
                _ => StorageError::Internal(format!("read object: {e}")),
            })?
        };

        // Record overall operation metrics
        let duration = start_time.elapsed().as_secs_f64();
        metrics::record_storage_op("get", "file", duration);

        tracing::debug!(
            bucket = %bucket,
            key = %key,
            size = data.len(),
            duration_ms = duration * 1000.0,
            "FileStorage get object completed"
        );

        Ok((Bytes::from(data), to_object_metadata(stored)))
    }

    #[tracing::instrument(skip(self), fields(bucket = %bucket, key = %key))]
    async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMetadata, StorageError> {
        let start_time = std::time::Instant::now();

        validate_bucket(bucket)?;
        validate_key(key)?;

        // Try to find latest versioned object first
        if let Some(stored) = self.find_latest_version(bucket, key).await? {
            let duration = start_time.elapsed().as_secs_f64();
            metrics::record_storage_op("head", "file", duration);

            tracing::debug!(
                bucket = %bucket,
                key = %key,
                version_id = ?stored.version_id,
                duration_ms = (duration * 1000.0) as u64,
                "head versioned object completed"
            );

            return Ok(to_object_metadata(stored));
        }

        // Fall back to non-versioned path for backward compatibility
        let obj_path = self.object_path(bucket, key);
        let meta_path = self.meta_path(&obj_path);
        let stored = read_stored_meta(&meta_path).await.map_err(|e| match e {
            StorageError::ObjectNotFound { .. } => StorageError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            },
            other => other,
        })?;

        let duration = start_time.elapsed().as_secs_f64();
        metrics::record_storage_op("head", "file", duration);

        tracing::debug!(
            bucket = %bucket,
            key = %key,
            duration_ms = (duration * 1000.0) as u64,
            "head object completed"
        );

        Ok(to_object_metadata(stored))
    }

    #[tracing::instrument(skip(self), fields(bucket = %bucket, key = %key))]
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<bool, StorageError> {
        let start_time = std::time::Instant::now();

        validate_bucket(bucket)?;
        validate_key(key)?;

        // Check versioning status
        let versioning_status = self.read_versioning_status(bucket).await?;
        let is_versioned = versioning_status == crate::storage::VersioningStatus::Enabled;

        if is_versioned {
            // Create a delete marker instead of actually deleting
            let version_id = Self::generate_version_id();
            let meta_path = self.version_meta_path(bucket, key, &version_id);

            // Ensure parent directory exists
            if let Some(parent) = meta_path.parent() {
                fs::create_dir_all(parent)
                    .await
                    .map_err(|e| StorageError::Internal(format!("create version parents: {e}")))?;
            }

            let delete_marker_meta = StoredMeta {
                content_type: "application/octet-stream".to_string(),
                etag: "".to_string(),
                size: 0,
                last_modified_unix_secs: Utc::now().timestamp(),
                metadata: HashMap::new(),
                data_blocks: None,
                parity_blocks: None,
                block_size: None,
                storage_class: None,
                server_side_encryption: None,
                version_id: Some(version_id.clone()),
                is_latest: true,
                is_delete_marker: true,
            };

            let meta_bytes = serde_json::to_vec(&delete_marker_meta)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            fs::write(&meta_path, meta_bytes)
                .await
                .map_err(|e| StorageError::Internal(format!("write delete marker: {e}")))?;

            let duration = start_time.elapsed().as_secs_f64();
            metrics::record_storage_op("delete", "file", duration);

            tracing::debug!(
                bucket = %bucket,
                key = %key,
                version_id = %version_id,
                duration_ms = (duration * 1000.0) as u64,
                "created delete marker"
            );

            return Ok(true);
        }

        // Unversioned: permanently delete
        let obj_path = self.object_path(bucket, key);
        let meta_path = self.meta_path(&obj_path);
        let shard_dir = self.shard_dir(&obj_path);
        let obj_deleted = match fs::remove_file(&obj_path).await {
            Ok(_) => true,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(e) => return Err(StorageError::Internal(format!("delete object: {e}"))),
        };
        let _ = remove_dir_all(&shard_dir).await;
        let meta_deleted = fs::remove_file(&meta_path).await.is_ok();

        let duration = start_time.elapsed().as_secs_f64();
        metrics::record_storage_op("delete", "file", duration);

        tracing::debug!(
            bucket = %bucket,
            key = %key,
            deleted = obj_deleted || meta_deleted,
            duration_ms = (duration * 1000.0) as u64,
            "delete object completed"
        );

        Ok(obj_deleted || meta_deleted)
    }

    #[tracing::instrument(skip(self), fields(bucket = %bucket, prefix = %prefix, limit = limit))]
    async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, ObjectMetadata)>, StorageError> {
        let start_time = std::time::Instant::now();

        validate_bucket(bucket)?;
        let bucket_path = self.bucket_path(bucket);
        if fs::metadata(&bucket_path).await.is_err() {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }

        let limit = if limit == 0 { 1000 } else { limit.min(10_000) };
        let mut out = Vec::new();
        let mut dirs = vec![bucket_path];

        while let Some(dir) = dirs.pop() {
            let mut rd = fs::read_dir(&dir)
                .await
                .map_err(|e| StorageError::Internal(format!("list objects: {e}")))?;
            while let Some(entry) = rd
                .next_entry()
                .await
                .map_err(|e| StorageError::Internal(format!("list objects: {e}")))?
            {
                let path = entry.path();
                let meta = entry
                    .metadata()
                    .await
                    .map_err(|e| StorageError::Internal(format!("list objects meta: {e}")))?;
                if meta.is_dir() {
                    if path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .map(|s| s.ends_with(".shards"))
                        .unwrap_or(false)
                    {
                        continue;
                    }
                    dirs.push(path);
                    continue;
                }
                let dir_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if dir_name == ".versions" {
                    // Found a versions directory - get the latest non-delete-marker version
                    let parent_path = path.parent().ok_or_else(|| {
                        StorageError::Internal("versions dir has no parent".to_string())
                    })?;
                    let key_path = parent_path
                        .strip_prefix(self.bucket_path(bucket))
                        .map_err(|_| StorageError::Internal("strip prefix failed".into()))?;
                    let key_str = key_path.to_string_lossy().replace('\\', "/");

                    if !key_str.starts_with(prefix) {
                        continue;
                    }

                    if let Some(stored) = self.find_latest_version(bucket, &key_str).await? {
                        out.push((key_str, to_object_metadata(stored)));
                        if out.len() >= limit {
                            return Ok(out);
                        }
                    }
                } else if path.extension().and_then(|e| e.to_str()) == Some("meta") {
                    // Regular unversioned .meta file
                    let rel = path
                        .strip_prefix(self.bucket_path(bucket))
                        .map_err(|_| StorageError::Internal("strip prefix failed".into()))?;
                    let mut key_str = rel.to_string_lossy().replace('\\', "/");
                    if let Some(stripped) = key_str.strip_suffix(".meta") {
                        key_str = stripped.to_string();
                    }
                    if !key_str.starts_with(prefix) {
                        continue;
                    }
                    let obj_meta = self.head_object(bucket, &key_str).await?;
                    out.push((key_str, obj_meta));
                    if out.len() >= limit {
                        return Ok(out);
                    }
                } else if path.extension().is_none() {
                    // raw object file (legacy)
                    let rel = path
                        .strip_prefix(self.bucket_path(bucket))
                        .map_err(|_| StorageError::Internal("strip prefix failed".into()))?;
                    let key_str = rel.to_string_lossy().replace('\\', "/");
                    if !key_str.starts_with(prefix) {
                        continue;
                    }
                    let obj_meta = self.head_object(bucket, &key_str).await?;
                    out.push((key_str, obj_meta));
                    if out.len() >= limit {
                        return Ok(out);
                    }
                }
            }
        }

        let duration = start_time.elapsed().as_secs_f64();
        metrics::record_storage_op("list", "file", duration);

        tracing::debug!(
            bucket = %bucket,
            prefix = %prefix,
            count = out.len(),
            duration_ms = (duration * 1000.0) as u64,
            "list objects completed"
        );

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
        if content_type.is_empty() {
            return Err(StorageError::InvalidInput(
                "content_type must be non-empty".into(),
            ));
        }

        let dest_bucket_path = self.bucket_path(dest_bucket);
        if fs::metadata(&dest_bucket_path).await.is_err() {
            return Err(StorageError::BucketNotFound(dest_bucket.to_string()));
        }

        // Check dest versioning status early to determine paths
        let dest_versioning_status = self.read_versioning_status(dest_bucket).await?;
        let is_dest_versioned = dest_versioning_status == crate::storage::VersioningStatus::Enabled;
        let dest_version_id = if is_dest_versioned {
            Some(Self::generate_version_id())
        } else {
            None
        };

        // Read source metadata (handles both versioned and non-versioned sources)
        let (_, src_meta) = self.get_object(src_bucket, src_key).await?;
        let stored = if let Some(ref src_vid) = src_meta.version_id {
            // Source is versioned
            let src_meta_path = self.version_meta_path(src_bucket, src_key, src_vid);
            read_stored_meta(&src_meta_path).await.map_err(|e| match e {
                StorageError::ObjectNotFound { .. } => StorageError::ObjectNotFound {
                    bucket: src_bucket.to_string(),
                    key: src_key.to_string(),
                },
                other => other,
            })?
        } else {
            // Source is unversioned
            let src_obj_path = self.object_path(src_bucket, src_key);
            let src_meta_path = self.meta_path(&src_obj_path);
            read_stored_meta(&src_meta_path).await.map_err(|e| match e {
                StorageError::ObjectNotFound { .. } => StorageError::ObjectNotFound {
                    bucket: src_bucket.to_string(),
                    key: src_key.to_string(),
                },
                other => other,
            })?
        };

        // Copy shards directly to avoid buffering the whole object.
        if let (Some(db), Some(pb)) = (stored.data_blocks, stored.parity_blocks) {
            let src_shard_dir = if let Some(src_vid) = &src_meta.version_id {
                self.version_shard_dir(src_bucket, src_key, src_vid)
            } else {
                let src_obj_path = self.object_path(src_bucket, src_key);
                self.shard_dir(&src_obj_path)
            };

            if fs::metadata(&src_shard_dir).await.is_err() {
                return Err(StorageError::Internal(format!(
                    "shards missing for {src_bucket}/{src_key}"
                )));
            }

            let dest_shard_dir = if let Some(ref dest_vid) = dest_version_id {
                self.version_shard_dir(dest_bucket, dest_key, dest_vid)
            } else {
                let dest_obj_path = self.object_path(dest_bucket, dest_key);
                self.shard_dir(&dest_obj_path)
            };
            let tmp_shard_dir = dest_shard_dir.with_extension("tmpcopy");
            let _ = remove_dir_all(&tmp_shard_dir).await;
            fs::create_dir_all(&tmp_shard_dir)
                .await
                .map_err(|e| StorageError::Internal(format!("create temp shards: {e}")))?;

            let mut rd = fs::read_dir(&src_shard_dir)
                .await
                .map_err(|e| StorageError::Internal(format!("read src shards: {e}")))?;
            while let Some(entry) = rd
                .next_entry()
                .await
                .map_err(|e| StorageError::Internal(format!("read src shards: {e}")))?
            {
                let meta = entry
                    .metadata()
                    .await
                    .map_err(|e| StorageError::Internal(format!("shard metadata: {e}")))?;
                if !meta.is_file() {
                    continue;
                }
                let name = entry.file_name();
                let dest_path = tmp_shard_dir.join(name);
                tokio::fs::copy(entry.path(), &dest_path)
                    .await
                    .map_err(|e| StorageError::Internal(format!("copy shard: {e}")))?;
            }

            let _ = remove_dir_all(&dest_shard_dir).await;
            fs::rename(&tmp_shard_dir, &dest_shard_dir)
                .await
                .map_err(|e| StorageError::Internal(format!("commit shards: {e}")))?;

            // Silence unused warnings for db/pb when metadata is missing.
            let _ = (db, pb);
        } else {
            // Handle non-sharded copy (shouldn't happen in production but keep for compatibility)
            let src_obj_path = self.object_path(src_bucket, src_key);
            let dest_obj_path = self.object_path(dest_bucket, dest_key);
            if let Some(parent) = dest_obj_path.parent() {
                fs::create_dir_all(parent)
                    .await
                    .map_err(|e| StorageError::Internal(format!("create dest parents: {e}")))?;
            }
            tokio::fs::copy(&src_obj_path, &dest_obj_path)
                .await
                .map_err(|e| StorageError::Internal(format!("copy object: {e}")))?;
        }

        let dest_meta = StoredMeta {
            content_type: content_type.to_string(),
            etag: stored.etag.clone(),
            size: stored.size,
            last_modified_unix_secs: Utc::now().timestamp(),
            metadata,
            data_blocks: stored.data_blocks,
            parity_blocks: stored.parity_blocks,
            block_size: stored.block_size,
            storage_class,
            server_side_encryption,
            version_id: dest_version_id.clone(),
            is_latest: true,
            is_delete_marker: false,
        };
        let meta_bytes = serde_json::to_vec(&dest_meta)
            .map_err(|e| StorageError::Internal(format!("encode meta: {e}")))?;

        let dest_meta_path = if let Some(ref dest_vid) = dest_version_id {
            self.version_meta_path(dest_bucket, dest_key, dest_vid)
        } else {
            let dest_obj_path = self.object_path(dest_bucket, dest_key);
            self.meta_path(&dest_obj_path)
        };

        // Ensure parent directory exists
        if let Some(parent) = dest_meta_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| StorageError::Internal(format!("create dest meta parents: {e}")))?;
        }

        fs::write(&dest_meta_path, meta_bytes)
            .await
            .map_err(|e| StorageError::Internal(format!("write dest meta: {e}")))?;

        Ok(to_object_metadata(dest_meta))
    }

    async fn initiate_multipart(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<String, StorageError> {
        validate_bucket(bucket)?;
        validate_key(key)?;

        // Check bucket exists
        let bucket_path = self.bucket_path(bucket);
        if fs::metadata(&bucket_path).await.is_err() {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
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
        let data_len = data.len();
        let etag = manager
            .upload_part(bucket, key, upload_id, part_number, data)
            .await?;

        tracing::debug!(
            bucket = %bucket,
            key = %key,
            upload_id = %upload_id,
            part_number = part_number,
            size = data_len,
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

    async fn get_bucket_versioning(
        &self,
        bucket: &str,
    ) -> Result<crate::storage::VersioningStatus, StorageError> {
        validate_bucket(bucket)?;

        // Check bucket exists
        let bucket_path = self.bucket_path(bucket);
        if fs::metadata(&bucket_path).await.is_err() {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }

        // Delegate to VersionManager
        let manager = VersionManager::new(self);
        manager.get_bucket_versioning(bucket).await
    }

    async fn put_bucket_versioning(
        &self,
        bucket: &str,
        status: crate::storage::VersioningStatus,
    ) -> Result<(), StorageError> {
        validate_bucket(bucket)?;

        // Check bucket exists
        let bucket_path = self.bucket_path(bucket);
        if fs::metadata(&bucket_path).await.is_err() {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
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
        let bucket_path = self.bucket_path(bucket);
        if fs::metadata(&bucket_path).await.is_err() {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }

        let limit = if limit == 0 { 1000 } else { limit.min(10_000) };
        let mut versions = Vec::new();
        let mut dirs = vec![bucket_path];

        while let Some(dir) = dirs.pop() {
            let mut rd = fs::read_dir(&dir)
                .await
                .map_err(|e| StorageError::Internal(format!("list versions: {e}")))?;

            while let Some(entry) = rd
                .next_entry()
                .await
                .map_err(|e| StorageError::Internal(format!("list versions: {e}")))?
            {
                let path = entry.path();
                let meta = entry
                    .metadata()
                    .await
                    .map_err(|e| StorageError::Internal(format!("list versions meta: {e}")))?;

                if meta.is_dir() {
                    let dir_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                    if dir_name == ".versions" {
                        // Found a versions directory - list all versions in it
                        let parent_path = path.parent().unwrap();
                        let key_path = parent_path
                            .strip_prefix(self.bucket_path(bucket))
                            .map_err(|_| StorageError::Internal("strip prefix failed".into()))?;
                        let key_str = key_path.to_string_lossy().replace('\\', "/");

                        if !key_str.starts_with(prefix) {
                            continue;
                        }

                        // List all version IDs for this key
                        let version_ids = self.list_version_ids(bucket, &key_str).await?;
                        for version_id in version_ids {
                            let meta_path = self.version_meta_path(bucket, &key_str, &version_id);
                            if let Ok(stored) = read_stored_meta(&meta_path).await {
                                versions.push(to_object_metadata(stored));
                                if versions.len() >= limit {
                                    return Ok(versions);
                                }
                            }
                        }
                    } else if !dir_name.ends_with(".shards") {
                        dirs.push(path);
                    }
                }
            }
        }

        Ok(versions)
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

    async fn get_bucket_policy(
        &self,
        bucket: &str,
    ) -> Result<Option<crate::storage::bucket_policy::BucketPolicy>, StorageError> {
        let path = self.bucket_policy_path(bucket);
        match fs::read(&path).await {
            Ok(data) => {
                let policy: crate::storage::bucket_policy::BucketPolicy =
                    serde_json::from_slice(&data).map_err(|e| {
                        StorageError::Internal(format!("deserialize bucket policy: {}", e))
                    })?;
                Ok(Some(policy))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::Internal(format!(
                "read bucket policy: {}",
                e
            ))),
        }
    }

    async fn put_bucket_policy(
        &self,
        bucket: &str,
        policy: crate::storage::bucket_policy::BucketPolicy,
    ) -> Result<(), StorageError> {
        // Verify bucket exists
        let bucket_path = self.bucket_path(bucket);
        if fs::metadata(&bucket_path).await.is_err() {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }

        // Validate policy
        policy.validate()?;

        let path = self.bucket_policy_path(bucket);
        let json = serde_json::to_vec_pretty(&policy).map_err(|e| {
            StorageError::Internal(format!("serialize bucket policy: {}", e))
        })?;

        fs::write(&path, json)
            .await
            .map_err(|e| StorageError::Internal(format!("write bucket policy: {}", e)))?;
        Ok(())
    }

    async fn delete_bucket_policy(&self, bucket: &str) -> Result<bool, StorageError> {
        let path = self.bucket_policy_path(bucket);
        match fs::remove_file(&path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(StorageError::Internal(format!(
                "delete bucket policy: {}",
                e
            ))),
        }
    }
}

// Implement MultipartStorage trait for low-level multipart operations
#[async_trait]
impl MultipartStorage for FileStorage {
    async fn store_upload_metadata(
        &self,
        upload_id: &str,
        bucket: &str,
        key: &str,
    ) -> Result<(), StorageError> {
        let upload_dir = self.multipart_upload_dir(upload_id);
        fs::create_dir_all(&upload_dir)
            .await
            .map_err(|e| StorageError::Internal(format!("create multipart dir: {e}")))?;

        let meta = UploadMetadata {
            bucket: bucket.to_string(),
            key: key.to_string(),
        };
        let meta_bytes = serde_json::to_vec(&meta)
            .map_err(|e| StorageError::Internal(format!("serialize multipart meta: {e}")))?;
        fs::write(self.multipart_meta_path(upload_id), meta_bytes)
            .await
            .map_err(|e| StorageError::Internal(format!("write multipart meta: {e}")))?;

        Ok(())
    }

    async fn get_upload_metadata(&self, upload_id: &str) -> Result<(String, String), StorageError> {
        let meta_path = self.multipart_meta_path(upload_id);
        let meta_bytes = fs::read(&meta_path).await.map_err(|_| {
            StorageError::Internal("Upload not found".to_string())
        })?;

        let meta: UploadMetadata = serde_json::from_slice(&meta_bytes)
            .map_err(|e| StorageError::Internal(format!("parse multipart meta: {e}")))?;

        Ok((meta.bucket, meta.key))
    }

    async fn store_part(
        &self,
        upload_id: &str,
        part_number: u32,
        data: Bytes,
    ) -> Result<String, StorageError> {
        let part_path = self.multipart_part_path(upload_id, part_number);
        fs::write(&part_path, &data)
            .await
            .map_err(|e| StorageError::Internal(format!("write multipart part: {e}")))?;

        let etag = blake3::hash(&data).to_hex().to_string();
        Ok(etag)
    }

    async fn get_part(
        &self,
        upload_id: &str,
        part_number: u32,
    ) -> Result<(Bytes, String), StorageError> {
        let part_path = self.multipart_part_path(upload_id, part_number);
        let data = fs::read(&part_path).await.map_err(|_| {
            StorageError::Internal(format!("Part {} not found", part_number))
        })?;

        let etag = blake3::hash(&data).to_hex().to_string();
        Ok((Bytes::from(data), etag))
    }

    async fn list_parts(&self, upload_id: &str) -> Result<Vec<(u32, String)>, StorageError> {
        let upload_dir = self.multipart_upload_dir(upload_id);
        let mut discovered_parts = Vec::new();

        let mut entries = fs::read_dir(&upload_dir).await.map_err(|e| {
            StorageError::Internal(format!("read upload directory: {e}"))
        })?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            StorageError::Internal(format!("read directory entry: {e}"))
        })? {
            let file_name = entry.file_name();
            let name_str = file_name.to_string_lossy();

            if let Some(part_str) = name_str.strip_prefix("part-") {
                if let Ok(part_num) = part_str.parse::<u32>() {
                    let part_data = fs::read(entry.path()).await.map_err(|e| {
                        StorageError::Internal(format!("read part file: {e}"))
                    })?;
                    let etag = blake3::hash(&part_data).to_hex().to_string();
                    discovered_parts.push((part_num, etag));
                }
            }
        }

        discovered_parts.sort_by_key(|(n, _)| *n);
        Ok(discovered_parts)
    }

    async fn remove_upload(&self, upload_id: &str) -> Result<(), StorageError> {
        let upload_dir = self.multipart_upload_dir(upload_id);
        remove_dir_all(&upload_dir)
            .await
            .map_err(|e| StorageError::Internal(format!("remove multipart dir: {e}")))?;
        Ok(())
    }

    async fn upload_exists(&self, upload_id: &str) -> bool {
        let meta_path = self.multipart_meta_path(upload_id);
        fs::metadata(&meta_path).await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_file_storage_put_get() {
        let dir = tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).await.unwrap();
        storage.create_bucket("bucket").await.unwrap();
        let mut meta_map = HashMap::new();
        meta_map.insert("color".into(), "blue".into());
        storage
            .put_object(
                "bucket",
                "path/to/key.txt",
                Bytes::from("data"),
                "text/plain",
                meta_map.clone(),
                None,
                None,
            )
            .await
            .unwrap();

        let (data, meta) = storage
            .get_object("bucket", "path/to/key.txt")
            .await
            .unwrap();
        assert_eq!(data, Bytes::from("data"));
        assert_eq!(meta.content_type, "text/plain");
        assert_eq!(meta.metadata.get("color").unwrap(), "blue");
    }

    #[tokio::test]
    async fn test_file_storage_list_and_delete() {
        let dir = tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).await.unwrap();
        storage.create_bucket("bucket").await.unwrap();
        storage
            .put_object(
                "bucket",
                "a/one.txt",
                Bytes::from("one"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();
        storage
            .put_object(
                "bucket",
                "b/two.txt",
                Bytes::from("two"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();

        let objs = storage.list_objects("bucket", "a/", 10).await.unwrap();
        assert_eq!(objs.len(), 1);
        assert_eq!(objs[0].0, "a/one.txt");

        let deleted = storage.delete_object("bucket", "a/one.txt").await.unwrap();
        assert!(deleted);
        let deleted_again = storage.delete_object("bucket", "a/one.txt").await.unwrap();
        assert!(!deleted_again);
    }

    #[tokio::test]
    async fn test_file_storage_copy() {
        let dir = tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).await.unwrap();
        storage.create_bucket("bucket").await.unwrap();
        storage
            .put_object(
                "bucket",
                "src.txt",
                Bytes::from("copy me"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();

        let meta = storage
            .copy_object(
                "bucket",
                "src.txt",
                "bucket",
                "dst.txt",
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(meta.size, 7);
        let (data, _) = storage.get_object("bucket", "dst.txt").await.unwrap();
        assert_eq!(data, Bytes::from("copy me"));
    }

    #[tokio::test]
    async fn test_file_storage_copy_large_streaming() {
        let dir = tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).await.unwrap();
        storage.create_bucket("bucket").await.unwrap();
        let large = vec![
            b'x';
            storage.erasure.block_size * storage.erasure.data_blocks * 2
                + storage.erasure.block_size / 2
        ];
        storage
            .put_object(
                "bucket",
                "big.bin",
                Bytes::from(large.clone()),
                "application/octet-stream",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();

        let meta = storage
            .copy_object(
                "bucket",
                "big.bin",
                "bucket",
                "big-copy.bin",
                "application/octet-stream",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(meta.size as usize, large.len());

        let (copied, copied_meta) = storage.get_object("bucket", "big-copy.bin").await.unwrap();
        assert_eq!(copied.len(), large.len());
        assert_eq!(&copied[..], &large[..]);
        assert_eq!(copied_meta.content_type, "application/octet-stream");
    }

    #[tokio::test]
    async fn test_file_storage_bucket_not_empty() {
        let dir = tempdir().unwrap();
        let storage = FileStorage::new(dir.path()).await.unwrap();
        storage.create_bucket("bucket").await.unwrap();
        storage
            .put_object(
                "bucket",
                "obj",
                Bytes::from("data"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();
        let res = storage.delete_bucket("bucket").await;
        assert!(matches!(res, Err(StorageError::BucketNotEmpty(_))));
    }

    #[tokio::test]
    async fn test_file_storage_bucket_policy_operations() {
        use crate::storage::bucket_policy::{Action, BucketPolicy, Effect, Principal, Resource, Statement};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let storage = FileStorage::new(temp_dir.path().to_str().unwrap())
            .await
            .unwrap();

        storage.create_bucket("test-bucket").await.unwrap();

        // Test getting policy when none exists
        let policy = storage.get_bucket_policy("test-bucket").await.unwrap();
        assert!(policy.is_none());

        // Test putting a policy
        let test_policy = BucketPolicy {
            version: "2012-10-17".to_string(),
            statements: vec![Statement {
                sid: Some("PublicRead".to_string()),
                effect: Effect::Allow,
                principal: Principal::All("*".to_string()),
                action: Action::Single("s3:GetObject".to_string()),
                resource: Resource::Single("arn:aws:s3:::test-bucket/*".to_string()),
                condition: None,
            }],
        };

        storage
            .put_bucket_policy("test-bucket", test_policy.clone())
            .await
            .unwrap();

        // Test getting the policy
        let retrieved = storage.get_bucket_policy("test-bucket").await.unwrap();
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.version, "2012-10-17");
        assert_eq!(retrieved.statements.len(), 1);
        assert_eq!(retrieved.statements[0].effect, Effect::Allow);

        // Test deleting the policy
        let deleted = storage.delete_bucket_policy("test-bucket").await.unwrap();
        assert!(deleted);

        // Verify policy is gone
        let policy = storage.get_bucket_policy("test-bucket").await.unwrap();
        assert!(policy.is_none());

        // Test deleting non-existent policy
        let deleted = storage.delete_bucket_policy("test-bucket").await.unwrap();
        assert!(!deleted);
    }

    #[tokio::test]
    async fn test_file_storage_bucket_policy_persistence() {
        use crate::storage::bucket_policy::{Action, BucketPolicy, Effect, Principal, Resource, Statement};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_str().unwrap();

        // Create storage and add policy
        {
            let storage = FileStorage::new(path).await.unwrap();
            storage.create_bucket("test-bucket").await.unwrap();

            let test_policy = BucketPolicy {
                version: "2012-10-17".to_string(),
                statements: vec![Statement {
                    sid: Some("PublicRead".to_string()),
                    effect: Effect::Allow,
                    principal: Principal::All("*".to_string()),
                    action: Action::Single("s3:GetObject".to_string()),
                    resource: Resource::Single("arn:aws:s3:::test-bucket/*".to_string()),
                    condition: None,
                }],
            };

            storage
                .put_bucket_policy("test-bucket", test_policy)
                .await
                .unwrap();
        }

        // Create new storage instance and verify policy persisted
        {
            let storage = FileStorage::new(path).await.unwrap();
            let retrieved = storage.get_bucket_policy("test-bucket").await.unwrap();
            assert!(retrieved.is_some());
            let retrieved = retrieved.unwrap();
            assert_eq!(retrieved.version, "2012-10-17");
            assert_eq!(retrieved.statements.len(), 1);
        }
    }
}
