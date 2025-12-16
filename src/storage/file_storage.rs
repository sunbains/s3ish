use crate::observability::metrics;
use crate::storage::common::{validate_bucket, validate_key};
use crate::storage::erasure::Erasure;
use crate::storage::{ObjectMetadata, StorageBackend, StorageError};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::fs::remove_dir_all;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
}

/// Simple filesystem-backed storage. Objects are stored under `root/bucket/key`.
/// Metadata is persisted alongside each object as `<key>.meta` JSON.
#[derive(Debug, Clone)]
pub struct FileStorage {
    root: PathBuf,
    erasure: Erasure,
}

impl FileStorage {
    pub async fn new<P: AsRef<Path>>(root: P) -> Result<Self, StorageError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)
            .await
            .map_err(|e| StorageError::Internal(format!("init fs storage: {e}")))?;
        Ok(Self {
            root,
            erasure: Erasure::new(4, 1, 1024 * 1024)?,
        })
    }

    fn bucket_path(&self, bucket: &str) -> PathBuf {
        self.root.join(bucket)
    }

    fn object_path(&self, bucket: &str, key: &str) -> PathBuf {
        let mut path = self.bucket_path(bucket);
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
}

fn to_object_metadata(meta: StoredMeta) -> ObjectMetadata {
    ObjectMetadata {
        content_type: meta.content_type,
        etag: meta.etag,
        size: meta.size,
        last_modified_unix_secs: meta.last_modified_unix_secs,
        metadata: meta.metadata,
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

#[async_trait]
impl StorageBackend for FileStorage {
    async fn list_buckets(&self) -> Result<Vec<String>, StorageError> {
        let mut out = Vec::new();
        let mut rd = fs::read_dir(&self.root)
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
                    out.push(name.to_string());
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

        let obj_path = self.object_path(bucket, key);
        if let Some(parent) = obj_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| StorageError::Internal(format!("create parents: {e}")))?;
        }

        // Encode shards streaming: write data shards and XOR parity shard(s).
        let shard_dir = self.shard_dir(&obj_path);
        if shard_dir.exists() {
            let _ = fs::remove_dir_all(&shard_dir).await;
        }
        fs::create_dir_all(&shard_dir)
            .await
            .map_err(|e| StorageError::Internal(format!("create shard dir: {e}")))?;
        let parity_blocks = self.erasure.parity_blocks.max(1);
        let shard_count = self.erasure.data_blocks + parity_blocks;

        let mut files = Vec::new();
        for idx in 0..shard_count {
            let tmp_shard = shard_dir.join(format!("{idx}.tmp"));
            files.push(
                fs::File::create(&tmp_shard)
                    .await
                    .map_err(|e| StorageError::Internal(format!("create shard: {e}")))?,
            );
        }

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

        // flush and rename
        for (idx, mut f) in files.into_iter().enumerate() {
            f.flush()
                .await
                .map_err(|e| StorageError::Internal(format!("flush shard: {e}")))?;
            let tmp = shard_dir.join(format!("{idx}.tmp"));
            let final_path = shard_dir.join(format!("{idx}"));
            fs::rename(tmp, final_path)
                .await
                .map_err(|e| StorageError::Internal(format!("commit shard: {e}")))?;
        }

        let size = data.len() as u64;
        let etag = format!("{:x}", md5::compute(&data));
        let stored_meta = StoredMeta {
            content_type: content_type.to_string(),
            etag: etag.clone(),
            size,
            last_modified_unix_secs: Utc::now().timestamp(),
            metadata,
            data_blocks: Some(self.erasure.data_blocks),
            parity_blocks: Some(self.erasure.parity_blocks),
            block_size: Some(self.erasure.block_size),
        };
        let meta_bytes =
            serde_json::to_vec(&stored_meta).map_err(|e| StorageError::Internal(e.to_string()))?;
        let meta_path = self.meta_path(&obj_path);
        fs::write(&meta_path, meta_bytes)
            .await
            .map_err(|e| StorageError::Internal(format!("write meta: {e}")))?;

        // Record overall operation metrics
        let duration = start_time.elapsed().as_secs_f64();
        metrics::record_storage_op("put", "file", duration);

        tracing::debug!(
            bucket = %bucket,
            key = %key,
            size = size,
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
            let shard_dir = self.shard_dir(&obj_path);
            let total = db + pb.max(1);
            let mut readers = Vec::new();
            for idx in 0..total {
                let shard_path = shard_dir.join(format!("{idx}"));
                match fs::File::open(&shard_path).await {
                    Ok(f) => readers.push(Some(f)),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => readers.push(None),
                    Err(e) => return Err(StorageError::Internal(format!("open shard {idx}: {e}"))),
                }
            }
            // streaming decode
            let mut out = Vec::with_capacity(stored.size as usize);
            let mut offset = 0usize;
            while offset < stored.size as usize {
                let mut parity_block = vec![0u8; self.erasure.block_size];
                let mut blocks: Vec<Option<Vec<u8>>> = Vec::new();
                for reader_opt in readers.iter_mut().take(db) {
                    if let Some(f) = reader_opt {
                        let mut buf = vec![0u8; self.erasure.block_size];
                        let n = f
                            .read(&mut buf)
                            .await
                            .map_err(|e| StorageError::Internal(format!("read shard: {e}")))?;
                        buf.truncate(n);
                        for (i, b) in buf.iter().enumerate() {
                            parity_block[i] ^= b;
                        }
                        blocks.push(Some(buf));
                    } else {
                        blocks.push(None);
                    }
                }
                // parity shard
                if let Some(parity_file) = readers[total - 1].as_mut() {
                    let mut buf = vec![0u8; self.erasure.block_size];
                    let n = parity_file
                        .read(&mut buf)
                        .await
                        .map_err(|e| StorageError::Internal(format!("read parity: {e}")))?;
                    buf.truncate(n);
                    parity_block = buf;
                }

                // reconstruct if exactly one missing
                let missing_idx =
                    blocks
                        .iter()
                        .enumerate()
                        .find_map(|(i, b)| if b.is_none() { Some(i) } else { None });
                if let Some(miss) = missing_idx {
                    if pb == 0 {
                        return Err(StorageError::Internal("missing shard".into()));
                    }
                    let mut rec = parity_block.clone();
                    for (i, blk) in blocks.iter().enumerate() {
                        if i == miss {
                            continue;
                        }
                        if let Some(b) = blk {
                            for (j, byte) in b.iter().enumerate() {
                                rec[j] ^= byte;
                            }
                        }
                    }
                    blocks[miss] = Some(rec);
                }

                for mut b in blocks.into_iter().take(db).flatten() {
                    if out.len() + b.len() > stored.size as usize {
                        b.truncate(stored.size as usize - out.len());
                    }
                    out.extend_from_slice(&b);
                    if out.len() >= stored.size as usize {
                        break;
                    }
                }
                offset = out.len();
            }
            out
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
                if path.extension().and_then(|e| e.to_str()) == Some("meta") {
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

        let src_obj_path = self.object_path(src_bucket, src_key);
        let dest_obj_path = self.object_path(dest_bucket, dest_key);
        if let Some(parent) = dest_obj_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| StorageError::Internal(format!("create dest parents: {e}")))?;
        }

        let src_meta_path = self.meta_path(&src_obj_path);
        let stored = read_stored_meta(&src_meta_path)
            .await
            .map_err(|e| match e {
                StorageError::ObjectNotFound { .. } => StorageError::ObjectNotFound {
                    bucket: src_bucket.to_string(),
                    key: src_key.to_string(),
                },
                other => other,
            })?;

        // Copy shards directly to avoid buffering the whole object.
        if let (Some(db), Some(pb)) = (stored.data_blocks, stored.parity_blocks) {
            let src_shard_dir = self.shard_dir(&src_obj_path);
            if fs::metadata(&src_shard_dir).await.is_err() {
                return Err(StorageError::Internal(format!(
                    "shards missing for {src_bucket}/{src_key}"
                )));
            }
            let dest_shard_dir = self.shard_dir(&dest_obj_path);
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
        };
        let meta_bytes = serde_json::to_vec(&dest_meta)
            .map_err(|e| StorageError::Internal(format!("encode meta: {e}")))?;
        let dest_meta_path = self.meta_path(&dest_obj_path);
        fs::write(&dest_meta_path, meta_bytes)
            .await
            .map_err(|e| StorageError::Internal(format!("write dest meta: {e}")))?;

        Ok(to_object_metadata(dest_meta))
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
            )
            .await
            .unwrap();
        let res = storage.delete_bucket("bucket").await;
        assert!(matches!(res, Err(StorageError::BucketNotEmpty(_))));
    }
}
