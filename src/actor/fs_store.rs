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

//! Filesystem storage actor
//!
//! This actor owns all filesystem operations. It processes messages
//! sequentially, eliminating the need for locks entirely. All state
//! is private to this actor.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use bytes::Bytes;
use chrono::Utc;
use crossbeam_queue::ArrayQueue;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::mpsc;

use crate::actor::messages::FsCommand;
use crate::actor::metrics::Metrics;
use crate::observability::metrics as prom_metrics;
use crate::storage::common::{validate_bucket, validate_key};
use crate::storage::erasure::Erasure;
use crate::storage::{ObjectMetadata, StorageError};

const ERASURE_CHUNK_SIZE: usize = 1024 * 1024; // 1MiB chunks
const SMALL_OBJECT_THRESHOLD: usize = 128 * 1024; // 128KB - store inline without sharding
const FD_CACHE_SIZE: usize = 10000; // Keep 10k file descriptors open
const METADATA_CACHE_SIZE: usize = 100000; // Keep 100k metadata entries in memory

/// Global buffer pool for reusing allocations
/// Similar to Go's sync.Pool, reduces GC pressure and improves performance
static BUFFER_POOL: Lazy<ArrayQueue<Vec<u8>>> = Lazy::new(|| {
    ArrayQueue::new(256) // Pool capacity = 256 buffers
});

/// Acquire a buffer from the pool, or allocate a new one if pool is empty
/// Returns a Vec with length=size, filled with zeros
fn acquire_buffer(size: usize) -> Vec<u8> {
    BUFFER_POOL.pop()
        .and_then(|mut buf| {
            if buf.capacity() >= size {
                // Buffer is large enough, resize and clear
                buf.clear();
                buf.resize(size, 0);
                Some(buf)
            } else {
                // Buffer too small, return to pool and allocate fresh
                let _ = BUFFER_POOL.push(buf);
                None
            }
        })
        .unwrap_or_else(|| vec![0u8; size])
}

/// Acquire a buffer from the pool with reserved capacity but zero length
/// Used for buffers that will be filled with extend_from_slice()
fn acquire_buffer_with_capacity(capacity: usize) -> Vec<u8> {
    BUFFER_POOL.pop()
        .and_then(|mut buf| {
            if buf.capacity() >= capacity {
                // Buffer is large enough, clear it (length becomes 0)
                buf.clear();
                Some(buf)
            } else {
                // Buffer too small, return to pool and allocate fresh
                let _ = BUFFER_POOL.push(buf);
                None
            }
        })
        .unwrap_or_else(|| Vec::with_capacity(capacity))
}

/// Return a buffer to the pool for reuse
fn release_buffer(buf: Vec<u8>) {
    // Only pool buffers that are reasonably sized (not massive outliers)
    if buf.capacity() <= 10 * 1024 * 1024 { // Max 10MB buffers in pool
        let _ = BUFFER_POOL.push(buf); // Drop if pool is full
    }
    // Otherwise, drop the buffer (let it deallocate)
}

/// Metadata stored on disk (includes erasure coding params)
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

/// Helper function to read a file with buffering for better performance
async fn read_file_buffered(path: &Path) -> Result<Vec<u8>, std::io::Error> {
    let file = fs::File::open(path).await?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file); // 1MB buffer
    let mut data = Vec::new();
    reader.read_to_end(&mut data).await?;
    Ok(data)
}

async fn read_stored_meta(meta_path: &Path) -> Result<StoredMeta, StorageError> {
    let data = read_file_buffered(meta_path)
        .await
        .map_err(|e| StorageError::Internal(format!("read meta: {}", e)))?;
    serde_json::from_slice(&data)
        .map_err(|e| StorageError::Internal(format!("deserialize meta: {}", e)))
}

/// Cached metadata entry with access tracking
struct CachedMetadata {
    stored: StoredMeta,
    accessed: std::sync::atomic::AtomicU64,
}

/// Shared immutable state for read operations
/// Can be cloned and used from multiple threads without actor message passing
/// Clone is cheap: just copying a few paths + Erasure (Copy) + Arc pointers
#[derive(Clone)]
pub struct FsStoreReader {
    /// Storage drives (for multi-drive sharding) - small Vec, cheap to clone
    drives: Vec<PathBuf>,

    /// Erasure coding enabled flag
    erasure_enabled: bool,

    /// Erasure coding configuration (Copy type, zero overhead)
    erasure: Erasure,

    /// Lock-free metrics (Arc is shared, cheap to clone)
    metrics: Arc<Metrics>,

    /// File descriptor cache - keep files open to avoid open() syscalls
    /// DashMap provides lock-free concurrent access with internal sharding
    fd_cache: Arc<DashMap<PathBuf, Arc<fs::File>>>,

    /// Metadata cache - keep metadata in memory to avoid disk reads
    /// DashMap eliminates all lock contention for cache hits
    metadata_cache: Arc<DashMap<(String, String), Arc<CachedMetadata>>>,
}

impl FsStoreReader {
    pub fn new(drives: Vec<PathBuf>, erasure_enabled: bool, data_blocks: usize, parity_blocks: usize, metrics: Arc<Metrics>) -> Self {
        let erasure = Erasure::new(data_blocks, parity_blocks, ERASURE_CHUNK_SIZE)
            .expect("Failed to create erasure coding");

        Self {
            drives,
            erasure_enabled,
            erasure,
            metrics,
            fd_cache: Arc::new(DashMap::with_capacity(FD_CACHE_SIZE)),
            metadata_cache: Arc::new(DashMap::with_capacity(METADATA_CACHE_SIZE)),
        }
    }

    /// Get or open a file, caching the file descriptor
    /// DashMap provides lock-free reads - zero contention on cache hits!
    async fn get_or_open_file(&self, path: &Path) -> Result<Arc<fs::File>, StorageError> {
        // Fast path: lock-free check with DashMap
        if let Some(file) = self.fd_cache.get(path) {
            return Ok(file.clone());
        }

        // Slow path: open file and cache it
        let file = fs::File::open(path).await
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    StorageError::Internal(format!("file not found: {:?}", path))
                } else {
                    StorageError::Internal(format!("open file: {}", e))
                }
            })?;

        let arc_file = Arc::new(file);

        // Simple eviction: if cache is full, clear 10% of entries
        if self.fd_cache.len() >= FD_CACHE_SIZE {
            let to_remove = FD_CACHE_SIZE / 10;
            let keys: Vec<_> = self.fd_cache.iter().take(to_remove).map(|r| r.key().clone()).collect();
            for key in keys {
                self.fd_cache.remove(&key);
            }
        }

        self.fd_cache.insert(path.to_path_buf(), arc_file.clone());

        Ok(arc_file)
    }

    /// Get cached metadata or read from disk
    /// DashMap provides lock-free reads - zero contention on cache hits!
    async fn get_metadata(&self, bucket: &str, key: &str, meta_path: &Path) -> Result<StoredMeta, StorageError> {
        let cache_key = (bucket.to_string(), key.to_string());

        // Fast path: lock-free check with DashMap
        if let Some(cached) = self.metadata_cache.get(&cache_key) {
            cached.accessed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(cached.stored.clone());
        }

        // Slow path: read from disk and cache
        let stored = read_stored_meta(meta_path).await?;

        // Simple eviction: if cache is full, clear 10% of least recently used entries
        if self.metadata_cache.len() >= METADATA_CACHE_SIZE {
            let to_remove = METADATA_CACHE_SIZE / 10;
            let mut entries: Vec<_> = self.metadata_cache.iter()
                .map(|r| (r.key().clone(), r.value().accessed.load(std::sync::atomic::Ordering::Relaxed)))
                .collect();
            entries.sort_by_key(|(_, accessed)| *accessed);
            for (key, _) in entries.into_iter().take(to_remove) {
                self.metadata_cache.remove(&key);
            }
        }

        let cached = Arc::new(CachedMetadata {
            stored: stored.clone(),
            accessed: std::sync::atomic::AtomicU64::new(1),
        });
        self.metadata_cache.insert(cache_key, cached);

        Ok(stored)
    }

    // Multi-drive helper methods
    fn primary_drive(&self) -> &Path {
        &self.drives[0]
    }

    fn drive_for_shard(&self, shard_index: usize) -> &Path {
        let drive_index = shard_index % self.drives.len();
        &self.drives[drive_index]
    }

    // Path helper methods
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

    /// Direct GET without actor message passing + aggressive caching
    /// This bypasses the 20-30ms actor overhead AND eliminates disk I/O on cache hits
    pub async fn get_object_direct(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(Bytes, ObjectMetadata), StorageError> {
        validate_bucket(bucket)?;
        validate_key(key)?;

        let obj_path = self.object_path(bucket, key);
        let meta_path = self.meta_path(&obj_path);

        // Get cached metadata (zero disk I/O on cache hit!)
        let stored = self.get_metadata(bucket, key, &meta_path).await.map_err(|_| {
            StorageError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            }
        })?;

        // Read data based on storage mode
        let data = match (stored.data_blocks, stored.parity_blocks) {
            (Some(db), Some(pb)) if pb > 0 => {
                // Erasure coded: read shards and reconstruct
                self.read_shards_direct(bucket, key, db, pb, stored.size).await?
            }
            (Some(db), Some(0)) => {
                // Striped without EC: read chunks and concatenate
                self.read_striped_chunks(bucket, key, db, stored.size).await?
            }
            _ => {
                // Single file storage (no sharding)
                let drive = self.primary_drive();
                let drive_name = drive.display().to_string();
                let read_start = std::time::Instant::now();

                let data = read_file_buffered(&obj_path).await.map_err(|e| match e.kind() {
                    std::io::ErrorKind::NotFound => StorageError::ObjectNotFound {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                    },
                    _ => StorageError::Internal(format!("read object: {e}")),
                })?;

                prom_metrics::record_disk_shard_read(&drive_name, read_start.elapsed().as_secs_f64());
                prom_metrics::increment_disk_shard_ops("read", &drive_name);
                data
            }
        };

        // Update metrics
        self.metrics.inc_get();
        self.metrics.add_bytes_out(data.len() as u64);

        Ok((Bytes::from(data), to_object_metadata(stored)))
    }

    async fn read_shards_direct(
        &self,
        bucket: &str,
        key: &str,
        db: usize,
        pb: usize,
        size: u64,
    ) -> Result<Vec<u8>, StorageError> {
        use tokio::io::AsyncReadExt;

        let total = db + pb.max(1);
        let block_size = 1024 * 1024; // 1MB blocks for streaming
        let mut out = acquire_buffer_with_capacity(size as usize);

        // Get cached file descriptors (zero syscalls on cache hit!)
        let mut readers: Vec<Option<(BufReader<fs::File>, String)>> = Vec::with_capacity(total);

        for idx in 0..total {
            let drive = self.drive_for_shard(idx);
            let drive_name = drive.display().to_string();
            let shard_dir = self.shard_dir_on_drive(drive, bucket, key);
            let shard_path = shard_dir.join(format!("{}", idx));

            match self.get_or_open_file(&shard_path).await {
                Ok(arc_file) => {
                    // try_clone() creates a new File pointing to the same FD
                    // This is cheap - just dup() syscall, not open()
                    match arc_file.as_ref().try_clone().await {
                        Ok(mut f) => {
                            // CRITICAL: Reset file position to 0
                            // try_clone() shares the file offset, so we must reset it
                            use tokio::io::AsyncSeekExt;
                            if let Err(e) = f.seek(std::io::SeekFrom::Start(0)).await {
                                return Err(StorageError::Internal(format!(
                                    "seek shard {}: {}",
                                    idx, e
                                )));
                            }
                            let reader = BufReader::with_capacity(block_size, f);
                            readers.push(Some((reader, drive_name)));
                        }
                        Err(e) => {
                            return Err(StorageError::Internal(format!(
                                "clone file handle for shard {}: {}",
                                idx, e
                            )))
                        }
                    }
                }
                Err(_) => {
                    // File not found - might be reconstructible from parity
                    readers.push(None);
                }
            }
        }

        let mut bytes_read = 0u64;

        // Stream blocks from all shards
        while bytes_read < size {
            let mut blocks: Vec<Option<Vec<u8>>> = vec![None; db];
            let parity_idx = total - 1;

            // Read one block from each data shard
            for idx in 0..db {
                if let Some((reader, drive_name)) = &mut readers[idx] {
                    let mut buf = vec![0u8; block_size];
                    let read_start = std::time::Instant::now();

                    match reader.read(&mut buf).await {
                        Ok(n) => {
                            buf.truncate(n);
                            if n > 0 {
                                prom_metrics::record_disk_shard_read(&drive_name, read_start.elapsed().as_secs_f64());
                                prom_metrics::increment_disk_shard_ops("read", &drive_name);
                                blocks[idx] = Some(buf);
                            }
                        }
                        Err(e) => {
                            return Err(StorageError::Internal(format!("read shard {}: {}", idx, e)))
                        }
                    }
                }
            }

            // Read one block from parity shard
            let mut parity_block: Option<Vec<u8>> = None;
            if let Some((reader, drive_name)) = &mut readers[parity_idx] {
                let mut buf = vec![0u8; block_size];
                let read_start = std::time::Instant::now();

                match reader.read(&mut buf).await {
                    Ok(n) => {
                        buf.truncate(n);
                        if n > 0 {
                            prom_metrics::record_disk_shard_read(&drive_name, read_start.elapsed().as_secs_f64());
                            prom_metrics::increment_disk_shard_ops("read", &drive_name);
                            parity_block = Some(buf);
                        }
                    }
                    Err(e) => {
                        return Err(StorageError::Internal(format!("read parity: {}", e)))
                    }
                }
            }

            // Check for missing shard in this stripe
            let missing_idx = blocks.iter().enumerate()
                .find_map(|(i, b)| if b.is_none() { Some(i) } else { None });

            if let Some(miss) = missing_idx {
                // Reconstruct missing block using XOR
                if pb == 0 || parity_block.is_none() {
                    return Err(StorageError::Internal("missing shard with no parity".into()));
                }

                let parity = parity_block.as_ref().unwrap();
                let block_len = blocks.iter().flatten().next().map(|b| b.len()).unwrap_or(parity.len());
                let mut reconstructed = vec![0u8; block_len];

                // Start with parity
                reconstructed[..parity.len().min(block_len)].copy_from_slice(&parity[..parity.len().min(block_len)]);

                // XOR with all other blocks
                for (i, block_opt) in blocks.iter().enumerate() {
                    if i != miss {
                        if let Some(block) = block_opt {
                            for (j, &byte) in block.iter().enumerate().take(block_len) {
                                reconstructed[j] ^= byte;
                            }
                        }
                    }
                }

                blocks[miss] = Some(reconstructed);
            }

            // Concatenate blocks into output
            for block_opt in blocks.iter() {
                if let Some(block) = block_opt {
                    let remaining = size as usize - out.len();
                    let to_copy = std::cmp::min(block.len(), remaining);
                    out.extend_from_slice(&block[..to_copy]);
                    bytes_read += to_copy as u64;

                    if out.len() >= size as usize {
                        break;
                    }
                }
            }

            // Check if all shards are exhausted
            if blocks.iter().all(|b| b.is_none()) {
                break;
            }
        }

        // Truncate to exact size
        out.truncate(size as usize);

        Ok(out)
    }

    /// Read striped chunks without erasure coding (parallel I/O, no reconstruction)
    async fn read_striped_chunks(
        &self,
        bucket: &str,
        key: &str,
        num_chunks: usize,
        size: u64,
    ) -> Result<Vec<u8>, StorageError> {
        let mut out = Vec::with_capacity(size as usize);

        // Read each chunk in order and concatenate
        for idx in 0..num_chunks {
            let drive = self.drive_for_shard(idx);
            let drive_name = drive.display().to_string();
            let shard_dir = self.shard_dir_on_drive(drive, bucket, key);
            let shard_path = shard_dir.join(format!("{}", idx));

            let read_start = std::time::Instant::now();

            // Read chunk with caching
            let chunk_data = match self.get_or_open_file(&shard_path).await {
                Ok(arc_file) => {
                    // Clone the file handle and read its contents
                    match arc_file.as_ref().try_clone().await {
                        Ok(mut f) => {
                            // Reset file position
                            use tokio::io::AsyncSeekExt;
                            if let Err(e) = f.seek(std::io::SeekFrom::Start(0)).await {
                                return Err(StorageError::Internal(format!("seek chunk {}: {}", idx, e)));
                            }

                            // Read entire chunk
                            use tokio::io::AsyncReadExt;
                            let mut data = Vec::new();
                            if let Err(e) = f.read_to_end(&mut data).await {
                                return Err(StorageError::Internal(format!("read chunk {}: {}", idx, e)));
                            }
                            data
                        }
                        Err(e) => {
                            return Err(StorageError::Internal(format!("clone file handle for chunk {}: {}", idx, e)));
                        }
                    }
                }
                Err(_) => {
                    return Err(StorageError::Internal(format!("chunk {} not found", idx)));
                }
            };

            prom_metrics::record_disk_shard_read(&drive_name, read_start.elapsed().as_secs_f64());
            prom_metrics::increment_disk_shard_ops("read", &drive_name);

            out.extend_from_slice(&chunk_data);
        }

        // Truncate to exact size (last chunk may have padding)
        out.truncate(size as usize);

        Ok(out)
    }
}

/// Filesystem storage actor
///
/// Processes filesystem operations sequentially. All state is private
/// to this actor, so no locks are needed.
pub struct FsStoreActor {
    /// Shared reader for direct access
    reader: FsStoreReader,

    /// Incoming command channel
    rx: mpsc::Receiver<FsCommand>,

    /// Cache of created directories (optimization)
    created_dirs: std::collections::HashSet<PathBuf>,
}

impl FsStoreActor {
    /// Create a new FsStoreActor
    pub fn new(
        drives: Vec<PathBuf>,
        erasure_enabled: bool,
        data_blocks: usize,
        parity_blocks: usize,
        rx: mpsc::Receiver<FsCommand>,
        metrics: Arc<Metrics>,
    ) -> Self {
        assert!(!drives.is_empty(), "At least one drive required");

        let reader = FsStoreReader::new(drives, erasure_enabled, data_blocks, parity_blocks, metrics);

        Self {
            reader,
            rx,
            created_dirs: std::collections::HashSet::new(),
        }
    }

    /// Get a cloneable reader for direct access (bypasses actor)
    pub fn reader(&self) -> FsStoreReader {
        self.reader.clone()
    }

    // Multi-drive helper methods - delegate to reader
    fn primary_drive(&self) -> &Path {
        self.reader.primary_drive()
    }

    fn drive_for_shard(&self, shard_index: usize) -> &Path {
        self.reader.drive_for_shard(shard_index)
    }

    // Path helper methods - delegate to reader
    fn bucket_path(&self, bucket: &str) -> PathBuf {
        self.reader.bucket_path(bucket)
    }

    fn bucket_path_on_drive(&self, drive: &Path, bucket: &str) -> PathBuf {
        self.reader.bucket_path_on_drive(drive, bucket)
    }

    fn object_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.reader.object_path(bucket, key)
    }

    fn object_path_on_drive(&self, drive: &Path, bucket: &str, key: &str) -> PathBuf {
        self.reader.object_path_on_drive(drive, bucket, key)
    }

    fn meta_path(&self, obj_path: &Path) -> PathBuf {
        self.reader.meta_path(obj_path)
    }

    fn shard_dir_on_drive(&self, drive: &Path, bucket: &str, key: &str) -> PathBuf {
        self.reader.shard_dir_on_drive(drive, bucket, key)
    }

    fn shard_dir(&self, bucket: &str, key: &str) -> PathBuf {
        let obj_path = self.object_path(bucket, key);
        let mut p = obj_path;
        let file_name = p
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("obj")
            .to_string();
        p.set_file_name(format!("{file_name}.shards"));
        p
    }

    /// Run the actor event loop
    ///
    /// This processes messages sequentially, so no locks are needed.
    /// The actor stops when the channel is closed.
    pub async fn run(mut self) {
        tracing::info!(
            "FsStoreActor started, drives={} ({})",
            self.reader.drives.len(),
            self.reader.drives
                .iter()
                .map(|d| d.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );

        while let Some(cmd) = self.rx.recv().await {
            self.reader.metrics.inc_message_received();

            match cmd {
                FsCommand::PutObject { bucket, key, data, headers, reply } => {
                    let op_start = std::time::Instant::now();
                    let res = self.handle_put_object(&bucket, &key, data, headers).await;
                    let _ = reply.send(res);
                    prom_metrics::record_actor_op_total("put_object", op_start.elapsed().as_secs_f64());
                }

                FsCommand::GetObject { bucket, key, reply } => {
                    let op_start = std::time::Instant::now();
                    let res = self.handle_get_object(&bucket, &key).await;
                    let _ = reply.send(res);
                    prom_metrics::record_actor_op_total("get_object", op_start.elapsed().as_secs_f64());
                }

                FsCommand::HeadObject { bucket, key, reply } => {
                    let op_start = std::time::Instant::now();
                    let res = self.handle_head_object(&bucket, &key).await;
                    let _ = reply.send(res);
                    prom_metrics::record_actor_op_total("head_object", op_start.elapsed().as_secs_f64());
                }

                FsCommand::DeleteObject { bucket, key, reply } => {
                    let op_start = std::time::Instant::now();
                    let res = self.handle_delete_object(&bucket, &key).await;
                    let _ = reply.send(res);
                    prom_metrics::record_actor_op_total("delete_object", op_start.elapsed().as_secs_f64());
                }

                FsCommand::ListObjects { bucket, prefix, limit, reply } => {
                    let op_start = std::time::Instant::now();
                    let res = self.handle_list_objects(&bucket, &prefix, limit).await;
                    let _ = reply.send(res);
                    prom_metrics::record_actor_op_total("list_objects", op_start.elapsed().as_secs_f64());
                }

                FsCommand::CreateBucket { bucket, reply } => {
                    let op_start = std::time::Instant::now();
                    let res = self.handle_create_bucket(&bucket).await;
                    let _ = reply.send(res);
                    prom_metrics::record_actor_op_total("create_bucket", op_start.elapsed().as_secs_f64());
                }

                FsCommand::DeleteBucket { bucket, reply } => {
                    let op_start = std::time::Instant::now();
                    let res = self.handle_delete_bucket(&bucket).await;
                    let _ = reply.send(res);
                    prom_metrics::record_actor_op_total("delete_bucket", op_start.elapsed().as_secs_f64());
                }

                FsCommand::ListBuckets { reply } => {
                    let op_start = std::time::Instant::now();
                    let res = self.handle_list_buckets().await;
                    let _ = reply.send(res);
                    prom_metrics::record_actor_op_total("list_buckets", op_start.elapsed().as_secs_f64());
                }

                FsCommand::CopyObject {
                    src_bucket,
                    src_key,
                    dest_bucket,
                    dest_key,
                    headers,
                    reply,
                } => {
                    let op_start = std::time::Instant::now();
                    let res = self
                        .handle_copy_object(&src_bucket, &src_key, &dest_bucket, &dest_key, headers)
                        .await;
                    let _ = reply.send(res);
                    prom_metrics::record_actor_op_total("copy_object", op_start.elapsed().as_secs_f64());
                }

                FsCommand::InitiateMultipart { bucket, key, headers, reply } => {
                    let op_start = std::time::Instant::now();
                    let res = self.handle_initiate_multipart(&bucket, &key, headers).await;
                    let _ = reply.send(res);
                    prom_metrics::record_actor_op_total("initiate_multipart", op_start.elapsed().as_secs_f64());
                }

                FsCommand::UploadPart {
                    upload_id,
                    part_number,
                    data,
                    reply,
                } => {
                    let op_start = std::time::Instant::now();
                    let res = self.handle_upload_part(&upload_id, part_number, data).await;
                    let _ = reply.send(res);
                    prom_metrics::record_actor_op_total("upload_part", op_start.elapsed().as_secs_f64());
                }

                FsCommand::CompleteMultipart { upload_id, parts, reply } => {
                    let op_start = std::time::Instant::now();
                    let res = self.handle_complete_multipart(&upload_id, parts).await;
                    let _ = reply.send(res);
                    prom_metrics::record_actor_op_total("complete_multipart", op_start.elapsed().as_secs_f64());
                }

                FsCommand::AbortMultipart { upload_id, reply } => {
                    let op_start = std::time::Instant::now();
                    let res = self.handle_abort_multipart(&upload_id).await;
                    let _ = reply.send(res);
                    prom_metrics::record_actor_op_total("abort_multipart", op_start.elapsed().as_secs_f64());
                }
            }
        }

        tracing::info!("FsStoreActor stopped");
    }

    // Handler methods (to be implemented in Phase 2)
    // For now, these are stubs that return "not implemented"

    async fn handle_put_object(
        &mut self,
        bucket: &str,
        key: &str,
        data: Bytes,
        headers: crate::actor::messages::ObjectHeaders,
    ) -> Result<ObjectMetadata, StorageError> {
        validate_bucket(bucket)?;
        validate_key(key)?;
        if headers.content_type.is_empty() {
            return Err(StorageError::InvalidInput(
                "content_type must be non-empty".into(),
            ));
        }

        // Check bucket exists
        let bucket_path = self.bucket_path(bucket);
        if fs::metadata(&bucket_path).await.is_err() {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }

        // Get object paths (metadata goes on primary drive)
        let obj_path = self.object_path(bucket, key);
        if let Some(parent) = obj_path.parent() {
            // Use directory cache to avoid redundant create_dir_all() calls
            if !self.created_dirs.contains(parent) {
                fs::create_dir_all(parent)
                    .await
                    .map_err(|e| StorageError::Internal(format!("create parents: {e}")))?;
                self.created_dirs.insert(parent.to_path_buf());
            }
        }

        let meta_path = self.meta_path(&obj_path);
        let size = data.len() as u64;

        // Fast path: store inline without erasure coding if:
        // 1. Erasure coding is disabled, OR
        // 2. Object is small (<= 128KB)
        if !self.reader.erasure_enabled || data.len() <= SMALL_OBJECT_THRESHOLD {
            return self.handle_put_object_inline(bucket, key, data, headers, obj_path, meta_path, size).await;
        }

        let parity_blocks = self.reader.erasure.parity_blocks.max(1);
        let shard_count = self.reader.erasure.data_blocks + parity_blocks;

        // Open buffered writers for all shards, each on its designated drive
        let mut files = Vec::new();
        for idx in 0..shard_count {
            let drive = self.drive_for_shard(idx);
            let shard_dir = self.shard_dir_on_drive(drive, bucket, key);

            // Create shard directory on this drive
            fs::create_dir_all(&shard_dir)
                .await
                .map_err(|e| {
                    StorageError::Internal(format!(
                        "create shard dir on {:?}: {e}",
                        drive
                    ))
                })?;

            let shard_path = shard_dir.join(format!("{}", idx));
            let file = fs::File::create(&shard_path)
                .await
                .map_err(|e| {
                    StorageError::Internal(format!(
                        "create shard {} on {:?}: {}",
                        idx, drive, e
                    ))
                })?;
            files.push(BufWriter::new(file));
        }

        // Compute BLAKE3 hash incrementally while writing shards
        let mut hasher = blake3::Hasher::new();

        // Calculate stripe size
        let stripe_size = self.reader.erasure.block_size * self.reader.erasure.data_blocks;

        for stripe_start in (0..data.len()).step_by(stripe_size) {
            let mut parity_block = acquire_buffer(self.reader.erasure.block_size);

            let stripe_end = std::cmp::min(stripe_start + stripe_size, data.len());
            let stripe_data_len = stripe_end - stripe_start;

            let chunk_size = stripe_data_len.div_ceil(self.reader.erasure.data_blocks);

            for data_idx in 0..self.reader.erasure.data_blocks {
                let shard_idx = data_idx;
                let offset = stripe_start + data_idx * chunk_size;
                if offset >= data.len() {
                    break;
                }
                let end = std::cmp::min(offset + chunk_size, data.len());
                let slice = &data[offset..end];

                // Update BLAKE3 hash
                hasher.update(slice);

                // Write data (with timing)
                let drive = self.drive_for_shard(shard_idx);
                let drive_name = drive.display().to_string();
                let write_start = std::time::Instant::now();

                files[shard_idx]
                    .write_all(slice)
                    .await
                    .map_err(|e| StorageError::Internal(format!("write shard {}: {e}", shard_idx)))?;

                prom_metrics::record_disk_shard_write(&drive_name, write_start.elapsed().as_secs_f64());
                prom_metrics::increment_disk_shard_ops("write", &drive_name);

                // XOR into parity block
                for (i, b) in slice.iter().enumerate() {
                    parity_block[i] ^= b;
                }
            }

            // Write parity to all parity shards
            for (idx, parity_file) in files
                .iter_mut()
                .enumerate()
                .skip(self.reader.erasure.data_blocks)
                .take(parity_blocks)
            {
                let drive = self.drive_for_shard(idx);
                let drive_name = drive.display().to_string();
                let write_start = std::time::Instant::now();

                parity_file
                    .write_all(&parity_block)
                    .await
                    .map_err(|e| StorageError::Internal(format!("write parity: {e}")))?;

                prom_metrics::record_disk_shard_write(&drive_name, write_start.elapsed().as_secs_f64());
                prom_metrics::increment_disk_shard_ops("write", &drive_name);
            }

            // Return parity_block to pool
            release_buffer(parity_block);
        }

        // Flush all files
        for (idx, mut f) in files.into_iter().enumerate() {
            f.flush()
                .await
                .map_err(|e| StorageError::Internal(format!("flush shard {}: {}", idx, e)))?;
        }

        // Create metadata
        let size = data.len() as u64;
        let etag = hasher.finalize().to_hex().to_string();
        let stored_meta = StoredMeta {
            content_type: headers.content_type,
            etag: etag.clone(),
            size,
            last_modified_unix_secs: Utc::now().timestamp(),
            metadata: headers.metadata,
            data_blocks: Some(self.reader.erasure.data_blocks),
            parity_blocks: Some(self.reader.erasure.parity_blocks),
            block_size: Some(self.reader.erasure.block_size),
            storage_class: headers.storage_class,
            server_side_encryption: headers.server_side_encryption,
            version_id: None,
            is_latest: true,
            is_delete_marker: false,
        };

        // Write metadata
        let meta_bytes =
            serde_json::to_vec(&stored_meta).map_err(|e| StorageError::Internal(e.to_string()))?;
        fs::write(&meta_path, meta_bytes)
            .await
            .map_err(|e| StorageError::Internal(format!("write meta: {}", e)))?;

        // Update metrics
        self.reader.metrics.inc_put();
        self.reader.metrics.add_bytes_in(size);

        Ok(to_object_metadata(stored_meta))
    }

    // Inline storage: No erasure coding, but stripe across drives for parallelism
    async fn handle_put_object_inline(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        headers: crate::actor::messages::ObjectHeaders,
        obj_path: PathBuf,
        meta_path: PathBuf,
        size: u64,
    ) -> Result<ObjectMetadata, StorageError> {
        // Compute hash
        let etag = blake3::hash(&data).to_hex().to_string();

        // OPTIMIZATION: Stripe across multiple drives for parallel I/O
        // Even without erasure coding, we split the object into chunks and write to different drives
        let num_drives = self.reader.drives.len();
        let is_striped = num_drives > 1 && data.len() > 64 * 1024;

        if is_striped {
            // Multi-drive striping: split into equal chunks
            let chunk_size = data.len().div_ceil(num_drives);
            let shard_dir = self.shard_dir(bucket, key);

            // Create shard directory
            fs::create_dir_all(&shard_dir)
                .await
                .map_err(|e| StorageError::Internal(format!("create shard dir: {}", e)))?;

            // Write chunks to different drives in parallel (well, sequentially for now)
            for (idx, chunk) in data.chunks(chunk_size).enumerate() {
                let drive = self.drive_for_shard(idx);
                let drive_name = drive.display().to_string();
                let shard_dir_on_drive = self.shard_dir_on_drive(drive, bucket, key);
                let shard_path = shard_dir_on_drive.join(format!("{}", idx));

                // Create parent dirs if needed
                if let Some(parent) = shard_path.parent() {
                    fs::create_dir_all(parent).await.ok();
                }

                let write_start = std::time::Instant::now();
                fs::write(&shard_path, chunk)
                    .await
                    .map_err(|e| StorageError::Internal(format!("write striped chunk {}: {}", idx, e)))?;

                prom_metrics::record_disk_shard_write(&drive_name, write_start.elapsed().as_secs_f64());
                prom_metrics::increment_disk_shard_ops("write", &drive_name);
            }
        } else {
            // Single drive or small object: write to one file
            let write_start = std::time::Instant::now();
            fs::write(&obj_path, &data)
                .await
                .map_err(|e| StorageError::Internal(format!("write inline object: {}", e)))?;

            let drive = self.primary_drive();
            let drive_name = drive.display().to_string();
            prom_metrics::record_disk_shard_write(&drive_name, write_start.elapsed().as_secs_f64());
            prom_metrics::increment_disk_shard_ops("write", &drive_name);
        }

        // Create metadata
        // For striped objects: data_blocks=num_chunks, parity_blocks=0 (no EC)
        // For single file: data_blocks=None, parity_blocks=None
        let (data_blocks, parity_blocks, block_size) = if is_striped {
            let chunk_size = data.len().div_ceil(num_drives);
            let num_chunks = data.len().div_ceil(chunk_size);
            (Some(num_chunks), Some(0), Some(chunk_size))
        } else {
            (None, None, None)
        };

        let stored_meta = StoredMeta {
            content_type: headers.content_type,
            etag: etag.clone(),
            size,
            last_modified_unix_secs: Utc::now().timestamp(),
            metadata: headers.metadata,
            data_blocks,
            parity_blocks,
            block_size,
            storage_class: headers.storage_class,
            server_side_encryption: headers.server_side_encryption,
            version_id: None,
            is_latest: true,
            is_delete_marker: false,
        };

        // Write metadata
        let meta_bytes =
            serde_json::to_vec(&stored_meta).map_err(|e| StorageError::Internal(e.to_string()))?;
        fs::write(&meta_path, meta_bytes)
            .await
            .map_err(|e| StorageError::Internal(format!("write meta: {}", e)))?;

        // Update metrics
        self.reader.metrics.inc_put();
        self.reader.metrics.add_bytes_in(size);

        Ok(to_object_metadata(stored_meta))
    }

    async fn handle_get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(Bytes, ObjectMetadata), StorageError> {
        validate_bucket(bucket)?;
        validate_key(key)?;

        let obj_path = self.object_path(bucket, key);
        let meta_path = self.meta_path(&obj_path);

        // Read metadata
        let stored = read_stored_meta(&meta_path).await.map_err(|_| {
            StorageError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            }
        })?;

        // Read data based on storage mode
        let data = match (stored.data_blocks, stored.parity_blocks) {
            (Some(db), Some(pb)) if pb > 0 => {
                // Erasure coded: read shards and reconstruct
                self.read_shards(bucket, key, db, pb, stored.size).await?
            }
            (Some(db), Some(0)) => {
                // Striped without EC: use reader's striped method
                self.reader.read_striped_chunks(bucket, key, db, stored.size).await?
            }
            _ => {
                // Single file storage (no sharding)
                let drive = self.primary_drive();
                let drive_name = drive.display().to_string();
                let read_start = std::time::Instant::now();

                let data = read_file_buffered(&obj_path).await.map_err(|e| match e.kind() {
                    std::io::ErrorKind::NotFound => StorageError::ObjectNotFound {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                    },
                    _ => StorageError::Internal(format!("read object: {e}")),
                })?;

                prom_metrics::record_disk_shard_read(&drive_name, read_start.elapsed().as_secs_f64());
                prom_metrics::increment_disk_shard_ops("read", &drive_name);
                data
            }
        };

        // Update metrics
        self.reader.metrics.inc_get();
        self.reader.metrics.add_bytes_out(data.len() as u64);

        Ok((Bytes::from(data), to_object_metadata(stored)))
    }

    async fn read_shards(
        &self,
        bucket: &str,
        key: &str,
        db: usize,
        pb: usize,
        size: u64,
    ) -> Result<Vec<u8>, StorageError> {
        use tokio::io::AsyncReadExt;

        let total = db + pb.max(1);
        let block_size = 1024 * 1024; // 1MB blocks for streaming
        let mut out = acquire_buffer_with_capacity(size as usize);

        // Open all shard files with BufReader for streaming
        let mut readers: Vec<Option<(BufReader<fs::File>, String)>> = Vec::with_capacity(total);

        for idx in 0..total {
            let drive = self.drive_for_shard(idx);
            let drive_name = drive.display().to_string();
            let shard_dir = self.shard_dir_on_drive(drive, bucket, key);
            let shard_path = shard_dir.join(format!("{}", idx));

            match fs::File::open(&shard_path).await {
                Ok(f) => {
                    let reader = BufReader::with_capacity(block_size, f);
                    readers.push(Some((reader, drive_name)));
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    readers.push(None);
                }
                Err(e) => {
                    return Err(StorageError::Internal(format!(
                        "open shard {} from {:?}: {}",
                        idx, drive, e
                    )))
                }
            }
        }

        let mut bytes_read = 0u64;

        // Stream blocks from all shards
        while bytes_read < size {
            let mut blocks: Vec<Option<Vec<u8>>> = vec![None; db];
            let parity_idx = total - 1;

            // Read one block from each data shard
            for idx in 0..db {
                if let Some((reader, drive_name)) = &mut readers[idx] {
                    let mut buf = vec![0u8; block_size];
                    let read_start = std::time::Instant::now();

                    match reader.read(&mut buf).await {
                        Ok(n) => {
                            buf.truncate(n);
                            if n > 0 {
                                prom_metrics::record_disk_shard_read(&drive_name, read_start.elapsed().as_secs_f64());
                                prom_metrics::increment_disk_shard_ops("read", &drive_name);
                                blocks[idx] = Some(buf);
                            }
                        }
                        Err(e) => {
                            return Err(StorageError::Internal(format!("read shard {}: {}", idx, e)))
                        }
                    }
                }
            }

            // Read one block from parity shard
            let mut parity_block: Option<Vec<u8>> = None;
            if let Some((reader, drive_name)) = &mut readers[parity_idx] {
                let mut buf = vec![0u8; block_size];
                let read_start = std::time::Instant::now();

                match reader.read(&mut buf).await {
                    Ok(n) => {
                        buf.truncate(n);
                        if n > 0 {
                            prom_metrics::record_disk_shard_read(&drive_name, read_start.elapsed().as_secs_f64());
                            prom_metrics::increment_disk_shard_ops("read", &drive_name);
                            parity_block = Some(buf);
                        }
                    }
                    Err(e) => {
                        return Err(StorageError::Internal(format!("read parity: {}", e)))
                    }
                }
            }

            // Check for missing shard in this stripe
            let missing_idx = blocks.iter().enumerate()
                .find_map(|(i, b)| if b.is_none() { Some(i) } else { None });

            if let Some(miss) = missing_idx {
                // Reconstruct missing block using XOR
                if pb == 0 || parity_block.is_none() {
                    return Err(StorageError::Internal("missing shard with no parity".into()));
                }

                let parity = parity_block.as_ref().unwrap();
                let block_len = blocks.iter().flatten().next().map(|b| b.len()).unwrap_or(parity.len());
                let mut reconstructed = vec![0u8; block_len];

                // Start with parity
                reconstructed[..parity.len().min(block_len)].copy_from_slice(&parity[..parity.len().min(block_len)]);

                // XOR with all other blocks
                for (i, block_opt) in blocks.iter().enumerate() {
                    if i != miss {
                        if let Some(block) = block_opt {
                            for (j, &byte) in block.iter().enumerate().take(block_len) {
                                reconstructed[j] ^= byte;
                            }
                        }
                    }
                }

                blocks[miss] = Some(reconstructed);
            }

            // Concatenate blocks into output
            for block_opt in blocks.iter() {
                if let Some(block) = block_opt {
                    let remaining = size as usize - out.len();
                    let to_copy = std::cmp::min(block.len(), remaining);
                    out.extend_from_slice(&block[..to_copy]);
                    bytes_read += to_copy as u64;

                    if out.len() >= size as usize {
                        break;
                    }
                }
            }

            // Check if all shards are exhausted
            if blocks.iter().all(|b| b.is_none()) {
                break;
            }
        }

        // Truncate to exact size
        out.truncate(size as usize);

        Ok(out)
    }

    async fn handle_head_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<ObjectMetadata, StorageError> {
        validate_bucket(bucket)?;
        validate_key(key)?;

        let obj_path = self.object_path(bucket, key);
        let meta_path = self.meta_path(&obj_path);

        let stored = read_stored_meta(&meta_path).await.map_err(|_| {
            StorageError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            }
        })?;

        Ok(to_object_metadata(stored))
    }

    async fn handle_delete_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<bool, StorageError> {
        validate_bucket(bucket)?;
        validate_key(key)?;

        let obj_path = self.object_path(bucket, key);
        let meta_path = self.meta_path(&obj_path);

        // Check if object exists
        if fs::metadata(&meta_path).await.is_err() {
            return Ok(false);
        }

        // Delete metadata file
        let _ = fs::remove_file(&meta_path).await;

        // Delete shard directories from all drives
        for drive in self.reader.drives.iter() {
            let shard_dir = self.shard_dir_on_drive(drive, bucket, key);
            let _ = fs::remove_dir_all(&shard_dir).await;
        }

        // Update metrics
        self.reader.metrics.inc_delete();

        Ok(true)
    }

    async fn handle_list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, ObjectMetadata)>, StorageError> {
        validate_bucket(bucket)?;

        let bucket_path = self.bucket_path(bucket);
        if fs::metadata(&bucket_path).await.is_err() {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }

        let mut results = Vec::new();
        self.list_objects_recursive(&bucket_path, bucket, prefix, limit, &mut results)
            .await?;

        // Update metrics
        self.reader.metrics.inc_list();

        Ok(results)
    }

    async fn list_objects_recursive(
        &self,
        dir: &Path,
        bucket: &str,
        prefix: &str,
        limit: usize,
        results: &mut Vec<(String, ObjectMetadata)>,
    ) -> Result<(), StorageError> {
        if results.len() >= limit {
            return Ok(());
        }

        let mut entries = match fs::read_dir(dir).await {
            Ok(e) => e,
            Err(_) => return Ok(()),
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            if results.len() >= limit {
                break;
            }

            let path = entry.path();
            let file_name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n,
                None => continue,
            };

            // Skip .meta, .shards directories
            if file_name.ends_with(".meta") || file_name.ends_with(".shards") {
                continue;
            }

            if path.is_dir() {
                Box::pin(self.list_objects_recursive(&path, bucket, prefix, limit, results))
                    .await?;
            } else {
                // Check if this is a metadata file
                let meta_path = self.meta_path(&path);
                if fs::metadata(&meta_path).await.is_ok() {
                    // Construct object key from path
                    let key = path
                        .strip_prefix(self.bucket_path(bucket))
                        .ok()
                        .and_then(|p| p.to_str())
                        .unwrap_or("");

                    // Check prefix match
                    if key.starts_with(prefix) {
                        if let Ok(stored) = read_stored_meta(&meta_path).await {
                            results.push((key.to_string(), to_object_metadata(stored)));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_create_bucket(&mut self, bucket: &str) -> Result<bool, StorageError> {
        validate_bucket(bucket)?;

        let bucket_path = self.bucket_path(bucket);

        // Check if bucket already exists
        if fs::metadata(&bucket_path).await.is_ok() {
            return Ok(false);
        }

        // Create bucket directory
        fs::create_dir_all(&bucket_path)
            .await
            .map_err(|e| StorageError::Internal(format!("create bucket: {}", e)))?;

        Ok(true)
    }

    async fn handle_delete_bucket(&mut self, bucket: &str) -> Result<bool, StorageError> {
        validate_bucket(bucket)?;

        let bucket_path = self.bucket_path(bucket);

        // Check if bucket exists
        if fs::metadata(&bucket_path).await.is_err() {
            return Ok(false);
        }

        // Delete bucket directory
        fs::remove_dir_all(&bucket_path)
            .await
            .map_err(|e| StorageError::Internal(format!("delete bucket: {}", e)))?;

        Ok(true)
    }

    async fn handle_list_buckets(&mut self) -> Result<Vec<String>, StorageError> {
        let mut buckets = Vec::new();
        let mut entries = match fs::read_dir(self.primary_drive()).await {
            Ok(e) => e,
            Err(e) => return Err(StorageError::Internal(format!("read root dir: {}", e))),
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    buckets.push(name.to_string());
                }
            }
        }

        Ok(buckets)
    }

    async fn handle_copy_object(
        &self,
        _src_bucket: &str,
        _src_key: &str,
        _dest_bucket: &str,
        _dest_key: &str,
        _headers: crate::actor::messages::ObjectHeaders,
    ) -> Result<crate::storage::ObjectMetadata, crate::storage::StorageError> {
        Err(crate::storage::StorageError::Internal(
            "Actor not yet implemented".to_string(),
        ))
    }

    async fn handle_initiate_multipart(
        &self,
        _bucket: &str,
        _key: &str,
        _headers: crate::actor::messages::ObjectHeaders,
    ) -> Result<String, crate::storage::StorageError> {
        Err(crate::storage::StorageError::Internal(
            "Actor not yet implemented".to_string(),
        ))
    }

    async fn handle_upload_part(
        &self,
        _upload_id: &str,
        _part_number: u32,
        _data: bytes::Bytes,
    ) -> Result<String, crate::storage::StorageError> {
        Err(crate::storage::StorageError::Internal(
            "Actor not yet implemented".to_string(),
        ))
    }

    async fn handle_complete_multipart(
        &self,
        _upload_id: &str,
        _parts: Vec<(u32, String)>,
    ) -> Result<crate::storage::ObjectMetadata, crate::storage::StorageError> {
        Err(crate::storage::StorageError::Internal(
            "Actor not yet implemented".to_string(),
        ))
    }

    async fn handle_abort_multipart(
        &self,
        _upload_id: &str,
    ) -> Result<(), crate::storage::StorageError> {
        Err(crate::storage::StorageError::Internal(
            "Actor not yet implemented".to_string(),
        ))
    }
}
