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

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;

/// Configuration for write-back cache
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Flush interval in seconds (0 = disabled)
    pub flush_interval_secs: u64,

    /// Flush after this many bytes written (0 = disabled)
    pub flush_threshold_bytes: usize,

    /// Enable batched fsync
    pub enabled: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            flush_interval_secs: 5,
            flush_threshold_bytes: 100 * 1024 * 1024, // 100 MB
            enabled: true,
        }
    }
}

/// Write-back cache that batches fsync operations
pub struct WriteBackCache {
    /// Files that need fsync
    dirty_files: Arc<RwLock<HashSet<PathBuf>>>,

    /// Bytes written since last flush
    bytes_written: Arc<AtomicUsize>,

    /// Configuration
    config: CacheConfig,

    /// Shutdown signal
    shutdown: Arc<AtomicBool>,

    /// Background flush task
    flush_task: Option<tokio::task::JoinHandle<()>>,
}

impl WriteBackCache {
    /// Create a new write-back cache
    pub fn new(config: CacheConfig) -> Self {
        let dirty_files = Arc::new(RwLock::new(HashSet::new()));
        let bytes_written = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));

        let mut cache = Self {
            dirty_files: dirty_files.clone(),
            bytes_written: bytes_written.clone(),
            config: config.clone(),
            shutdown: shutdown.clone(),
            flush_task: None,
        };

        if config.enabled {
            cache.flush_task = Some(Self::spawn_flush_task(
                dirty_files,
                bytes_written,
                config,
                shutdown,
            ));
        }

        cache
    }

    /// Spawn background task that periodically flushes dirty files
    fn spawn_flush_task(
        dirty_files: Arc<RwLock<HashSet<PathBuf>>>,
        bytes_written: Arc<AtomicUsize>,
        config: CacheConfig,
        shutdown: Arc<AtomicBool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(config.flush_interval_secs));

            loop {
                ticker.tick().await;

                if shutdown.load(Ordering::Relaxed) {
                    tracing::info!("Write-back cache shutting down, flushing all dirty files");
                    Self::flush_dirty_files_internal(&dirty_files, &bytes_written).await;
                    break;
                }

                // Check if we should flush based on threshold
                let bytes = bytes_written.load(Ordering::Relaxed);
                let should_flush = config.flush_threshold_bytes > 0
                    && bytes >= config.flush_threshold_bytes;

                if should_flush || config.flush_interval_secs > 0 {
                    Self::flush_dirty_files_internal(&dirty_files, &bytes_written).await;
                }
            }
        })
    }

    /// Check if cache is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Register a file as dirty (needs fsync)
    pub async fn mark_dirty(&self, path: PathBuf, bytes: usize) {
        if !self.config.enabled {
            return;
        }

        let mut dirty = self.dirty_files.write().await;
        dirty.insert(path);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Force flush all dirty files
    pub async fn flush(&self) {
        Self::flush_dirty_files_internal(&self.dirty_files, &self.bytes_written).await;
    }

    /// Internal flush implementation
    async fn flush_dirty_files_internal(
        dirty_files: &Arc<RwLock<HashSet<PathBuf>>>,
        bytes_written: &Arc<AtomicUsize>,
    ) {
        // Take snapshot of dirty files
        let files_to_flush: Vec<PathBuf> = {
            let mut dirty = dirty_files.write().await;
            let snapshot: Vec<PathBuf> = dirty.drain().collect();
            snapshot
        };

        if files_to_flush.is_empty() {
            return;
        }

        let bytes = bytes_written.swap(0, Ordering::Relaxed);
        tracing::debug!(
            file_count = files_to_flush.len(),
            bytes_mb = bytes / (1024 * 1024),
            "Flushing dirty files"
        );

        // Flush files in parallel using spawn_blocking
        let mut handles = Vec::new();
        for path in files_to_flush {
            let handle = tokio::task::spawn_blocking(move || {
                if let Ok(file) = std::fs::OpenOptions::new().write(true).open(&path) {
                    if let Err(e) = file.sync_data() {
                        tracing::warn!(path = ?path, error = %e, "Failed to sync file");
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for all syncs to complete
        for handle in handles {
            let _ = handle.await;
        }

        tracing::debug!("Flush complete");
    }

    /// Shutdown the cache and flush all pending writes
    pub async fn shutdown(&mut self) {
        if !self.config.enabled {
            return;
        }

        tracing::info!("Shutting down write-back cache");
        self.shutdown.store(true, Ordering::Relaxed);

        if let Some(task) = self.flush_task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for WriteBackCache {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_write_back_cache() {
        let config = CacheConfig {
            flush_interval_secs: 1,
            flush_threshold_bytes: 1024,
            enabled: true,
        };

        let cache = WriteBackCache::new(config);
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.dat");

        // Write some data
        {
            let mut file = std::fs::File::create(&test_file).unwrap();
            file.write_all(b"test data").unwrap();
        }

        // Mark as dirty
        cache.mark_dirty(test_file.clone(), 9).await;

        // Wait for flush
        tokio::time::sleep(Duration::from_secs(2)).await;

        // File should have been synced
        // (In a real test, we'd verify sync was called)
    }
}
