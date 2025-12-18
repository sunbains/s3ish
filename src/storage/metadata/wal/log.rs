// Copyright PingCAP Inc. 2025.

//! Log implementation - manages buffer pool and write operations

use super::buffer::{Buffer, BufferConfig};
use super::types::*;
use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicU64, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;

/// Callback type for writing buffers to storage
/// Takes slices (headers, data, crcs) and returns bytes written
pub type WriteCallback = Arc<dyn Fn(&[BlockHeader], &[u8], &[Crc32]) -> io::Result<usize> + Send + Sync>;

/// Callback type for writing checkpoint headers
/// Takes checkpoint header and block index (0 or 1) and returns success
pub type CheckpointCallback = Arc<dyn Fn(&CheckpointHeader, usize) -> io::Result<()> + Send + Sync>;

/// Configuration for the Log
#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Buffer configuration
    pub buffer_config: BufferConfig,
    /// Number of buffers in the pool
    pub buffer_pool_size: usize,
    /// Checkpoint interval in seconds (0 = disabled)
    pub checkpoint_interval_secs: u64,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            buffer_config: BufferConfig::default(),
            buffer_pool_size: 3, // Active + 2 for double buffering
            checkpoint_interval_secs: 1, // Default 1 second
        }
    }
}

/// Write-Ahead Log managing a pool of circular buffers
///
/// The Log maintains a pool of buffers and rotates through them as they fill.
/// When a buffer is ready to write, the user-provided callback is invoked.
pub struct Log {
    /// Configuration
    config: LogConfig,

    /// All buffers in the pool
    buffers: Vec<Buffer>,

    /// Index of the currently active buffer
    active_buffer: Option<usize>,

    /// Queue of free buffer indices
    free_buffers: VecDeque<usize>,

    /// Queue of buffer indices ready for I/O
    ready_for_io: VecDeque<usize>,

    /// Current LSN (virtual offset)
    current_lsn: AtomicU64,

    /// Current checkpoint number
    checkpoint_no: AtomicU32,

    /// Write callback for flushing buffers
    write_callback: Option<WriteCallback>,

    /// Checkpoint callback for writing checkpoint headers
    checkpoint_callback: Option<CheckpointCallback>,
}

impl Log {
    /// Create a new Log with the given configuration
    pub fn new(config: LogConfig) -> Self {
        let pool_size = config.buffer_pool_size;

        // Create all buffers
        let mut buffers = Vec::with_capacity(pool_size);
        let mut free_buffers = VecDeque::with_capacity(pool_size);

        for i in 0..pool_size {
            buffers.push(Buffer::new(config.buffer_config.clone()));
            free_buffers.push_back(i);
        }

        Self {
            config,
            buffers,
            active_buffer: None,
            free_buffers,
            ready_for_io: VecDeque::new(),
            current_lsn: AtomicU64::new(0),
            checkpoint_no: AtomicU32::new(0),
            write_callback: None,
            checkpoint_callback: None,
        }
    }

    /// Initialize the log with a starting LSN
    pub fn initialize(&mut self, start_lsn: Lsn) {
        self.current_lsn.store(start_lsn, Ordering::Release);

        // Get first buffer from free list
        if let Some(buffer_idx) = self.free_buffers.pop_front() {
            self.buffers[buffer_idx].initialize(start_lsn);
            self.active_buffer = Some(buffer_idx);
        }
    }

    /// Set the write callback
    pub fn set_write_callback(&mut self, callback: WriteCallback) {
        self.write_callback = Some(callback);
    }

    /// Set the checkpoint callback
    pub fn set_checkpoint_callback(&mut self, callback: CheckpointCallback) {
        self.checkpoint_callback = Some(callback);
    }

    /// Append data to the log, returns slot (LSN + length)
    pub fn append(&mut self, data: &[u8]) -> WalResult<Slot> {
        // Ensure we have an active buffer
        if self.active_buffer.is_none() {
            return Err(WalError::BufferFull);
        }

        let buffer_idx = self.active_buffer.unwrap();
        let buffer = &mut self.buffers[buffer_idx];

        // Try to append to current buffer
        match buffer.append(data) {
            Ok(slot) => {
                // Update global LSN
                self.current_lsn.store(buffer.hwm(), Ordering::Release);
                Ok(slot)
            }
            Err(WalError::NotEnoughSpace) => {
                // Buffer is full, need to rotate
                self.rotate_buffer()?;

                // Retry with new buffer
                let buffer_idx = self.active_buffer.ok_or(WalError::BufferFull)?;
                let buffer = &mut self.buffers[buffer_idx];
                let slot = buffer.append(data)?;

                self.current_lsn.store(buffer.hwm(), Ordering::Release);
                Ok(slot)
            }
            Err(e) => Err(e),
        }
    }

    /// Rotate to a new buffer
    fn rotate_buffer(&mut self) -> WalResult<()> {
        if let Some(old_idx) = self.active_buffer.take() {
            // Mark old buffer as ready for I/O
            self.ready_for_io.push_back(old_idx);
        }

        // Get a new buffer from free list
        match self.free_buffers.pop_front() {
            Some(new_idx) => {
                let current_lsn = self.current_lsn.load(Ordering::Acquire);
                self.buffers[new_idx].initialize(current_lsn);
                self.active_buffer = Some(new_idx);
                Ok(())
            }
            None => Err(WalError::BufferFull),
        }
    }

    /// Flush all pending data to storage
    pub fn flush(&mut self) -> WalResult<()> {
        // Mark active buffer as ready if it has data
        if let Some(active_idx) = self.active_buffer.take() {
            if self.buffers[active_idx].bytes_ready() > 0 {
                self.ready_for_io.push_back(active_idx);
            } else {
                // No data, return to free list
                self.free_buffers.push_back(active_idx);
            }
        }

        // Write all ready buffers
        self.write_ready_buffers()?;

        // Get a new active buffer
        if let Some(new_idx) = self.free_buffers.pop_front() {
            let current_lsn = self.current_lsn.load(Ordering::Acquire);
            self.buffers[new_idx].initialize(current_lsn);
            self.active_buffer = Some(new_idx);
        }

        Ok(())
    }

    /// Write all buffers in ready_for_io queue
    fn write_ready_buffers(&mut self) -> WalResult<()> {
        let callback = self.write_callback.as_ref()
            .ok_or_else(|| WalError::Io(io::Error::new(
                io::ErrorKind::NotConnected,
                "No write callback set"
            )))?;

        while let Some(buffer_idx) = self.ready_for_io.pop_front() {
            let buffer = &mut self.buffers[buffer_idx];

            // Prepare buffer for writing (calculates CRCs, converts to big-endian)
            let _ = buffer.prepare_for_write();

            // Get slices to write
            let (headers, data, crcs) = buffer.get_write_slices();

            // Invoke callback to write data
            let bytes_written = callback(headers, data, crcs)
                .map_err(WalError::Io)?;

            // Mark as written
            buffer.mark_written(bytes_written);

            // Reset buffer and return to free list
            let new_lwm = buffer.hwm();
            buffer.reset(new_lwm);
            self.free_buffers.push_back(buffer_idx);
        }

        Ok(())
    }

    /// Get current LSN
    pub fn current_lsn(&self) -> Lsn {
        self.current_lsn.load(Ordering::Acquire)
    }

    /// Check if there's space for N bytes in the current buffer
    pub fn has_space(&self, bytes: usize) -> bool {
        if let Some(buffer_idx) = self.active_buffer {
            self.buffers[buffer_idx].has_space(bytes)
        } else {
            false
        }
    }

    /// Get number of free buffers available
    pub fn free_buffer_count(&self) -> usize {
        self.free_buffers.len() + if self.active_buffer.is_none() { 0 } else { 0 }
    }

    /// Get number of buffers ready for I/O
    pub fn ready_buffer_count(&self) -> usize {
        self.ready_for_io.len()
    }

    /// Get current checkpoint number
    pub fn checkpoint_number(&self) -> CheckpointNo {
        self.checkpoint_no.load(Ordering::Acquire)
    }

    /// Create a checkpoint
    /// Flushes all pending data and writes checkpoint header to reserved block
    pub fn checkpoint(&mut self) -> WalResult<CheckpointNo> {
        // Flush all pending data first
        self.flush()?;

        // Get checkpoint callback
        let callback = self.checkpoint_callback.as_ref()
            .ok_or(WalError::NoCheckpointCallback)?;

        // Increment checkpoint number
        let checkpoint_no = self.checkpoint_no.fetch_add(1, Ordering::SeqCst) + 1;
        let current_lsn = self.current_lsn.load(Ordering::Acquire);

        // Create checkpoint header (in native byte order)
        let header = CheckpointHeader::new(checkpoint_no, current_lsn);

        // Determine which block to write (0 for even, 1 for odd)
        let block_idx = header.block_index();

        // Write checkpoint header via callback
        // Note: callback is responsible for byte order conversion if needed for I/O
        callback(&header, block_idx)
            .map_err(WalError::Io)?;

        Ok(checkpoint_no)
    }

    /// Spawn a periodic checkpoint task
    /// Returns a JoinHandle that can be used to cancel the task
    pub fn spawn_checkpoint_task(
        log: Arc<tokio::sync::Mutex<Self>>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let interval_secs = {
                let log_guard = log.lock().await;
                log_guard.config.checkpoint_interval_secs
            };

            if interval_secs == 0 {
                // Checkpointing disabled
                return;
            }

            let mut ticker = interval(Duration::from_secs(interval_secs));

            loop {
                ticker.tick().await;

                let mut log_guard = log.lock().await;
                if let Err(e) = log_guard.checkpoint() {
                    tracing::error!("Checkpoint failed: {}", e);
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn test_log_creation() {
        let config = LogConfig::default();
        let mut log = Log::new(config.clone());
        log.initialize(0);

        assert_eq!(log.current_lsn(), 0);
        assert!(log.active_buffer.is_some());
    }

    #[test]
    fn test_append() {
        let config = LogConfig {
            buffer_config: BufferConfig {
                block_count: 4,
                block_size: 4096,
            },
            buffer_pool_size: 3,
            checkpoint_interval_secs: 0,
        };

        let mut log = Log::new(config);
        log.initialize(0);

        // Append some data
        let data = b"Hello, WAL!";
        let slot = log.append(data).unwrap();

        assert_eq!(slot.lsn, 0);
        assert_eq!(slot.len, data.len() as u16);
        assert_eq!(log.current_lsn(), data.len() as u64);
    }

    #[test]
    fn test_buffer_rotation() {
        let config = LogConfig {
            buffer_config: BufferConfig {
                block_count: 1,
                block_size: 4096,
            },
            buffer_pool_size: 3,
            checkpoint_interval_secs: 0,
        };

        let mut log = Log::new(config.clone());
        log.initialize(0);

        // Fill up first buffer
        let block_data_size = config.buffer_config.data_size_per_block();
        let data = vec![0u8; block_data_size];
        let slot1 = log.append(&data).unwrap();
        assert_eq!(slot1.lsn, 0);

        // Next append should trigger rotation
        let data2 = b"overflow";
        let slot2 = log.append(data2).unwrap();
        assert_eq!(slot2.lsn, block_data_size as u64);

        // Should have 1 buffer ready for I/O
        assert_eq!(log.ready_buffer_count(), 1);
    }

    #[test]
    fn test_flush_with_callback() {
        let config = LogConfig {
            buffer_config: BufferConfig {
                block_count: 2,
                block_size: 4096,
            },
            buffer_pool_size: 3,
            checkpoint_interval_secs: 0,
        };

        let mut log = Log::new(config);
        log.initialize(0);

        // Track bytes written
        let written = Arc::new(Mutex::new(0usize));
        let written_clone = written.clone();

        log.set_write_callback(Arc::new(move |_headers, _data, _crcs| {
            let mut w = written_clone.lock().unwrap();
            *w += 1;
            Ok(100) // Simulate writing 100 bytes
        }));

        // Append some data
        let data = b"Test data for flush";
        log.append(data).unwrap();

        // Flush should trigger write
        log.flush().unwrap();

        let writes = *written.lock().unwrap();
        assert_eq!(writes, 1);
    }

    #[test]
    fn test_has_space() {
        let config = LogConfig {
            buffer_config: BufferConfig {
                block_count: 1,
                block_size: 4096,
            },
            buffer_pool_size: 2,
            checkpoint_interval_secs: 0,
        };

        let mut log = Log::new(config.clone());
        log.initialize(0);

        let block_data_size = config.buffer_config.data_size_per_block();

        assert!(log.has_space(100));
        assert!(log.has_space(block_data_size));

        // Fill buffer
        let data = vec![0u8; block_data_size];
        log.append(&data).unwrap();

        assert!(!log.has_space(1));
    }

    #[test]
    fn test_checkpoint() {
        let config = LogConfig {
            buffer_config: BufferConfig {
                block_count: 4,
                block_size: 4096,
            },
            buffer_pool_size: 3,
            checkpoint_interval_secs: 0, // Disabled
        };

        let mut log = Log::new(config);
        log.initialize(0);

        // Set up write callback
        log.set_write_callback(Arc::new(|_headers, _data, _crcs| Ok(100)));

        // Track checkpoint writes
        let checkpoints = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let checkpoints_clone = checkpoints.clone();

        log.set_checkpoint_callback(Arc::new(move |header, block_idx| {
            let mut cps = checkpoints_clone.blocking_lock();
            cps.push((header.checkpoint_no, header.lsn, block_idx));
            Ok(())
        }));

        // Append some data
        log.append(b"test data 1").unwrap();
        log.append(b"test data 2").unwrap();

        // Create first checkpoint
        let cp1 = log.checkpoint().unwrap();
        assert_eq!(cp1, 1);
        assert_eq!(log.checkpoint_number(), 1);

        // Verify checkpoint was written
        let cps = checkpoints.blocking_lock();
        assert_eq!(cps.len(), 1);
        assert_eq!(cps[0].0, 1); // checkpoint_no
        assert_eq!(cps[0].2, 1); // block_idx (odd checkpoint goes to block 1)
        drop(cps);

        // Append more data
        log.append(b"test data 3").unwrap();

        // Create second checkpoint
        let cp2 = log.checkpoint().unwrap();
        assert_eq!(cp2, 2);
        assert_eq!(log.checkpoint_number(), 2);

        // Verify second checkpoint
        let cps = checkpoints.blocking_lock();
        assert_eq!(cps.len(), 2);
        assert_eq!(cps[1].0, 2); // checkpoint_no
        assert_eq!(cps[1].2, 0); // block_idx (even checkpoint goes to block 0)
    }

    #[test]
    fn test_checkpoint_alternating_blocks() {
        let config = LogConfig::default();
        let mut log = Log::new(config);
        log.initialize(0);

        log.set_write_callback(Arc::new(|_headers, _data, _crcs| Ok(0)));

        let blocks = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let blocks_clone = blocks.clone();

        log.set_checkpoint_callback(Arc::new(move |header, block_idx| {
            let mut b = blocks_clone.blocking_lock();
            b.push((header.checkpoint_no, block_idx));
            Ok(())
        }));

        // Create multiple checkpoints
        for i in 1..=10 {
            let cp = log.checkpoint().unwrap();
            assert_eq!(cp, i);
        }

        // Verify alternating pattern
        let b = blocks.blocking_lock();
        assert_eq!(b.len(), 10);

        for (i, &(checkpoint_no, block_idx)) in b.iter().enumerate() {
            let expected_no = (i + 1) as u32;
            let expected_block = if expected_no % 2 == 0 { 0 } else { 1 };
            assert_eq!(checkpoint_no, expected_no);
            assert_eq!(block_idx, expected_block);
        }
    }

    #[test]
    fn test_checkpoint_without_callback() {
        let config = LogConfig::default();
        let mut log = Log::new(config);
        log.initialize(0);

        log.set_write_callback(Arc::new(|_headers, _data, _crcs| Ok(0)));

        // Try to checkpoint without setting callback
        let result = log.checkpoint();
        assert!(matches!(result, Err(WalError::NoCheckpointCallback)));
    }

    #[tokio::test]
    async fn test_periodic_checkpoint() {
        let config = LogConfig {
            buffer_config: BufferConfig {
                block_count: 4,
                block_size: 4096,
            },
            buffer_pool_size: 3,
            checkpoint_interval_secs: 1, // 1 second
        };

        let mut log = Log::new(config);
        log.initialize(0);

        log.set_write_callback(Arc::new(|_headers, _data, _crcs| Ok(0)));

        let checkpoint_count = Arc::new(tokio::sync::Mutex::new(0u32));
        let checkpoint_count_clone = checkpoint_count.clone();

        log.set_checkpoint_callback(Arc::new(move |_header, _block_idx| {
            let count = checkpoint_count_clone.clone();
            tokio::spawn(async move {
                let mut c = count.lock().await;
                *c += 1;
            });
            Ok(())
        }));

        let log = Arc::new(tokio::sync::Mutex::new(log));

        // Spawn checkpoint task
        let handle = Log::spawn_checkpoint_task(log.clone());

        // Wait for a few checkpoints
        tokio::time::sleep(Duration::from_millis(2500)).await;

        // Should have at least 2 checkpoints in 2.5 seconds (with 1s interval)
        let count = *checkpoint_count.lock().await;
        assert!(count >= 2, "Expected at least 2 checkpoints, got {}", count);

        // Cancel task
        handle.abort();
    }
}
