// Copyright PingCAP Inc. 2025.

//! Buffer implementation - circular buffer divided into blocks

use super::types::*;
use std::io::IoSlice;
use std::sync::atomic::{AtomicU16, Ordering};

/// Configuration for a buffer
#[derive(Debug, Clone)]
pub struct BufferConfig {
    /// Number of blocks in the buffer
    pub block_count: usize,
    /// Size of each block in bytes (must be power of 2)
    pub block_size: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            block_count: 16384,  // 16K blocks
            block_size: 4096,    // 4KB per block = 64MB total
        }
    }
}

impl BufferConfig {
    /// Get usable data size in each block (block_size - header - crc)
    pub fn data_size_per_block(&self) -> usize {
        self.block_size - std::mem::size_of::<BlockHeader>() - std::mem::size_of::<Crc32>()
    }

    /// Total buffer capacity
    pub fn total_capacity(&self) -> usize {
        self.block_count * self.data_size_per_block()
    }
}

/// A circular buffer divided into fixed-size blocks
///
/// Each block contains:
/// - BlockHeader (64 bytes, cache-line aligned)
/// - Data (configurable)
/// - CRC32 (4 bytes)
pub struct Buffer {
    /// Configuration
    config: BufferConfig,

    /// Block headers (one per block, cache-line aligned)
    headers: Vec<BlockHeader>,

    /// Data area (all blocks concatenated)
    data: Vec<u8>,

    /// CRC32 checksums (one per block)
    crcs: Vec<Crc32>,

    /// Current high water mark (virtual offset)
    hwm: Lsn,

    /// Low water mark (start of buffer)
    lwm: Lsn,

    /// Current write position within buffer
    write_pos: AtomicU16,

    /// Current block index
    current_block: u32,
}

impl Buffer {
    pub fn new(config: BufferConfig) -> Self {
        let block_count = config.block_count;

        // Allocate headers (cache-line aligned)
        let mut headers = Vec::with_capacity(block_count);
        for i in 0..block_count {
            headers.push(BlockHeader::new(i as BlockNo));
        }

        // Allocate data area
        let data_size = block_count * config.data_size_per_block();
        let data = vec![0u8; data_size];

        // Allocate CRCs
        let crcs = vec![0u32; block_count];

        Self {
            config,
            headers,
            data,
            crcs,
            hwm: 0,
            lwm: 0,
            write_pos: AtomicU16::new(0),
            current_block: 0,
        }
    }

    /// Initialize with starting LSN
    pub fn initialize(&mut self, start_lsn: Lsn) {
        self.hwm = start_lsn;
        self.lwm = start_lsn;
        self.write_pos.store(0, Ordering::Release);
        self.current_block = 0;

        // Reset all headers
        for (i, header) in self.headers.iter_mut().enumerate() {
            *header = BlockHeader::new(i as BlockNo);
        }
    }

    /// Append data to buffer, returns slot (LSN + length)
    pub fn append(&mut self, data: &[u8]) -> WalResult<Slot> {
        let data_len = data.len();

        // Check size limit (u16::MAX)
        if data_len > u16::MAX as usize {
            return Err(WalError::RecordTooLarge(data_len, u16::MAX as usize));
        }

        let data_len_u16 = data_len as u16;

        // Get current position
        let block_data_size = self.config.data_size_per_block();
        let current_pos = self.write_pos.load(Ordering::Acquire) as usize;

        // Check if data fits in current block
        if current_pos + data_len > block_data_size {
            // Data doesn't fit, caller needs to rotate to next buffer
            return Err(WalError::NotEnoughSpace);
        }

        // Record starting LSN
        let start_lsn = self.hwm;

        // Get current block's data area
        let block_offset = self.current_block as usize * block_data_size;
        let write_offset = block_offset + current_pos;

        // Copy data
        self.data[write_offset..write_offset + data_len].copy_from_slice(data);

        // Update write position
        self.write_pos.store((current_pos + data_len) as u16, Ordering::Release);

        // Update header
        self.headers[self.current_block as usize].data_len = (current_pos + data_len) as u16;

        // Update high water mark
        self.hwm += data_len as u64;

        Ok(Slot {
            lsn: start_lsn,
            len: data_len_u16,
        })
    }

    /// Check if buffer has space for N bytes
    pub fn has_space(&self, bytes: usize) -> bool {
        let current_pos = self.write_pos.load(Ordering::Acquire) as usize;
        let block_data_size = self.config.data_size_per_block();
        current_pos + bytes <= block_data_size
    }

    /// Get current HWM
    pub fn hwm(&self) -> Lsn {
        self.hwm
    }

    /// Get current LWM
    pub fn lwm(&self) -> Lsn {
        self.lwm
    }

    /// Prepare buffer for writing to storage
    /// Returns IoSlice array for vectored I/O
    pub fn prepare_for_write(&mut self) -> Vec<IoSlice<'_>> {
        let iovecs = Vec::new();

        // For each block with data, create iovec triplet:
        // [header, data, crc]
        let block_data_size = self.config.data_size_per_block();

        for block_idx in 0..self.config.block_count {
            let header = &mut self.headers[block_idx];

            // Only write blocks with data
            if header.data_len == 0 {
                break;
            }

            // Calculate CRC over data
            let block_offset = block_idx * block_data_size;
            let data_slice = &self.data[block_offset..block_offset + header.data_len as usize];
            self.crcs[block_idx] = crc32fast::hash(data_slice);

            // Convert header to big-endian
            header.to_be();

            // Add to iovec array
            // Note: We can't actually create IoSlice here safely because of lifetime issues
            // The caller needs to handle this
        }

        iovecs
    }

    /// Get data for writing as slices
    /// Returns (headers, data, crcs) that caller can write
    pub fn get_write_slices(&self) -> (&[BlockHeader], &[u8], &[Crc32]) {
        // Return entire arrays - caller filters by data_len
        (&self.headers, &self.data, &self.crcs)
    }

    /// Calculate how many bytes are ready to write
    pub fn bytes_ready(&self) -> usize {
        (self.hwm - self.lwm) as usize
    }

    /// Mark buffer as written (advance LWM)
    pub fn mark_written(&mut self, bytes: usize) {
        self.lwm += bytes as u64;
    }

    /// Reset buffer for reuse
    pub fn reset(&mut self, new_lwm: Lsn) {
        self.lwm = new_lwm;
        self.hwm = new_lwm;
        self.write_pos.store(0, Ordering::Release);
        self.current_block = 0;

        // Reset headers
        for (i, header) in self.headers.iter_mut().enumerate() {
            *header = BlockHeader::new(i as BlockNo);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_creation() {
        let config = BufferConfig::default();
        let buffer = Buffer::new(config.clone());

        assert_eq!(buffer.hwm(), 0);
        assert_eq!(buffer.lwm(), 0);
        assert_eq!(buffer.headers.len(), config.block_count);
    }

    #[test]
    fn test_append() {
        let config = BufferConfig {
            block_count: 4,
            block_size: 4096,
        };
        let mut buffer = Buffer::new(config);
        buffer.initialize(0);

        // Append some data
        let data = b"Hello, WAL!";
        let slot = buffer.append(data).unwrap();

        assert_eq!(slot.lsn, 0);
        assert_eq!(slot.len, data.len() as u16);
        assert_eq!(buffer.hwm(), data.len() as u64);
    }

    #[test]
    fn test_append_too_large() {
        let config = BufferConfig {
            block_count: 1,
            block_size: 4096,
        };
        let mut buffer = Buffer::new(config);

        // Try to append more than u16::MAX
        let data = vec![0u8; 70000];
        let result = buffer.append(&data);

        assert!(matches!(result, Err(WalError::RecordTooLarge(_, _))));
    }

    #[test]
    fn test_not_enough_space() {
        let config = BufferConfig {
            block_count: 1,
            block_size: 4096,
        };
        let mut buffer = Buffer::new(config.clone());
        buffer.initialize(0);

        // Fill up the block
        let block_data_size = config.data_size_per_block();
        let data = vec![0u8; block_data_size];
        let slot = buffer.append(&data).unwrap();
        assert_eq!(slot.len, block_data_size as u16);

        // Try to append more - should fail
        let result = buffer.append(b"overflow");
        assert!(matches!(result, Err(WalError::NotEnoughSpace)));
    }
}
