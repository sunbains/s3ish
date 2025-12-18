// Copyright PingCAP Inc. 2025.

//! Core types for WAL

use std::io;

/// Log Sequence Number - virtual offset (not physical)
/// This is a monotonically increasing counter
pub type Lsn = u64;

/// A slot represents a reserved region in the WAL
#[derive(Debug, Clone, Copy)]
pub struct Slot {
    /// Start LSN of this record
    pub lsn: Lsn,
    /// Length of the record in bytes
    pub len: u16,
}

/// Block number type
pub type BlockNo = u32;

/// Checkpoint number type
pub type CheckpointNo = u32;

/// CRC32 checksum type
pub type Crc32 = u32;

/// Checkpoint header stored in reserved blocks (0 and 1)
/// Block 0 = even checkpoint numbers, Block 1 = odd checkpoint numbers
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct CheckpointHeader {
    /// Magic number to identify valid checkpoint (0x57414C43 = "WALC")
    pub magic: u32,
    /// Checkpoint number (monotonically increasing)
    pub checkpoint_no: CheckpointNo,
    /// LSN at the time of checkpoint
    pub lsn: Lsn,
    /// Unix timestamp (seconds)
    pub timestamp: u64,
    /// CRC32 checksum of this header
    pub crc: Crc32,
    /// Padding to fill cache line
    _padding: [u8; 28],
}

impl CheckpointHeader {
    const MAGIC: u32 = 0x57414C43; // "WALC" in hex

    pub fn new(checkpoint_no: CheckpointNo, lsn: Lsn) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut header = Self {
            magic: Self::MAGIC,
            checkpoint_no,
            lsn,
            timestamp,
            crc: 0,
            _padding: [0u8; 28],
        };

        // Calculate CRC over all fields except crc itself
        header.crc = header.calculate_crc();
        header
    }

    fn calculate_crc(&self) -> Crc32 {
        let mut data = Vec::new();
        data.extend_from_slice(&self.magic.to_le_bytes());
        data.extend_from_slice(&self.checkpoint_no.to_le_bytes());
        data.extend_from_slice(&self.lsn.to_le_bytes());
        data.extend_from_slice(&self.timestamp.to_le_bytes());
        crc32fast::hash(&data)
    }

    pub fn is_valid(&self) -> bool {
        if self.magic != Self::MAGIC {
            return false;
        }
        let expected_crc = self.calculate_crc();
        self.crc == expected_crc
    }

    /// Get the block index for this checkpoint (0 for even, 1 for odd)
    pub fn block_index(&self) -> usize {
        (self.checkpoint_no % 2) as usize
    }

    /// Convert to network byte order before writing
    pub fn to_be(&mut self) {
        self.magic = self.magic.to_be();
        self.checkpoint_no = self.checkpoint_no.to_be();
        self.lsn = self.lsn.to_be();
        self.timestamp = self.timestamp.to_be();
        self.crc = self.crc.to_be();
    }

    /// Convert from network byte order after reading
    pub fn from_be(&mut self) {
        self.magic = u32::from_be(self.magic);
        self.checkpoint_no = CheckpointNo::from_be(self.checkpoint_no);
        self.lsn = Lsn::from_be(self.lsn);
        self.timestamp = u64::from_be(self.timestamp);
        self.crc = Crc32::from_be(self.crc);
    }
}

// Verify size
const _: () = assert!(std::mem::size_of::<CheckpointHeader>() == 64);
const _: () = assert!(std::mem::align_of::<CheckpointHeader>() == 64);

/// Block header stored at the beginning of each block
/// Must be packed and aligned correctly
#[repr(C, align(64))] // Cache-line aligned (64 bytes)
#[derive(Debug, Clone, Copy)]
pub struct BlockHeader {
    /// Block number (top bit is flush bit)
    pub block_no: BlockNo,
    /// Checkpoint number
    pub checkpoint_no: CheckpointNo,
    /// Offset of first record in block
    pub first_rec_offset: u16,
    /// Valid data length in block
    pub data_len: u16,
}

impl BlockHeader {
    const FLUSH_BIT_MASK: BlockNo = 0x8000_0000;

    pub fn new(block_no: BlockNo) -> Self {
        Self {
            block_no,
            checkpoint_no: 0,
            first_rec_offset: 0,
            data_len: 0,
        }
    }

    pub fn get_block_no(&self) -> BlockNo {
        self.block_no & !Self::FLUSH_BIT_MASK
    }

    pub fn set_flush_bit(&mut self, val: bool) {
        if val {
            self.block_no |= Self::FLUSH_BIT_MASK;
        } else {
            self.block_no &= !Self::FLUSH_BIT_MASK;
        }
    }

    pub fn get_flush_bit(&self) -> bool {
        (self.block_no & Self::FLUSH_BIT_MASK) != 0
    }

    /// Convert to network byte order before writing
    pub fn to_be(&mut self) {
        self.block_no = self.block_no.to_be();
        self.checkpoint_no = self.checkpoint_no.to_be();
        self.first_rec_offset = self.first_rec_offset.to_be();
        self.data_len = self.data_len.to_be();
    }

    /// Convert from network byte order after reading
    pub fn from_be(&mut self) {
        self.block_no = BlockNo::from_be(self.block_no);
        self.checkpoint_no = CheckpointNo::from_be(self.checkpoint_no);
        self.first_rec_offset = u16::from_be(self.first_rec_offset);
        self.data_len = u16::from_be(self.data_len);
    }
}

// Verify sizes
const _: () = assert!(std::mem::size_of::<BlockHeader>() == 64);
const _: () = assert!(std::mem::align_of::<BlockHeader>() == 64);

/// WAL errors
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("Not enough space in buffer")]
    NotEnoughSpace,

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Buffer is full")]
    BufferFull,

    #[error("Invalid record size: {0} (max: {1})")]
    RecordTooLarge(usize, usize),

    #[error("Invalid checkpoint header")]
    InvalidCheckpoint,

    #[error("No write callback set")]
    NoWriteCallback,

    #[error("No checkpoint callback set")]
    NoCheckpointCallback,
}

pub type WalResult<T> = Result<T, WalError>;
