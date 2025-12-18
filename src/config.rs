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

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// gRPC listen address, e.g. "127.0.0.1:50051"
    pub listen_addr: String,

    /// Path to the auth file (credentials), e.g. "./creds.txt"
    pub auth_file: String,

    /// Region reported to S3 clients (used for CreateBucketConfiguration validation)
    #[serde(default = "default_region")]
    pub region: String,

    /// Prefix for x-amz-request-id / x-amz-id-2 headers (helpful for log correlation)
    #[serde(default = "default_request_id_prefix")]
    pub request_id_prefix: String,

    #[serde(default)]
    pub storage: StorageConfig,

    #[serde(default)]
    pub lifecycle: LifecycleConfig,

    #[serde(default)]
    pub test: TestConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    /// backend can be "in-memory" or "file"
    #[serde(default = "default_backend")]
    pub backend: String,
    /// path for file backend (deprecated - use drives for multi-drive setups)
    #[serde(default = "default_path")]
    pub path: String,
    /// multiple drive paths for file backend (spreads shards across drives)
    #[serde(default)]
    pub drives: Vec<String>,
    /// optional dedicated drive for metadata files (.meta, .versioning, .lifecycle)
    /// if not specified, metadata goes to first data drive
    #[serde(default)]
    pub metadata_drive: Option<String>,
    /// erasure coding configuration
    #[serde(default)]
    pub erasure: ErasureConfig,
    /// write-back cache configuration
    #[serde(default)]
    pub cache: CacheConfig,
    /// I/O buffer configuration
    #[serde(default)]
    pub io: IoConfig,
    /// Actor configuration
    #[serde(default)]
    pub actors: ActorConfig,
    /// WAL (Write-Ahead Log) configuration
    #[serde(default)]
    pub wal: WalConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ActorConfig {
    /// Number of parallel actors for concurrent processing
    /// Higher values = better throughput but more memory/CPU
    /// Recommended: 3-8 for most systems
    #[serde(default = "default_num_actors")]
    pub num_actors: usize,
}

impl Default for ActorConfig {
    fn default() -> Self {
        Self {
            num_actors: default_num_actors(),
        }
    }
}

fn default_num_actors() -> usize {
    8
}

#[derive(Debug, Clone, Deserialize)]
pub struct WalConfig {
    /// Checkpoint interval in seconds (0 = disabled, default = 1)
    #[serde(default = "default_checkpoint_interval_secs")]
    pub checkpoint_interval_secs: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            checkpoint_interval_secs: default_checkpoint_interval_secs(),
        }
    }
}

fn default_checkpoint_interval_secs() -> u64 {
    1
}

#[derive(Debug, Clone, Deserialize)]
pub struct ErasureConfig {
    /// Number of data blocks (shards) per object
    #[serde(default = "default_data_blocks")]
    pub data_blocks: usize,
    /// Number of parity blocks for redundancy/recovery
    #[serde(default = "default_parity_blocks")]
    pub parity_blocks: usize,
    /// Block size in bytes (size of each shard)
    #[serde(default = "default_block_size")]
    pub block_size: usize,
}

impl Default for ErasureConfig {
    fn default() -> Self {
        Self {
            data_blocks: default_data_blocks(),
            parity_blocks: default_parity_blocks(),
            block_size: default_block_size(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    /// Enable write-back cache for batched fsync
    #[serde(default = "default_cache_enabled")]
    pub enabled: bool,
    /// Flush interval in seconds (0 = only on threshold)
    #[serde(default = "default_cache_flush_interval_secs")]
    pub flush_interval_secs: u64,
    /// Flush after this many bytes written (0 = only on timer)
    #[serde(default = "default_cache_flush_threshold_bytes")]
    pub flush_threshold_bytes: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IoConfig {
    /// Buffer size for writes in bytes (larger = fewer syscalls, more memory)
    #[serde(default = "default_write_buffer_size")]
    pub write_buffer_size: usize,
    /// Buffer size for reads in bytes (larger = fewer syscalls, more memory)
    #[serde(default = "default_read_buffer_size")]
    pub read_buffer_size: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: default_cache_enabled(),
            flush_interval_secs: default_cache_flush_interval_secs(),
            flush_threshold_bytes: default_cache_flush_threshold_bytes(),
        }
    }
}

impl Default for IoConfig {
    fn default() -> Self {
        Self {
            write_buffer_size: default_write_buffer_size(),
            read_buffer_size: default_read_buffer_size(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: default_backend(),
            path: default_path(),
            drives: Vec::new(),
            metadata_drive: None,
            erasure: ErasureConfig::default(),
            cache: CacheConfig::default(),
            io: IoConfig::default(),
            actors: ActorConfig::default(),
            wal: WalConfig::default(),
        }
    }
}

impl StorageConfig {
    /// Get effective drives - returns drives vec if populated, otherwise single path
    pub fn effective_drives(&self) -> Vec<String> {
        if !self.drives.is_empty() {
            self.drives.clone()
        } else {
            vec![self.path.clone()]
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LifecycleConfig {
    /// Whether lifecycle execution is enabled
    #[serde(default = "default_lifecycle_enabled")]
    pub enabled: bool,

    /// Interval between lifecycle policy checks (in seconds)
    #[serde(default = "default_lifecycle_interval")]
    pub check_interval_secs: u64,

    /// Maximum number of objects to delete concurrently
    #[serde(default = "default_max_concurrent_deletes")]
    pub max_concurrent_deletes: usize,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            enabled: default_lifecycle_enabled(),
            check_interval_secs: default_lifecycle_interval(),
            max_concurrent_deletes: default_max_concurrent_deletes(),
        }
    }
}

impl Config {
    pub fn from_path(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let s = std::fs::read_to_string(path)?;
        let cfg: Config = toml::from_str(&s)?;
        Ok(cfg)
    }
}

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_request_id_prefix() -> String {
    "req-".to_string()
}

fn default_backend() -> String {
    "in-memory".to_string()
}

fn default_path() -> String {
    "./data".to_string()
}

fn default_lifecycle_enabled() -> bool {
    true
}

fn default_lifecycle_interval() -> u64 {
    3600 // 1 hour
}

fn default_max_concurrent_deletes() -> usize {
    100
}

fn default_data_blocks() -> usize {
    4
}

fn default_parity_blocks() -> usize {
    1
}

fn default_block_size() -> usize {
    1024 * 1024 // 1 MB
}

fn default_cache_enabled() -> bool {
    true
}

fn default_cache_flush_interval_secs() -> u64 {
    5 // 5 seconds
}

fn default_cache_flush_threshold_bytes() -> usize {
    100 * 1024 * 1024 // 100 MB
}

fn default_write_buffer_size() -> usize {
    1024 * 1024 // 1 MB - matches block_size for optimal batching
}

fn default_read_buffer_size() -> usize {
    1024 * 1024 // 1 MB - matches block_size for optimal sequential reads
}

#[derive(Debug, Clone, Deserialize)]
pub struct TestConfig {
    /// Test duration in seconds
    #[serde(default = "default_test_duration_secs")]
    pub duration_secs: u64,
    /// Object size for tests in bytes
    #[serde(default = "default_test_object_size")]
    pub object_size: usize,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            duration_secs: default_test_duration_secs(),
            object_size: default_test_object_size(),
        }
    }
}

fn default_test_duration_secs() -> u64 {
    10 // 10 seconds
}

fn default_test_object_size() -> usize {
    1024 * 1024 // 1 MB
}
