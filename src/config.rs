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
    /// erasure coding configuration
    #[serde(default)]
    pub erasure: ErasureConfig,
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

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: default_backend(),
            path: default_path(),
            drives: Vec::new(),
            erasure: ErasureConfig::default(),
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
