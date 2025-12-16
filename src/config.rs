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
