use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// gRPC listen address, e.g. "127.0.0.1:50051"
    pub listen_addr: String,

    /// Path to the auth file (credentials), e.g. "./creds.txt"
    pub auth_file: String,
}

impl Config {
    pub fn from_path(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let s = std::fs::read_to_string(path)?;
        let cfg: Config = toml::from_str(&s)?;
        Ok(cfg)
    }
}
