use mems3_grpc::auth::file_auth::FileAuthenticator;
use mems3_grpc::config::Config;
use mems3_grpc::server::{ConnectionManager, GrpcConnectionManager};
use mems3_grpc::storage::in_memory::InMemoryStorage;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cfg_path = std::env::var("MEMS3_CONFIG").unwrap_or_else(|_| "config.toml".into());
    let cfg = Config::from_path(&cfg_path)?;

    let addr: SocketAddr = cfg.listen_addr.parse()?;

    let auth = Arc::new(FileAuthenticator::new(cfg.auth_file).await?);
    let storage = Arc::new(InMemoryStorage::new());

    let server = GrpcConnectionManager::new(auth, storage);

    tracing::info!("mems3-grpc listening on {}", addr);
    tracing::info!("auth via metadata headers: x-access-key / x-secret-key");

    // Serve until killed (Ctrl-C).
    tokio::select! {
        r = server.serve(addr) => {
            if let Err(e) = r {
                tracing::error!("server exited with error: {e}");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::warn!("ctrl-c received, shutting down");
        }
    }

    Ok(())
}
