use clap::Parser;
use s3ish::auth::file_auth::FileAuthenticator;
use s3ish::auth::Authenticator;
use s3ish::config::Config;
use s3ish::handler::BaseHandler;
use s3ish::observability::tracing_setup;
use s3ish::s3_http::ResponseContext;
use s3ish::server::{ConnectionManager, GrpcConnectionManager, S3HttpConnectionManager};
use s3ish::storage::file_storage::FileStorage;
use s3ish::storage::in_memory::InMemoryStorage;
use s3ish::storage::StorageBackend;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(name = "s3ish")]
#[command(about = "In-memory S3-like object store with gRPC and HTTP interfaces", long_about = None)]
struct Args {
    /// Address to listen on (e.g., 0.0.0.0:9000, 127.0.0.1:9000)
    #[arg(short, long)]
    listen: Option<String>,

    /// Protocol to use (grpc or http)
    #[arg(short, long, default_value = "grpc")]
    protocol: String,

    /// Path to configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: String,

    /// Path to credentials file
    #[arg(short, long)]
    auth_file: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing with format from environment
    tracing_setup::init_tracing_from_env();

    let args = Args::parse();

    // Load config from file
    let cfg = Config::from_path(&args.config)?;

    // Command line args override config file
    let addr: SocketAddr = args.listen.as_ref().unwrap_or(&cfg.listen_addr).parse()?;

    // Create shared components
    let auth_file = args.auth_file.as_ref().unwrap_or(&cfg.auth_file);
    let auth: Arc<dyn Authenticator> = Arc::new(FileAuthenticator::new(auth_file).await?);
    let storage: Arc<dyn StorageBackend> = match cfg.storage.backend.as_str() {
        "file" => {
            let fs = FileStorage::new(&cfg.storage.path).await?;
            Arc::new(fs)
        }
        _ => Arc::new(InMemoryStorage::new()),
    };
    let handler = BaseHandler::new(auth, storage);
    let response_ctx = ResponseContext::new(cfg.region.clone(), cfg.request_id_prefix.clone());

    // Use protocol from command line args
    let protocol = args.protocol.as_str();

    match protocol {
        "http" | "s3" => {
            let server = S3HttpConnectionManager::new(handler, response_ctx);
            tracing::info!("s3ish HTTP server listening on {}", addr);
            tracing::info!("auth via headers: x-access-key / x-secret-key (or x-amz-*)");

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
        }
        _ => {
            let server = GrpcConnectionManager::new(handler);
            tracing::info!("s3ish gRPC server listening on {}", addr);
            tracing::info!("auth via metadata headers: x-access-key / x-secret-key");

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
        }
    }

    Ok(())
}
