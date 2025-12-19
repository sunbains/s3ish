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

use clap::Parser;
use s3ish::actor::{ActorStorageBackend, FsStoreActor, FsStoreReader, Metrics as ActorMetrics};
use s3ish::auth::file_auth::FileAuthenticator;
use s3ish::auth::Authenticator;
use s3ish::config::Config;
use s3ish::handler::BaseHandler;
use s3ish::observability::tracing_setup;
use s3ish::s3_http::ResponseContext;
use s3ish::server::{ConnectionManager, GrpcConnectionManager, S3HttpConnectionManager};
use s3ish::storage::in_memory::InMemoryStorage;
use s3ish::storage::StorageBackend;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Parser, Debug)]
#[command(name = "s3ish")]
#[command(about = "In-memory S3-like object store with gRPC and HTTP interfaces", long_about = None)]
struct Args {
    /// Address to listen on (e.g., 0.0.0.0:9000, 127.0.0.1:9000)
    #[arg(short, long)]
    listen: Option<String>,

    /// Protocol to use (grpc or http)
    #[arg(short, long, default_value = "http")]
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

    // Initialize actor system and storage
    let storage: Arc<dyn StorageBackend> = match cfg.storage.backend.as_str() {
        "file" => {
            // Spawn multiple actors for parallel processing
            let num_actors = cfg.storage.actors.num_actors;
            let mut actor_channels = Vec::new();

            // Use all configured drives for multi-drive sharding
            let drives: Vec<std::path::PathBuf> = cfg
                .storage
                .effective_drives()
                .into_iter()
                .map(|s| s.into())
                .collect();

            // Create shared reader for direct GET access (bypasses actor overhead)
            let shared_metrics = Arc::new(ActorMetrics::new());
            let reader = FsStoreReader::new(
                drives.clone(),
                cfg.storage.erasure.enabled,
                cfg.storage.erasure.data_blocks,
                cfg.storage.erasure.parity_blocks,
                shared_metrics.clone(),
            );

            for i in 0..num_actors {
                let actor_metrics = Arc::new(ActorMetrics::new());
                let (fs_tx, fs_rx) = mpsc::channel(10000);

                let actor = FsStoreActor::new(
                    drives.clone(),
                    cfg.storage.erasure.enabled,
                    cfg.storage.erasure.data_blocks,
                    cfg.storage.erasure.parity_blocks,
                    fs_rx,
                    actor_metrics.clone(),
                );

                tokio::spawn(actor.run());
                actor_channels.push(fs_tx);

                tracing::info!("FsStoreActor {} started", i);
            }

            if cfg.storage.erasure.enabled {
                tracing::info!("FsStoreActor started (actor model, multi-drive, {} actors, erasure coding: {}/{})",
                    num_actors, cfg.storage.erasure.data_blocks, cfg.storage.erasure.parity_blocks);
            } else {
                tracing::info!("FsStoreActor started (actor model, multi-drive, {} actors, erasure coding: DISABLED)", num_actors);
            }
            tracing::info!("GET operations bypass actor model for 10× latency reduction");

            Arc::new(ActorStorageBackend::new(actor_channels, reader))
        }
        _ => Arc::new(InMemoryStorage::new()),
    };

    let handler = BaseHandler::new(auth, storage.clone());
    let response_ctx = ResponseContext::new(cfg.region.clone(), cfg.request_id_prefix.clone());

    // Start lifecycle executor if enabled
    if cfg.lifecycle.enabled {
        use s3ish::storage::lifecycle_executor::LifecycleExecutor;

        let lifecycle_config = s3ish::storage::lifecycle_executor::LifecycleConfig {
            enabled: cfg.lifecycle.enabled,
            check_interval_secs: cfg.lifecycle.check_interval_secs,
            max_concurrent_deletes: cfg.lifecycle.max_concurrent_deletes,
        };

        let executor = LifecycleExecutor::new(storage.clone(), lifecycle_config);
        let _lifecycle_handle = executor.spawn();

        tracing::info!(
            "Lifecycle executor started (check interval: {}s)",
            cfg.lifecycle.check_interval_secs
        );
    } else {
        tracing::info!("Lifecycle executor disabled");
    }

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
