use crate::handler::BaseHandler;
use crate::pb::object_store_server::ObjectStoreServer;
use crate::s3_http::S3HttpHandler;
use crate::service::ObjectStoreService;
use async_trait::async_trait;
use std::net::SocketAddr;
use tonic::transport::Server;

#[async_trait]
pub trait ConnectionManager: Send + Sync + 'static {
    async fn serve(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// gRPC connection manager (tonic) that hosts the ObjectStore service.
#[derive(Clone)]
pub struct GrpcConnectionManager {
    svc: ObjectStoreService,
}

impl GrpcConnectionManager {
    pub fn new(handler: BaseHandler) -> Self {
        Self {
            svc: ObjectStoreService::new(handler),
        }
    }
}

#[async_trait]
impl ConnectionManager for GrpcConnectionManager {
    async fn serve(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Starting gRPC server on {}", addr);
        Server::builder()
            .add_service(ObjectStoreServer::new(self.svc.clone()))
            .serve(addr)
            .await?;
        Ok(())
    }
}

/// S3 HTTP connection manager using Axum.
#[derive(Clone)]
pub struct S3HttpConnectionManager {
    handler: S3HttpHandler,
}

impl S3HttpConnectionManager {
    pub fn new(handler: BaseHandler) -> Self {
        Self {
            handler: S3HttpHandler::new(handler),
        }
    }
}

#[async_trait]
impl ConnectionManager for S3HttpConnectionManager {
    async fn serve(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Starting S3 HTTP server on {}", addr);
        let app = self.handler.clone().router();
        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }
}
