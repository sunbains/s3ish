use crate::auth::Authenticator;
use crate::pb::object_store_server::ObjectStoreServer;
use crate::service::ObjectStoreService;
use crate::storage::StorageBackend;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use tonic::transport::Server;

#[async_trait]
pub trait ConnectionManager: Send + Sync + 'static {
    async fn serve(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// gRPC connection manager (tonic) that hosts the ObjectStore service.
///
/// This intentionally abstracts the underlying transport so you can later swap
/// gRPC for HTTP (real S3 API) without changing the storage/auth layers.
#[derive(Clone)]
pub struct GrpcConnectionManager<A: Authenticator + Clone, S: StorageBackend + Clone> {
    svc: ObjectStoreService<A, S>,
}

impl<A: Authenticator + Clone, S: StorageBackend + Clone> GrpcConnectionManager<A, S> {
    pub fn new(auth: Arc<A>, storage: Arc<S>) -> Self {
        Self {
            svc: ObjectStoreService::new(auth, storage),
        }
    }
}

#[async_trait]
impl<A: Authenticator + Clone, S: StorageBackend + Clone> ConnectionManager for GrpcConnectionManager<A, S> {
    async fn serve(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Server::builder()
            .add_service(ObjectStoreServer::new(self.svc.clone()))
            .serve(addr)
            .await?;
        Ok(())
    }
}
