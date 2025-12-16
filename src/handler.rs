use crate::auth::Authenticator;
use crate::storage::StorageBackend;
use std::sync::Arc;

/// Base handler implementation that holds common components
/// Both gRPC and S3 HTTP handlers will wrap this
#[derive(Clone)]
pub struct BaseHandler {
    pub auth: Arc<dyn Authenticator>,
    pub storage: Arc<dyn StorageBackend>,
}

impl BaseHandler {
    pub fn new(auth: Arc<dyn Authenticator>, storage: Arc<dyn StorageBackend>) -> Self {
        Self { auth, storage }
    }
}
