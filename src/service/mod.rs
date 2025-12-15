use crate::auth::{AuthError, Authenticator};
use crate::pb::{
    object_store_server::ObjectStore, CreateBucketRequest, CreateBucketResponse, DeleteBucketRequest,
    DeleteBucketResponse, DeleteObjectRequest, DeleteObjectResponse, GetObjectRequest,
    GetObjectResponse, ListObjectsRequest, ListObjectsResponse, ObjectInfo, PutObjectRequest,
    PutObjectResponse,
};
use crate::storage::{StorageBackend, StorageError};
use prost_types::Timestamp;
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct ObjectStoreService<A: Authenticator + Clone, S: StorageBackend + Clone> {
    auth: Arc<A>,
    storage: Arc<S>,
}

impl<A: Authenticator + Clone, S: StorageBackend + Clone> ObjectStoreService<A, S> {
    pub fn new(auth: Arc<A>, storage: Arc<S>) -> Self {
        Self { auth, storage }
    }

    async fn authenticate<T>(&self, req: &Request<T>) -> Result<(), Status> {
        // We need to create a Request<()> from the metadata for the authenticator
        // Since we can't easily construct a Request<()> from just metadata,
        // we'll need to use a workaround
        let mut dummy = Request::new(());
        *dummy.metadata_mut() = req.metadata().clone();

        self.auth
            .authenticate(&dummy)
            .await
            .map(|_ctx| ())
            .map_err(map_auth_err)
    }
}

fn ts_from_unix_secs(secs: i64) -> Timestamp {
    Timestamp {
        seconds: secs,
        nanos: 0,
    }
}

fn map_storage_err(e: StorageError) -> Status {
    match e {
        StorageError::BucketNotFound(b) => Status::not_found(format!("bucket not found: {b}")),
        StorageError::BucketNotEmpty(b) => Status::failed_precondition(format!("bucket not empty: {b}")),
        StorageError::ObjectNotFound { bucket, key } => Status::not_found(format!("object not found: {bucket}/{key}")),
        StorageError::InvalidInput(m) => Status::invalid_argument(m),
        StorageError::Internal(m) => Status::internal(m),
    }
}

fn map_auth_err(e: AuthError) -> Status {
    match e {
        AuthError::MissingCredentials => Status::unauthenticated("missing credentials"),
        AuthError::InvalidCredentials => Status::permission_denied("invalid credentials"),
        AuthError::Internal(m) => Status::internal(m),
    }
}

#[tonic::async_trait]
impl<A: Authenticator + Clone, S: StorageBackend + Clone> ObjectStore for ObjectStoreService<A, S> {
    async fn create_bucket(
        &self,
        req: Request<CreateBucketRequest>,
    ) -> Result<Response<CreateBucketResponse>, Status> {
        self.authenticate(&req).await?;
        let bucket = req
            .into_inner()
            .bucket
            .ok_or_else(|| Status::invalid_argument("bucket is required"))?
            .name;

        let created = self.storage.create_bucket(&bucket).await.map_err(map_storage_err)?;
        Ok(Response::new(CreateBucketResponse { created }))
    }

    async fn delete_bucket(
        &self,
        req: Request<DeleteBucketRequest>,
    ) -> Result<Response<DeleteBucketResponse>, Status> {
        self.authenticate(&req).await?;
        let bucket = req
            .into_inner()
            .bucket
            .ok_or_else(|| Status::invalid_argument("bucket is required"))?
            .name;

        let deleted = self.storage.delete_bucket(&bucket).await.map_err(map_storage_err)?;
        Ok(Response::new(DeleteBucketResponse { deleted }))
    }

    async fn put_object(
        &self,
        req: Request<PutObjectRequest>,
    ) -> Result<Response<PutObjectResponse>, Status> {
        self.authenticate(&req).await?;
        let inner = req.into_inner();
        let obj = inner
            .object
            .ok_or_else(|| Status::invalid_argument("object is required"))?;
        let data = bytes::Bytes::from(inner.data);
        let meta = self
            .storage
            .put_object(&obj.bucket, &obj.key, data, &inner.content_type)
            .await
            .map_err(map_storage_err)?;

        Ok(Response::new(PutObjectResponse {
            etag: meta.etag,
            size: meta.size,
            last_modified: Some(ts_from_unix_secs(meta.last_modified_unix_secs)),
        }))
    }

    async fn get_object(
        &self,
        req: Request<GetObjectRequest>,
    ) -> Result<Response<GetObjectResponse>, Status> {
        self.authenticate(&req).await?;
        let obj = req
            .into_inner()
            .object
            .ok_or_else(|| Status::invalid_argument("object is required"))?;

        let (data, meta) = self
            .storage
            .get_object(&obj.bucket, &obj.key)
            .await
            .map_err(map_storage_err)?;

        Ok(Response::new(GetObjectResponse {
            data: data.to_vec(),
            content_type: meta.content_type,
            etag: meta.etag,
            last_modified: Some(ts_from_unix_secs(meta.last_modified_unix_secs)),
        }))
    }

    async fn delete_object(
        &self,
        req: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        self.authenticate(&req).await?;
        let obj = req
            .into_inner()
            .object
            .ok_or_else(|| Status::invalid_argument("object is required"))?;

        let deleted = self
            .storage
            .delete_object(&obj.bucket, &obj.key)
            .await
            .map_err(map_storage_err)?;
        Ok(Response::new(DeleteObjectResponse { deleted }))
    }

    async fn list_objects(
        &self,
        req: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        self.authenticate(&req).await?;
        let inner = req.into_inner();

        let limit = if inner.limit == 0 { 1000 } else { inner.limit as usize };
        let objs = self
            .storage
            .list_objects(&inner.bucket, &inner.prefix, limit)
            .await
            .map_err(map_storage_err)?;

        let objects = objs
            .into_iter()
            .map(|(key, meta)| ObjectInfo {
                key,
                size: meta.size,
                etag: meta.etag,
                content_type: meta.content_type,
                last_modified: Some(ts_from_unix_secs(meta.last_modified_unix_secs)),
            })
            .collect();

        Ok(Response::new(ListObjectsResponse { objects }))
    }
}
