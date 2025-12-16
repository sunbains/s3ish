use crate::auth::AuthError;
use crate::handler::BaseHandler;
use crate::observability::metrics;
use crate::pb::{
    object_store_server::ObjectStore, CreateBucketRequest, CreateBucketResponse,
    DeleteBucketRequest, DeleteBucketResponse, DeleteObjectRequest, DeleteObjectResponse,
    GetObjectRequest, GetObjectResponse, ListObjectsRequest, ListObjectsResponse, ObjectInfo,
    PutObjectRequest, PutObjectResponse,
};
use crate::storage::StorageError;
use prost_types::Timestamp;
use tonic::{Request, Response, Status};

/// gRPC service implementation wrapping BaseHandler
#[derive(Clone)]
pub struct ObjectStoreService {
    handler: BaseHandler,
}

impl ObjectStoreService {
    pub fn new(handler: BaseHandler) -> Self {
        Self { handler }
    }

    async fn authenticate<T>(&self, req: &Request<T>) -> Result<(), Status> {
        // We need to create a Request<()> from the metadata for the authenticator
        // Since we can't easily construct a Request<()> from just metadata,
        // we'll need to use a workaround
        let mut dummy = Request::new(());
        *dummy.metadata_mut() = req.metadata().clone();

        self.handler
            .auth
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
        StorageError::BucketNotEmpty(b) => {
            Status::failed_precondition(format!("bucket not empty: {b}"))
        }
        StorageError::ObjectNotFound { bucket, key } => {
            Status::not_found(format!("object not found: {bucket}/{key}"))
        }
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
impl ObjectStore for ObjectStoreService {
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

        let created = self
            .handler
            .storage
            .create_bucket(&bucket)
            .await
            .map_err(map_storage_err)?;
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

        let deleted = self
            .handler
            .storage
            .delete_bucket(&bucket)
            .await
            .map_err(map_storage_err)?;
        Ok(Response::new(DeleteBucketResponse { deleted }))
    }

    #[tracing::instrument(skip(self, req), fields(method = "put_object"))]
    async fn put_object(
        &self,
        req: Request<PutObjectRequest>,
    ) -> Result<Response<PutObjectResponse>, Status> {
        let start_time = std::time::Instant::now();

        let result = async {
            self.authenticate(&req).await?;
            let inner = req.into_inner();
            let obj = inner
                .object
                .ok_or_else(|| Status::invalid_argument("object is required"))?;
            let data = bytes::Bytes::from(inner.data);
            let metadata = inner.metadata;
            let meta = self
                .handler
                .storage
                .put_object(
                    &obj.bucket,
                    &obj.key,
                    data,
                    &inner.content_type,
                    metadata.clone(),
                    None, // storage_class: Not exposed in gRPC API yet
                    None, // server_side_encryption: Not exposed in gRPC API yet
                )
                .await
                .map_err(map_storage_err)?;

            Ok(Response::new(PutObjectResponse {
                etag: meta.etag,
                size: meta.size,
                last_modified: Some(ts_from_unix_secs(meta.last_modified_unix_secs)),
                metadata,
            }))
        }
        .await;

        // Record metrics
        let duration = start_time.elapsed().as_secs_f64();
        let status = if result.is_ok() { "success" } else { "error" };
        metrics::record_grpc_duration("put_object", status, duration);
        metrics::increment_grpc_request("put_object", status);

        result
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
            .handler
            .storage
            .get_object(&obj.bucket, &obj.key)
            .await
            .map_err(map_storage_err)?;

        Ok(Response::new(GetObjectResponse {
            data: data.to_vec(),
            content_type: meta.content_type,
            etag: meta.etag,
            last_modified: Some(ts_from_unix_secs(meta.last_modified_unix_secs)),
            metadata: meta.metadata,
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
            .handler
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

        let limit = if inner.limit == 0 {
            1000
        } else {
            inner.limit as usize
        };
        let objs = self
            .handler
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
                metadata: meta.metadata,
            })
            .collect();

        Ok(Response::new(ListObjectsResponse { objects }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{AuthContext, AuthError, Authenticator};
    use crate::pb::{BucketName, ObjectKey};
    use crate::storage::in_memory::InMemoryStorage;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[derive(Clone)]
    struct MockAuthenticator {
        should_fail: bool,
    }

    #[async_trait]
    impl Authenticator for MockAuthenticator {
        async fn authenticate(&self, req: &Request<()>) -> Result<AuthContext, AuthError> {
            if self.should_fail {
                return Err(AuthError::InvalidCredentials);
            }
            // Check for required metadata
            if req.metadata().get("x-access-key").is_none() {
                return Err(AuthError::MissingCredentials);
            }
            Ok(AuthContext {
                access_key: "test-user".to_string(),
            })
        }
    }

    fn create_test_service(auth_should_fail: bool) -> ObjectStoreService {
        let auth: Arc<dyn Authenticator> = Arc::new(MockAuthenticator {
            should_fail: auth_should_fail,
        });
        let storage: Arc<dyn crate::storage::StorageBackend> = Arc::new(InMemoryStorage::new());
        let handler = BaseHandler::new(auth, storage);
        ObjectStoreService::new(handler)
    }

    fn create_authenticated_request<T>(inner: T) -> Request<T> {
        let mut req = Request::new(inner);
        req.metadata_mut()
            .insert("x-access-key", "test-user".parse().unwrap());
        req.metadata_mut()
            .insert("x-secret-key", "test-pass".parse().unwrap());
        req
    }

    #[tokio::test]
    async fn test_service_create_bucket_success() {
        let service = create_test_service(false);
        let req = create_authenticated_request(CreateBucketRequest {
            bucket: Some(BucketName {
                name: "test-bucket".to_string(),
            }),
        });

        let response = service.create_bucket(req).await.unwrap();
        assert!(response.into_inner().created);
    }

    #[tokio::test]
    async fn test_service_create_bucket_unauthenticated() {
        let service = create_test_service(false);
        let req = Request::new(CreateBucketRequest {
            bucket: Some(BucketName {
                name: "test-bucket".to_string(),
            }),
        });

        let result = service.create_bucket(req).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn test_service_create_bucket_auth_failed() {
        let service = create_test_service(true);
        let req = create_authenticated_request(CreateBucketRequest {
            bucket: Some(BucketName {
                name: "test-bucket".to_string(),
            }),
        });

        let result = service.create_bucket(req).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn test_service_create_bucket_missing_name() {
        let service = create_test_service(false);
        let req = create_authenticated_request(CreateBucketRequest { bucket: None });

        let result = service.create_bucket(req).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_service_delete_bucket_success() {
        let service = create_test_service(false);

        // First create a bucket
        let create_req = create_authenticated_request(CreateBucketRequest {
            bucket: Some(BucketName {
                name: "test-bucket".to_string(),
            }),
        });
        service.create_bucket(create_req).await.unwrap();

        // Then delete it
        let delete_req = create_authenticated_request(DeleteBucketRequest {
            bucket: Some(BucketName {
                name: "test-bucket".to_string(),
            }),
        });
        let response = service.delete_bucket(delete_req).await.unwrap();
        assert!(response.into_inner().deleted);
    }

    #[tokio::test]
    async fn test_service_put_object_success() {
        let service = create_test_service(false);

        // Create bucket first
        let create_req = create_authenticated_request(CreateBucketRequest {
            bucket: Some(BucketName {
                name: "test-bucket".to_string(),
            }),
        });
        service.create_bucket(create_req).await.unwrap();

        // Put object
        let put_req = create_authenticated_request(PutObjectRequest {
            object: Some(ObjectKey {
                bucket: "test-bucket".to_string(),
                key: "test-key".to_string(),
            }),
            data: b"test data".to_vec(),
            content_type: "text/plain".to_string(),
            metadata: HashMap::new(),
        });

        let response = service.put_object(put_req).await.unwrap();
        let inner = response.into_inner();
        assert_eq!(inner.size, 9);
        assert!(!inner.etag.is_empty());
        assert!(inner.last_modified.is_some());
    }

    #[tokio::test]
    async fn test_service_put_object_missing_object() {
        let service = create_test_service(false);
        let req = create_authenticated_request(PutObjectRequest {
            object: None,
            data: b"test data".to_vec(),
            content_type: "text/plain".to_string(),
            metadata: HashMap::new(),
        });

        let result = service.put_object(req).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_service_get_object_success() {
        let service = create_test_service(false);

        // Create bucket
        let create_req = create_authenticated_request(CreateBucketRequest {
            bucket: Some(BucketName {
                name: "test-bucket".to_string(),
            }),
        });
        service.create_bucket(create_req).await.unwrap();

        // Put object
        let put_req = create_authenticated_request(PutObjectRequest {
            object: Some(ObjectKey {
                bucket: "test-bucket".to_string(),
                key: "test-key".to_string(),
            }),
            data: b"test data".to_vec(),
            content_type: "text/plain".to_string(),
            metadata: HashMap::new(),
        });
        service.put_object(put_req).await.unwrap();

        // Get object
        let get_req = create_authenticated_request(GetObjectRequest {
            object: Some(ObjectKey {
                bucket: "test-bucket".to_string(),
                key: "test-key".to_string(),
            }),
        });

        let response = service.get_object(get_req).await.unwrap();
        let inner = response.into_inner();
        assert_eq!(inner.data, b"test data");
        assert_eq!(inner.content_type, "text/plain");
        assert!(!inner.etag.is_empty());
        assert!(inner.last_modified.is_some());
    }

    #[tokio::test]
    async fn test_service_get_object_not_found() {
        let service = create_test_service(false);

        // Create bucket
        let create_req = create_authenticated_request(CreateBucketRequest {
            bucket: Some(BucketName {
                name: "test-bucket".to_string(),
            }),
        });
        service.create_bucket(create_req).await.unwrap();

        // Try to get non-existent object
        let get_req = create_authenticated_request(GetObjectRequest {
            object: Some(ObjectKey {
                bucket: "test-bucket".to_string(),
                key: "nonexistent".to_string(),
            }),
        });

        let result = service.get_object(get_req).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_service_delete_object_success() {
        let service = create_test_service(false);

        // Create bucket
        let create_req = create_authenticated_request(CreateBucketRequest {
            bucket: Some(BucketName {
                name: "test-bucket".to_string(),
            }),
        });
        service.create_bucket(create_req).await.unwrap();

        // Put object
        let put_req = create_authenticated_request(PutObjectRequest {
            object: Some(ObjectKey {
                bucket: "test-bucket".to_string(),
                key: "test-key".to_string(),
            }),
            data: b"test data".to_vec(),
            content_type: "text/plain".to_string(),
            metadata: HashMap::new(),
        });
        service.put_object(put_req).await.unwrap();

        // Delete object
        let delete_req = create_authenticated_request(DeleteObjectRequest {
            object: Some(ObjectKey {
                bucket: "test-bucket".to_string(),
                key: "test-key".to_string(),
            }),
        });

        let response = service.delete_object(delete_req).await.unwrap();
        assert!(response.into_inner().deleted);
    }

    #[tokio::test]
    async fn test_service_list_objects_empty() {
        let service = create_test_service(false);

        // Create bucket
        let create_req = create_authenticated_request(CreateBucketRequest {
            bucket: Some(BucketName {
                name: "test-bucket".to_string(),
            }),
        });
        service.create_bucket(create_req).await.unwrap();

        // List objects
        let list_req = create_authenticated_request(ListObjectsRequest {
            bucket: "test-bucket".to_string(),
            prefix: "".to_string(),
            limit: 100,
        });

        let response = service.list_objects(list_req).await.unwrap();
        let inner = response.into_inner();
        assert!(inner.objects.is_empty());
    }

    #[tokio::test]
    async fn test_service_list_objects_with_data() {
        let service = create_test_service(false);

        // Create bucket
        let create_req = create_authenticated_request(CreateBucketRequest {
            bucket: Some(BucketName {
                name: "test-bucket".to_string(),
            }),
        });
        service.create_bucket(create_req).await.unwrap();

        // Put multiple objects
        for i in 1..=3 {
            let put_req = create_authenticated_request(PutObjectRequest {
                object: Some(ObjectKey {
                    bucket: "test-bucket".to_string(),
                    key: format!("key{}", i),
                }),
                data: format!("data{}", i).into_bytes(),
                content_type: "text/plain".to_string(),
                metadata: HashMap::new(),
            });
            service.put_object(put_req).await.unwrap();
        }

        // List objects
        let list_req = create_authenticated_request(ListObjectsRequest {
            bucket: "test-bucket".to_string(),
            prefix: "".to_string(),
            limit: 100,
        });

        let response = service.list_objects(list_req).await.unwrap();
        let inner = response.into_inner();
        assert_eq!(inner.objects.len(), 3);
        assert_eq!(inner.objects[0].key, "key1");
        assert_eq!(inner.objects[1].key, "key2");
        assert_eq!(inner.objects[2].key, "key3");
    }

    #[tokio::test]
    async fn test_service_list_objects_with_prefix() {
        let service = create_test_service(false);

        // Create bucket
        let create_req = create_authenticated_request(CreateBucketRequest {
            bucket: Some(BucketName {
                name: "test-bucket".to_string(),
            }),
        });
        service.create_bucket(create_req).await.unwrap();

        // Put objects with different prefixes
        for key in ["photos/1.jpg", "photos/2.jpg", "docs/readme.txt"] {
            let put_req = create_authenticated_request(PutObjectRequest {
                object: Some(ObjectKey {
                    bucket: "test-bucket".to_string(),
                    key: key.to_string(),
                }),
                data: b"data".to_vec(),
                content_type: "text/plain".to_string(),
                metadata: HashMap::new(),
            });
            service.put_object(put_req).await.unwrap();
        }

        // List with prefix
        let list_req = create_authenticated_request(ListObjectsRequest {
            bucket: "test-bucket".to_string(),
            prefix: "photos/".to_string(),
            limit: 100,
        });

        let response = service.list_objects(list_req).await.unwrap();
        let inner = response.into_inner();
        assert_eq!(inner.objects.len(), 2);
        assert!(inner.objects[0].key.starts_with("photos/"));
        assert!(inner.objects[1].key.starts_with("photos/"));
    }

    #[tokio::test]
    async fn test_service_metadata_roundtrip() {
        let service = create_test_service(false);

        // Create bucket
        let create_req = create_authenticated_request(CreateBucketRequest {
            bucket: Some(BucketName {
                name: "meta-bucket".to_string(),
            }),
        });
        service.create_bucket(create_req).await.unwrap();

        // Put object with metadata
        let mut meta = HashMap::new();
        meta.insert("color".to_string(), "blue".to_string());
        meta.insert("flag".to_string(), "yes".to_string());
        let put_req = create_authenticated_request(PutObjectRequest {
            object: Some(ObjectKey {
                bucket: "meta-bucket".to_string(),
                key: "item".to_string(),
            }),
            data: b"data".to_vec(),
            content_type: "text/plain".to_string(),
            metadata: meta.clone(),
        });
        service.put_object(put_req).await.unwrap();

        // Get should include metadata
        let get_req = create_authenticated_request(GetObjectRequest {
            object: Some(ObjectKey {
                bucket: "meta-bucket".to_string(),
                key: "item".to_string(),
            }),
        });
        let get_resp = service.get_object(get_req).await.unwrap().into_inner();
        assert_eq!(get_resp.metadata.get("color").unwrap(), "blue");
        assert_eq!(get_resp.metadata.get("flag").unwrap(), "yes");

        // List should include metadata
        let list_req = create_authenticated_request(ListObjectsRequest {
            bucket: "meta-bucket".to_string(),
            prefix: "".to_string(),
            limit: 10,
        });
        let list_resp = service.list_objects(list_req).await.unwrap().into_inner();
        assert_eq!(list_resp.objects.len(), 1);
        assert_eq!(list_resp.objects[0].metadata.get("color").unwrap(), "blue");
    }

    #[tokio::test]
    async fn test_service_error_mapping() {
        let service = create_test_service(false);

        // Test bucket not found
        let get_req = create_authenticated_request(GetObjectRequest {
            object: Some(ObjectKey {
                bucket: "nonexistent".to_string(),
                key: "key".to_string(),
            }),
        });
        let result = service.get_object(get_req).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }
}
