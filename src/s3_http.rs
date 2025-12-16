use crate::auth::AuthError;
use crate::handler::BaseHandler;
use crate::storage::StorageError;
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, put},
    Router,
};
use bytes::Bytes;
use serde::Deserialize;
use std::sync::Arc;

/// S3 HTTP handler wrapping BaseHandler
#[derive(Clone)]
pub struct S3HttpHandler {
    handler: Arc<BaseHandler>,
}

impl S3HttpHandler {
    pub fn new(handler: BaseHandler) -> Self {
        Self {
            handler: Arc::new(handler),
        }
    }

    /// Create the router for S3 HTTP API
    pub fn router(self) -> Router {
        Router::new()
            .route("/:bucket", put(create_bucket).delete(delete_bucket))
            .route("/:bucket/", get(list_objects))
            .route(
                "/:bucket/*key",
                put(put_object).get(get_object).delete(delete_object),
            )
            .with_state(self.handler)
    }

    /// Authenticate a request using custom headers
    async fn authenticate(&self, headers: &HeaderMap) -> Result<(), S3Error> {
        let access_key = headers
            .get("x-amz-access-key")
            .or_else(|| headers.get("x-access-key"))
            .ok_or(S3Error::MissingCredentials)?
            .to_str()
            .map_err(|_| S3Error::InvalidCredentials)?;

        let secret_key = headers
            .get("x-amz-secret-key")
            .or_else(|| headers.get("x-secret-key"))
            .ok_or(S3Error::MissingCredentials)?
            .to_str()
            .map_err(|_| S3Error::InvalidCredentials)?;

        // Create a simple Request<()> for authentication
        // In a real implementation, you'd use proper AWS signature verification
        let mut req = tonic::Request::new(());
        req.metadata_mut()
            .insert("x-access-key", access_key.parse().unwrap());
        req.metadata_mut()
            .insert("x-secret-key", secret_key.parse().unwrap());

        self.handler
            .auth
            .authenticate(&req)
            .await
            .map(|_| ())
            .map_err(|e| match e {
                AuthError::MissingCredentials => S3Error::MissingCredentials,
                AuthError::InvalidCredentials => S3Error::InvalidCredentials,
                AuthError::Internal(msg) => S3Error::Internal(msg),
            })
    }
}

#[derive(Debug)]
enum S3Error {
    MissingCredentials,
    InvalidCredentials,
    BucketNotFound(String),
    BucketNotEmpty(String),
    ObjectNotFound { bucket: String, key: String },
    InvalidInput(String),
    Internal(String),
}

impl From<StorageError> for S3Error {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::BucketNotFound(b) => S3Error::BucketNotFound(b),
            StorageError::BucketNotEmpty(b) => S3Error::BucketNotEmpty(b),
            StorageError::ObjectNotFound { bucket, key } => S3Error::ObjectNotFound { bucket, key },
            StorageError::InvalidInput(msg) => S3Error::InvalidInput(msg),
            StorageError::Internal(msg) => S3Error::Internal(msg),
        }
    }
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        match self {
            S3Error::MissingCredentials => {
                (StatusCode::UNAUTHORIZED, "Missing credentials").into_response()
            }
            S3Error::InvalidCredentials => {
                (StatusCode::FORBIDDEN, "Invalid credentials").into_response()
            }
            S3Error::BucketNotFound(b) => {
                (StatusCode::NOT_FOUND, format!("Bucket not found: {}", b)).into_response()
            }
            S3Error::BucketNotEmpty(b) => {
                (StatusCode::CONFLICT, format!("Bucket not empty: {}", b)).into_response()
            }
            S3Error::ObjectNotFound { bucket, key } => (
                StatusCode::NOT_FOUND,
                format!("Object not found: {}/{}", bucket, key),
            )
                .into_response(),
            S3Error::InvalidInput(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
            S3Error::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg).into_response(),
        }
    }
}

/// PUT /{bucket} - Create bucket
async fn create_bucket(
    State(handler): State<Arc<BaseHandler>>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, S3Error> {
    let s3_handler = S3HttpHandler {
        handler: handler.clone(),
    };
    s3_handler.authenticate(&headers).await?;

    let created = handler.storage.create_bucket(&bucket).await?;
    Ok((
        StatusCode::OK,
        format!("{{\"created\": {}}}", created),
    ))
}

/// DELETE /{bucket} - Delete bucket
async fn delete_bucket(
    State(handler): State<Arc<BaseHandler>>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, S3Error> {
    let s3_handler = S3HttpHandler {
        handler: handler.clone(),
    };
    s3_handler.authenticate(&headers).await?;

    let deleted = handler.storage.delete_bucket(&bucket).await?;
    Ok((
        StatusCode::OK,
        format!("{{\"deleted\": {}}}", deleted),
    ))
}

/// PUT /{bucket}/{key} - Put object
async fn put_object(
    State(handler): State<Arc<BaseHandler>>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, S3Error> {
    let s3_handler = S3HttpHandler {
        handler: handler.clone(),
    };
    s3_handler.authenticate(&headers).await?;

    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream");

    let meta = handler
        .storage
        .put_object(&bucket, &key, body, content_type)
        .await?;

    Ok((
        StatusCode::OK,
        [(header::ETAG, meta.etag.clone())],
        format!(
            "{{\"etag\": \"{}\", \"size\": {}}}",
            meta.etag, meta.size
        ),
    ))
}

/// GET /{bucket}/{key} - Get object
async fn get_object(
    State(handler): State<Arc<BaseHandler>>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, S3Error> {
    let s3_handler = S3HttpHandler {
        handler: handler.clone(),
    };
    s3_handler.authenticate(&headers).await?;

    let (data, meta) = handler.storage.get_object(&bucket, &key).await?;

    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, meta.content_type.clone()),
            (header::ETAG, meta.etag),
        ],
        Body::from(data),
    ))
}

/// DELETE /{bucket}/{key} - Delete object
async fn delete_object(
    State(handler): State<Arc<BaseHandler>>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, S3Error> {
    let s3_handler = S3HttpHandler {
        handler: handler.clone(),
    };
    s3_handler.authenticate(&headers).await?;

    let deleted = handler.storage.delete_object(&bucket, &key).await?;
    Ok((
        StatusCode::OK,
        format!("{{\"deleted\": {}}}", deleted),
    ))
}

#[derive(Deserialize)]
struct ListObjectsParams {
    #[serde(default)]
    prefix: String,
    #[serde(default)]
    #[serde(rename = "max-keys")]
    max_keys: Option<u32>,
}

/// GET /{bucket}/ - List objects
async fn list_objects(
    State(handler): State<Arc<BaseHandler>>,
    Path(bucket): Path<String>,
    Query(params): Query<ListObjectsParams>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, S3Error> {
    let s3_handler = S3HttpHandler {
        handler: handler.clone(),
    };
    s3_handler.authenticate(&headers).await?;

    let limit = params.max_keys.unwrap_or(1000) as usize;
    let objects = handler
        .storage
        .list_objects(&bucket, &params.prefix, limit)
        .await?;

    // Simple JSON response
    let mut json = String::from("{\"objects\":[");
    for (i, (key, meta)) in objects.iter().enumerate() {
        if i > 0 {
            json.push(',');
        }
        json.push_str(&format!(
            "{{\"key\":\"{}\",\"size\":{},\"etag\":\"{}\"}}",
            key, meta.size, meta.etag
        ));
    }
    json.push_str("]}");

    Ok((StatusCode::OK, json))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{AuthContext, Authenticator};
    use crate::storage::in_memory::InMemoryStorage;
    use async_trait::async_trait;
    use axum::http::Request;
    use tower::ServiceExt;

    #[derive(Clone)]
    struct MockAuthenticator {
        should_fail: bool,
    }

    #[async_trait]
    impl Authenticator for MockAuthenticator {
        async fn authenticate(
            &self,
            req: &tonic::Request<()>,
        ) -> Result<AuthContext, AuthError> {
            if self.should_fail {
                return Err(AuthError::InvalidCredentials);
            }
            if req.metadata().get("x-access-key").is_none() {
                return Err(AuthError::MissingCredentials);
            }
            Ok(AuthContext {
                access_key: "test-user".to_string(),
            })
        }
    }

    fn create_test_handler(auth_should_fail: bool) -> S3HttpHandler {
        let auth: Arc<dyn Authenticator> = Arc::new(MockAuthenticator {
            should_fail: auth_should_fail,
        });
        let storage: Arc<dyn crate::storage::StorageBackend> = Arc::new(InMemoryStorage::new());
        let base_handler = BaseHandler::new(auth, storage);
        S3HttpHandler::new(base_handler)
    }

    #[tokio::test]
    async fn test_create_bucket() {
        let handler = create_test_handler(false);
        let app = handler.router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/test-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_create_bucket_unauthorized() {
        let handler = create_test_handler(false);
        let app = handler.router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/test-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_put_and_get_object() {
        let handler = create_test_handler(false);
        let app = handler.clone().router();

        // Create bucket first
        app.clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/test-bucket")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Put object
        let app2 = handler.clone().router();
        let response = app2
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/test-bucket/test-key")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .header("content-type", "text/plain")
                    .body(Body::from("test data"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Get object
        let app3 = handler.router();
        let response = app3
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/test-bucket/test-key")
                    .header("x-access-key", "test-user")
                    .header("x-secret-key", "test-pass")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain"
        );
    }
}
