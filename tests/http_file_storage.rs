use axum::body::Body;
use axum::http::{HeaderMap, Method, Request};
use axum::response::Response;
use axum::Router;
use http_body_util::BodyExt;
use s3ish::auth::file_auth::FileAuthenticator;
use s3ish::auth::Authenticator;
use s3ish::handler::BaseHandler;
use s3ish::s3_http::{ResponseContext, S3HttpHandler};
use s3ish::storage::file_storage::FileStorage;
use s3ish::storage::StorageBackend;
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;

fn auth_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("x-access-key", "test".parse().unwrap());
    headers.insert("x-secret-key", "testpass".parse().unwrap());
    headers
}

#[tokio::test]
async fn test_http_file_storage_put_get_list_delete() {
    let dir = tempdir().unwrap();
    let auth_file = dir.path().join("creds.txt");
    std::fs::write(&auth_file, "test:testpass\n").unwrap();
    let auth: Arc<dyn Authenticator> = Arc::new(FileAuthenticator::new(&auth_file).await.unwrap());
    let storage: Arc<dyn StorageBackend> =
        Arc::new(FileStorage::new(dir.path().join("data")).await.unwrap());
    storage.create_bucket("bucket").await.unwrap();
    let handler = BaseHandler::new(auth, storage);
    let ctx = ResponseContext::default();
    let app = S3HttpHandler::new_with_context(handler, ctx).router();

    // PUT object
    let mut req = Request::builder()
        .method(Method::PUT)
        .uri("/bucket/obj.txt")
        .header("content-type", "text/plain")
        .body(axum::body::Body::from("hello"))
        .unwrap();
    *req.headers_mut() = auth_headers();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert!(resp.status().is_success());

    // GET object
    let mut req = Request::builder()
        .method(Method::GET)
        .uri("/bucket/obj.txt")
        .body(axum::body::Body::empty())
        .unwrap();
    *req.headers_mut() = auth_headers();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body, "hello");

    // LIST objects
    let mut req = Request::builder()
        .method(Method::GET)
        .uri("/bucket")
        .body(axum::body::Body::empty())
        .unwrap();
    *req.headers_mut() = auth_headers();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let body_str = std::str::from_utf8(&body).unwrap();
    assert!(body_str.contains("<Key>obj.txt</Key>"));

    // DELETE object
    let mut req = Request::builder()
        .method(Method::DELETE)
        .uri("/bucket/obj.txt")
        .body(axum::body::Body::empty())
        .unwrap();
    *req.headers_mut() = auth_headers();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 204);
}

async fn send_resp(app: Router, mut req: Request<Body>) -> Response {
    let auth = auth_headers();
    req.headers_mut().extend(auth);
    app.oneshot(req).await.unwrap()
}

async fn send(app: Router, req: Request<Body>) -> (u16, String) {
    let resp = send_resp(app, req).await;
    let status = resp.status().as_u16();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&body).to_string())
}

#[tokio::test]
async fn test_http_file_storage_copy() {
    let dir = tempdir().unwrap();
    let auth_file = dir.path().join("creds.txt");
    std::fs::write(&auth_file, "test:testpass\n").unwrap();
    let auth: Arc<dyn Authenticator> = Arc::new(FileAuthenticator::new(&auth_file).await.unwrap());
    let storage: Arc<dyn StorageBackend> =
        Arc::new(FileStorage::new(dir.path().join("data")).await.unwrap());
    storage.create_bucket("bucket").await.unwrap();
    let handler = BaseHandler::new(auth, storage);
    let ctx = ResponseContext::default();
    let app = S3HttpHandler::new_with_context(handler, ctx).router();

    // PUT source
    let req = Request::builder()
        .method(Method::PUT)
        .uri("/bucket/src.txt")
        .header("content-type", "text/plain")
        .body(axum::body::Body::from("copyme"))
        .unwrap();
    let (status, body) = send(app.clone(), req).await;
    assert_eq!(status, 200, "put source failed: {body}");

    // COPY to dest
    let req = Request::builder()
        .method(Method::PATCH)
        .uri("/bucket/dst.txt")
        .header("x-amz-copy-source", "/bucket/src.txt")
        .body(axum::body::Body::empty())
        .unwrap();
    let (status, body) = send(app.clone(), req).await;
    assert_eq!(status, 200, "copy failed: {body}");

    // GET dest
    let req = Request::builder()
        .method(Method::GET)
        .uri("/bucket/dst.txt")
        .body(axum::body::Body::empty())
        .unwrap();
    let (status, body) = send(app.clone(), req).await;
    assert_eq!(status, 200, "get dest failed: {body}");
    assert_eq!(body, "copyme");
}

#[tokio::test]
async fn test_http_file_storage_copy_metadata_replace() {
    let dir = tempdir().unwrap();
    let auth_file = dir.path().join("creds.txt");
    std::fs::write(&auth_file, "test:testpass\n").unwrap();
    let auth: Arc<dyn Authenticator> = Arc::new(FileAuthenticator::new(&auth_file).await.unwrap());
    let storage: Arc<dyn StorageBackend> =
        Arc::new(FileStorage::new(dir.path().join("data")).await.unwrap());
    storage.create_bucket("bucket").await.unwrap();
    let handler = BaseHandler::new(auth, storage);
    let ctx = ResponseContext::default();
    let app = S3HttpHandler::new_with_context(handler, ctx).router();

    // PUT source with metadata
    let req = Request::builder()
        .method(Method::PUT)
        .uri("/bucket/src.txt")
        .header("content-type", "application/json")
        .header("x-amz-meta-src", "keep")
        .body(axum::body::Body::from("{}"))
        .unwrap();
    let (status, body) = send(app.clone(), req).await;
    assert_eq!(status, 200, "put source failed: {body}");

    // COPY with REPLACE and new metadata
    let req = Request::builder()
        .method(Method::PATCH)
        .uri("/bucket/dst.txt")
        .header("x-amz-copy-source", "/bucket/src.txt")
        .header("x-amz-metadata-directive", "REPLACE")
        .header("content-type", "text/plain")
        .header("x-amz-meta-new", "yes")
        .body(axum::body::Body::empty())
        .unwrap();
    let (status, body) = send(app.clone(), req).await;
    assert_eq!(status, 200, "copy replace failed: {body}");

    // GET dest and check metadata
    let req = Request::builder()
        .method(Method::GET)
        .uri("/bucket/dst.txt")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = send_resp(app.clone(), req).await;
    assert_eq!(resp.status(), 200);
    let headers = resp.headers();
    assert_eq!(headers.get("content-type").unwrap(), "text/plain");
    assert_eq!(headers.get("x-amz-meta-new").unwrap(), "yes");
    assert!(headers.get("x-amz-meta-src").is_none());
}

#[tokio::test]
async fn test_http_file_storage_copy_if_match_precondition() {
    let dir = tempdir().unwrap();
    let auth_file = dir.path().join("creds.txt");
    std::fs::write(&auth_file, "test:testpass\n").unwrap();
    let auth: Arc<dyn Authenticator> = Arc::new(FileAuthenticator::new(&auth_file).await.unwrap());
    let storage: Arc<dyn StorageBackend> =
        Arc::new(FileStorage::new(dir.path().join("data")).await.unwrap());
    storage.create_bucket("bucket").await.unwrap();
    let handler = BaseHandler::new(auth, storage);
    let ctx = ResponseContext::default();
    let app = S3HttpHandler::new_with_context(handler, ctx).router();

    // PUT source
    let req = Request::builder()
        .method(Method::PUT)
        .uri("/bucket/src.txt")
        .header("content-type", "text/plain")
        .body(axum::body::Body::from("etagdata"))
        .unwrap();
    let resp = send_resp(app.clone(), req).await;
    let etag = resp
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .to_string();

    // PUT dest with different etag
    let req = Request::builder()
        .method(Method::PUT)
        .uri("/bucket/dst.txt")
        .header("content-type", "text/plain")
        .body(axum::body::Body::from("other"))
        .unwrap();
    let _ = send_resp(app.clone(), req).await;

    // Copy with If-Match mismatch should fail
    let req = Request::builder()
        .method(Method::PATCH)
        .uri("/bucket/dst.txt")
        .header("x-amz-copy-source", "/bucket/src.txt")
        .header("if-match", format!("{etag}-extra"))
        .body(axum::body::Body::empty())
        .unwrap();
    let (status, body) = send(app.clone(), req).await;
    assert_eq!(status, 412, "expected precondition failed: {body}");
}

#[tokio::test]
async fn test_http_file_storage_multipart() {
    let dir = tempdir().unwrap();
    let auth_file = dir.path().join("creds.txt");
    std::fs::write(&auth_file, "test:testpass\n").unwrap();
    let auth: Arc<dyn Authenticator> = Arc::new(FileAuthenticator::new(&auth_file).await.unwrap());
    let storage: Arc<dyn StorageBackend> =
        Arc::new(FileStorage::new(dir.path().join("data")).await.unwrap());
    storage.create_bucket("bucket").await.unwrap();
    let handler = BaseHandler::new(auth, storage);
    let ctx = ResponseContext::default();
    let app = S3HttpHandler::new_with_context(handler, ctx).router();

    // Initiate
    let req = Request::builder()
        .method(Method::POST)
        .uri("/bucket/multi.txt?uploads")
        .body(axum::body::Body::empty())
        .unwrap();
    let (status, body) = send(app.clone(), req).await;
    assert_eq!(status, 200, "init failed: {body}");
    let body_str = body;
    let upload_id = body_str
        .split("<UploadId>")
        .nth(1)
        .and_then(|s| s.split("</UploadId>").next())
        .unwrap()
        .to_string();

    // Upload part 1
    let req = Request::builder()
        .method(Method::PUT)
        .uri(format!(
            "/bucket/multi.txt?uploadId={upload_id}&partNumber=1"
        ))
        .body(axum::body::Body::from("hello "))
        .unwrap();
    let resp = send_resp(app.clone(), req).await;
    assert_eq!(resp.status(), 200);
    let etag1 = resp
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .trim_matches('"')
        .to_string();

    // Upload part 2
    let req = Request::builder()
        .method(Method::PUT)
        .uri(format!(
            "/bucket/multi.txt?uploadId={upload_id}&partNumber=2"
        ))
        .body(axum::body::Body::from("world"))
        .unwrap();
    let resp = send_resp(app.clone(), req).await;
    assert_eq!(resp.status(), 200);
    let etag2 = resp
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .trim_matches('"')
        .to_string();

    // Complete
    let complete_body = format!(
        r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"{etag1}"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>"{etag2}"</ETag></Part></CompleteMultipartUpload>"#,
        etag1 = etag1,
        etag2 = etag2
    );
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("/bucket/multi.txt?uploadId={upload_id}"))
        .body(axum::body::Body::from(complete_body))
        .unwrap();
    let (status, body) = send(app.clone(), req).await;
    assert_eq!(status, 200, "complete failed: {body}");

    // GET final object
    let req = Request::builder()
        .method(Method::GET)
        .uri("/bucket/multi.txt")
        .body(axum::body::Body::empty())
        .unwrap();
    let (status, body) = send(app.clone(), req).await;
    assert_eq!(status, 200, "get failed: {body}");
    assert_eq!(body, "hello world");
}
