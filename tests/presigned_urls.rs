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

use axum::body::Body;
use axum::http::{Method, Request};
use http_body_util::BodyExt;
use s3ish::auth::file_auth::FileAuthenticator;
use s3ish::auth::Authenticator;
use s3ish::handler::BaseHandler;
use s3ish::s3_http::{generate_presigned_url, ResponseContext, S3HttpHandler};
use s3ish::storage::in_memory::InMemoryStorage;
use s3ish::storage::StorageBackend;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;

const ACCESS_KEY: &str = "demo";
const SECRET_KEY: &str = "demo-secret";
const REGION: &str = "us-east-1";

async fn setup_test_server() -> (Arc<dyn StorageBackend>, axum::Router) {
    let dir = tempdir().unwrap();
    let auth_file = dir.path().join("creds.txt");
    std::fs::write(&auth_file, format!("{}:{}\n", ACCESS_KEY, SECRET_KEY)).unwrap();

    let auth: Arc<dyn Authenticator> =
        Arc::new(FileAuthenticator::new(&auth_file).await.unwrap());
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());

    // Create a test bucket
    storage.create_bucket("test-bucket").await.unwrap();

    let handler = BaseHandler::new(auth, storage.clone());
    let ctx = ResponseContext::default();
    let app = S3HttpHandler::new_with_context(handler, ctx).router();

    (storage, app)
}

#[tokio::test]
async fn test_presigned_url_get() {
    let (storage, app) = setup_test_server().await;

    // First, put an object using regular authentication
    storage
        .put_object(
            "test-bucket",
            "test-file.txt",
            bytes::Bytes::from("Hello, World!"),
            "text/plain",
            HashMap::new(),
            None,
            None,
        )
        .await
        .unwrap();

    // Generate a pre-signed URL for GET
    let presigned_url = generate_presigned_url(
        "http://localhost:9000",
        "test-bucket",
        "test-file.txt",
        "GET",
        3600, // 1 hour
        ACCESS_KEY,
        SECRET_KEY,
        REGION,
        &["host"],
    )
    .unwrap();

    // Extract the path and query from the pre-signed URL
    let url_parts: Vec<&str> = presigned_url.split("?").collect();
    let path = url_parts[0].replace("http://localhost:9000", "");
    let query = url_parts.get(1).unwrap_or(&"");

    // Make a request using the pre-signed URL (no auth headers needed)
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("{}?{}", path, query))
        .header("host", "localhost:9000")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 200, "Pre-signed GET request failed");

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body, "Hello, World!");
}

#[tokio::test]
async fn test_presigned_url_put() {
    let (storage, app) = setup_test_server().await;

    // Generate a pre-signed URL for PUT
    let presigned_url = generate_presigned_url(
        "http://localhost:9000",
        "test-bucket",
        "new-file.txt",
        "PUT",
        3600,
        ACCESS_KEY,
        SECRET_KEY,
        REGION,
        &["host"],
    )
    .unwrap();

    // Extract the path and query from the pre-signed URL
    let url_parts: Vec<&str> = presigned_url.split("?").collect();
    let path = url_parts[0].replace("http://localhost:9000", "");
    let query = url_parts.get(1).unwrap_or(&"");

    // Make a PUT request using the pre-signed URL
    let req = Request::builder()
        .method(Method::PUT)
        .uri(format!("{}?{}", path, query))
        .header("host", "localhost:9000")
        .header("content-type", "text/plain")
        .body(Body::from("Pre-signed upload"))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 200, "Pre-signed PUT request failed");

    // Verify the object was uploaded
    let (data, _) = storage
        .get_object("test-bucket", "new-file.txt")
        .await
        .unwrap();
    assert_eq!(&data[..], b"Pre-signed upload");
}

#[tokio::test]
async fn test_presigned_url_invalid_signature() {
    let (_storage, app) = setup_test_server().await;

    // Generate a pre-signed URL
    let presigned_url = generate_presigned_url(
        "http://localhost:9000",
        "test-bucket",
        "test-file.txt",
        "GET",
        3600,
        ACCESS_KEY,
        SECRET_KEY,
        REGION,
        &["host"],
    )
    .unwrap();

    // Tamper with the signature
    let tampered_url = presigned_url.replace(
        &presigned_url[presigned_url.len() - 10..],
        "0000000000",
    );

    let url_parts: Vec<&str> = tampered_url.split("?").collect();
    let path = url_parts[0].replace("http://localhost:9000", "");
    let query = url_parts.get(1).unwrap_or(&"");

    // Make a request with tampered signature
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("{}?{}", path, query))
        .header("host", "localhost:9000")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        403,
        "Expected 403 for invalid signature, got {}",
        resp.status()
    );
}

#[tokio::test]
async fn test_presigned_url_wrong_method() {
    let (storage, app) = setup_test_server().await;

    // Put an object
    storage
        .put_object(
            "test-bucket",
            "test-file.txt",
            bytes::Bytes::from("Hello, World!"),
            "text/plain",
            HashMap::new(),
            None,
            None,
        )
        .await
        .unwrap();

    // Generate a pre-signed URL for GET
    let presigned_url = generate_presigned_url(
        "http://localhost:9000",
        "test-bucket",
        "test-file.txt",
        "GET",
        3600,
        ACCESS_KEY,
        SECRET_KEY,
        REGION,
        &["host"],
    )
    .unwrap();

    let url_parts: Vec<&str> = presigned_url.split("?").collect();
    let path = url_parts[0].replace("http://localhost:9000", "");
    let query = url_parts.get(1).unwrap_or(&"");

    // Try to use it with PUT method (should fail signature verification)
    let req = Request::builder()
        .method(Method::PUT)
        .uri(format!("{}?{}", path, query))
        .header("host", "localhost:9000")
        .body(Body::from("data"))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        403,
        "Expected 403 for wrong method, got {}",
        resp.status()
    );
}

#[tokio::test]
async fn test_presigned_url_max_expiration() {
    // Test that expiration > 7 days is rejected
    let result = generate_presigned_url(
        "http://localhost:9000",
        "test-bucket",
        "test-file.txt",
        "GET",
        604801, // 7 days + 1 second
        ACCESS_KEY,
        SECRET_KEY,
        REGION,
        &["host"],
    );

    assert!(
        result.is_err(),
        "Expected error for expiration > 7 days, got {:?}",
        result
    );
}

#[tokio::test]
async fn test_presigned_url_delete() {
    let (storage, app) = setup_test_server().await;

    // First, put an object
    storage
        .put_object(
            "test-bucket",
            "to-delete.txt",
            bytes::Bytes::from("Delete me"),
            "text/plain",
            HashMap::new(),
            None,
            None,
        )
        .await
        .unwrap();

    // Generate a pre-signed URL for DELETE
    let presigned_url = generate_presigned_url(
        "http://localhost:9000",
        "test-bucket",
        "to-delete.txt",
        "DELETE",
        3600,
        ACCESS_KEY,
        SECRET_KEY,
        REGION,
        &["host"],
    )
    .unwrap();

    let url_parts: Vec<&str> = presigned_url.split("?").collect();
    let path = url_parts[0].replace("http://localhost:9000", "");
    let query = url_parts.get(1).unwrap_or(&"");

    // Make a DELETE request using the pre-signed URL
    let req = Request::builder()
        .method(Method::DELETE)
        .uri(format!("{}?{}", path, query))
        .header("host", "localhost:9000")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 204, "Pre-signed DELETE request failed");

    // Verify the object was deleted
    let result = storage.get_object("test-bucket", "to-delete.txt").await;
    assert!(result.is_err(), "Object should have been deleted");
}
