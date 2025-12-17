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
use axum::http::{Request, StatusCode};
use axum::Router;
use s3ish::auth::file_auth::FileAuthenticator;
use s3ish::auth::Authenticator;
use s3ish::handler::BaseHandler;
use s3ish::s3_http::{ResponseContext, S3HttpHandler};
use s3ish::storage::in_memory::InMemoryStorage;
use s3ish::storage::StorageBackend;
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;

async fn setup_app() -> (Router, Arc<dyn StorageBackend>) {
    let dir = tempdir().unwrap();
    let auth_file = dir.path().join("creds.txt");
    std::fs::write(&auth_file, "test-key:test-secret\n").unwrap();

    let auth: Arc<dyn Authenticator> =
        Arc::new(FileAuthenticator::new(&auth_file).await.unwrap());
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let handler = BaseHandler::new(auth, storage.clone());
    let ctx = ResponseContext::default();
    let app = S3HttpHandler::new_with_context(handler, ctx).router();

    (app, storage)
}

#[tokio::test]
async fn test_put_lifecycle_configuration() {
    let (app, storage) = setup_app().await;

    // Create bucket first
    storage.create_bucket("test-bucket").await.unwrap();

    let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
    <Rule>
        <ID>expire-old-objects</ID>
        <Status>Enabled</Status>
        <Prefix>logs/</Prefix>
        <Expiration>
            <Days>30</Days>
        </Expiration>
    </Rule>
</LifecycleConfiguration>"#;

    let request = Request::builder()
        .uri("/test-bucket?lifecycle")
        .method("PUT")
        .header("x-access-key", "test-key")
        .header("x-secret-key", "test-secret")
        .header("content-type", "application/xml")
        .body(Body::from(lifecycle_xml))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_get_lifecycle_configuration() {
    let (app, storage) = setup_app().await;

    // Create bucket
    storage.create_bucket("test-bucket").await.unwrap();

    // Put lifecycle configuration
    let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
    <Rule>
        <ID>expire-old-objects</ID>
        <Status>Enabled</Status>
        <Prefix>logs/</Prefix>
        <Expiration>
            <Days>30</Days>
        </Expiration>
    </Rule>
</LifecycleConfiguration>"#;

    let put_request = Request::builder()
        .uri("/test-bucket?lifecycle")
        .method("PUT")
        .header("x-access-key", "test-key")
        .header("x-secret-key", "test-secret")
        .header("content-type", "application/xml")
        .body(Body::from(lifecycle_xml))
        .unwrap();

    let app_clone = app.clone();
    app_clone.oneshot(put_request).await.unwrap();

    // Get lifecycle configuration
    let get_request = Request::builder()
        .uri("/test-bucket?lifecycle")
        .method("GET")
        .header("x-access-key", "test-key")
        .header("x-secret-key", "test-secret")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(get_request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify response body contains our rule
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();

    assert!(body_str.contains("expire-old-objects"));
    assert!(body_str.contains("Enabled"));
    assert!(body_str.contains("logs/"));
    assert!(body_str.contains("<Days>30</Days>"));
}

#[tokio::test]
async fn test_delete_lifecycle_configuration() {
    let (app, storage) = setup_app().await;

    // Create bucket
    storage.create_bucket("test-bucket").await.unwrap();

    // Put lifecycle configuration
    let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
    <Rule>
        <ID>test-rule</ID>
        <Status>Enabled</Status>
        <Prefix></Prefix>
        <Expiration>
            <Days>7</Days>
        </Expiration>
    </Rule>
</LifecycleConfiguration>"#;

    let put_request = Request::builder()
        .uri("/test-bucket?lifecycle")
        .method("PUT")
        .header("x-access-key", "test-key")
        .header("x-secret-key", "test-secret")
        .header("content-type", "application/xml")
        .body(Body::from(lifecycle_xml))
        .unwrap();

    let app_clone = app.clone();
    app_clone.oneshot(put_request).await.unwrap();

    // Delete lifecycle configuration
    let delete_request = Request::builder()
        .uri("/test-bucket?lifecycle")
        .method("DELETE")
        .header("x-access-key", "test-key")
        .header("x-secret-key", "test-secret")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(delete_request).await.unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn test_lifecycle_bucket_not_found() {
    let (app, _storage) = setup_app().await;

    // Try to put lifecycle on non-existent bucket
    let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
    <Rule>
        <ID>test-rule</ID>
        <Status>Enabled</Status>
        <Prefix></Prefix>
        <Expiration>
            <Days>7</Days>
        </Expiration>
    </Rule>
</LifecycleConfiguration>"#;

    let request = Request::builder()
        .uri("/nonexistent-bucket?lifecycle")
        .method("PUT")
        .header("x-access-key", "test-key")
        .header("x-secret-key", "test-secret")
        .header("content-type", "application/xml")
        .body(Body::from(lifecycle_xml))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_lifecycle_invalid_xml() {
    let (app, storage) = setup_app().await;

    // Create bucket
    storage.create_bucket("test-bucket").await.unwrap();

    // Try to put invalid XML
    let invalid_xml = r#"<InvalidRoot><SomeTag>test</SomeTag></InvalidRoot>"#;

    let request = Request::builder()
        .uri("/test-bucket?lifecycle")
        .method("PUT")
        .header("x-access-key", "test-key")
        .header("x-secret-key", "test-secret")
        .header("content-type", "application/xml")
        .body(Body::from(invalid_xml))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    // Should return error for invalid lifecycle configuration
    assert!(response.status().is_client_error());
}

#[tokio::test]
async fn test_lifecycle_get_nonexistent() {
    let (app, storage) = setup_app().await;

    // Create bucket without lifecycle policy
    storage.create_bucket("test-bucket").await.unwrap();

    let request = Request::builder()
        .uri("/test-bucket?lifecycle")
        .method("GET")
        .header("x-access-key", "test-key")
        .header("x-secret-key", "test-secret")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    // Should return 404 when no lifecycle policy exists
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_lifecycle_multiple_rules() {
    let (app, storage) = setup_app().await;

    // Create bucket
    storage.create_bucket("test-bucket").await.unwrap();

    let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
    <Rule>
        <ID>expire-logs</ID>
        <Status>Enabled</Status>
        <Prefix>logs/</Prefix>
        <Expiration>
            <Days>7</Days>
        </Expiration>
    </Rule>
    <Rule>
        <ID>expire-temp</ID>
        <Status>Enabled</Status>
        <Prefix>temp/</Prefix>
        <Expiration>
            <Days>1</Days>
        </Expiration>
    </Rule>
</LifecycleConfiguration>"#;

    let put_request = Request::builder()
        .uri("/test-bucket?lifecycle")
        .method("PUT")
        .header("x-access-key", "test-key")
        .header("x-secret-key", "test-secret")
        .header("content-type", "application/xml")
        .body(Body::from(lifecycle_xml))
        .unwrap();

    let response = app.oneshot(put_request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_lifecycle_with_transitions() {
    let (app, storage) = setup_app().await;

    // Create bucket
    storage.create_bucket("test-bucket").await.unwrap();

    let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
    <Rule>
        <ID>transition-rule</ID>
        <Status>Enabled</Status>
        <Prefix>archive/</Prefix>
        <Transition>
            <Days>30</Days>
            <StorageClass>GLACIER</StorageClass>
        </Transition>
        <Expiration>
            <Days>365</Days>
        </Expiration>
    </Rule>
</LifecycleConfiguration>"#;

    let put_request = Request::builder()
        .uri("/test-bucket?lifecycle")
        .method("PUT")
        .header("x-access-key", "test-key")
        .header("x-secret-key", "test-secret")
        .header("content-type", "application/xml")
        .body(Body::from(lifecycle_xml))
        .unwrap();

    let app_clone = app.clone();
    let response = app_clone.oneshot(put_request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Get and verify
    let get_request = Request::builder()
        .uri("/test-bucket?lifecycle")
        .method("GET")
        .header("x-access-key", "test-key")
        .header("x-secret-key", "test-secret")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(get_request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();

    assert!(body_str.contains("GLACIER"));
    assert!(body_str.contains("<Days>30</Days>"));
}

#[tokio::test]
async fn test_lifecycle_disabled_rule() {
    let (app, storage) = setup_app().await;

    // Create bucket
    storage.create_bucket("test-bucket").await.unwrap();

    let lifecycle_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
    <Rule>
        <ID>disabled-rule</ID>
        <Status>Disabled</Status>
        <Prefix>old/</Prefix>
        <Expiration>
            <Days>90</Days>
        </Expiration>
    </Rule>
</LifecycleConfiguration>"#;

    let put_request = Request::builder()
        .uri("/test-bucket?lifecycle")
        .method("PUT")
        .header("x-access-key", "test-key")
        .header("x-secret-key", "test-secret")
        .header("content-type", "application/xml")
        .body(Body::from(lifecycle_xml))
        .unwrap();

    let response = app.oneshot(put_request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_lifecycle_roundtrip() {
    let (app, storage) = setup_app().await;

    // Create bucket
    storage.create_bucket("test-bucket").await.unwrap();

    let original_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
    <Rule>
        <ID>roundtrip-test</ID>
        <Status>Enabled</Status>
        <Prefix>data/</Prefix>
        <Expiration>
            <Days>180</Days>
        </Expiration>
    </Rule>
</LifecycleConfiguration>"#;

    // PUT
    let put_request = Request::builder()
        .uri("/test-bucket?lifecycle")
        .method("PUT")
        .header("x-access-key", "test-key")
        .header("x-secret-key", "test-secret")
        .header("content-type", "application/xml")
        .body(Body::from(original_xml))
        .unwrap();

    let app_clone = app.clone();
    let put_response = app_clone.oneshot(put_request).await.unwrap();
    assert_eq!(put_response.status(), StatusCode::OK);

    // GET
    let get_request = Request::builder()
        .uri("/test-bucket?lifecycle")
        .method("GET")
        .header("x-access-key", "test-key")
        .header("x-secret-key", "test-secret")
        .body(Body::empty())
        .unwrap();

    let app_clone2 = app.clone();
    let get_response = app_clone2.oneshot(get_request).await.unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();

    // Verify all key elements are preserved
    assert!(body_str.contains("roundtrip-test"));
    assert!(body_str.contains("Enabled"));
    assert!(body_str.contains("data/"));
    assert!(body_str.contains("<Days>180</Days>"));

    // DELETE
    let delete_request = Request::builder()
        .uri("/test-bucket?lifecycle")
        .method("DELETE")
        .header("x-access-key", "test-key")
        .header("x-secret-key", "test-secret")
        .body(Body::empty())
        .unwrap();

    let delete_response = app.oneshot(delete_request).await.unwrap();
    assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);
}
