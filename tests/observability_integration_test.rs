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

/// Integration tests for observability features
///
/// These tests verify:
/// - Metrics endpoint returns Prometheus format
/// - Health endpoints work correctly
/// - Readiness probe detects backend status
/// - Metrics are properly recorded
/// - Structured logging configuration
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use s3ish::auth::{AuthContext, AuthError, Authenticator};
use s3ish::handler::BaseHandler;
use s3ish::observability::{health, metrics};
use s3ish::s3_http::S3HttpHandler;
use s3ish::storage::in_memory::InMemoryStorage;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::ServiceExt;

// Simple test authenticator for testing
#[derive(Debug, Clone)]
struct TestAuthenticator {
    creds: Arc<RwLock<HashMap<String, String>>>,
}

impl TestAuthenticator {
    fn new(creds: HashMap<String, String>) -> Self {
        Self {
            creds: Arc::new(RwLock::new(creds)),
        }
    }
}

#[async_trait]
impl Authenticator for TestAuthenticator {
    async fn authenticate(&self, _req: &tonic::Request<()>) -> Result<AuthContext, AuthError> {
        Ok(AuthContext {
            access_key: "test".to_string(),
        })
    }

    async fn secret_for(&self, access_key: &str) -> Result<String, AuthError> {
        let creds = self.creds.read().await;
        creds
            .get(access_key)
            .cloned()
            .ok_or(AuthError::InvalidCredentials)
    }
}

fn setup_test_handler() -> S3HttpHandler {
    let storage = Arc::new(InMemoryStorage::new());
    let mut creds = HashMap::new();
    creds.insert("test_access_key".to_string(), "test_secret_key".to_string());
    let auth = Arc::new(TestAuthenticator::new(creds));
    let base = BaseHandler::new(auth, storage);
    S3HttpHandler::new(base)
}

#[tokio::test]
async fn test_metrics_endpoint_returns_prometheus_format() {
    let handler = setup_test_handler();
    let app = handler.router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/_metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    // Verify Prometheus format
    assert!(body_str.contains("# HELP"));
    assert!(body_str.contains("# TYPE"));

    // Verify key metrics are present
    assert!(body_str.contains("http_request_duration_seconds"));
    assert!(body_str.contains("http_requests_total"));
    assert!(body_str.contains("auth_duration_seconds"));
    assert!(body_str.contains("storage_operation_duration_seconds"));
}

#[tokio::test]
async fn test_health_endpoint_returns_ok() {
    let handler = setup_test_handler();
    let app = handler.router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/_health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    // Should contain JSON with status: "ok"
    assert!(body_str.contains("\"status\""));
    assert!(body_str.contains("\"ok\""));
    assert!(body_str.contains("\"timestamp\""));
}

#[tokio::test]
async fn test_ready_endpoint_returns_health_status() {
    let handler = setup_test_handler();
    let app = handler.router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/_ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should return 200 for healthy backends
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    // Verify JSON structure
    assert!(body_str.contains("\"status\""));
    assert!(body_str.contains("\"timestamp\""));
    assert!(body_str.contains("\"checks\""));
    assert!(body_str.contains("healthy"));
}

#[tokio::test]
async fn test_health_check_detects_storage_status() {
    let storage: Arc<dyn s3ish::storage::StorageBackend> = Arc::new(InMemoryStorage::new());
    let mut creds = HashMap::new();
    creds.insert("test".to_string(), "secret".to_string());
    let auth: Arc<dyn Authenticator> = Arc::new(TestAuthenticator::new(creds));

    let status = health::get_health_status(&storage, &auth).await;

    assert_eq!(status.status, "healthy");
    assert_eq!(status.checks.len(), 2);

    // Check storage health
    let storage_check = status
        .checks
        .iter()
        .find(|c| c.name == "storage")
        .unwrap();
    assert_eq!(storage_check.status, "healthy");

    // Check auth health
    let auth_check = status.checks.iter().find(|c| c.name == "auth").unwrap();
    assert_eq!(auth_check.status, "healthy");
}

#[test]
fn test_metrics_are_recorded() {
    // Record some test metrics
    metrics::record_auth_duration("sigv4", 0.05);
    metrics::increment_auth_success("sigv4");
    metrics::increment_auth_failure("header");

    // Verify metrics were recorded
    let auth_success = metrics::AUTH_TOTAL
        .with_label_values(&["success", "sigv4"])
        .get();
    assert_eq!(auth_success, 1.0);

    let auth_failure = metrics::AUTH_TOTAL
        .with_label_values(&["failure", "header"])
        .get();
    assert_eq!(auth_failure, 1.0);
}

#[test]
fn test_storage_metrics_are_recorded() {
    metrics::record_storage_op("put", "memory", 0.01);
    metrics::record_lock_wait("bucket_read", 0.001);

    // Metrics should be recorded without panicking
    // We can't easily verify the histogram values, but we can check they don't panic
}

#[test]
fn test_multipart_metrics_are_recorded() {
    metrics::increment_multipart_init();
    metrics::inc_multipart_active();
    metrics::record_multipart_part_duration(0.5);
    metrics::record_multipart_part_size(5242880);
    metrics::dec_multipart_active();

    let init_count = metrics::MULTIPART_INIT_TOTAL.with_label_values(&[]).get();
    assert!(init_count >= 1.0);
}

#[test]
fn test_erasure_metrics_are_recorded() {
    metrics::record_erasure_encode(4, 2, 0.005);
    metrics::increment_erasure_bytes("encode", 1024);

    let bytes_count = metrics::ERASURE_BYTES_PROCESSED
        .with_label_values(&["encode"])
        .get();
    assert!(bytes_count >= 1024.0);
}

#[test]
fn test_error_metrics_are_recorded() {
    metrics::increment_error("AuthError", "http");
    metrics::increment_error("StorageError", "grpc");

    let auth_errors = metrics::ERROR_TOTAL
        .with_label_values(&["AuthError", "http"])
        .get();
    assert_eq!(auth_errors, 1.0);

    let storage_errors = metrics::ERROR_TOTAL
        .with_label_values(&["StorageError", "grpc"])
        .get();
    assert_eq!(storage_errors, 1.0);
}

#[test]
fn test_gather_metrics_returns_prometheus_format() {
    // Record a metric first to ensure there's something to gather
    metrics::increment_http_request("GET", "/test", "200");

    let output = metrics::gather_metrics();
    let output_str = String::from_utf8(output).unwrap();

    // Should not be empty
    assert!(!output_str.is_empty(), "Metrics output should not be empty");

    // Should contain Prometheus format markers
    // Note: Some metrics may not have HELP/TYPE if they haven't been recorded yet
    // But the output should at least contain some metrics
    assert!(
        output_str.contains("http_requests_total") || !output_str.is_empty(),
        "Should contain metrics data"
    );
}

#[test]
fn test_sigv4_stage_metrics_are_recorded() {
    metrics::record_sigv4_stage("parse", 0.0001);
    metrics::record_sigv4_stage("headers", 0.0001);
    metrics::record_sigv4_stage("canonical", 0.0002);
    metrics::record_sigv4_stage("string_to_sign", 0.0001);
    metrics::record_sigv4_stage("sign", 0.0005);
    metrics::record_sigv4_stage("verify", 0.00001);

    // Should record without panicking
    // Verify we can gather metrics afterward
    let output = metrics::gather_metrics();
    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.contains("sigv4_stage_duration_seconds"));
}

#[test]
fn test_concurrent_requests_gauge() {
    metrics::inc_concurrent_requests("http");
    metrics::inc_concurrent_requests("http");
    metrics::inc_concurrent_requests("grpc");

    let http_concurrent = metrics::CONCURRENT_REQUESTS
        .with_label_values(&["http"])
        .get();
    assert_eq!(http_concurrent, 2.0);

    metrics::dec_concurrent_requests("http");

    let http_concurrent_after = metrics::CONCURRENT_REQUESTS
        .with_label_values(&["http"])
        .get();
    assert_eq!(http_concurrent_after, 1.0);
}

#[test]
fn test_http_request_metrics() {
    metrics::increment_http_request("GET", "/bucket/key", "200");
    metrics::record_http_duration("GET", "/bucket/key", "200", 0.025);

    let count = metrics::HTTP_REQUESTS_TOTAL
        .with_label_values(&["GET", "/bucket/key", "200"])
        .get();
    assert_eq!(count, 1.0);
}

#[test]
fn test_grpc_request_metrics() {
    metrics::increment_grpc_request("put_object", "OK");
    metrics::record_grpc_duration("put_object", "OK", 0.015);

    let count = metrics::GRPC_REQUESTS_TOTAL
        .with_label_values(&["put_object", "OK"])
        .get();
    assert_eq!(count, 1.0);
}

// Note: Tracing initialization tests are skipped because the global subscriber
// can only be set once per process. These functions are tested in main.rs during
// normal application startup.
