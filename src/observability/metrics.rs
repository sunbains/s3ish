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

/// Prometheus metrics definitions for s3ish
use lazy_static::lazy_static;
use prometheus::{
    register_counter_vec, register_gauge_vec, register_histogram_vec, CounterVec, GaugeVec,
    HistogramVec, TextEncoder,
};

lazy_static! {
    // ============================================================================
    // HTTP Metrics
    // ============================================================================

    /// HTTP request duration in seconds
    pub static ref HTTP_REQUEST_DURATION: HistogramVec = register_histogram_vec!(
        "http_request_duration_seconds",
        "HTTP request duration in seconds",
        &["method", "endpoint", "status"],
        vec![0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0]
    ).unwrap();

    /// HTTP request count
    pub static ref HTTP_REQUESTS_TOTAL: CounterVec = register_counter_vec!(
        "http_requests_total",
        "Total number of HTTP requests",
        &["method", "endpoint", "status"]
    ).unwrap();

    // ============================================================================
    // gRPC Metrics
    // ============================================================================

    /// gRPC request duration in seconds
    pub static ref GRPC_REQUEST_DURATION: HistogramVec = register_histogram_vec!(
        "grpc_request_duration_seconds",
        "gRPC request duration in seconds",
        &["method", "status"],
        vec![0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0]
    ).unwrap();

    /// gRPC request count
    pub static ref GRPC_REQUESTS_TOTAL: CounterVec = register_counter_vec!(
        "grpc_requests_total",
        "Total number of gRPC requests",
        &["method", "status"]
    ).unwrap();

    // ============================================================================
    // Authentication Metrics (M1, M27, M54-M58)
    // ============================================================================

    /// Authentication duration in seconds
    pub static ref AUTH_DURATION: HistogramVec = register_histogram_vec!(
        "auth_duration_seconds",
        "Authentication duration in seconds",
        &["auth_type"],
        vec![0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0]
    ).unwrap();

    /// Authentication attempt count
    pub static ref AUTH_TOTAL: CounterVec = register_counter_vec!(
        "auth_total",
        "Total authentication attempts",
        &["result", "auth_type"]
    ).unwrap();

    /// SigV4 verification stage duration
    pub static ref SIGV4_STAGE_DURATION: HistogramVec = register_histogram_vec!(
        "sigv4_stage_duration_seconds",
        "SigV4 verification stage duration",
        &["stage"],
        vec![0.0001, 0.0005, 0.001, 0.005, 0.010, 0.025, 0.050, 0.100]
    ).unwrap();

    // ============================================================================
    // Storage Metrics (M8, M12, M39, M40, M44)
    // ============================================================================

    /// Storage operation duration in seconds
    pub static ref STORAGE_OP_DURATION: HistogramVec = register_histogram_vec!(
        "storage_operation_duration_seconds",
        "Storage operation duration in seconds",
        &["operation", "backend"],
        vec![0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0]
    ).unwrap();

    /// Storage lock wait duration in seconds
    pub static ref STORAGE_LOCK_WAIT: HistogramVec = register_histogram_vec!(
        "storage_lock_wait_duration_seconds",
        "Storage lock wait duration in seconds",
        &["lock_type"],
        vec![0.0001, 0.0005, 0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.500, 1.0]
    ).unwrap();

    /// Total number of objects in storage
    pub static ref STORAGE_OBJECTS_TOTAL: GaugeVec = register_gauge_vec!(
        "storage_objects_total",
        "Total number of objects in storage",
        &["bucket"]
    ).unwrap();

    /// Total bytes in storage
    pub static ref STORAGE_BYTES_TOTAL: GaugeVec = register_gauge_vec!(
        "storage_bytes_total",
        "Total bytes in storage",
        &["bucket"]
    ).unwrap();

    // ============================================================================
    // Erasure Coding Metrics (M39, M40)
    // ============================================================================

    /// Erasure encoding duration in seconds
    pub static ref ERASURE_ENCODE_DURATION: HistogramVec = register_histogram_vec!(
        "erasure_encode_duration_seconds",
        "Erasure encoding duration in seconds",
        &["data_blocks", "parity_blocks"],
        vec![0.0001, 0.0005, 0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.500]
    ).unwrap();

    /// Erasure decoding duration in seconds
    pub static ref ERASURE_DECODE_DURATION: HistogramVec = register_histogram_vec!(
        "erasure_decode_duration_seconds",
        "Erasure decoding duration in seconds",
        &["data_blocks", "parity_blocks"],
        vec![0.0001, 0.0005, 0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.500]
    ).unwrap();

    /// Erasure bytes processed
    pub static ref ERASURE_BYTES_PROCESSED: CounterVec = register_counter_vec!(
        "erasure_bytes_processed_total",
        "Total bytes processed by erasure coding",
        &["operation"]
    ).unwrap();

    // ============================================================================
    // Multipart Upload Metrics (M21-M26)
    // ============================================================================

    /// Multipart upload initiation count
    pub static ref MULTIPART_INIT_TOTAL: CounterVec = register_counter_vec!(
        "multipart_init_total",
        "Total multipart upload initiations",
        &[]
    ).unwrap();

    /// Multipart upload part duration
    pub static ref MULTIPART_UPLOAD_PART_DURATION: HistogramVec = register_histogram_vec!(
        "multipart_upload_part_duration_seconds",
        "Multipart upload part duration",
        &[],
        vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    ).unwrap();

    /// Multipart upload completion duration
    pub static ref MULTIPART_COMPLETE_DURATION: HistogramVec = register_histogram_vec!(
        "multipart_complete_duration_seconds",
        "Multipart upload completion duration",
        &[],
        vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]
    ).unwrap();

    /// Active multipart uploads
    pub static ref MULTIPART_ACTIVE_UPLOADS: GaugeVec = register_gauge_vec!(
        "multipart_active_uploads",
        "Number of active multipart uploads",
        &[]
    ).unwrap();

    /// Multipart part size in bytes
    pub static ref MULTIPART_PART_SIZE: HistogramVec = register_histogram_vec!(
        "multipart_part_size_bytes",
        "Multipart upload part size in bytes",
        &[],
        vec![1024.0, 10240.0, 102400.0, 1048576.0, 5242880.0, 10485760.0, 52428800.0]
    ).unwrap();

    // ============================================================================
    // Error Metrics
    // ============================================================================

    /// Error count by type and component
    pub static ref ERROR_TOTAL: CounterVec = register_counter_vec!(
        "error_total",
        "Total number of errors",
        &["error_type", "component"]
    ).unwrap();

    // ============================================================================
    // System Metrics
    // ============================================================================

    /// Concurrent requests gauge
    pub static ref CONCURRENT_REQUESTS: GaugeVec = register_gauge_vec!(
        "concurrent_requests",
        "Number of concurrent requests being processed",
        &["protocol"]
    ).unwrap();
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Record authentication duration
pub fn record_auth_duration(auth_type: &str, duration: f64) {
    AUTH_DURATION.with_label_values(&[auth_type]).observe(duration);
}

/// Increment successful authentication counter
pub fn increment_auth_success(auth_type: &str) {
    AUTH_TOTAL
        .with_label_values(&["success", auth_type])
        .inc();
}

/// Increment failed authentication counter
pub fn increment_auth_failure(auth_type: &str) {
    AUTH_TOTAL
        .with_label_values(&["failure", auth_type])
        .inc();
}

/// Record SigV4 stage duration
pub fn record_sigv4_stage(stage: &str, duration: f64) {
    SIGV4_STAGE_DURATION
        .with_label_values(&[stage])
        .observe(duration);
}

/// Record storage operation duration
pub fn record_storage_op(operation: &str, backend: &str, duration: f64) {
    STORAGE_OP_DURATION
        .with_label_values(&[operation, backend])
        .observe(duration);
}

/// Record lock wait duration
pub fn record_lock_wait(lock_type: &str, duration: f64) {
    STORAGE_LOCK_WAIT
        .with_label_values(&[lock_type])
        .observe(duration);
}

/// Record erasure encode duration
pub fn record_erasure_encode(data_blocks: usize, parity_blocks: usize, duration: f64) {
    ERASURE_ENCODE_DURATION
        .with_label_values(&[&data_blocks.to_string(), &parity_blocks.to_string()])
        .observe(duration);
}

/// Record erasure decode duration
pub fn record_erasure_decode(data_blocks: usize, parity_blocks: usize, duration: f64) {
    ERASURE_DECODE_DURATION
        .with_label_values(&[&data_blocks.to_string(), &parity_blocks.to_string()])
        .observe(duration);
}

/// Increment erasure bytes processed
pub fn increment_erasure_bytes(operation: &str, bytes: u64) {
    ERASURE_BYTES_PROCESSED
        .with_label_values(&[operation])
        .inc_by(bytes as f64);
}

/// Increment multipart init counter
pub fn increment_multipart_init() {
    MULTIPART_INIT_TOTAL.with_label_values(&[]).inc();
}

/// Record multipart part upload duration
pub fn record_multipart_part_duration(duration: f64) {
    MULTIPART_UPLOAD_PART_DURATION
        .with_label_values(&[])
        .observe(duration);
}

/// Record multipart complete duration
pub fn record_multipart_complete_duration(duration: f64) {
    MULTIPART_COMPLETE_DURATION
        .with_label_values(&[])
        .observe(duration);
}

/// Increment active multipart uploads
pub fn inc_multipart_active() {
    MULTIPART_ACTIVE_UPLOADS.with_label_values(&[]).inc();
}

/// Decrement active multipart uploads
pub fn dec_multipart_active() {
    MULTIPART_ACTIVE_UPLOADS.with_label_values(&[]).dec();
}

/// Record multipart part size
pub fn record_multipart_part_size(size: usize) {
    MULTIPART_PART_SIZE
        .with_label_values(&[])
        .observe(size as f64);
}

/// Increment error counter
pub fn increment_error(error_type: &str, component: &str) {
    ERROR_TOTAL
        .with_label_values(&[error_type, component])
        .inc();
}

/// Increment HTTP request counter
pub fn increment_http_request(method: &str, endpoint: &str, status: &str) {
    HTTP_REQUESTS_TOTAL
        .with_label_values(&[method, endpoint, status])
        .inc();
}

/// Record HTTP request duration
pub fn record_http_duration(method: &str, endpoint: &str, status: &str, duration: f64) {
    HTTP_REQUEST_DURATION
        .with_label_values(&[method, endpoint, status])
        .observe(duration);
}

/// Increment gRPC request counter
pub fn increment_grpc_request(method: &str, status: &str) {
    GRPC_REQUESTS_TOTAL
        .with_label_values(&[method, status])
        .inc();
}

/// Record gRPC request duration
pub fn record_grpc_duration(method: &str, status: &str, duration: f64) {
    GRPC_REQUEST_DURATION
        .with_label_values(&[method, status])
        .observe(duration);
}

/// Increment concurrent requests
pub fn inc_concurrent_requests(protocol: &str) {
    CONCURRENT_REQUESTS.with_label_values(&[protocol]).inc();
}

/// Decrement concurrent requests
pub fn dec_concurrent_requests(protocol: &str) {
    CONCURRENT_REQUESTS.with_label_values(&[protocol]).dec();
}

/// Gather all metrics for Prometheus exposition
pub fn gather_metrics() -> Vec<u8> {
    use prometheus::Encoder;
    let encoder = TextEncoder::new();
    // Use the default registry since our metrics are registered there
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    buffer
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_metrics() {
        record_auth_duration("sigv4", 0.05);
        increment_auth_success("sigv4");
        increment_auth_failure("header");

        // Verify metrics were recorded
        assert_eq!(AUTH_TOTAL.with_label_values(&["success", "sigv4"]).get(), 1.0);
        assert_eq!(AUTH_TOTAL.with_label_values(&["failure", "header"]).get(), 1.0);
    }

    #[test]
    fn test_storage_metrics() {
        record_storage_op("put", "memory", 0.01);
        record_lock_wait("bucket", 0.001);

        // Metrics should be recorded without panicking
    }

    #[test]
    fn test_erasure_metrics() {
        record_erasure_encode(4, 2, 0.005);
        increment_erasure_bytes("encode", 1024);

        assert_eq!(
            ERASURE_BYTES_PROCESSED
                .with_label_values(&["encode"])
                .get(),
            1024.0
        );
    }

    #[test]
    fn test_multipart_metrics() {
        increment_multipart_init();
        inc_multipart_active();
        record_multipart_part_duration(0.5);
        dec_multipart_active();

        assert_eq!(MULTIPART_INIT_TOTAL.with_label_values(&[]).get(), 1.0);
    }

    #[test]
    fn test_error_metrics() {
        increment_error("AuthError", "http");
        increment_error("StorageError", "grpc");

        assert_eq!(ERROR_TOTAL.with_label_values(&["AuthError", "http"]).get(), 1.0);
        assert_eq!(
            ERROR_TOTAL
                .with_label_values(&["StorageError", "grpc"])
                .get(),
            1.0
        );
    }

    #[test]
    fn test_gather_metrics() {
        // Record a metric first to ensure there's something to gather
        increment_http_request("GET", "/test", "200");

        let output = gather_metrics();
        let output_str = String::from_utf8(output).unwrap();

        // Should not be empty and contain metrics data
        assert!(!output_str.is_empty(), "Metrics output should not be empty");
    }
}
