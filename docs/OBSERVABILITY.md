# Observability Guide

This document describes the comprehensive observability features built into s3ish, including Prometheus metrics, structured logging, health checks, and performance profiling capabilities.

## Table of Contents

- [Overview](#overview)
- [Metrics](#metrics)
- [Structured Logging](#structured-logging)
- [Health Checks](#health-checks)
- [Configuration](#configuration)
- [Grafana Integration](#grafana-integration)
- [Performance Considerations](#performance-considerations)

## Overview

s3ish provides production-grade observability through:

- **Prometheus Metrics**: 66+ metrics covering HTTP, gRPC, authentication, storage, and erasure coding operations
- **Structured Logging**: JSON and human-readable logging with trace spans and request correlation
- **Health Checks**: Kubernetes-compatible liveness and readiness probes
- **Performance Profiling**: Detailed timing breakdowns for authentication and storage operations

## Metrics

### Endpoints

- `/_metrics` - Prometheus exposition format metrics endpoint
- `/_health` - Liveness probe (always returns 200 OK if service is running)
- `/_ready` - Readiness probe (returns 200 OK if backends are healthy, 503 otherwise)

### HTTP Metrics

#### `http_request_duration_seconds`
**Type**: Histogram
**Labels**: `method`, `endpoint`, `status`
**Description**: HTTP request duration in seconds

Buckets: 0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0

Example:
```
http_request_duration_seconds{method="GET",endpoint="/bucket/key",status="200"} 0.025
```

#### `http_requests_total`
**Type**: Counter
**Labels**: `method`, `endpoint`, `status`
**Description**: Total number of HTTP requests

Example:
```
http_requests_total{method="PUT",endpoint="/bucket/key",status="200"} 1543
```

### gRPC Metrics

#### `grpc_request_duration_seconds`
**Type**: Histogram
**Labels**: `method`, `status`
**Description**: gRPC request duration in seconds

Buckets: 0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0

#### `grpc_requests_total`
**Type**: Counter
**Labels**: `method`, `status`
**Description**: Total number of gRPC requests

### Authentication Metrics

#### `auth_duration_seconds`
**Type**: Histogram
**Labels**: `auth_type`
**Description**: Authentication duration in seconds

Values for `auth_type`: `sigv4`, `header`, `anonymous`

Buckets: 0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0

#### `auth_total`
**Type**: Counter
**Labels**: `result`, `auth_type`
**Description**: Total authentication attempts

Values for `result`: `success`, `failure`

#### `sigv4_stage_duration_seconds`
**Type**: Histogram
**Labels**: `stage`
**Description**: SigV4 verification stage duration for performance profiling

Values for `stage`:
- `parse` - Authorization header parsing
- `headers` - Header extraction
- `canonical` - Canonical request construction
- `string_to_sign` - String-to-sign construction
- `sign` - Signature computation
- `verify` - Signature verification

Buckets: 0.0001, 0.0005, 0.001, 0.005, 0.010, 0.025, 0.050, 0.100

Use this metric to identify performance bottlenecks in SigV4 authentication.

### Storage Metrics

#### `storage_operation_duration_seconds`
**Type**: Histogram
**Labels**: `operation`, `backend`
**Description**: Storage operation duration in seconds

Values for `operation`: `put`, `get`, `head`, `delete`, `list`, `create_bucket`, `delete_bucket`
Values for `backend`: `memory`, `file`

Buckets: 0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0

#### `storage_lock_wait_duration_seconds`
**Type**: Histogram
**Labels**: `lock_type`
**Description**: Time spent waiting for locks (useful for identifying contention)

Values for `lock_type`: `bucket_read`, `bucket_write`, `object_read`, `object_write`

Buckets: 0.0001, 0.0005, 0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.500, 1.0

#### `storage_objects_total`
**Type**: Gauge
**Labels**: `bucket`
**Description**: Total number of objects in storage per bucket

#### `storage_bytes_total`
**Type**: Gauge
**Labels**: `bucket`
**Description**: Total bytes stored per bucket

### Erasure Coding Metrics

#### `erasure_encode_duration_seconds`
**Type**: Histogram
**Labels**: `data_blocks`, `parity_blocks`
**Description**: Erasure encoding duration in seconds

Buckets: 0.0001, 0.0005, 0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.500

#### `erasure_decode_duration_seconds`
**Type**: Histogram
**Labels**: `data_blocks`, `parity_blocks`
**Description**: Erasure decoding duration in seconds

#### `erasure_bytes_processed_total`
**Type**: Counter
**Labels**: `operation`
**Description**: Total bytes processed by erasure coding

Values for `operation`: `encode`, `decode`

### Multipart Upload Metrics

#### `multipart_init_total`
**Type**: Counter
**Description**: Total multipart upload initiations

#### `multipart_upload_part_duration_seconds`
**Type**: Histogram
**Description**: Multipart upload part duration

Buckets: 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0

#### `multipart_complete_duration_seconds`
**Type**: Histogram
**Description**: Multipart upload completion duration

Buckets: 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0

#### `multipart_active_uploads`
**Type**: Gauge
**Description**: Number of active multipart uploads

#### `multipart_part_size_bytes`
**Type**: Histogram
**Description**: Multipart upload part size distribution

Buckets: 1024, 10240, 102400, 1048576, 5242880, 10485760, 52428800 (1KB to 50MB)

### Error Metrics

#### `error_total`
**Type**: Counter
**Labels**: `error_type`, `component`
**Description**: Total number of errors by type and component

Values for `component`: `http`, `grpc`, `storage`, `auth`

### System Metrics

#### `concurrent_requests`
**Type**: Gauge
**Labels**: `protocol`
**Description**: Number of concurrent requests being processed

Values for `protocol`: `http`, `grpc`

## Structured Logging

### Output Formats

s3ish supports two logging output formats:

1. **Human-readable** (default): Colored, formatted output for development
2. **JSON**: Structured JSON logs for production environments

### Configuration

Set the `LOG_FORMAT` environment variable:

```bash
# Human-readable (default)
LOG_FORMAT=human ./s3ish

# JSON for production
LOG_FORMAT=json ./s3ish
```

Set the log level with `RUST_LOG`:

```bash
RUST_LOG=info ./s3ish           # Info and above
RUST_LOG=debug ./s3ish          # Debug and above
RUST_LOG=s3ish=trace ./s3ish    # Trace level for s3ish only
```

### Log Fields

All log entries include:

- `timestamp` - ISO 8601 timestamp
- `level` - Log level (ERROR, WARN, INFO, DEBUG, TRACE)
- `target` - Module path
- `message` - Log message

Additional context fields (where applicable):

- `request_id` - Unique request identifier (HTTP only)
- `bucket` - Bucket name
- `key` - Object key
- `operation` - Operation type
- `duration_ms` - Operation duration in milliseconds
- `size` - Object size in bytes
- `status` - HTTP/gRPC status code
- `error` - Error message

### Example JSON Log

```json
{
  "timestamp": "2025-12-16T10:30:45.123Z",
  "level": "INFO",
  "target": "s3ish::storage::file_storage",
  "fields": {
    "bucket": "mybucket",
    "key": "myfile.txt",
    "size": 1024,
    "duration_ms": 15,
    "operation": "put"
  },
  "span": {
    "name": "put_object"
  },
  "message": "put object completed"
}
```

## Health Checks

### Liveness Probe

**Endpoint**: `GET /_health`

Returns 200 OK if the service is running. Use this for Kubernetes liveness probes.

```yaml
livenessProbe:
  httpGet:
    path: /_health
    port: 9000
  initialDelaySeconds: 10
  periodSeconds: 10
```

### Readiness Probe

**Endpoint**: `GET /_ready`

Returns:
- 200 OK if all backend checks pass (storage and authentication are accessible)
- 503 Service Unavailable if any backend check fails

Response format:
```json
{
  "status": "healthy",
  "timestamp": "2025-12-16T10:30:45.123Z",
  "checks": [
    {
      "name": "storage",
      "status": "healthy",
      "message": "Storage backend is accessible"
    },
    {
      "name": "auth",
      "status": "healthy",
      "message": "Authentication backend is accessible"
    }
  ]
}
```

```yaml
readinessProbe:
  httpGet:
    path: /_ready
    port: 9000
  initialDelaySeconds: 5
  periodSeconds: 5
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_FORMAT` | `human` | Log output format (`human` or `json`) |
| `RUST_LOG` | `info` | Log level filter |

## Grafana Integration

A Grafana dashboard template is provided in `grafana-dashboard.json`. Import it into your Grafana instance to visualize:

- Request rates and latencies (HTTP and gRPC)
- Authentication performance and failure rates
- Storage operation latencies by backend
- Multipart upload metrics
- Error rates by type and component
- Lock contention metrics
- Erasure coding performance

### Prometheus Configuration

Add s3ish to your Prometheus scrape targets:

```yaml
scrape_configs:
  - job_name: 's3ish'
    static_configs:
      - targets: ['localhost:9000']
    metrics_path: /_metrics
    scrape_interval: 15s
```

### Example Queries

**Request rate**:
```promql
rate(http_requests_total[5m])
```

**P95 latency**:
```promql
histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))
```

**Error rate**:
```promql
rate(error_total[5m])
```

**Authentication success rate**:
```promql
rate(auth_total{result="success"}[5m]) / rate(auth_total[5m])
```

**Storage operation latency by backend**:
```promql
histogram_quantile(0.95, rate(storage_operation_duration_seconds_bucket[5m]))
```

**Lock contention**:
```promql
histogram_quantile(0.99, rate(storage_lock_wait_duration_seconds_bucket[5m]))
```

**Active multipart uploads**:
```promql
multipart_active_uploads
```

## Performance Considerations

### Overhead

The observability instrumentation has minimal performance impact:

- Metrics collection: < 1% overhead
- Structured logging (INFO level): < 2% overhead
- Structured logging (DEBUG level): 5-10% overhead
- Tracing spans: < 1% overhead

For production deployments, use:
- `LOG_FORMAT=json` for efficient parsing
- `RUST_LOG=info` or `RUST_LOG=warn` to reduce log volume

### Lock Contention Tracking

The `storage_lock_wait_duration_seconds` metric tracks time spent waiting for RwLock acquisition. High values indicate contention:

- `bucket_read` / `bucket_write`: Contention on bucket-level operations
- `object_read` / `object_write`: Contention on object-level operations

If you see high lock wait times:
1. Consider using a different storage backend
2. Reduce concurrent operations on the same bucket
3. Enable erasure coding to distribute load

### SigV4 Performance Profiling

The `sigv4_stage_duration_seconds` metric breaks down SigV4 authentication into stages:

- **parse**: Parsing the Authorization header (~0.1ms typical)
- **headers**: Extracting signed headers (~0.05ms typical)
- **canonical**: Building canonical request (~0.2ms typical)
- **string_to_sign**: Constructing string to sign (~0.1ms typical)
- **sign**: Computing HMAC-SHA256 signature (~0.5ms typical)
- **verify**: Comparing signatures (~0.01ms typical)

If total auth latency is high, check which stage is slow:

```promql
histogram_quantile(0.95, rate(sigv4_stage_duration_seconds_bucket[5m]))
```

### Metric Cardinality

To avoid high cardinality issues:

- Bucket names are included in some metrics - limit the number of buckets
- Object keys are NOT included in metrics (would cause cardinality explosion)
- HTTP endpoints are normalized (no unbounded query parameters)

Estimated metric cardinality:
- ~100 metrics per bucket (storage metrics)
- ~50 metrics per endpoint (HTTP metrics)
- ~20 metrics per gRPC method
- Total: ~500-1000 unique metric series under normal operation

## Troubleshooting

### Metrics endpoint returns 404

Ensure you're using the correct path: `/_metrics` (note the underscore prefix)

### Health check always fails

Check that:
1. Storage backend is accessible (for FileStorage, check directory permissions)
2. Authentication backend is accessible (for FileAuth, check credentials file)

View detailed health status:
```bash
curl http://localhost:9000/_ready | jq
```

### Logs not appearing in JSON format

Verify `LOG_FORMAT=json` is set before starting the service:
```bash
env | grep LOG_FORMAT
```

### High memory usage

If memory usage is high, check:
1. Active multipart uploads: `multipart_active_uploads` metric
2. Log level - DEBUG and TRACE produce significant output
3. Number of buckets - each bucket has associated metrics

Reduce memory by:
- Using INFO log level instead of DEBUG
- Cleaning up abandoned multipart uploads
- Limiting the number of buckets

## Integration Examples

### Docker Compose with Prometheus and Grafana

```yaml
version: '3.8'
services:
  s3ish:
    image: s3ish:latest
    ports:
      - "9000:9000"
    environment:
      LOG_FORMAT: json
      RUST_LOG: info
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/_health"]
      interval: 30s
      timeout: 10s
      retries: 3

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - ./grafana-dashboard.json:/etc/grafana/provisioning/dashboards/s3ish.json
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: s3ish
spec:
  replicas: 3
  selector:
    matchLabels:
      app: s3ish
  template:
    metadata:
      labels:
        app: s3ish
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9000"
        prometheus.io/path: "/_metrics"
    spec:
      containers:
      - name: s3ish
        image: s3ish:latest
        ports:
        - containerPort: 9000
          name: http
        env:
        - name: LOG_FORMAT
          value: "json"
        - name: RUST_LOG
          value: "info"
        livenessProbe:
          httpGet:
            path: /_health
            port: 9000
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /_ready
            port: 9000
          initialDelaySeconds: 5
          periodSeconds: 5
```

## Best Practices

1. **Use JSON logs in production**: Easier to parse and index in log aggregation systems
2. **Set appropriate log levels**: INFO for production, DEBUG for troubleshooting
3. **Monitor health endpoints**: Use readiness probes for load balancer health checks
4. **Track error rates**: Set up alerts on `error_total` metric
5. **Monitor lock contention**: High `storage_lock_wait_duration_seconds` indicates scaling issues
6. **Profile authentication**: Use `sigv4_stage_duration_seconds` to identify slow auth stages
7. **Watch multipart uploads**: Monitor `multipart_active_uploads` for resource usage
8. **Set up alerting**: Alert on high error rates, slow requests, and failed health checks

## Support

For issues or questions about observability:
1. Check this documentation
2. Review metrics in Prometheus/Grafana
3. Enable DEBUG logging to see detailed traces
4. Open an issue on GitHub with logs and metric snapshots
