# S3 Compatibility Roadmap

This document outlines the path to full S3 compatibility for s3ish. Many core features have been implemented.

## Implementation Status Summary

### ✅ Completed (Production-Ready)
- **AWS SigV4 Authentication** - Full implementation with performance profiling
- **Pre-signed URLs** - Query string auth with expiration validation (up to 7 days)
- **XML Response Format** - Complete S3-compliant XML for all operations
- **S3-Compatible Endpoints** - All major endpoints (path-style)
- **S3 Headers Support** - Comprehensive request/response header handling
- **File System Backend** - Persistent storage with erasure coding
- **Multipart Uploads** - Complete multipart upload protocol
- **CopyObject** - Full CopyObject support with metadata directives
- **User Metadata** - x-amz-meta-* headers fully supported
- **Range Requests** - HTTP Range header support
- **Observability** - 66+ Prometheus metrics, structured logging, health checks
- **Storage Class Headers** - x-amz-storage-class request/response support
- **Encryption Headers** - x-amz-server-side-encryption request/response support
- **Content-MD5 Validation** - Integrity checking for uploads

### 🚧 Partial / In Progress
- None currently

### ❌ Not Yet Implemented
- **Object Versioning** - Not implemented
- **Bucket Policies & ACLs** - Not implemented
- **Server-Side Encryption** - Not implemented
- **Virtual-Hosted Style URLs** - Only path-style supported
- **Lifecycle Policies** - Not implemented

## Phase 1: Core S3 API ✅ MOSTLY COMPLETED

### 1.1 AWS Signature V4 Authentication ✅ COMPLETED
**Status:** ✅ Fully implemented
**Details:** Full AWS SigV4 implementation with performance profiling and pre-signed URLs

```rust
// Implemented:
- [x] Parse Authorization header (AWS4-HMAC-SHA256 format)
- [x] Extract signature components (access key, date, region, service)
- [x] Calculate canonical request
- [x] Calculate string to sign
- [x] Verify HMAC-SHA256 signature
- [x] Stage-by-stage performance profiling metrics
- [x] Support query string authentication (pre-signed URLs)
- [x] Pre-signed URL expiration validation (max 7 days)
- [x] Generate pre-signed URLs programmatically
- [x] Handle credential scope validation
```

**Implementation:** See `src/s3_http.rs` - SigV4 verification with detailed metrics, pre-signed URL support
**Impact:** S3 clients (AWS CLI, boto3) can now connect, and temporary URLs can be generated for sharing

### 1.2 XML Response Format ✅ COMPLETED
**Status:** ✅ Fully implemented
**Details:** Complete S3-compliant XML response system

```rust
// Implemented:
- [x] ListBucketResult XML format
- [x] ListAllMyBucketsResult XML format
- [x] Error response XML format
- [x] CreateBucketConfiguration
- [x] CompleteMultipartUploadResult
- [x] InitiateMultipartUploadResult
- [x] CopyObjectResult
- [x] DeleteResult (multi-object delete)
- [x] Custom XML serialization via ResponseRenderer trait
```

**Implementation:** See `src/s3_http.rs` - ResponseRenderer trait with XML implementation
**Impact:** S3 clients can parse all responses correctly

### 1.3 S3-Compatible Endpoints ✅ COMPLETED
**Status:** ✅ Fully implemented (path-style)
**Details:** Complete S3 endpoint coverage with query parameter routing

```rust
// Implemented (Path-style):
- [x] GET    /{bucket}/{key}           // GetObject
- [x] PUT    /{bucket}/{key}           // PutObject
- [x] DELETE /{bucket}/{key}           // DeleteObject
- [x] HEAD   /{bucket}/{key}           // HeadObject
- [x] PATCH  /{bucket}/{key}           // CopyObject (also via PUT with x-amz-copy-source)
- [x] GET    /{bucket}/               // ListObjects
- [x] HEAD   /{bucket}/               // HeadBucket
- [x] PUT    /{bucket}/               // CreateBucket
- [x] DELETE /{bucket}/               // DeleteBucket
- [x] GET    /                        // ListBuckets

// Query parameter operations:
- [x] POST   /{bucket}/{key}?uploads                    // InitiateMultipartUpload
- [x] PUT    /{bucket}/{key}?uploadId=X&partNumber=N   // UploadPart
- [x] POST   /{bucket}/{key}?uploadId=X                // CompleteMultipartUpload
- [x] DELETE /{bucket}/{key}?uploadId=X                // AbortMultipartUpload
- [x] POST   /{bucket}/?delete                         // DeleteObjects (multi-delete)
```

**Implementation:** See `src/s3_http.rs` - Full Axum router with query parameter handling
**Impact:** AWS CLI/SDKs work correctly
**Note:** Virtual-hosted-style not yet implemented (not critical for single-host deployment)

### 1.4 S3 Headers Support ✅ COMPLETED
**Status:** ✅ Comprehensive header support implemented
**Details:** Full S3 header support for requests and responses, including storage class and encryption

```rust
// Request headers supported:
- [x] x-amz-content-sha256       // Payload hash (for SigV4)
- [x] x-amz-date                 // Request timestamp (for SigV4)
- [ ] x-amz-security-token       // STS token - NOT YET (Phase 2)
- [x] x-amz-meta-*               // User metadata (full support)
- [x] x-amz-storage-class        // Storage class (accepted and stored)
- [x] x-amz-server-side-encryption  // Encryption header (accepted and stored)
- [x] Content-MD5                // Integrity check with validation
- [x] If-Match, If-None-Match    // Conditional requests
- [x] x-amz-copy-source-if-match, x-amz-copy-source-if-none-match
- [x] Range                      // Partial downloads
- [x] x-amz-copy-source          // CopyObject source
- [x] x-amz-metadata-directive   // COPY/REPLACE for CopyObject

// Response headers returned:
- [x] x-amz-request-id           // Unique request ID (UUID)
- [x] x-amz-id-2                 // Extended request ID
- [x] x-amz-bucket-region        // Bucket region
- [ ] x-amz-version-id           // Object version - NOT YET (no versioning)
- [x] ETag                       // MD5 of content (quoted)
- [x] Last-Modified              // ISO 8601 timestamp
- [x] Content-Type               // MIME type
- [x] Content-Length             // Size
- [x] Accept-Ranges: bytes       // Range support
- [x] x-amz-meta-*               // User metadata echoed back
- [x] x-amz-storage-class        // Storage class echoed back
- [x] x-amz-server-side-encryption  // Encryption echoed back
```

**Implementation:**
- Request header parsing: `src/s3_http.rs` (put_object, copy_object handlers)
- Response header application: `apply_storage_headers()` function
- Content-MD5 validation with base64 decoding
- Storage: `src/storage/mod.rs` - ObjectMetadata includes storage_class and server_side_encryption
- All 126 tests pass

**Impact:** Full compatibility with S3 clients for storage class and encryption headers, plus integrity validation

## Phase 2: Persistent Storage (Critical for Production)

### 2.1 File System Backend ✅ COMPLETED
**Status:** ✅ Fully implemented with erasure coding support
**Details:** Production-ready filesystem storage backend

```rust
pub struct FileStorage {
    base_path: PathBuf,           // e.g., ./s3-data
    enable_erasure: bool,         // Optional erasure coding
    data_blocks: usize,           // Data shards (default: 2)
    parity_blocks: usize,         // Parity shards (default: 1)
}

// Implemented directory structure:
// ./s3-data/
//   {bucket}/
//     {key}/
//       data.0, data.1, ...       // Data shards (if erasure coding)
//       parity.0, parity.1, ...   // Parity shards (if erasure coding)
//       data                      // Single file (if no erasure coding)
//       metadata.json             // Object metadata sidecar

// Object metadata (stored as JSON sidecar):
struct ObjectMetadata {
    content_type: String,
    etag: String,
    size: u64,
    last_modified_unix_secs: i64,
    metadata: HashMap<String, String>,  // User metadata
}
```

**Implemented features:**
- [x] Implement FileStorage backend (src/storage/file_storage.rs)
- [x] Optional erasure coding with configurable data/parity blocks
- [x] Streaming shard encode/decode (no full buffering)
- [x] Atomic file operations via tempfile crate
- [x] JSON sidecar files for metadata
- [x] Directory-based bucket/key hierarchy
- [x] Full test coverage including large file streaming

**Configuration:**
```toml
[storage]
backend = "file"
path = "./s3-data"
erasure_coding = true
data_blocks = 2
parity_blocks = 1
```

**Implementation:** See `src/storage/file_storage.rs`
**Impact:** Data persists across restarts with optional redundancy

### 2.2 Storage Backend Abstraction Updates
```rust
#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    // Add new methods for S3 features
    async fn list_buckets(&self) -> Result<Vec<BucketInfo>, StorageError>;
    async fn head_object(&self, bucket: &str, key: &str)
        -> Result<ObjectMetadata, StorageError>;
    async fn put_object_metadata(&self, bucket: &str, key: &str,
        metadata: HashMap<String, String>) -> Result<(), StorageError>;

    // Multipart upload support
    async fn initiate_multipart(&self, bucket: &str, key: &str)
        -> Result<String, StorageError>;  // Returns upload_id
    async fn upload_part(&self, bucket: &str, key: &str, upload_id: &str,
        part_number: u32, data: Bytes) -> Result<PartInfo, StorageError>;
    async fn complete_multipart(&self, bucket: &str, key: &str, upload_id: &str,
        parts: Vec<CompletedPart>) -> Result<ObjectMetadata, StorageError>;
    async fn abort_multipart(&self, bucket: &str, key: &str, upload_id: &str)
        -> Result<(), StorageError>;

    // Range requests
    async fn get_object_range(&self, bucket: &str, key: &str,
        range: Range<u64>) -> Result<(Bytes, ObjectMetadata), StorageError>;
}
```

**Complexity:** Medium (2-3 days)

## Phase 3: Multipart Upload ✅ COMPLETED

### 3.1 Multipart Upload State Management ✅ COMPLETED
**Status:** ✅ Fully implemented
**Details:** Complete multipart upload support with in-memory state management

```rust
// Implemented state management:
struct MultipartUpload {
    upload_id: String,            // UUID-based upload ID
    bucket: String,
    key: String,
    initiated_at: i64,
    parts: BTreeMap<u32, UploadedPart>,  // part_number -> UploadedPart
}

struct UploadedPart {
    part_number: u32,
    etag: String,
    data: Bytes,                  // In-memory storage of part data
}
```

**Implemented features:**
- [x] Generate unique upload IDs (UUID-based)
- [x] Store parts in memory during upload
- [x] Validate part ETags on completion
- [x] Combine parts into final object
- [x] Abort multipart uploads
- [x] Handle concurrent part uploads with mutex
- [x] Full XML request/response support
- [x] Prometheus metrics for multipart operations

**Implementation:** See `src/s3_http.rs` - MultipartUpload state in S3State
**Impact:** Large file uploads work correctly via multipart protocol

### 3.2 Part Size Validation
```rust
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;        // 5 MB
const MAX_PART_SIZE: u64 = 5 * 1024 * 1024 * 1024; // 5 GB
const MAX_PARTS: u32 = 10_000;
const MAX_OBJECT_SIZE: u64 = 5 * 1024 * 1024 * 1024 * 1024; // 5 TB
```

## Phase 4: Performance Optimizations (For Vertical Scaling)

### 4.1 Efficient I/O
```rust
// Use tokio::fs for async file operations
- [ ] Async file reads/writes with tokio::fs
- [ ] Buffered I/O for large objects
- [ ] Memory-mapped files for very large objects (optional)
- [ ] Zero-copy file serving with sendfile (Linux)
- [ ] Stream processing (don't load entire object in memory)
```

**Example:**
```rust
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn stream_object_to_response(path: &Path) -> Result<impl Stream<Item = Result<Bytes>>> {
    let file = File::open(path).await?;
    let reader = BufReader::new(file);
    // Stream chunks instead of loading all
    Ok(ReaderStream::new(reader))
}
```

### 4.2 Connection Limits & Resource Management
```rust
// In server configuration
struct ServerConfig {
    max_connections: usize,        // e.g., 10,000
    max_request_body_size: u64,    // e.g., 5 GB
    read_timeout: Duration,
    write_timeout: Duration,
    idle_timeout: Duration,

    // Disk I/O limits
    max_concurrent_uploads: usize,  // e.g., 100
    max_concurrent_downloads: usize, // e.g., 500
}

// Use semaphore for rate limiting
use tokio::sync::Semaphore;
let upload_limiter = Arc::new(Semaphore::new(config.max_concurrent_uploads));
```

### 4.3 Caching Strategy
```rust
use moka::future::Cache;

struct CachedMetadata {
    metadata_cache: Cache<String, ObjectMetadata>,  // key -> metadata
    bucket_cache: Cache<String, BucketInfo>,        // bucket -> info
}

// Cache hot metadata in memory
// Cache TTL: 5-60 seconds
```

### 4.4 Request Coalescing
```rust
// If multiple clients request the same object simultaneously,
// serve from a single file read
use dashmap::DashMap;

struct InFlightRequests {
    active: DashMap<String, Arc<tokio::sync::Notify>>,
}
```

## Phase 5: Additional S3 Features (Nice to Have)

### 5.1 Bucket Operations
- [ ] ListBuckets
- [ ] HeadBucket
- [ ] GetBucketLocation
- [ ] GetBucketVersioning (simplified - can return disabled)
- [ ] PutBucketVersioning (can be no-op)

### 5.2 Object Operations
- [ ] CopyObject
- [ ] HeadObject
- [ ] Range requests (GET with Range header)
- [ ] Conditional requests (If-Match, If-None-Match)
- [ ] Object tagging (simplified)

### 5.3 Batch Operations
- [ ] DeleteObjects (multi-object delete)

### 5.4 Pre-signed URLs
- [ ] Generate pre-signed URLs with expiration
- [ ] Validate pre-signed URL signatures

## Implementation Priority & Timeline

### Must Have (Weeks 1-2): Basic S3 Compatibility
1. ✅ File system storage backend (4-5 days)
2. ✅ AWS SigV4 authentication (3-5 days)
3. ✅ XML response format (2-3 days)
4. ✅ Core S3 endpoints (2-3 days)

**Deliverable:** Works with AWS CLI/SDK for basic operations

### Should Have (Weeks 3-4): Production Ready
5. ✅ Multipart upload support (3-4 days)
6. ✅ S3 headers support (1-2 days)
7. ✅ Range requests (1-2 days)
8. ✅ Performance optimizations (2-3 days)
9. ✅ Error handling & logging (1-2 days)

**Deliverable:** Production-ready for most workloads

### Nice to Have (Week 5+): Advanced Features
10. Pre-signed URLs (2-3 days)
11. CopyObject operation (1-2 days)
12. Metadata caching (1-2 days)
13. Metrics & monitoring (2-3 days)
14. Bucket policies (simplified) (3-4 days)

## Hardware Recommendations for Vertical Scaling

### Single Host Configuration

**Small Scale (< 1TB, < 1000 req/s):**
- CPU: 4-8 cores
- RAM: 8-16 GB
- Storage: SSD (NVMe preferred)
- Network: 1 Gbps

**Medium Scale (1-10 TB, 1000-5000 req/s):**
- CPU: 16-32 cores
- RAM: 32-64 GB
- Storage: NVMe SSD RAID 10
- Network: 10 Gbps

**Large Scale (10-100 TB, 5000-20000 req/s):**
- CPU: 32-64 cores (or more)
- RAM: 128-256 GB
- Storage: Multiple NVMe SSDs with software RAID
- Network: 25-100 Gbps

### Performance Tuning

```toml
# config.toml example
[server]
listen_addr = "0.0.0.0:9000"
max_connections = 10000
read_timeout_secs = 300
write_timeout_secs = 300

[storage]
base_path = "/var/lib/s3ish/data"
metadata_path = "/var/lib/s3ish/metadata"
max_concurrent_uploads = 100
max_concurrent_downloads = 500
enable_fsync = true  # Durability vs performance trade-off

[cache]
metadata_cache_size_mb = 1024
metadata_cache_ttl_secs = 60
enable_request_coalescing = true

[limits]
max_object_size_gb = 5000
max_multipart_parts = 10000
min_part_size_mb = 5
```

## Testing S3 Compatibility

### Tools for Testing
```bash
# AWS CLI
aws --endpoint-url http://localhost:9000 s3 ls
aws --endpoint-url http://localhost:9000 s3 mb s3://test-bucket
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://test-bucket/

# s3cmd
s3cmd --host=localhost:9000 --host-bucket=localhost:9000 ls

# MinIO Client (mc)
mc alias set local http://localhost:9000 accesskey secretkey
mc ls local/

# boto3 (Python SDK)
import boto3
s3 = boto3.client('s3', endpoint_url='http://localhost:9000')
s3.list_buckets()
```

### Compatibility Test Suite
Create a test suite that validates S3 compatibility:
- [ ] AWS CLI operations
- [ ] boto3 SDK operations
- [ ] Multipart upload with aws-sdk
- [ ] Pre-signed URL generation and validation
- [ ] Range requests
- [ ] Concurrent operations

## Estimated Total Effort

- **Minimum Viable S3 Compatibility:** 2-3 weeks
- **Production Ready:** 4-5 weeks
- **Full Featured:** 6-8 weeks

## What You DON'T Need (Since Single Host)

❌ **Distributed Consensus:** No Raft/etcd needed
❌ **Data Replication:** No multi-node replication
❌ **Cluster Management:** No node discovery/health checks
❌ **Distributed Transactions:** Single node = simpler transactions
❌ **Load Balancing:** Can use a simple reverse proxy if needed
❌ **Sharding Logic:** All data on one host

## What You DO Need

✅ **Durability:** Fsync, RAID, backups
✅ **Concurrent I/O:** tokio async, semaphores
✅ **Efficient Storage:** Content-addressed, deduplication
✅ **Fast Metadata:** Embedded DB (sled/rocksdb)
✅ **Resource Limits:** Connection pools, rate limiting
✅ **Monitoring:** Metrics, logging, tracing

## Key Dependencies to Add

```toml
[dependencies]
# Current dependencies...
# (existing: tokio, tonic, axum, etc.)

# S3 Compatibility
aws-sigv4 = "1.2"                    # Signature verification
aws-credential-types = "1.2"         # Credential types
sha2 = "0.10"                        # SHA-256 hashing
hmac = "0.12"                        # HMAC for signatures
hex = "0.4"                          # Hex encoding
quick-xml = "0.31"                   # XML serialization
serde-xml-rs = "0.6"                 # XML serde support

# Storage
sled = "0.34"                        # Embedded KV database
# OR: rocksdb = "0.21"               # Alternative to sled
walkdir = "2.4"                      # Directory traversal
uuid = { version = "1.6", features = ["v4"] }  # Upload IDs

# Performance
moka = { version = "0.12", features = ["future"] }  # Async cache
dashmap = "5.5"                      # Concurrent hashmap
tokio-util = { version = "0.7", features = ["io"] }  # Stream utilities

# Optional: Advanced features
rust-s3 = "0.33"                     # For reference implementation
```

## References

- [AWS S3 API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)
- [MinIO Implementation](https://github.com/minio/minio) - Good reference for single-node S3
- [Garage](https://git.deuxfleurs.fr/Deuxfleurs/garage) - Distributed S3, but has useful patterns
- [AWS Signature V4](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html)
