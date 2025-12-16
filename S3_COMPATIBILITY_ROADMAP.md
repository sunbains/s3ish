# S3 Compatibility Roadmap

This document outlines what's needed to make s3ish fully S3-compatible for single-host vertical scaling.

## Phase 1: Core S3 API (Critical for Basic Compatibility)

### 1.1 AWS Signature V4 Authentication
**Current:** Simple header-based auth (`x-access-key`, `x-secret-key`)
**Needed:** Full AWS SigV4 implementation

```rust
// Add dependencies to Cargo.toml
aws-sigv4 = "1.2"
aws-credential-types = "1.2"
sha2 = "0.10"
hex = "0.4"
hmac = "0.12"

// Implementation tasks:
- [ ] Parse Authorization header (AWS4-HMAC-SHA256 format)
- [ ] Extract signature components (access key, date, region, service)
- [ ] Calculate canonical request
- [ ] Calculate string to sign
- [ ] Verify HMAC-SHA256 signature
- [ ] Support query string authentication (pre-signed URLs)
- [ ] Handle credential scope validation
```

**Complexity:** High (3-5 days)
**Impact:** Required for S3 clients to connect

### 1.2 XML Response Format
**Current:** JSON responses
**Needed:** S3-compliant XML

```rust
// Add dependency
quick-xml = "0.31"
serde-xml-rs = "0.6"

// Implementation tasks:
- [ ] ListBucketResult XML format
- [ ] Error response XML format
- [ ] CreateBucketConfiguration
- [ ] CompleteMultipartUploadResult
- [ ] Custom XML serialization for S3 schemas
```

**Example XML Response:**
```xml
<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>bucket</Name>
    <Prefix>photos/</Prefix>
    <MaxKeys>1000</MaxKeys>
    <IsTruncated>false</IsTruncated>
    <Contents>
        <Key>photos/2024/image.jpg</Key>
        <LastModified>2024-01-15T10:30:00.000Z</LastModified>
        <ETag>"d41d8cd98f00b204e9800998ecf8427e"</ETag>
        <Size>12345</Size>
        <StorageClass>STANDARD</StorageClass>
    </Contents>
</ListBucketResult>
```

**Complexity:** Medium (2-3 days)
**Impact:** Required for S3 clients to parse responses

### 1.3 S3-Compatible Endpoints
**Current:** Custom REST endpoints
**Needed:** S3 path-style and virtual-hosted-style

```rust
// Path-style (host/bucket/key)
GET    /{bucket}/{key}           // GetObject
PUT    /{bucket}/{key}           // PutObject
DELETE /{bucket}/{key}           // DeleteObject
HEAD   /{bucket}/{key}           // HeadObject
GET    /{bucket}/               // ListObjects
PUT    /{bucket}/               // CreateBucket
DELETE /{bucket}/               // DeleteBucket
GET    /                        // ListBuckets

// Query parameters for operations
POST   /{bucket}/{key}?uploads                    // InitiateMultipartUpload
PUT    /{bucket}/{key}?uploadId=X&partNumber=N   // UploadPart
POST   /{bucket}/{key}?uploadId=X                // CompleteMultipartUpload
DELETE /{bucket}/{key}?uploadId=X                // AbortMultipartUpload
POST   /{bucket}/?delete                         // DeleteObjects (multi-delete)
```

**Implementation tasks:**
- [ ] Update router to match S3 endpoint patterns
- [ ] Support query parameter routing
- [ ] Implement ListBuckets operation
- [ ] Support both path-style and virtual-hosted-style URLs
- [ ] Handle content-type for each operation

**Complexity:** Medium (2-3 days)
**Impact:** Required for AWS CLI/SDKs

### 1.4 S3 Headers Support
**Current:** Basic content-type, etag
**Needed:** Full S3 header suite

```rust
// Request headers to support:
- x-amz-content-sha256       // Payload hash
- x-amz-date                 // Request timestamp
- x-amz-security-token       // STS token (optional)
- x-amz-meta-*               // User metadata
- x-amz-storage-class        // Storage class
- x-amz-server-side-encryption  // Encryption (can fake)
- Content-MD5                // Integrity check
- If-Match, If-None-Match    // Conditional requests
- Range                      // Partial downloads

// Response headers to return:
- x-amz-request-id           // Unique request ID
- x-amz-id-2                 // Extended request ID
- x-amz-version-id           // Object version (if versioning)
- ETag                       // MD5 of content
- Last-Modified              // Timestamp
- Content-Type               // MIME type
- Content-Length             // Size
- Accept-Ranges: bytes       // Range support
```

**Complexity:** Low-Medium (1-2 days)
**Impact:** Required for proper client operation

## Phase 2: Persistent Storage (Critical for Production)

### 2.1 File System Backend
**Current:** In-memory BTreeMap
**Needed:** Disk-based storage

```rust
pub struct FileSystemStorage {
    base_path: PathBuf,           // e.g., /var/lib/s3ish/data
    metadata_db: sled::Db,        // Embedded KV store for metadata
}

// Directory structure:
// /var/lib/s3ish/
//   data/
//     {bucket}/
//       {hash-prefix}/
//         {object-hash}           // Actual data (content-addressed)
//   metadata/                     // sled database
//     buckets/                    // Bucket metadata
//     objects/                    // Object metadata
//     multipart/                  // In-progress uploads

// Object metadata (stored in sled):
struct ObjectMetadata {
    bucket: String,
    key: String,
    size: u64,
    etag: String,
    content_type: String,
    last_modified: i64,
    user_metadata: HashMap<String, String>,
    storage_hash: String,          // Content-addressed hash
    parts: Option<Vec<PartInfo>>,  // For multipart uploads
}
```

**Implementation tasks:**
- [ ] Add sled dependency for metadata storage
- [ ] Implement FileSystemStorage backend
- [ ] Content-addressed storage (dedupe identical objects)
- [ ] Atomic file operations (temp file + rename)
- [ ] Garbage collection for orphaned data
- [ ] Directory sharding (avoid too many files in one dir)
- [ ] Fsync for durability guarantees

**Complexity:** Medium-High (4-5 days)
**Impact:** Required for data persistence

**Dependencies:**
```toml
sled = "0.34"           # Embedded KV database
tempfile = "3.8"        # Atomic file operations
walkdir = "2.4"         # Directory traversal
```

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

## Phase 3: Multipart Upload (Important for Large Files)

### 3.1 Multipart Upload State Management
```rust
struct MultipartUpload {
    upload_id: String,
    bucket: String,
    key: String,
    initiated: i64,
    parts: BTreeMap<u32, PartInfo>,  // part_number -> PartInfo
}

struct PartInfo {
    part_number: u32,
    etag: String,
    size: u64,
    storage_path: PathBuf,
}
```

**Implementation tasks:**
- [ ] Generate unique upload IDs
- [ ] Store parts temporarily during upload
- [ ] Validate part ETags on completion
- [ ] Combine parts into final object
- [ ] Clean up aborted/expired uploads
- [ ] Handle concurrent part uploads

**Complexity:** Medium-High (3-4 days)
**Impact:** Required for files > 5GB, recommended for all large files

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
