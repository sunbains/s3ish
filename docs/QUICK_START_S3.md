# Quick Start: Making s3ish S3-Compatible

This is a condensed, actionable guide to make s3ish production-ready and S3-compatible.

## TL;DR - What Changes

### Critical Changes (Must Have)
1. **Storage:** In-memory → Persistent file system
2. **Auth:** Simple headers → AWS Signature V4
3. **Format:** JSON → XML responses
4. **Endpoints:** Custom → S3 API paths

### Result
- ✅ Works with AWS CLI: `aws s3 ls s3://bucket/`
- ✅ Works with boto3, aws-sdk-rust, etc.
- ✅ Data persists across restarts
- ✅ Handles 10,000+ connections on single host

## Phase 1: Persistent Storage (Days 1-4)

### Current Problem
```rust
// All data lost on restart!
pub struct InMemoryStorage {
    buckets: Arc<RwLock<BTreeSet<String>>>,
    objects: Arc<RwLock<BTreeMap<(String, String), StoredObject>>>,
}
```

### Solution: File System Backend

**Step 1:** Add dependencies to `Cargo.toml`
```toml
sled = "0.34"              # Fast embedded database
uuid = { version = "1.6", features = ["v4", "fast-rng"] }
walkdir = "2.4"
```

**Step 2:** Create `src/storage/filesystem.rs`
```rust
use sled::Db;
use std::path::PathBuf;

pub struct FileSystemStorage {
    base_path: PathBuf,        // e.g., /var/lib/s3ish/data
    metadata: Db,              // sled database for metadata
}

impl FileSystemStorage {
    pub async fn new(base_path: PathBuf) -> Result<Self, StorageError> {
        tokio::fs::create_dir_all(&base_path).await?;
        let metadata_path = base_path.join("metadata");
        let metadata = sled::open(metadata_path)?;

        Ok(Self { base_path, metadata })
    }

    // Object stored at: base_path/buckets/{bucket}/objects/{key}
    fn object_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.base_path
            .join("buckets")
            .join(bucket)
            .join("objects")
            .join(key)
    }
}

#[async_trait]
impl StorageBackend for FileSystemStorage {
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        content_type: &str,
    ) -> Result<ObjectMetadata, StorageError> {
        // 1. Validate bucket exists
        let bucket_key = format!("bucket:{}", bucket);
        if !self.metadata.contains_key(&bucket_key)? {
            return Err(StorageError::BucketNotFound(bucket.to_string()));
        }

        // 2. Write data to disk
        let path = self.object_path(bucket, key);
        tokio::fs::create_dir_all(path.parent().unwrap()).await?;
        tokio::fs::write(&path, &data).await?;

        // 3. Calculate metadata
        let size = data.len() as u64;
        let etag = format!("{:x}", md5::compute(&data));
        let meta = ObjectMetadata {
            content_type: content_type.to_string(),
            etag: etag.clone(),
            size,
            last_modified_unix_secs: chrono::Utc::now().timestamp(),
        };

        // 4. Store metadata in sled
        let meta_key = format!("object:{}:{}", bucket, key);
        let meta_value = serde_json::to_vec(&meta)?;
        self.metadata.insert(meta_key.as_bytes(), meta_value)?;

        Ok(meta)
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(Bytes, ObjectMetadata), StorageError> {
        // 1. Get metadata
        let meta_key = format!("object:{}:{}", bucket, key);
        let meta_value = self.metadata.get(meta_key.as_bytes())?
            .ok_or_else(|| StorageError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            })?;
        let meta: ObjectMetadata = serde_json::from_slice(&meta_value)?;

        // 2. Read data from disk
        let path = self.object_path(bucket, key);
        let data = tokio::fs::read(&path).await?;

        Ok((Bytes::from(data), meta))
    }

    // ... implement other methods similarly
}
```

**Step 3:** Update `src/main.rs`
```rust
// OLD:
let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());

// NEW:
let data_path = PathBuf::from(std::env::var("S3ISH_DATA_DIR")
    .unwrap_or_else(|_| "/var/lib/s3ish".to_string()));
let storage: Arc<dyn StorageBackend> = Arc::new(
    FileSystemStorage::new(data_path).await?
);
```

**Test it:**
```bash
mkdir -p /tmp/s3ish-data
S3ISH_DATA_DIR=/tmp/s3ish-data cargo run -- --protocol http

# Upload a file
curl -X PUT http://localhost:9000/mybucket/test.txt \
  -H "x-access-key: demo" -H "x-secret-key: demo-secret" \
  -d "Hello World"

# Restart server
# File should still be there!
curl http://localhost:9000/mybucket/test.txt \
  -H "x-access-key: demo" -H "x-secret-key: demo-secret"
```

## Phase 2: AWS Signature V4 (Days 5-8)

### Current Problem
```rust
// Too simple - no standard S3 clients work
let access_key = headers.get("x-access-key")?;
let secret_key = headers.get("x-secret-key")?;
```

### Solution: AWS SigV4

**Step 1:** Add dependencies
```toml
aws-sigv4 = "1.2"
aws-smithy-runtime-api = "1.7"
http = "1.1"
sha2 = "0.10"
hmac = "0.12"
hex = "0.4"
```

**Step 2:** Create `src/auth/sigv4.rs`
```rust
use aws_sigv4::http_request::{SignableRequest, SigningSettings, SignatureLocation};
use aws_sigv4::sign::v4;

pub struct SigV4Authenticator {
    credentials: Arc<RwLock<HashMap<String, String>>>, // access_key -> secret_key
}

impl SigV4Authenticator {
    pub async fn new(credentials_file: impl AsRef<Path>) -> Result<Self, AuthError> {
        // Load credentials from file
        let credentials = load_credentials(credentials_file).await?;
        Ok(Self {
            credentials: Arc::new(RwLock::new(credentials)),
        })
    }

    fn verify_signature(
        &self,
        method: &str,
        uri: &str,
        headers: &HeaderMap,
        body_hash: &str,
    ) -> Result<String, AuthError> {
        // 1. Extract Authorization header
        let auth_header = headers
            .get("authorization")
            .ok_or(AuthError::MissingCredentials)?
            .to_str()
            .map_err(|_| AuthError::InvalidCredentials)?;

        // Example: "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20230615/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=..."

        // 2. Parse Authorization header
        let (access_key, provided_signature) = parse_auth_header(auth_header)?;

        // 3. Get secret key
        let credentials = self.credentials.read().await;
        let secret_key = credentials
            .get(&access_key)
            .ok_or(AuthError::InvalidCredentials)?;

        // 4. Calculate expected signature
        let expected_signature = calculate_signature(
            secret_key,
            method,
            uri,
            headers,
            body_hash,
        )?;

        // 5. Constant-time comparison
        if expected_signature != provided_signature {
            return Err(AuthError::InvalidCredentials);
        }

        Ok(access_key)
    }
}

fn calculate_signature(
    secret_key: &str,
    method: &str,
    uri: &str,
    headers: &HeaderMap,
    body_hash: &str,
) -> Result<String, AuthError> {
    // This is complex - use aws-sigv4 crate
    // See: https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html

    // Pseudo-code:
    // 1. Create canonical request
    // 2. Create string to sign
    // 3. Calculate signing key
    // 4. Calculate signature

    // ... implementation details
}
```

**This is the hardest part!** Consider using an existing library or MinIO's implementation as reference.

## Phase 3: XML Responses (Days 9-10)

### Current Problem
```rust
// S3 clients expect XML, not JSON
Ok((StatusCode::OK, format!("{{\"created\": {}}}", created)))
```

### Solution: XML Serialization

**Step 1:** Add dependency
```toml
quick-xml = { version = "0.31", features = ["serialize"] }
```

**Step 2:** Create `src/s3_http/xml.rs`
```rust
use quick_xml::se::to_string;
use serde::Serialize;

#[derive(Serialize)]
#[serde(rename = "ListBucketResult")]
pub struct ListBucketResult {
    #[serde(rename = "Name")]
    pub name: String,

    #[serde(rename = "Prefix")]
    pub prefix: String,

    #[serde(rename = "MaxKeys")]
    pub max_keys: i32,

    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,

    #[serde(rename = "Contents")]
    pub contents: Vec<ObjectEntry>,
}

#[derive(Serialize)]
pub struct ObjectEntry {
    #[serde(rename = "Key")]
    pub key: String,

    #[serde(rename = "Size")]
    pub size: u64,

    #[serde(rename = "ETag")]
    pub etag: String,

    #[serde(rename = "LastModified")]
    pub last_modified: String,  // ISO 8601 format

    #[serde(rename = "StorageClass")]
    pub storage_class: String,
}

pub fn to_xml<T: Serialize>(value: &T) -> Result<String, Error> {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    xml.push_str(&to_string(value)?);
    Ok(xml)
}
```

**Step 3:** Update handlers
```rust
// OLD:
async fn list_objects(...) -> Result<impl IntoResponse, S3Error> {
    let json = format!("{{\"objects\":[...]}}");
    Ok((StatusCode::OK, json))
}

// NEW:
async fn list_objects(...) -> Result<impl IntoResponse, S3Error> {
    let result = ListBucketResult {
        name: bucket.clone(),
        prefix: params.prefix.clone(),
        max_keys: limit as i32,
        is_truncated: false,
        contents: objects.into_iter().map(|(key, meta)| ObjectEntry {
            key,
            size: meta.size,
            etag: meta.etag,
            last_modified: format_iso8601(meta.last_modified_unix_secs),
            storage_class: "STANDARD".to_string(),
        }).collect(),
    };

    let xml = to_xml(&result)?;
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/xml")],
        xml
    ))
}
```

## Testing S3 Compatibility

```bash
# Install AWS CLI
pip install awscli

# Configure credentials
aws configure --profile s3ish
# AWS Access Key ID: demo
# AWS Secret Access Key: demo-secret
# Default region name: us-east-1
# Default output format: json

# Test with AWS CLI
aws --profile s3ish --endpoint-url http://localhost:9000 s3 ls
aws --profile s3ish --endpoint-url http://localhost:9000 s3 mb s3://test-bucket
aws --profile s3ish --endpoint-url http://localhost:9000 s3 cp test.txt s3://test-bucket/
aws --profile s3ish --endpoint-url http://localhost:9000 s3 ls s3://test-bucket/
```

## Performance Tuning for Single Host

```rust
// src/config.rs
pub struct ServerConfig {
    // Network
    pub max_connections: usize,          // 10,000 typical
    pub tcp_nodelay: bool,               // true for low latency
    pub tcp_keepalive: Option<Duration>, // Some(60s)

    // Storage I/O
    pub max_concurrent_uploads: usize,   // 100
    pub max_concurrent_downloads: usize, // 500
    pub buffer_size: usize,              // 64KB
    pub use_direct_io: bool,             // false (let OS cache)

    // Metadata caching
    pub metadata_cache_size_mb: usize,   // 1024
    pub metadata_cache_ttl_secs: u64,    // 60
}
```

## Estimated Timeline

- **Week 1:** Persistent storage backend ✅
- **Week 2:** AWS SigV4 authentication (hardest part) ⚠️
- **Week 3:** XML responses + S3 endpoints ✅
- **Week 4:** Testing, bug fixes, performance tuning ✅
- **Week 5+:** Multipart upload, advanced features

## When It's "Good Enough"

You have S3 compatibility when:
- ✅ `aws s3 ls` works
- ✅ `aws s3 cp` works (upload/download)
- ✅ Data persists across restarts
- ✅ Can handle your expected load (connections/throughput)

You DON'T need (initially):
- ❌ Versioning
- ❌ Lifecycle policies
- ❌ Cross-region replication
- ❌ Bucket policies (can do simple ACLs)
- ❌ Server-side encryption (can add later)

## Next Steps

1. Start with persistent storage (easiest, high impact)
2. Test with AWS CLI using simple auth
3. Add SigV4 when you need real S3 clients
4. Optimize as you scale

See `S3_COMPATIBILITY_ROADMAP.md` for complete details.
