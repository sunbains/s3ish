## Overview

This project exposes S3-style HTTP endpoints (path-style) and a gRPC service. It supports SigV4 and header-based auth, XML responses, multipart uploads, range requests, and CopyObject. Storage backends include in-memory and filesystem (selectable via `config.toml`).

## Authentication

- **Header auth (simple)**: `x-access-key`, `x-secret-key`
- **SigV4**: AWS Signature V4 headers (`Authorization`, `x-amz-date`, `x-amz-content-sha256`, etc.)

Requests without valid credentials return:
- HTTP 403 with XML `<Code>AccessDenied</Code>`

## HTTP Endpoints (path-style)

- `PUT /{bucket}` — Create bucket  
  - Errors: `BucketAlreadyExists` (409), `InvalidLocationConstraint` (400)
- `DELETE /{bucket}` — Delete bucket  
  - Errors: `BucketNotFound` (404), `BucketNotEmpty` (409)
- `GET /` — List buckets
- `GET|HEAD /{bucket}` — Head bucket / list objects (`max-keys`, `prefix`)
  - Errors: `NoSuchBucket` (404)
- `PUT /{bucket}/{key}` — Put object (with `Content-Type`, optional `x-amz-meta-*`)
  - Errors: `NoSuchBucket` (404), `InvalidArgument` (400)
- `GET|HEAD /{bucket}/{key}` — Get/Head object (supports `Range`, `If-None-Match`)
  - Errors: `NoSuchBucket` (404), `NoSuchKey` (404), `InvalidRange` (416)
- `DELETE /{bucket}/{key}` — Delete object  
  - Errors: `NoSuchBucket` (404), `NoSuchKey` (404)
- **CopyObject**: `PATCH /{bucket}/{key}` with `x-amz-copy-source: /src_bucket/src_key`
  - Optional conditionals: `x-amz-copy-source-if-match`, `x-amz-copy-source-if-none-match`, `If-Match`, `If-None-Match`
  - Metadata directive: `x-amz-metadata-directive: COPY|REPLACE` (+ new `Content-Type` / `x-amz-meta-*` on REPLACE)
  - Errors: `InvalidArgument` (missing/invalid source), `PreconditionFailed` (ETag conditions), `NoSuchBucket`/`NoSuchKey`

### Multipart Upload
- Initiate: `POST /{bucket}/{key}?uploads`
- Upload part: `PUT /{bucket}/{key}?uploadId=ID&partNumber=N`
- Complete: `POST /{bucket}/{key}?uploadId=ID` with `<CompleteMultipartUpload>` XML
- Abort: `DELETE /{bucket}/{key}?uploadId=ID`
- Errors: `NoSuchUpload` (404), `InvalidPart` (400), `NoSuchBucket`/`NoSuchKey`

### Object Versioning
S3-compatible object versioning allows multiple versions of the same object to coexist. When enabled, PUT operations create new versions instead of overwriting, and DELETE operations create delete markers.

**Bucket Versioning Status**:
- Get versioning: `GET /{bucket}?versioning`
- Enable versioning: `PUT /{bucket}?versioning` with XML body:
  ```xml
  <VersioningConfiguration>
    <Status>Enabled</Status>
  </VersioningConfiguration>
  ```
- Suspend versioning: `PUT /{bucket}?versioning` with `<Status>Suspended</Status>`

**Versioned Object Operations**:
- Put object (creates new version): `PUT /{bucket}/{key}` — Returns `x-amz-version-id` header
- Get specific version: `GET /{bucket}/{key}?versionId=VERSION_ID`
- Head specific version: `HEAD /{bucket}/{key}?versionId=VERSION_ID`
- Delete specific version: `DELETE /{bucket}/{key}?versionId=VERSION_ID`
- List all versions: `GET /{bucket}?versions` — Returns `<ListVersionsResult>` XML

**Versioning Behavior**:
- **Unversioned buckets**: PUT overwrites, DELETE permanently removes
- **Versioned buckets**: PUT creates new version with `version_id`, DELETE creates delete marker
- Delete markers: Special version indicating object is deleted (not returned by GET)
- Latest version: Marked with `is_latest=true`, returned by GET/HEAD/LIST without version ID

**Example: Enable versioning and create versions**:
```bash
# Enable versioning
curl -X PUT "http://localhost:9000/my-bucket?versioning" \
  -H "x-access-key: test" -H "x-secret-key: pass" \
  -d '<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>'

# Put object (version 1)
curl -X PUT http://localhost:9000/my-bucket/file.txt \
  -H "x-access-key: test" -H "x-secret-key: pass" \
  -d "content v1"

# Put object (version 2 - doesn't overwrite v1)
curl -X PUT http://localhost:9000/my-bucket/file.txt \
  -H "x-access-key: test" -H "x-secret-key: pass" \
  -d "content v2"

# Get latest version
curl http://localhost:9000/my-bucket/file.txt \
  -H "x-access-key: test" -H "x-secret-key: pass"

# List all versions
curl "http://localhost:9000/my-bucket?versions" \
  -H "x-access-key: test" -H "x-secret-key: pass"
```

### Pre-signed URLs
Pre-signed URLs allow temporary authenticated access to S3 objects without requiring AWS credentials in the request. URLs are signed using AWS Signature V4 and include an expiration time.

**Key Parameters (query string)**:
- `X-Amz-Algorithm`: Always `AWS4-HMAC-SHA256`
- `X-Amz-Credential`: `{access_key}/{date}/{region}/s3/aws4_request`
- `X-Amz-Date`: Timestamp in format `YYYYMMDDTHHMMSSZ`
- `X-Amz-Expires`: Duration in seconds (max 604800 = 7 days)
- `X-Amz-SignedHeaders`: Semicolon-separated list of headers (usually `host`)
- `X-Amz-Signature`: HMAC-SHA256 signature

**Supported Methods**: GET, PUT, DELETE, HEAD

**Features**:
- No authentication headers needed in the request
- Automatic expiration validation
- Full signature verification
- Compatible with AWS S3 pre-signed URL format

## Headers Returned

- `x-amz-request-id`, `x-amz-id-2`, `x-amz-bucket-region`
- Object responses: `ETag`, `Content-Type`, `Content-Length`, `Last-Modified`, `Accept-Ranges`
- User metadata echoed as `x-amz-meta-*`

## Error Codes (XML)

- `AccessDenied` (403)
- `NoSuchBucket` (404)
- `BucketNotEmpty` (409)
- `BucketAlreadyExists` (409)
- `NoSuchKey` (404)
- `NoSuchUpload` (404)
- `InvalidArgument` (400)
- `InvalidLocationConstraint` (400)
- `InvalidPart` (400)
- `InvalidRange` (416)
- `PreconditionFailed` (412)
- `InternalError` (500)

Example error body:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist.</Message>
  <Resource>/bucket/missing.txt</Resource>
  <RequestId>req-...</RequestId>
  <HostId>req-....host</HostId>
</Error>
```

## Quick Examples (HTTP)

Create bucket:
```bash
curl -X PUT http://localhost:9000/my-bucket \
  -H "x-access-key: test" -H "x-secret-key: pass"
```

Put object with metadata:
```bash
curl -X PUT http://localhost:9000/my-bucket/notes.txt \
  -H "x-access-key: test" -H "x-secret-key: pass" \
  -H "Content-Type: text/plain" \
  -H "x-amz-meta-color: blue" \
  --data 'hello'
```

Range GET:
```bash
curl -H "x-access-key: test" -H "x-secret-key: pass" \
  -H "Range: bytes=0-3" \
  http://localhost:9000/my-bucket/notes.txt
```

CopyObject:
```bash
curl -X PATCH http://localhost:9000/my-bucket/copy.txt \
  -H "x-access-key: test" -H "x-secret-key: pass" \
  -H "x-amz-copy-source: /my-bucket/notes.txt"
```

Multipart upload:
```bash
# Initiate
UPLOAD_ID=$(curl -s -X POST "http://localhost:9000/bucket/big.dat?uploads" \
  -H "x-access-key: test" -H "x-secret-key: pass" | \
  sed -n 's:.*<UploadId>\\(.*\\)</UploadId>.*:\\1:p')

# Upload parts
curl -X PUT "http://localhost:9000/bucket/big.dat?uploadId=$UPLOAD_ID&partNumber=1" \
  -H "x-access-key: test" -H "x-secret-key: pass" --data 'part1'
curl -X PUT "http://localhost:9000/bucket/big.dat?uploadId=$UPLOAD_ID&partNumber=2" \
  -H "x-access-key: test" -H "x-secret-key: pass" --data 'part2'

# Complete
cat > complete.xml <<'XML'
<CompleteMultipartUpload>
  <Part><PartNumber>1</PartNumber><ETag>"5d41402abc4b2a76b9719d911017c592"</ETag></Part>
  <Part><PartNumber>2</PartNumber><ETag>"7d793037a0760186574b0282f2f435e7"</ETag></Part>
</CompleteMultipartUpload>
XML
curl -X POST "http://localhost:9000/bucket/big.dat?uploadId=$UPLOAD_ID" \
  -H "x-access-key: test" -H "x-secret-key: pass" \
  --data-binary @complete.xml
```

Pre-signed URLs (programmatic generation):
```rust
use s3ish::s3_http::generate_presigned_url;

// Generate a pre-signed URL for GET (1 hour expiration)
let url = generate_presigned_url(
    "http://localhost:9000",
    "my-bucket",
    "my-file.txt",
    "GET",
    3600,  // expires in 1 hour
    "demo",
    "demo-secret",
    "us-east-1",
    &["host"],
)?;

// Use the URL - no auth headers needed!
curl "${url}"
```

Pre-signed URL for upload:
```rust
// Generate a pre-signed URL for PUT
let upload_url = generate_presigned_url(
    "http://localhost:9000",
    "my-bucket",
    "upload.txt",
    "PUT",
    3600,
    "demo",
    "demo-secret",
    "us-east-1",
    &["host"],
)?;

// Anyone with this URL can upload within 1 hour
curl -X PUT "${upload_url}" --data "uploaded content"
```

## Backends
- **InMemory**: default, keeps data in RAM with simulated erasure stripes.
- **FileStorage**: set in `config.toml` (`[storage] backend = "file"; path = "./data"`). Stores shards+parity to disk with JSON sidecars; supports multipart, copy, range.

## gRPC (ObjectStore service)

Methods: `CreateBucket`, `DeleteBucket`, `PutObject`, `GetObject`, `DeleteObject`, `ListObjects`.
Metadata headers: `x-access-key`, `x-secret-key`.

Example tonic client snippet:
```rust
let mut client = ObjectStoreClient::connect("http://localhost:50051").await?;
let mut req = tonic::Request::new(CreateBucketRequest {
    bucket: Some(BucketName { name: "bucket".into() }),
});
req.metadata_mut().insert("x-access-key", "test".parse().unwrap());
req.metadata_mut().insert("x-secret-key", "pass".parse().unwrap());
client.create_bucket(req).await?;
```
