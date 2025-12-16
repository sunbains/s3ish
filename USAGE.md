# Usage Guide

This guide shows how to use both the gRPC and S3 HTTP handlers.

## Running the Server

### gRPC Mode (Default)

```bash
# Create credentials file
echo "admin:secret123" > creds.txt

# Run the server
cargo run
```

### S3 HTTP Mode

```bash
# Create credentials file
echo "admin:secret123" > creds.txt

# Run with HTTP protocol
S3ISH_PROTOCOL=http cargo run
```

## Using the S3 HTTP API

### Authentication

All requests require authentication headers:
- `x-access-key` or `x-amz-access-key`: Your access key
- `x-secret-key` or `x-amz-secret-key`: Your secret key

### Bucket Operations

#### Create Bucket

```bash
curl -X PUT http://localhost:9000/my-bucket \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123"

# Response: {"created": true}
```

#### Delete Bucket

```bash
curl -X DELETE http://localhost:9000/my-bucket \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123"

# Response: {"deleted": true}
```

### Object Operations

#### Put Object

```bash
curl -X PUT http://localhost:9000/my-bucket/hello.txt \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123" \
  -H "content-type: text/plain" \
  -d "Hello, World!"

# Response: {"etag": "65a8e27d8879283831b664bd8b7f0ad4", "size": 13}
```

#### Get Object

```bash
curl http://localhost:9000/my-bucket/hello.txt \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123"

# Response: Hello, World!
```

#### Delete Object

```bash
curl -X DELETE http://localhost:9000/my-bucket/hello.txt \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123"

# Response: {"deleted": true}
```

#### List Objects

```bash
# List all objects in bucket
curl "http://localhost:9000/my-bucket/" \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123"

# List with prefix filter
curl "http://localhost:9000/my-bucket/?prefix=photos/&max-keys=10" \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123"

# Response: {"objects":[{"key":"photos/1.jpg","size":1024,"etag":"..."}]}
```

## Complete Example Workflow

```bash
# 1. Start the server in HTTP mode
S3ISH_PROTOCOL=http cargo run &
SERVER_PID=$!

# Wait for server to start
sleep 2

# 2. Create a bucket
curl -X PUT http://localhost:9000/my-data \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123"

# 3. Upload some files
echo "Document 1" | curl -X PUT http://localhost:9000/my-data/doc1.txt \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123" \
  -H "content-type: text/plain" \
  --data-binary @-

echo "Document 2" | curl -X PUT http://localhost:9000/my-data/doc2.txt \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123" \
  -H "content-type: text/plain" \
  --data-binary @-

# 4. List all objects
curl "http://localhost:9000/my-data/" \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123"

# 5. Download a file
curl http://localhost:9000/my-data/doc1.txt \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123"

# 6. Delete a file
curl -X DELETE http://localhost:9000/my-data/doc1.txt \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123"

# 7. List again (should only show doc2.txt)
curl "http://localhost:9000/my-data/" \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123"

# Cleanup
kill $SERVER_PID
```

## Using with gRPC

For gRPC usage, you'll need a gRPC client. Here's an example using grpcurl:

```bash
# Start gRPC server
cargo run &

# Create bucket
grpcurl -plaintext \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123" \
  -d '{"bucket": {"name": "test-bucket"}}' \
  localhost:9000 \
  mems3.v1.ObjectStore/CreateBucket

# Put object
echo '{"object": {"bucket": "test-bucket", "key": "hello.txt"}, "data": "'$(echo -n "Hello" | base64)'", "content_type": "text/plain"}' | \
grpcurl -plaintext \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123" \
  -d @ \
  localhost:9000 \
  mems3.v1.ObjectStore/PutObject
```

## Configuration

The server reads configuration from `config.toml`:

```toml
listen_addr = "127.0.0.1:9000"
auth_file = "creds.txt"
```

You can specify a different config file:

```bash
S3ISH_CONFIG=/path/to/config.toml cargo run
```

## Credentials File Format

The `creds.txt` file contains one credential per line in the format:

```
access_key:secret_key
```

Example:
```
admin:secret123
user1:password1
user2:password2
# This is a comment
alice:alicepass
```

Features:
- Lines starting with `#` are comments
- Blank lines are ignored
- Whitespace around keys is trimmed
- The file is loaded on startup
- In production, you'd implement a database-backed authenticator

## Environment Variables

- `S3ISH_CONFIG`: Path to config file (default: `config.toml`)
- `S3ISH_PROTOCOL`: Protocol to use (`grpc` or `http`/`s3`, default: `grpc`)
- `RUST_LOG`: Logging level (e.g., `info`, `debug`, `trace`)

## Error Responses

The S3 HTTP API returns appropriate HTTP status codes:

- `200 OK`: Success
- `400 Bad Request`: Invalid input (e.g., empty bucket name)
- `401 Unauthorized`: Missing credentials
- `403 Forbidden`: Invalid credentials
- `404 Not Found`: Bucket or object not found
- `409 Conflict`: Bucket not empty (on delete)
- `500 Internal Server Error`: Server error

## Performance Tips

1. **Connection Pooling**: Reuse HTTP connections for better performance
2. **Batch Operations**: Upload multiple small files in parallel
3. **Content-Type**: Always set appropriate content-type headers
4. **Compression**: The server doesn't compress; use client-side compression if needed

## Comparison: gRPC vs HTTP

| Feature | gRPC | S3 HTTP |
|---------|------|---------|
| **Protocol** | HTTP/2 + Protobuf | HTTP/1.1 + JSON |
| **Performance** | Higher (binary protocol) | Good (text-based) |
| **Compatibility** | Requires gRPC client | Works with curl, browsers |
| **Streaming** | Bidirectional | Request/Response only |
| **Use Case** | Internal services | Public APIs, CLIs |
| **Tools** | grpcurl, BloomRPC | curl, wget, Postman |

## Advanced Features

### Multipart Uploads

For uploading large objects in parts:

```bash
# Initiate multipart upload
UPLOAD_ID=$(curl -s -X POST "http://localhost:9000/my-bucket/largefile.bin?uploads" \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123" | grep -oP '(?<=<UploadId>)[^<]+')

# Upload part 1
curl -X PUT "http://localhost:9000/my-bucket/largefile.bin?uploadId=$UPLOAD_ID&partNumber=1" \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123" \
  --data-binary @part1.bin

# Upload part 2
curl -X PUT "http://localhost:9000/my-bucket/largefile.bin?uploadId=$UPLOAD_ID&partNumber=2" \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123" \
  --data-binary @part2.bin

# Complete multipart upload
curl -X POST "http://localhost:9000/my-bucket/largefile.bin?uploadId=$UPLOAD_ID" \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123" \
  -H "Content-Type: application/xml" \
  -d '<CompleteMultipartUpload>
        <Part><PartNumber>1</PartNumber><ETag>"etag1"</ETag></Part>
        <Part><PartNumber>2</PartNumber><ETag>"etag2"</ETag></Part>
      </CompleteMultipartUpload>'
```

### CopyObject

Copy objects within or across buckets:

```bash
# Copy object
curl -X PATCH http://localhost:9000/dest-bucket/newfile.txt \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123" \
  -H "x-amz-copy-source: /source-bucket/original.txt"

# Copy with metadata replacement
curl -X PATCH http://localhost:9000/dest-bucket/newfile.txt \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123" \
  -H "x-amz-copy-source: /source-bucket/original.txt" \
  -H "x-amz-metadata-directive: REPLACE" \
  -H "content-type: text/plain" \
  -H "x-amz-meta-version: 2.0"
```

### Range Requests

Download partial object content:

```bash
# Get first 100 bytes
curl http://localhost:9000/my-bucket/largefile.bin \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123" \
  -H "Range: bytes=0-99"
```

### AWS SigV4 Authentication

Use AWS Signature Version 4 for authentication:

```bash
# See QUICK_START_S3.md for examples using aws-cli and boto3
# SigV4 is automatically supported alongside header-based auth
```

### User Metadata

Store and retrieve custom metadata with objects:

```bash
# Upload with metadata
curl -X PUT http://localhost:9000/my-bucket/file.txt \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123" \
  -H "x-amz-meta-author: John Doe" \
  -H "x-amz-meta-department: Engineering" \
  -d "File content"

# Metadata is returned in GET/HEAD responses
curl -I http://localhost:9000/my-bucket/file.txt \
  -H "x-access-key: admin" \
  -H "x-secret-key: secret123"
```

## Observability

### Prometheus Metrics

```bash
# View all metrics
curl http://localhost:9000/_metrics

# Metrics include:
# - http_request_duration_seconds
# - storage_operation_duration_seconds
# - auth_duration_seconds
# - multipart_active_uploads
# - storage_bytes_total
# ... and 60+ more
```

### Health Checks

```bash
# Liveness probe
curl http://localhost:9000/_health

# Readiness probe (checks backends)
curl http://localhost:9000/_ready
```

### Structured Logging

```bash
# JSON logging for production
LOG_FORMAT=json RUST_LOG=info ./target/release/s3ish --protocol http

# Human-readable for development
LOG_FORMAT=human RUST_LOG=debug ./target/release/s3ish --protocol http
```

See [OBSERVABILITY.md](OBSERVABILITY.md) for detailed metrics documentation.

## Storage Backends

### In-Memory Storage (Default)

Fast ephemeral storage for development and testing.

### Filesystem Storage

Persistent storage with optional erasure coding:

```toml
# config.toml
[storage]
backend = "file"
path = "./s3-data"
erasure_coding = true
data_blocks = 2
parity_blocks = 1
```

## Configuration Options

See [API_USAGE.md](API_USAGE.md) for complete configuration reference.

## Next Steps

- Object versioning support
- Lifecycle policies
- Bucket policies and ACLs
- Server-side encryption
- Replication
