# s3ish

In-memory S3-like object store with pluggable gRPC and HTTP interfaces.

## Features

- **Dual Protocol Support**: gRPC and S3-compatible HTTP APIs
- **Pluggable Architecture**: Easy to swap authentication and storage backends
- **Multiple Storage Backends**: In-memory and filesystem-based storage with optional erasure coding
- **AWS SigV4 Authentication**: Full AWS Signature V4 support plus simple header-based auth
- **S3-Compatible Operations**: PutObject, GetObject, DeleteObject, CopyObject, ListObjects
- **Multipart Uploads**: Complete multipart upload support for large objects
- **Range Requests**: Streaming support with HTTP range headers
- **Production-Ready Observability**:
  - Prometheus metrics (66+ metrics)
  - Structured logging (JSON/human-readable)
  - Health/readiness probes
  - Grafana dashboard
- **Comprehensive Tests**: 126+ tests covering all components

## Quick Start

```bash
# Build the project
cargo build --release

# Create credentials file
echo "demo:demo-secret" > creds.txt

# Run with HTTP protocol (listens on 0.0.0.0:9000)
./target/release/s3ish --protocol http --listen 0.0.0.0:9000

# Run with gRPC protocol
./target/release/s3ish --protocol grpc --listen 0.0.0.0:50051

# Or run the automated demo script
./demo.sh
```

## Command Line Options

```
Usage: s3ish [OPTIONS]

Options:
  -l, --listen <LISTEN>        Address to listen on (e.g., 0.0.0.0:9000, 127.0.0.1:9000)
  -p, --protocol <PROTOCOL>    Protocol to use (grpc or http) [default: grpc]
  -c, --config <CONFIG>        Path to configuration file [default: config.toml]
  -a, --auth-file <AUTH_FILE>  Path to credentials file
  -h, --help                   Print help
```

## Usage Examples

### Create a bucket

```bash
curl -X PUT http://localhost:9000/my-bucket \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret"
```

### Upload an object

```bash
curl -X PUT http://localhost:9000/my-bucket/hello.txt \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret" \
  -H "content-type: text/plain" \
  -d "Hello, World!"
```

### Download an object

```bash
curl http://localhost:9000/my-bucket/hello.txt \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret"
```

### List objects

```bash
curl "http://localhost:9000/my-bucket/?prefix=&max-keys=100" \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret"
```

### Delete an object

```bash
curl -X DELETE http://localhost:9000/my-bucket/hello.txt \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret"
```

### Copy an object

```bash
curl -X PATCH http://localhost:9000/my-bucket/hello-copy.txt \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret" \
  -H "x-amz-copy-source: /my-bucket/hello.txt"
```

## Observability

s3ish includes comprehensive observability features for production deployments.

### Metrics and Health Endpoints

```bash
# Prometheus metrics
curl http://localhost:9000/_metrics

# Liveness probe (returns 200 if service is running)
curl http://localhost:9000/_health

# Readiness probe (returns 200 if backends are healthy)
curl http://localhost:9000/_ready
```

### Structured Logging

```bash
# JSON logging for production
LOG_FORMAT=json RUST_LOG=info ./target/release/s3ish

# Human-readable logging for development
LOG_FORMAT=human RUST_LOG=debug ./target/release/s3ish
```

See [OBSERVABILITY.md](OBSERVABILITY.md) for detailed metrics documentation and Grafana integration.

## Configuration

Create a `config.toml` file to configure storage backends and authentication:

```toml
listen_addr = "0.0.0.0:9000"
auth_file = "./creds.txt"

# Storage backend: "memory" or "file"
storage_backend = "memory"

# File storage options (when storage_backend = "file")
# storage_root = "/tmp/s3ish-data"
# enable_erasure_coding = false
# erasure_data_blocks = 2
# erasure_parity_blocks = 1
```

### Storage Backends

- **memory**: Fast in-memory storage (ephemeral)
- **file**: Persistent filesystem-based storage with optional erasure coding for data redundancy

See [API_USAGE.md](API_USAGE.md) for complete API documentation and [QUICK_START_S3.md](QUICK_START_S3.md) for S3-compatible client examples.

## Architecture

The project uses a pluggable architecture where protocol handlers (gRPC, HTTP) share common authentication and storage backends through the `BaseHandler`. Key components:

- **Protocol Handlers**: HTTP (S3-compatible) and gRPC interfaces
- **Authentication**: File-based auth and AWS SigV4 signature verification
- **Storage Backends**: In-memory and filesystem with optional erasure coding
- **Observability**: Prometheus metrics, structured logging, and health checks

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed documentation.

## Testing

```bash
# Run all tests
cargo test

# Run with output
cargo test -- --nocapture

# Run specific test suite
cargo test --test http_file_storage
cargo test --test observability_integration_test
```

All 126+ tests pass covering authentication, storage, gRPC, HTTP handlers, multipart uploads, erasure coding, and observability.

## License

MIT
