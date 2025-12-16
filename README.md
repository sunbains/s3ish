# s3ish

In-memory S3-like object store with pluggable gRPC and HTTP interfaces.

## Project Vision: S3 Tiered Storage Engine

This project is building toward a **log-authoritative tiered storage architecture** - a design that looks like an S3 proxy on the surface, but is fundamentally something different.

### The Core Principle

**The Raft log is the source of truth.**
**The Raft low-water mark becomes the S3 high-water mark.**

Unlike traditional S3 proxies where S3 is authoritative and the proxy adds caching, this architecture inverts the relationship:

```
           ┌──────────────────┐
           │   Raft Log       │
           │ (Source of Truth)│
           └────────┬─────────┘
                    |
                    v
        ┌────────────────────────────┐
        │ Local ARIES Storage Engine │
        │  - buffer pool             │
        │  - redo / undo             │
        │  - hot working set (~10%)  │
        └───────────┬────────────────┘
                    |
           WAL / segment shipping
                    |
                    v
        ┌─────────────────────────────┐
        │           S3                │
        │  immutable objects          │
        │  cold tier                  │
        └─────────────────────────────┘
```

### The Key Invariant

The entire system is defined by a single invariant:

**Raft Low-Water Mark (LWM) = S3 High-Water Mark (HWM)**

```
LSN timeline
│
├── ≤ S3_HWM   → safely stored in S3
├── ≤ Raft_LWM → applied everywhere
└── > S3_HWM   → may exist only in hot storage
```

### Why This Is Not an S3 Proxy

| Aspect | Traditional S3 Proxy | This Design |
|--------|---------------------|-------------|
| **Source of Truth** | S3 | Raft Log |
| **Local State** | Cache | ARIES Storage Engine |
| **S3 Role** | Authoritative | Derived, Monotonic Materialization |
| **Correctness** | Depends on cache coherence | Independent of S3 |
| **Writes** | Synchronously wait for S3 | S3 shipping is asynchronous |

### Relationship to ARIES

The local storage engine follows classic ARIES principles:
- Write-ahead logging
- Redo for durability
- Undo for rollback

The WAL has two consumers:
```
Raft Log Entry
     |
     +--> Local redo / undo
     |
     +--> Background S3 shipping
```

S3 objects are created via:
- Log segment sealing
- Compaction output
- Checkpoint materialization

### Failure and Recovery

**Crash Recovery:**
1. Replay Raft log
2. Recover hot tier
3. Fetch cold segments from S3 as needed

**Disk Loss:**
- Raft provides ordering
- S3 provides historical state
- Rehydration is deterministic

**S3 Lag:**
- Explicitly allowed
- Bounded by Raft LWM
- Never affects correctness

### Classification

This system belongs to a new class of storage systems:

**Log-authoritative, consensus-replicated storage engine with tiered persistence.**

Once you decide that the Raft log is the source of truth, everything else becomes an optimization. S3 stops being a cache target and becomes a materialized view of history.

---

## Current Status

The current implementation provides the S3-compatible foundation layer with pluggable storage backends. The log-authoritative ARIES-based storage engine is planned for future implementation.

**See [STORAGE_ENGINE_PLAN.md](docs/STORAGE_ENGINE_PLAN.md) for the detailed implementation plan.**

Key points about the planned architecture:
- **External orchestrator**: Consensus/Raft layer is handled externally and provides ordered log entries
- **Storage engine**: Receives sequentially-ordered log entries (by timestamp, LSN, HLC, or any monotonically increasing number) and maintains hot/cold tiers
- **ARIES recovery**: Write-ahead logging with redo/undo for durability
- **Asynchronous S3 shipping**: Background process ships sealed segments to S3
- **Bounded lag**: S3 HWM always ≤ orchestrator LWM

## Features

- **Dual Protocol Support**: gRPC and S3-compatible HTTP APIs
- **Pluggable Architecture**: Easy to swap authentication and storage backends
- **Multiple Storage Backends**: In-memory and filesystem-based storage with optional erasure coding
- **AWS SigV4 Authentication**: Full AWS Signature V4 support plus simple header-based auth
- **Pre-signed URLs**: Generate temporary authenticated URLs with expiration (up to 7 days)
- **S3-Compatible Operations**: PutObject, GetObject, DeleteObject, CopyObject, ListObjects
- **Multipart Uploads**: Complete multipart upload support for large objects
- **Range Requests**: Streaming support with HTTP range headers
- **Production-Ready Observability**:
  - Prometheus metrics (66+ metrics)
  - Structured logging (JSON/human-readable)
  - Health/readiness probes
  - Grafana dashboard
- **Comprehensive Tests**: 132+ tests covering all components

## Documentation

Comprehensive documentation is available in the [docs/](docs/) directory:

- **[API_USAGE.md](docs/API_USAGE.md)** - Complete API reference with request/response examples
- **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** - System architecture and design patterns
- **[FUZZING.md](docs/FUZZING.md)** - Fuzz testing guide and best practices
- **[OBSERVABILITY.md](docs/OBSERVABILITY.md)** - Metrics, logging, and monitoring
- **[QUICK_START_S3.md](docs/QUICK_START_S3.md)** - S3-compatible client examples
- **[S3_COMPATIBILITY_ROADMAP.md](docs/S3_COMPATIBILITY_ROADMAP.md)** - S3 feature implementation status
- **[STORAGE_ENGINE_PLAN.md](docs/STORAGE_ENGINE_PLAN.md)** - Future storage engine design
- **[Progress.md](docs/Progress.md)** - Development progress log

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

See [OBSERVABILITY.md](docs/OBSERVABILITY.md) for detailed metrics documentation and Grafana integration.

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

See [API_USAGE.md](docs/API_USAGE.md) for complete API documentation and [QUICK_START_S3.md](docs/QUICK_START_S3.md) for S3-compatible client examples.

## Architecture

The project uses a pluggable architecture where protocol handlers (gRPC, HTTP) share common authentication and storage backends through the `BaseHandler`. Key components:

- **Protocol Handlers**: HTTP (S3-compatible) and gRPC interfaces
- **Authentication**: File-based auth and AWS SigV4 signature verification
- **Storage Backends**: In-memory and filesystem with optional erasure coding
- **Observability**: Prometheus metrics, structured logging, and health checks

See [ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed documentation.

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

All 132+ tests pass covering authentication, storage, gRPC, HTTP handlers, multipart uploads, erasure coding, pre-signed URLs, and observability.

### Fuzz Testing

s3ish includes comprehensive fuzz testing to discover edge cases and security vulnerabilities:

```bash
# Install cargo-fuzz (one time)
cargo install cargo-fuzz

# Run fuzz tests
cargo fuzz run storage_backend -- -max_total_time=60
cargo fuzz run erasure_coding -- -max_total_time=60
cargo fuzz run sigv4_parsing -- -max_total_time=60
cargo fuzz run xml_parsing -- -max_total_time=60
```

See [FUZZING.md](docs/FUZZING.md) for detailed fuzzing documentation.

## License

MIT
