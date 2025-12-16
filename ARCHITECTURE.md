# Pluggable Architecture

This project implements a pluggable architecture that allows you to serve the same storage backend through different protocols.

## Architecture Overview

```
┌─────────────────────────────────────────────────┐
│                 Application                      │
│              (main.rs)                           │
└─────────────────┬───────────────────────────────┘
                  │
                  │ Creates BaseHandler
                  ▼
┌─────────────────────────────────────────────────┐
│            BaseHandler                           │
│  ┌──────────────┐    ┌──────────────┐          │
│  │ Authenticator│    │StorageBackend│          │
│  │    (trait)   │    │   (trait)    │          │
│  └──────────────┘    └──────────────┘          │
└─────────────────┬───────────────────┬───────────┘
                  │                   │
        ┌─────────┴─────────┐         │
        │                   │         │
        ▼                   ▼         │
┌──────────────┐   ┌──────────────┐  │
│ gRPC Handler │   │ S3 HTTP      │  │
│ (service/)   │   │ Handler      │  │
│              │   │ (s3_http.rs) │  │
└──────┬───────┘   └──────┬───────┘  │
       │                  │          │
       │ Uses             │ Uses     │
       └─────────┬────────┘          │
                 │ Shared Access     │
                 ▼                   ▼
    ┌─────────────────────────────────────┐
    │      Storage & Auth Backends        │
    │  - FileAuthenticator                │
    │  - InMemoryStorage / FileStorage    │
    │  - Observability (Metrics, Logs)    │
    │  (Pluggable implementations)        │
    └─────────────────────────────────────┘
```

## Core Components

### 1. BaseHandler (`src/handler.rs`)

The `BaseHandler` struct holds the common components used by all protocol handlers:

```rust
pub struct BaseHandler {
    pub auth: Arc<dyn Authenticator>,
    pub storage: Arc<dyn StorageBackend>,
}
```

**Benefits:**
- Single source of truth for auth and storage components
- Easy to swap implementations (e.g., FileAuthenticator → DatabaseAuthenticator)
- Shared across multiple protocol handlers

### 2. Protocol Handlers

Both protocol handlers wrap `BaseHandler` and implement their specific protocol logic:

#### gRPC Handler (`src/service/mod.rs`)
```rust
pub struct ObjectStoreService {
    handler: BaseHandler,
}
```

Implements the gRPC ObjectStore service using tonic.

#### S3 HTTP Handler (`src/s3_http.rs`)
```rust
pub struct S3HttpHandler {
    handler: Arc<BaseHandler>,
}
```

Implements S3-compatible REST API using Axum.

**Key Features:**
- `PUT /{bucket}` - Create bucket
- `DELETE /{bucket}` - Delete bucket
- `PUT /{bucket}/{key}` - Put object
- `GET /{bucket}/{key}` - Get object
- `DELETE /{bucket}/{key}` - Delete object
- `GET /{bucket}/?prefix=&max-keys=` - List objects

### 3. Connection Managers (`src/server/mod.rs`)

Connection managers abstract the transport layer:

```rust
#[async_trait]
pub trait ConnectionManager: Send + Sync + 'static {
    async fn serve(&self, addr: SocketAddr)
        -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}
```

**Implementations:**
- `GrpcConnectionManager` - Serves gRPC protocol using tonic
- `S3HttpConnectionManager` - Serves HTTP protocol using Axum

## Pluggability

### Switching Protocols

Set the `MEMS3_PROTOCOL` environment variable to choose the protocol:

```bash
# Run with gRPC (default)
cargo run

# Run with S3 HTTP
MEMS3_PROTOCOL=http cargo run

# Or
MEMS3_PROTOCOL=s3 cargo run
```

### Custom Authenticators

Implement the `Authenticator` trait:

```rust
#[async_trait]
pub trait Authenticator: Send + Sync + 'static {
    async fn authenticate(&self, req: &Request<()>)
        -> Result<AuthContext, AuthError>;
}
```

**Built-in implementations:**
- `FileAuthenticator` - File-based auth (access:secret per line)

**Example custom implementation:**
```rust
struct DatabaseAuthenticator {
    db_pool: DatabasePool,
}

#[async_trait]
impl Authenticator for DatabaseAuthenticator {
    async fn authenticate(&self, req: &Request<()>) -> Result<AuthContext, AuthError> {
        // Your custom logic here
    }
}
```

Then use it:
```rust
let auth: Arc<dyn Authenticator> = Arc::new(DatabaseAuthenticator::new(pool));
let handler = BaseHandler::new(auth, storage);
```

### Custom Storage Backends

Implement the `StorageBackend` trait:

```rust
#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    async fn create_bucket(&self, bucket: &str) -> Result<bool, StorageError>;
    async fn delete_bucket(&self, bucket: &str) -> Result<bool, StorageError>;
    async fn put_object(&self, bucket: &str, key: &str, data: Bytes,
                        content_type: &str) -> Result<ObjectMetadata, StorageError>;
    async fn get_object(&self, bucket: &str, key: &str)
        -> Result<(Bytes, ObjectMetadata), StorageError>;
    async fn delete_object(&self, bucket: &str, key: &str)
        -> Result<bool, StorageError>;
    async fn list_objects(&self, bucket: &str, prefix: &str, limit: usize)
        -> Result<Vec<(String, ObjectMetadata)>, StorageError>;
}
```

**Built-in implementations:**
- `InMemoryStorage` - In-memory storage using BTreeMap with optional simulated erasure coding
- `FileStorage` - Filesystem-based persistent storage with optional erasure coding (data shards + parity)

**Example custom implementation:**
```rust
struct S3Storage {
    s3_client: aws_sdk_s3::Client,
}

#[async_trait]
impl StorageBackend for S3Storage {
    async fn create_bucket(&self, bucket: &str) -> Result<bool, StorageError> {
        // Implement using AWS S3 SDK
    }
    // ... implement other methods
}
```

Then use it:
```rust
let storage: Arc<dyn StorageBackend> = Arc::new(S3Storage::new(client));
let handler = BaseHandler::new(auth, storage);
```

### Adding New Protocol Handlers

To add a new protocol (e.g., WebDAV, FTP, etc.):

1. Create a new module (e.g., `src/webdav.rs`)
2. Create a handler struct that wraps `BaseHandler`
3. Implement your protocol's endpoints/handlers
4. Create a `ConnectionManager` implementation
5. Update `main.rs` to support the new protocol

Example:
```rust
// src/webdav.rs
pub struct WebDavHandler {
    handler: Arc<BaseHandler>,
}

impl WebDavHandler {
    pub fn new(handler: BaseHandler) -> Self {
        Self { handler: Arc::new(handler) }
    }

    // Implement WebDAV protocol handlers
}

// src/server/mod.rs
pub struct WebDavConnectionManager {
    handler: WebDavHandler,
}

#[async_trait]
impl ConnectionManager for WebDavConnectionManager {
    async fn serve(&self, addr: SocketAddr) -> Result<...> {
        // Start WebDAV server
    }
}
```

## Observability

s3ish includes comprehensive observability features built into the architecture:

### Metrics Collection

All protocol handlers automatically record metrics using Prometheus:

- **HTTP Metrics**: Request duration, count, status codes
- **gRPC Metrics**: RPC duration, status codes
- **Storage Metrics**: Operation latency, lock contention, object counts, bytes stored
- **Auth Metrics**: Authentication duration, success/failure rates, SigV4 stage profiling
- **Multipart Metrics**: Active uploads, part sizes, completion duration

Metrics endpoint: `GET /_metrics`

### Structured Logging

Configurable logging with two output formats:

- **JSON**: Structured logs for production environments
- **Human**: Colored, readable logs for development

Set via `LOG_FORMAT` environment variable. All operations include trace spans with context (bucket, key, operation, duration).

### Health Checks

Built-in health endpoints for orchestration:

- **Liveness**: `GET /_health` - Returns 200 if service is running
- **Readiness**: `GET /_ready` - Returns 200 if backends are healthy, 503 otherwise

### Configuration

```bash
# JSON logs for production
LOG_FORMAT=json RUST_LOG=info ./s3ish

# Human-readable for development
LOG_FORMAT=human RUST_LOG=debug ./s3ish
```

See [OBSERVABILITY.md](OBSERVABILITY.md) for complete metrics documentation and Grafana integration.

## Testing

All components have comprehensive unit and integration tests:

- **Auth tests**: 13+ tests covering metadata extraction, file-based auth, and SigV4
- **Storage tests**: 40+ tests covering CRUD operations, erasure coding, and filesystem storage
- **gRPC Service tests**: 14+ tests covering the gRPC protocol
- **S3 HTTP tests**: 50+ tests covering HTTP protocol, multipart uploads, CopyObject, and XML responses
- **Observability tests**: 10+ tests covering metrics, health checks, and logging
- **Total**: 126+ tests

Run tests:
```bash
# All tests
cargo test

# Specific test suites
cargo test --test http_file_storage
cargo test --test observability_integration_test
```

## Benefits of This Architecture

1. **Separation of Concerns**: Protocol handlers are separate from business logic
2. **Pluggable Components**: Easy to swap auth/storage implementations
3. **Protocol Agnostic**: Same backend works with gRPC, HTTP, or future protocols
4. **Testable**: Each component can be tested independently
5. **Type Safe**: Rust's type system ensures correctness
6. **Async by Default**: Full async/await support throughout
7. **Zero Cost Abstraction**: Trait objects only where needed, compile-time dispatch elsewhere

## Example: Using Both Protocols Simultaneously

You can even run both protocols on different ports:

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let auth: Arc<dyn Authenticator> = Arc::new(FileAuthenticator::new("creds.txt").await?);
    let storage: Arc<dyn StorageBackend> = Arc::new(InMemoryStorage::new());
    let handler = BaseHandler::new(auth, storage);

    // Clone handler for both servers (cheap Arc clone)
    let grpc_server = GrpcConnectionManager::new(handler.clone());
    let http_server = S3HttpConnectionManager::new(handler);

    // Run both concurrently
    tokio::try_join!(
        grpc_server.serve("127.0.0.1:50051".parse()?),
        http_server.serve("127.0.0.1:8080".parse()?)
    )?;

    Ok(())
}
```

This demonstrates the true power of the pluggable architecture - the same storage and auth backend serving multiple protocols simultaneously!
