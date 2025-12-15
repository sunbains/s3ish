# mems3-grpc

A small **in-memory, S3-like object store** implemented in Rust with:

- **gRPC transport** (connection management) via `tonic`
- Pluggable **authentication** (file-based implementation provided)
- Pluggable **storage** (in-memory BTreeMap/BTreeSet implementation provided)

This is not the AWS S3 HTTP API; it's an S3-*style* object store exposed over gRPC
(`CreateBucket`, `PutObject`, `GetObject`, etc.). It’s designed so you can swap the transport
layer later if you want to add a real S3 REST API.

## Features

- Buckets + objects (`bucket/key`)
- Deterministic listing order (BTreeMap)
- ETag = MD5(data)
- Metadata: content-type, size, last-modified
- Auth enforced on every RPC via gRPC metadata headers

## Quickstart

```bash
# 1) Build
cargo build

# 2) Run (reads ./config.toml by default)
RUST_LOG=info cargo run

# Or specify config:
MEMS3_CONFIG=./config.toml RUST_LOG=info cargo run
```

### Auth

Provide credentials via **gRPC metadata**:

- `x-access-key: <access_key>`
- `x-secret-key: <secret_key>`

Credentials come from `creds.txt` (see format below).

`creds.txt` format:

```
# comments allowed
access_key:secret_key
demo:demo-secret
```

## Example client (grpcurl)

If you have `grpcurl` installed:

```bash
grpcurl -plaintext   -H 'x-access-key: demo'   -H 'x-secret-key: demo-secret'   -d '{"bucket":{"name":"b1"}}'   127.0.0.1:50051 mems3.v1.ObjectStore/CreateBucket
```

Put object:

```bash
grpcurl -plaintext   -H 'x-access-key: demo'   -H 'x-secret-key: demo-secret'   -d '{"object":{"bucket":"b1","key":"k1"}, "data":"aGVsbG8=", "content_type":"text/plain"}'   127.0.0.1:50051 mems3.v1.ObjectStore/PutObject
```

Get object:

```bash
grpcurl -plaintext   -H 'x-access-key: demo'   -H 'x-secret-key: demo-secret'   -d '{"object":{"bucket":"b1","key":"k1"}}'   127.0.0.1:50051 mems3.v1.ObjectStore/GetObject
```

## Architecture

- `auth::Authenticator` — abstraction for authentication
  - `auth::file_auth::FileAuthenticator` — concrete, file-based
- `storage::StorageBackend` — abstraction for object storage
  - `storage::in_memory::InMemoryStorage` — concrete, in-memory BTreeMap
- `server::ConnectionManager` — abstraction for serving connections
  - `server::GrpcConnectionManager` — concrete gRPC implementation
- `service::ObjectStoreService` — message handling (business logic) wired to gRPC

## Notes / Extensions

- Add streaming upload/download RPCs for very large objects.
- Add per-bucket ACLs in `AuthContext`.
- Swap gRPC for a real S3 HTTP API while keeping `StorageBackend`.
