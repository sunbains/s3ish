# s3ish

In-memory S3-like object store with pluggable gRPC and HTTP interfaces.

## Features

- **Dual Protocol Support**: gRPC and S3-compatible HTTP APIs
- **Pluggable Architecture**: Easy to swap authentication and storage backends
- **In-Memory Storage**: Fast, ephemeral object storage
- **File-Based Auth**: Simple access key/secret key authentication
- **Comprehensive Tests**: 57 unit tests covering all components

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

## Configuration

Create a `config.toml` file:

```toml
listen_addr = "0.0.0.0:9000"
auth_file = "./creds.txt"
```

## Architecture

The project uses a pluggable architecture where protocol handlers (gRPC, HTTP) share common authentication and storage backends through the `BaseHandler`.

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed documentation.

## Testing

```bash
cargo test
```

All 57 tests pass covering authentication, storage, gRPC, and HTTP handlers.

## License

MIT
