#!/bin/bash
set -e

echo "=== S3 HTTP Handler Demo ==="
echo ""

# Build the project
echo "Building..."
cargo build --release 2>&1 | tail -3
echo ""

# Create credentials
echo "demo:demo-secret" > creds.txt
echo "Created credentials: demo:demo-secret"
echo ""

# Start server in background
echo "Starting HTTP server on 0.0.0.0:9000..."
./target/release/s3ish --protocol http --listen 0.0.0.0:9000 > /tmp/s3ish.log 2>&1 &
SERVER_PID=$!
sleep 2

# Check if server started
if ! ps -p $SERVER_PID > /dev/null; then
    echo "Server failed to start!"
    cat /tmp/s3ish.log
    exit 1
fi

echo "Server running (PID: $SERVER_PID)"
echo ""

# Cleanup function
cleanup() {
    echo ""
    echo "Stopping server..."
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null || true
}
trap cleanup EXIT

# Demo commands
echo "1. Creating bucket 'my-bucket'..."
curl -s -X PUT http://localhost:9000/my-bucket \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret" | jq .
echo ""

echo "2. Uploading 'hello.txt'..."
curl -s -X PUT http://localhost:9000/my-bucket/hello.txt \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret" \
  -H "content-type: text/plain" \
  -d "Hello from S3!" | jq .
echo ""

echo "3. Uploading 'world.txt'..."
curl -s -X PUT http://localhost:9000/my-bucket/world.txt \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret" \
  -H "content-type: text/plain" \
  -d "World!" | jq .
echo ""

echo "4. Listing objects..."
curl -s "http://localhost:9000/my-bucket/" \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret" | jq .
echo ""

echo "5. Getting 'hello.txt'..."
curl -s http://localhost:9000/my-bucket/hello.txt \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret"
echo ""
echo ""

echo "6. Deleting 'hello.txt'..."
curl -s -X DELETE http://localhost:9000/my-bucket/hello.txt \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret" | jq .
echo ""

echo "7. Listing objects again..."
curl -s "http://localhost:9000/my-bucket/" \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret" | jq .
echo ""

echo "8. Deleting 'world.txt'..."
curl -s -X DELETE http://localhost:9000/my-bucket/world.txt \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret" | jq .
echo ""

echo "9. Deleting bucket..."
curl -s -X DELETE http://localhost:9000/my-bucket \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret" | jq .
echo ""

echo "=== Demo Complete ==="
