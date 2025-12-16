#!/bin/bash
set -e

echo "=== S3 HTTP Handler Demo ==="
echo ""

# Check for xmllint (optional, for pretty XML)
if command -v xmllint &> /dev/null; then
    HAS_XMLLINT=1
    FORMAT_XML="xmllint --format -"
else
    HAS_XMLLINT=0
    FORMAT_XML="cat"
fi

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
HTTP_CODE=$(curl -s -w "%{http_code}" -o /dev/null -X PUT http://localhost:9000/my-bucket \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret")
echo "   Status: $HTTP_CODE (bucket created)"
echo ""

echo "2. Uploading 'hello.txt'..."
HTTP_CODE=$(curl -s -w "%{http_code}" -o /dev/null -X PUT http://localhost:9000/my-bucket/hello.txt \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret" \
  -H "content-type: text/plain" \
  -d "Hello from S3!")
echo "   Status: $HTTP_CODE (object uploaded)"
echo ""

echo "3. Uploading 'world.txt'..."
HTTP_CODE=$(curl -s -w "%{http_code}" -o /dev/null -X PUT http://localhost:9000/my-bucket/world.txt \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret" \
  -H "content-type: text/plain" \
  -d "World!")
echo "   Status: $HTTP_CODE (object uploaded)"
echo ""

echo "4. Listing objects..."
curl -s "http://localhost:9000/my-bucket/" \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret" | $FORMAT_XML
echo ""

echo "5. Getting 'hello.txt'..."
curl -s http://localhost:9000/my-bucket/hello.txt \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret"
echo ""
echo ""

echo "6. Deleting 'hello.txt'..."
HTTP_CODE=$(curl -s -w "%{http_code}" -o /dev/null -X DELETE http://localhost:9000/my-bucket/hello.txt \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret")
echo "   Status: $HTTP_CODE (object deleted)"
echo ""

echo "7. Listing objects again..."
curl -s "http://localhost:9000/my-bucket/" \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret" | $FORMAT_XML
echo ""

echo "8. Deleting 'world.txt'..."
HTTP_CODE=$(curl -s -w "%{http_code}" -o /dev/null -X DELETE http://localhost:9000/my-bucket/world.txt \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret")
echo "   Status: $HTTP_CODE (object deleted)"
echo ""

echo "9. Deleting bucket..."
HTTP_CODE=$(curl -s -w "%{http_code}" -o /dev/null -X DELETE http://localhost:9000/my-bucket \
  -H "x-access-key: demo" \
  -H "x-secret-key: demo-secret")
echo "   Status: $HTTP_CODE (bucket deleted)"
echo ""

echo "=== Demo Complete ==="
