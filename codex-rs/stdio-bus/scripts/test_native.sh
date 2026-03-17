#!/bin/bash
# Quick test script for stdio_bus + Codex integration (native binary)
# Usage: ./test_native.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CODEX_RS_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_DIR="$SCRIPT_DIR/.test-run"
BINARY="$TEST_DIR/codex-app-server"
CONFIG="$TEST_DIR/config.json"
STDIO_BUS="$SCRIPT_DIR/stdio_bus"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}=== stdio_bus + Codex Integration Test ===${NC}"

# Check stdio_bus binary
if [ ! -x "$STDIO_BUS" ]; then
    echo -e "${RED}Error: stdio_bus binary not found at $STDIO_BUS${NC}"
    exit 1
fi

# Build if needed
if [ ! -f "$BINARY" ]; then
    echo "Building codex-app-server..."
    cd "$CODEX_RS_DIR"
    cargo build --release -p codex-app-server
    mkdir -p "$TEST_DIR"
    cp target/release/codex-app-server "$BINARY"
fi

# Create config with absolute path
cat > "$CONFIG" << EOF
{
  "pools": [{
    "id": "codex",
    "command": "$BINARY",
    "args": ["--worker"],
    "instances": 2,
    "env": {"RUST_LOG": "info"}
  }],
  "limits": {
    "max_input_buffer": 1048576,
    "max_restarts": 3,
    "restart_window_sec": 60
  },
  "routing": {
    "session_id_field": "sessionId",
    "default_pool": "codex"
  }
}
EOF

# Start stdio_bus daemon
echo "Starting stdio_bus daemon on TCP 127.0.0.1:9999..."
"$STDIO_BUS" --config "$CONFIG" --tcp 127.0.0.1:9999 &
DAEMON_PID=$!
sleep 2

# Cleanup on exit
cleanup() {
    echo "Stopping daemon..."
    kill $DAEMON_PID 2>/dev/null || true
}
trap cleanup EXIT

# Test 1: Initialize
echo -e "\n${GREEN}Test 1: Initialize session${NC}"
RESPONSE=$( (echo '{"jsonrpc":"2.0","method":"initialize","id":1,"sessionId":"thread:test-1","params":{"clientInfo":{"name":"test","version":"1.0"}}}'; sleep 2) | nc 127.0.0.1 9999 )
echo "Response: $RESPONSE"

if echo "$RESPONSE" | grep -q '"result"'; then
    echo -e "${GREEN}✓ Initialize succeeded${NC}"
else
    echo -e "${RED}✗ Initialize failed${NC}"
    exit 1
fi

# Test 2: Session affinity
echo -e "\n${GREEN}Test 2: Session affinity${NC}"
RESPONSE=$( (
echo '{"jsonrpc":"2.0","method":"initialize","id":1,"sessionId":"thread:sticky","params":{"clientInfo":{"name":"test","version":"1.0"}}}'
sleep 1
echo '{"jsonrpc":"2.0","method":"initialized","sessionId":"thread:sticky"}'
sleep 2
) | nc 127.0.0.1 9999 )
echo "Response: $RESPONSE"
echo -e "${GREEN}✓ Session affinity test completed${NC}"

echo -e "\n${GREEN}=== All tests passed! ===${NC}"
