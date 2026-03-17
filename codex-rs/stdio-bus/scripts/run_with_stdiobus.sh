#!/bin/bash
# Script to run Codex app-server with real stdio_bus daemon
# Usage: ./run_with_stdiobus.sh [--build]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CODEX_RS_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORK_DIR="$CODEX_RS_DIR/stdio-bus/.stdiobus-test"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Codex + stdio_bus Integration Test ===${NC}"

# Check prerequisites
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed${NC}"
    exit 1
fi

if [ -z "$OPENAI_API_KEY" ]; then
    echo -e "${YELLOW}Warning: OPENAI_API_KEY not set. Real API calls will fail.${NC}"
fi

# Build if requested or binary doesn't exist
if [ "$1" == "--build" ] || [ ! -f "$CODEX_RS_DIR/target/release/codex-app-server" ]; then
    echo -e "${YELLOW}Building codex-app-server...${NC}"
    cd "$CODEX_RS_DIR"
    cargo build --release -p codex-app-server
fi

# Create work directory
mkdir -p "$WORK_DIR"
cd "$WORK_DIR"

# Create stdio_bus config
echo -e "${GREEN}Creating stdio_bus configuration...${NC}"
cat > config.json << 'EOF'
{
  "pools": [
    {
      "id": "codex-app-server",
      "command": "/app/codex-app-server",
      "args": ["--worker"],
      "instances": 2,
      "env": {
        "RUST_LOG": "info"
      }
    }
  ],
  "limits": {
    "max_input_buffer": 1048576,
    "max_output_queue": 4194304,
    "max_restarts": 5,
    "restart_window_sec": 60,
    "drain_timeout_sec": 30
  },
  "routing": {
    "session_id_field": "sessionId",
    "default_pool": "codex-app-server"
  }
}
EOF

# Copy binary
echo -e "${GREEN}Copying codex-app-server binary...${NC}"
cp "$CODEX_RS_DIR/target/release/codex-app-server" ./codex-app-server
chmod +x ./codex-app-server

# Copy Codex config if exists
if [ -d "$HOME/.codex" ]; then
    echo -e "${GREEN}Copying Codex configuration...${NC}"
    mkdir -p .codex
    cp -r "$HOME/.codex/"* .codex/ 2>/dev/null || true
fi

# Stop existing container if running
docker stop stdiobus-codex 2>/dev/null || true
docker rm stdiobus-codex 2>/dev/null || true

# Run stdio_bus daemon
echo -e "${GREEN}Starting stdio_bus daemon...${NC}"
docker run -d \
    --name stdiobus-codex \
    -p 8080:8080 \
    -v "$(pwd)/config.json:/config.json:ro" \
    -v "$(pwd)/codex-app-server:/app/codex-app-server:ro" \
    -v "$(pwd)/.codex:/root/.codex:ro" \
    -e "OPENAI_API_KEY=${OPENAI_API_KEY:-}" \
    -e "CODEX_HOME=/root/.codex" \
    stdiobus/stdiobus:latest

# Wait for startup
echo -e "${YELLOW}Waiting for stdio_bus to start...${NC}"
sleep 2

# Check if running
if docker ps | grep -q stdiobus-codex; then
    echo -e "${GREEN}stdio_bus daemon is running!${NC}"
    echo ""
    echo -e "${GREEN}=== Quick Test ===${NC}"
    
    # Test connection
    echo '{"method":"initialize","id":1,"sessionId":"thread:test","params":{"clientInfo":{"name":"test","version":"1.0"}}}' | nc -w 2 localhost 8080 || echo -e "${YELLOW}(netcat test - may need manual verification)${NC}"
    
    echo ""
    echo -e "${GREEN}=== Usage ===${NC}"
    echo "Send JSON-RPC messages to: localhost:8080"
    echo ""
    echo "Example with netcat:"
    echo '  echo '"'"'{"method":"initialize","id":1,"sessionId":"thread:my-session","params":{"clientInfo":{"name":"test","version":"1.0"}}}'"'"' | nc localhost 8080'
    echo ""
    echo "View logs:"
    echo "  docker logs -f stdiobus-codex"
    echo ""
    echo "Stop:"
    echo "  docker stop stdiobus-codex && docker rm stdiobus-codex"
else
    echo -e "${RED}Failed to start stdio_bus daemon${NC}"
    echo "Logs:"
    docker logs stdiobus-codex
    exit 1
fi
