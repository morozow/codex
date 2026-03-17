#!/bin/bash
# Start stdio_bus daemon with Codex worker pool

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CODEX_RS_DIR="$(cd "$SCRIPT_DIR/../../../.." && pwd)/codex-rs"
BINARY="$CODEX_RS_DIR/target/release/codex-app-server"
STDIO_BUS="$CODEX_RS_DIR/stdio-bus/scripts/stdio_bus"

# Check prerequisites
if [ ! -x "$BINARY" ]; then
    echo "Building codex-app-server..."
    cd "$CODEX_RS_DIR"
    cargo build --release -p codex-app-server
fi

if [ ! -x "$STDIO_BUS" ]; then
    echo "Error: stdio_bus binary not found at $STDIO_BUS"
    echo "Please place the stdio_bus binary there."
    exit 1
fi

# Create config
CONFIG="$SCRIPT_DIR/stdiobus_config.json"
cat > "$CONFIG" << EOF
{
  "pools": [{
    "id": "codex",
    "command": "$BINARY",
    "args": ["--worker"],
    "instances": 3,
    "env": {"RUST_LOG": "info"}
  }],
  "limits": {
    "max_input_buffer": 1048576,
    "max_restarts": 5,
    "restart_window_sec": 60
  },
  "routing": {
    "session_id_field": "sessionId",
    "default_pool": "codex"
  }
}
EOF

echo "Starting stdio_bus daemon on TCP 127.0.0.1:9999..."
echo "Workers: 3 instances of codex-app-server"
echo ""
exec "$STDIO_BUS" --config "$CONFIG" --tcp 127.0.0.1:9999
