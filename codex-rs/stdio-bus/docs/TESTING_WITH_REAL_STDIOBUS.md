# Testing Codex with Real stdio_bus Daemon

This guide describes how to test the Codex app-server integration with the real stdio_bus daemon.

## Test Results ✅

Integration successfully tested on 2026-03-17:

```
[INFO] stdio Bus kernel v0.1.0 starting on macos
[INFO] Process manager created with 2 workers across 1 pools
[INFO] [worker=0] Worker started (pid=81829, pool=codex)
[INFO] [worker=1] Worker started (pid=81830, pool=codex)
[INFO] All 2 workers started successfully
[INFO] Listening on TCP 127.0.0.1:9999
[INFO] stdio Bus kernel ready, entering main loop
[INFO] [session=thread:session-A] New session assigned to worker 0
[INFO] [session=thread:session-B] New session assigned to worker 1
```

Confirmed:
- ✅ Codex app-server starts in `--worker` mode
- ✅ stdio_bus daemon manages the worker pool
- ✅ Session affinity works (requests with the same sessionId go to the same worker)
- ✅ Load balancing distributes sessions between workers
- ✅ JSON-RPC requests/responses are correctly routed

## Prerequisites

- Rust toolchain (for building Codex)
- stdio_bus binary (native or Docker)
- OpenAI API key (for real model requests, optional)

## Quick Start (Native Binary)

```bash
# 1. Build Codex app-server
cd codex-rs
cargo build --release -p codex-app-server

# 2. Create test directory
mkdir -p stdio-bus/scripts/.test-run
cp target/release/codex-app-server stdio-bus/scripts/.test-run/

# 3. Create configuration (replace path with absolute!)
cat > stdio-bus/scripts/.test-run/config.json << 'EOF'
{
  "pools": [{
    "id": "codex",
    "command": "/ABSOLUTE/PATH/TO/codex-app-server",
    "args": ["--worker"],
    "instances": 2,
    "env": {"RUST_LOG": "info"}
  }],
  "routing": {"session_id_field": "sessionId", "default_pool": "codex"}
}
EOF

# 4. Start stdio_bus daemon
./stdio-bus/scripts/stdio_bus --config stdio-bus/scripts/.test-run/config.json --tcp 127.0.0.1:9999

# 5. In another terminal — send test request
(echo '{"jsonrpc":"2.0","method":"initialize","id":1,"sessionId":"thread:test","params":{"clientInfo":{"name":"test","version":"1.0"}}}'; sleep 2) | nc 127.0.0.1 9999
```

## Step 1: Build Codex app-server

```bash
cd codex-rs
cargo build --release -p codex-app-server
```

The binary will be in `target/release/codex-app-server`.

## Step 2: Create stdio_bus Configuration

**IMPORTANT:** The path to the binary must be absolute!

Create file `stdio_bus_config.json`:

```json
{
  "pools": [
    {
      "id": "codex-app-server",
      "command": "/app/codex-app-server",
      "args": ["--worker"],
      "instances": 2,
      "env": {
        "RUST_LOG": "info",
        "CODEX_HOME": "/root/.codex"
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
```

## Step 3: Prepare Directory for Mounting

```bash
# Create directory for workers
mkdir -p workers-registry/codex

# Copy built binary
cp target/release/codex-app-server workers-registry/codex/

# Copy Codex configuration (if exists)
mkdir -p workers-registry/codex/.codex
cp -r ~/.codex/config.toml workers-registry/codex/.codex/ 2>/dev/null || true
```

## Step 4: Start stdio_bus Daemon

### Option A: Simple Start (TCP Mode)

```bash
docker run -d \
  --name stdiobus-codex \
  -p 8080:8080 \
  -v $(pwd)/stdio_bus_config.json:/config.json:ro \
  -v $(pwd)/workers-registry/codex/codex-app-server:/app/codex-app-server:ro \
  -v $(pwd)/workers-registry/codex/.codex:/root/.codex:ro \
  -e OPENAI_API_KEY="${OPENAI_API_KEY}" \
  stdiobus/stdiobus:latest
```

### Option B: Docker Compose

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  stdiobus:
    image: stdiobus/stdiobus:latest
    ports:
      - "8080:8080"
    volumes:
      - ./stdio_bus_config.json:/config.json:ro
      - ./workers-registry/codex/codex-app-server:/app/codex-app-server:ro
      - ./workers-registry/codex/.codex:/root/.codex:ro
    environment:
      - OPENAI_API_KEY=${OPENAI_API_KEY}
    restart: unless-stopped
```

Start:
```bash
docker-compose up -d
```


## Step 5: Verify Operation

### Check Logs

```bash
docker logs -f stdiobus-codex
```

### Test Request via netcat

```bash
# Initialize session
echo '{"method":"initialize","id":1,"sessionId":"thread:test-session-1","params":{"clientInfo":{"name":"test-client","version":"1.0.0"}}}' | nc localhost 8080

# Should return response with the same sessionId
```

### Test Request via curl (if HTTP is supported)

```bash
curl -X POST http://localhost:8080 \
  -H "Content-Type: application/json" \
  -d '{"method":"initialize","id":1,"sessionId":"thread:test-1","params":{"clientInfo":{"name":"curl-test","version":"1.0"}}}'
```

## Step 6: Full Test with Multiple Sessions

Create test script `test_sessions.sh`:

```bash
#!/bin/bash

# Test 1: Initialize session A
echo "=== Session A: Initialize ==="
echo '{"method":"initialize","id":1,"sessionId":"thread:session-A","params":{"clientInfo":{"name":"test","version":"1.0"}}}' | nc -q 1 localhost 8080

# Test 2: Initialize session B (should go to another worker or the same one)
echo "=== Session B: Initialize ==="
echo '{"method":"initialize","id":1,"sessionId":"thread:session-B","params":{"clientInfo":{"name":"test","version":"1.0"}}}' | nc -q 1 localhost 8080

# Test 3: Second request to session A (should preserve affinity)
echo "=== Session A: Second request ==="
echo '{"method":"initialized","sessionId":"thread:session-A"}' | nc -q 1 localhost 8080

echo "=== Done ==="
```

## Step 7: Interactive Testing

For interactive testing use `socat`:

```bash
socat - TCP:localhost:8080
```

Then enter JSON-RPC messages manually:

```json
{"method":"initialize","id":1,"sessionId":"thread:interactive","params":{"clientInfo":{"name":"interactive","version":"1.0"}}}
{"method":"initialized","sessionId":"thread:interactive"}
{"method":"thread/start","id":2,"sessionId":"thread:interactive","params":{"cwd":"/tmp"}}
```

## Verifying Session Affinity

To verify that session affinity works:

1. Send multiple requests with the same `sessionId`
2. Check stdio_bus logs — requests should go to the same worker
3. Send requests with different `sessionId` — they may be distributed to different workers

## Troubleshooting

### Worker Does Not Start

```bash
# Check that the binary is executable
docker exec stdiobus-codex ls -la /app/codex-app-server

# Check logs
docker logs stdiobus-codex
```

### No Response to Requests

```bash
# Check that the port is open
nc -zv localhost 8080

# Check processes inside the container
docker exec stdiobus-codex ps aux
```

### OpenAI Authentication Errors

Make sure `OPENAI_API_KEY` is passed to the container:

```bash
docker exec stdiobus-codex env | grep OPENAI
```

## Cleanup

```bash
# Stop and remove container
docker stop stdiobus-codex
docker rm stdiobus-codex

# Or via docker-compose
docker-compose down
```

## Next Steps

After successful testing of basic integration:

1. Test real Codex operations (thread/start, turn/start)
2. Check graceful shutdown (send SIGTERM)
3. Test restart policy (kill worker and verify restart)
4. Measure performance with multiple clients
