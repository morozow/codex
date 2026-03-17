# Migration Guide: Direct stdio to stdio_bus Worker Mode

This guide documents the migration process from direct stdio communication to stdio_bus worker mode for Codex components. It covers backward compatibility guarantees, configuration examples, and troubleshooting procedures.

## Table of Contents

- [Overview](#overview)
- [Backward Compatibility Guarantees](#backward-compatibility-guarantees)
- [Migration Steps](#migration-steps)
- [Configuration Examples](#configuration-examples)
- [Common Migration Scenarios](#common-migration-scenarios)
- [Troubleshooting](#troubleshooting)
- [Rollback Procedures](#rollback-procedures)

## Overview

The stdio_bus integration provides a unified transport layer for stdio-based communication, enabling:

- Multiple IDE clients through session-aware message routing
- Process supervision with automatic restart policies
- Backpressure management and request-response correlation
- Session affinity for stateful conversations

The migration is designed to be non-breaking: existing deployments continue to work without changes, and worker mode is opt-in via the `--worker` flag.

## Backward Compatibility Guarantees

The following guarantees ensure existing deployments remain functional:

### Requirement 9.1: App-Server Works Without stdio_bus

When the `--worker` flag is not set, the app-server behaves exactly as before:

```bash
# Direct stdio mode (default) - unchanged behavior
codex app-server --listen stdio://

# WebSocket mode - unchanged behavior
codex app-server --listen ws://127.0.0.1:8080
```

No code changes are required for existing clients using direct stdio or WebSocket transports.

### Requirement 9.2: Existing Client Connections Unchanged

Clients connecting via direct stdio or WebSocket continue to work without modification:

- No new handshake requirements
- No mandatory session ID fields
- Same initialization sequence (`initialize` → `initialized`)
- Same request/response patterns

### Requirement 9.3: JSON-RPC Message Format Preserved

The wire format remains identical:

```json
// Request format - unchanged
{"method":"thread/start","id":1,"params":{"cwd":"/project"}}

// Response format - unchanged
{"id":1,"result":{"thread":{"id":"thr_123"}}}

// Notification format - unchanged
{"method":"turn/started","params":{"threadId":"thr_123"}}
```

The only addition in worker mode is the optional `sessionId` field for routing, which is ignored by clients that don't use it.

### Requirement 9.4: Existing config.toml Settings Work

All existing configuration options in `~/.codex/config.toml` continue to function:

- Model settings
- Sandbox policies
- Approval policies
- MCP server configurations
- All other user preferences

Worker mode reads the same configuration files and applies the same settings.

### Requirement 9.5: MCP Server Works Without stdio_bus

The MCP server also supports both modes:

```bash
# Direct stdio mode (default) - unchanged behavior
codex mcp-server

# Worker mode - opt-in
codex mcp-server --worker
```

## Migration Steps

### Step 1: Verify Current Setup

Before migrating, verify your current deployment works correctly:

```bash
# Test direct stdio mode
echo '{"method":"initialize","id":0,"params":{"clientInfo":{"name":"test","version":"1.0"}}}' | codex app-server --listen stdio://
```

### Step 2: Install stdio_bus Daemon

The stdio_bus daemon is a separate C11 binary. Install it according to your platform:

```bash
# Linux/macOS - example installation
# (Actual installation steps depend on your distribution method)
```

### Step 3: Generate Configuration

Use the `codex-stdio-bus` crate to generate daemon configuration:

```rust
use codex_stdio_bus::StdioBusConfig;
use std::path::Path;

// For development
let config = StdioBusConfig::development(Path::new("~/.codex"));
config.write_to_file(Path::new("stdio_bus_dev.json"))?;

// For production
let config = StdioBusConfig::production("/usr/bin/codex-app-server", 4);
config.write_to_file(Path::new("stdio_bus_prod.json"))?;
```

### Step 4: Test Worker Mode

Test the app-server in worker mode before full deployment:

```bash
# Start app-server in worker mode
codex app-server --worker

# Send a test message (with sessionId for routing)
echo '{"method":"initialize","id":0,"sessionId":"thread:test","params":{"clientInfo":{"name":"test","version":"1.0"}}}' | codex app-server --worker
```

### Step 5: Deploy with stdio_bus Daemon

Start the stdio_bus daemon with your configuration:

```bash
stdio_bus --config stdio_bus_prod.json
```

### Step 6: Update Client Connections

Update clients to connect through the stdio_bus daemon instead of directly to the app-server. Clients should include `sessionId` in requests for proper routing.

## Configuration Examples

### Development Environment

Single instance with debug logging for local development:

```json
{
  "pools": [
    {
      "id": "app-server",
      "command": "cargo",
      "args": ["run", "-p", "codex-app-server", "--", "--worker"],
      "instances": 1,
      "env": {
        "CODEX_HOME": "~/.codex",
        "RUST_LOG": "debug"
      }
    }
  ],
  "limits": {
    "max_input_buffer": 1048576,
    "max_output_queue": 4194304,
    "max_restarts": 10,
    "restart_window_sec": 300,
    "drain_timeout_sec": 30,
    "backpressure_timeout_sec": 60
  },
  "routing": {
    "session_id_field": "sessionId"
  }
}
```

Generate programmatically:

```rust
use codex_stdio_bus::StdioBusConfig;
use std::path::Path;

let config = StdioBusConfig::development(Path::new("~/.codex"));
config.write_to_file(Path::new("stdio_bus_dev.json"))?;
```

### Production Environment

Multiple instances with warn-level logging:

```json
{
  "pools": [
    {
      "id": "app-server",
      "command": "/usr/bin/codex-app-server",
      "args": ["--worker"],
      "instances": 4,
      "env": {
        "RUST_LOG": "warn"
      }
    }
  ],
  "limits": {
    "max_input_buffer": 1048576,
    "max_output_queue": 4194304,
    "max_restarts": 5,
    "restart_window_sec": 60,
    "drain_timeout_sec": 30,
    "backpressure_timeout_sec": 60
  },
  "routing": {
    "session_id_field": "sessionId",
    "default_pool": "app-server"
  }
}
```

Generate programmatically:

```rust
use codex_stdio_bus::StdioBusConfig;

let config = StdioBusConfig::production("/usr/bin/codex-app-server", 4);
config.write_to_file(Path::new("stdio_bus_prod.json"))?;
```

### Testing Environment

Configuration for integration tests with mock daemon:

```json
{
  "pools": [
    {
      "id": "app-server-test",
      "command": "${CARGO_TARGET_DIR}/debug/codex-app-server",
      "args": ["--worker"],
      "instances": 1,
      "env": {
        "CODEX_HOME": "${TEST_CODEX_HOME}",
        "RUST_LOG": "trace",
        "CODEX_TEST_MODE": "1"
      },
      "cwd": "${TEST_WORKSPACE}"
    }
  ],
  "limits": {
    "max_input_buffer": 1048576,
    "max_output_queue": 4194304,
    "max_restarts": 3,
    "restart_window_sec": 10,
    "drain_timeout_sec": 5,
    "backpressure_timeout_sec": 10
  },
  "routing": {
    "session_id_field": "sessionId",
    "default_pool": "app-server-test"
  }
}
```

Use environment variable substitution:

```rust
use codex_stdio_bus::StdioBusConfig;

let mut config = StdioBusConfig {
    pools: vec![/* ... */],
    limits: Default::default(),
    routing: Default::default(),
};

// Substitute ${VAR} and ${VAR} patterns with actual values
config.substitute_env_vars();
config.write_to_file(Path::new("stdio_bus_test.json"))?;
```

### Multi-Pool Configuration

Running multiple worker types (app-server + MCP proxy):

```json
{
  "pools": [
    {
      "id": "app-server",
      "command": "/usr/bin/codex-app-server",
      "args": ["--worker"],
      "instances": 4,
      "env": {
        "RUST_LOG": "warn"
      }
    },
    {
      "id": "mcp-proxy",
      "command": "/usr/bin/codex-mcp-proxy",
      "args": ["--worker"],
      "instances": 2,
      "env": {
        "RUST_LOG": "warn"
      }
    }
  ],
  "limits": {
    "max_input_buffer": 1048576,
    "max_output_queue": 4194304,
    "max_restarts": 5,
    "restart_window_sec": 60,
    "drain_timeout_sec": 30,
    "backpressure_timeout_sec": 60
  },
  "routing": {
    "session_id_field": "sessionId",
    "default_pool": "app-server"
  }
}
```

## Common Migration Scenarios

### Scenario 1: Single IDE Client (No Migration Needed)

If you have a single IDE client connecting directly to the app-server, no migration is required. Continue using direct stdio mode:

```bash
codex app-server --listen stdio://
```

### Scenario 2: Multiple IDE Clients

For multiple IDE clients sharing a single app-server deployment:

1. Deploy stdio_bus daemon with production configuration
2. Update IDE extensions to connect through stdio_bus
3. Include `sessionId` in all requests (format: `thread:{threadId}`)

### Scenario 3: High Availability Setup

For production deployments requiring high availability:

1. Configure multiple worker instances (e.g., 4-8)
2. Set appropriate restart limits and windows
3. Monitor worker health via stderr logs
4. Use load balancing at the stdio_bus daemon level

### Scenario 4: Development with Hot Reload

For development with frequent code changes:

1. Use development configuration with `cargo run`
2. Set higher `max_restarts` and `restart_window_sec`
3. Enable debug logging for troubleshooting

## Troubleshooting

### Issue: Worker Not Starting

Symptoms: Worker process exits immediately or fails to respond.

Solutions:
1. Check stderr for error messages
2. Verify the command path is correct
3. Ensure required environment variables are set
4. Test the worker directly: `codex app-server --worker`

### Issue: Messages Not Routing

Symptoms: Requests don't reach the worker or responses are lost.

Solutions:
1. Verify `sessionId` is included in requests
2. Check session ID format matches expected patterns (`thread:`, `conn:`, `mcp:`)
3. Ensure `session_id_field` in config matches your message format
4. Check stdio_bus daemon logs for routing errors

### Issue: Session Affinity Not Working

Symptoms: Requests with same `sessionId` go to different workers.

Solutions:
1. Verify `sessionId` is consistent across related requests
2. Check that responses include the same `sessionId`
3. Ensure the session hasn't timed out or been invalidated

### Issue: Worker Crashes and Restarts

Symptoms: Worker processes restart frequently.

Solutions:
1. Check stderr logs for crash reasons
2. Increase `max_restarts` if crashes are transient
3. Investigate memory usage (should be <10MB overhead)
4. Check for resource exhaustion (file descriptors, memory)

### Issue: Backpressure Errors

Symptoms: Clients receive `-32001` "Server overloaded" errors.

Solutions:
1. Increase `max_output_queue` if queue fills up
2. Add more worker instances
3. Implement client-side retry with exponential backoff
4. Check for slow consumers blocking the output queue

### Issue: Graceful Shutdown Timeout

Symptoms: Workers killed with SIGKILL instead of clean exit.

Solutions:
1. Increase `drain_timeout_sec` if workers need more time
2. Check for long-running requests blocking shutdown
3. Implement proper `on_shutdown()` handler in worker

## Rollback Procedures

If issues arise after migration, follow these steps to rollback:

### Immediate Rollback

1. Stop the stdio_bus daemon
2. Restart app-server in direct stdio mode:
   ```bash
   codex app-server --listen stdio://
   ```
3. Update clients to connect directly to app-server

### Gradual Rollback

For production environments, consider a gradual rollback:

1. Route new connections to direct stdio mode
2. Allow existing stdio_bus sessions to drain
3. Monitor for issues in both modes
4. Complete migration once direct mode is stable

### Configuration Backup

Before migration, backup your working configuration:

```bash
# Backup current config
cp ~/.codex/config.toml ~/.codex/config.toml.backup

# Backup any custom scripts or configurations
```

### Verification After Rollback

After rollback, verify the system is working:

1. Test basic operations (thread/start, turn/start)
2. Verify existing sessions can be resumed
3. Check that all client connections work
4. Monitor logs for any errors

## Session ID Reference

| Prefix | Format | Use Case |
|--------|--------|----------|
| `thread:` | `thread:{threadId}` | Codex conversation threads |
| `conn:` | `conn:{connectionId}` | Connection-based sessions |
| `mcp:` | `mcp:{serverName}` | External MCP server routing |

## Resource Limits Reference

| Limit | Default | Description |
|-------|---------|-------------|
| `max_input_buffer` | 1MB | Maximum size of input buffer per message |
| `max_output_queue` | 4MB | Maximum size of output queue per worker |
| `max_restarts` | 5 | Maximum restarts within restart window |
| `restart_window_sec` | 60s | Time window for restart counting |
| `drain_timeout_sec` | 30s | Graceful shutdown timeout |
| `backpressure_timeout_sec` | 60s | Backpressure timeout before disconnect |

## Further Reading

- [codex-stdio-bus README](../README.md) - Library documentation
- [App-Server README](../../app-server/README.md) - App-server documentation
- [stdio_bus Protocol Specification](../../../docs/stdio_bus/main.tex) - Protocol details
