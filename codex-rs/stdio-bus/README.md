# codex-stdio-bus

Rust client library for stdio_bus integration. This crate provides async NDJSON message handling, routing field extraction, session ID mapping, and worker runtime for stdio_bus daemon integration.

## Overview

The `codex-stdio-bus` crate enables Codex components (app-server, MCP server, MCP proxy) to run as stdio_bus workers. It provides:

- NDJSON message reading/writing with proper framing
- Routing field extraction (`id`, `sessionId`, `method`)
- Session ID mapping for different session types
- Worker runtime with signal handling
- Transport abstraction for backward compatibility
- Configuration generation for stdio_bus daemon

## Usage

### Worker Mode

Implement the `MessageHandler` trait and run the worker:

```rust
use codex_stdio_bus::{Message, MessageHandler, StdioBusWorker, WorkerError};
use async_trait::async_trait;

struct MyHandler;

#[async_trait]
impl MessageHandler for MyHandler {
    async fn handle(&self, msg: Message) -> Result<Option<Vec<u8>>, WorkerError> {
        // Process message and return optional response
        let response = serde_json::json!({
            "id": msg.routing.id,
            "result": {}
        });
        Ok(Some(serde_json::to_vec(&response)?))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut worker = StdioBusWorker::new();
    let handler = MyHandler;
    worker.run(handler).await?;
    Ok(())
}
```

### Session ID Mapping

Map different identifier types to session IDs:

```rust
use codex_stdio_bus::{
    thread_to_session_id,
    conn_to_session_id,
    mcp_to_session_id,
    SessionType,
};

// Thread-based sessions
let session_id = thread_to_session_id("thr_abc123");
assert_eq!(session_id, "thread:thr_abc123");

// Connection-based sessions
let session_id = conn_to_session_id(42);
assert_eq!(session_id, "conn:42");

// MCP server sessions
let session_id = mcp_to_session_id("my-server");
assert_eq!(session_id, "mcp:my-server");

// Parse session type
let session_type = SessionType::from_session_id("thread:thr_abc123");
assert_eq!(session_type, SessionType::Thread("thr_abc123".to_string()));
```

### Routing Field Extraction

Extract routing fields from JSON messages:

```rust
use codex_stdio_bus::{extract_routing_fields, parse_message};

let json = br#"{"id":1,"method":"test","sessionId":"thread:abc"}"#;
let fields = extract_routing_fields(json);

assert_eq!(fields.method, Some("test".to_string()));
assert_eq!(fields.session_id, Some("thread:abc".to_string()));
assert!(!fields.is_response);
```

### Configuration Generation

Generate stdio_bus daemon configuration:

```rust
use codex_stdio_bus::StdioBusConfig;
use std::path::Path;

// Development configuration
let config = StdioBusConfig::development(Path::new("~/.codex"));

// Production configuration
let config = StdioBusConfig::production("/usr/bin/codex-app-server", 4);

// Write to file
config.write_to_file(Path::new("stdio_bus.json"))?;
```

### Transport Abstraction

Use the transport abstraction for backward compatibility:

```rust
use codex_stdio_bus::create_transport;

// Create transport based on mode
let transport = create_transport(worker_mode);

// Use transport for message I/O
let msg = transport.recv().await?;
transport.send(&response).await?;
```

## Session ID Formats

| Prefix | Format | Description |
|--------|--------|-------------|
| `thread:` | `thread:{threadId}` | Thread-based sessions for Codex conversations |
| `conn:` | `conn:{connectionId}` | Connection-based sessions (numeric ID) |
| `mcp:` | `mcp:{serverName}` | MCP server sessions for external MCP servers |

## Configuration Options

### Pool Configuration

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique pool identifier |
| `command` | string | Command to execute |
| `args` | string[] | Command arguments |
| `instances` | u32 | Number of worker instances |
| `env` | map | Environment variables |
| `cwd` | string? | Working directory |

### Limits Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `max_input_buffer` | 1MB | Maximum input buffer size |
| `max_output_queue` | 4MB | Maximum output queue size |
| `max_restarts` | 5 | Maximum restarts within window |
| `restart_window_sec` | 60 | Restart window in seconds |
| `drain_timeout_sec` | 30 | Graceful shutdown timeout |
| `backpressure_timeout_sec` | 60 | Backpressure timeout |

### Routing Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `session_id_field` | `sessionId` | JSON field for session ID |
| `default_pool` | none | Default pool for unrouted messages |

## Signal Handling

The worker runtime handles:
- `SIGTERM`: Initiates graceful shutdown
- `SIGINT` (Ctrl+C): Initiates graceful shutdown

On shutdown, the worker:
1. Stops accepting new messages
2. Calls `on_shutdown()` on the handler
3. Exits cleanly

## Platform Support

- Linux (epoll-based event loop)
- macOS (kqueue-based event loop)
- Windows support planned for future releases
