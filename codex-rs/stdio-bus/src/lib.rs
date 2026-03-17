//! Rust client library for stdio_bus integration.
//!
//! This crate provides async NDJSON message handling, routing field extraction,
//! session ID mapping, and worker runtime for stdio_bus daemon integration.

pub mod config;
pub mod message;
pub mod routing;
pub mod session;
pub mod signal;
pub mod transport;
pub mod worker;

pub use config::{ConfigError, LimitsConfig, PoolConfig, RoutingConfig, StdioBusConfig};
pub use message::{Message, RequestId, RoutingFields};
pub use routing::{MAX_REQUEST_ID_LEN, MAX_SESSION_ID_LEN, extract_routing_fields, parse_message};
pub use session::{
    CONN_PREFIX, MCP_PREFIX, SessionType, THREAD_PREFIX, conn_to_session_id,
    extract_mcp_server_name, extract_thread_id, mcp_to_session_id, thread_to_session_id,
};
pub use transport::{DirectStdioTransport, StdioBusWorkerTransport, Transport, create_transport};
pub use worker::{MessageHandler, StdioBusWorker, WorkerError};
