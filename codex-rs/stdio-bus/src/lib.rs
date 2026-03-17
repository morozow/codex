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

pub use config::ConfigError;
pub use config::LimitsConfig;
pub use config::PoolConfig;
pub use config::RoutingConfig;
pub use config::StdioBusConfig;
pub use message::Message;
pub use message::RequestId;
pub use message::RoutingFields;
pub use routing::MAX_REQUEST_ID_LEN;
pub use routing::MAX_SESSION_ID_LEN;
pub use routing::extract_routing_fields;
pub use routing::parse_message;
pub use session::CONN_PREFIX;
pub use session::MCP_PREFIX;
pub use session::SessionType;
pub use session::THREAD_PREFIX;
pub use session::conn_to_session_id;
pub use session::extract_mcp_server_name;
pub use session::extract_thread_id;
pub use session::mcp_to_session_id;
pub use session::thread_to_session_id;
pub use transport::DirectStdioTransport;
pub use transport::StdioBusWorkerTransport;
pub use transport::Transport;
pub use transport::create_transport;
pub use worker::MessageHandler;
pub use worker::StdioBusWorker;
pub use worker::WorkerError;
