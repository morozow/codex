//! Worker mode message handler for app-server.
//!
//! This module provides the `AppServerWorkerHandler` which implements the
//! `MessageHandler` trait from codex-stdio-bus. It handles incoming NDJSON
//! messages and routes them to the appropriate session based on `sessionId`.
//!
//! # Requirements
//! - REQ-2.5: Extract and use sessionId for thread affinity
//! - REQ-2.6: Include sessionId in responses
//! - REQ-2.7: Include sessionId in notifications

use async_trait::async_trait;
use codex_arg0::Arg0DispatchPaths;
use codex_core::config::Config;
use codex_stdio_bus::message::Message;
use codex_stdio_bus::worker::{MessageHandler, WorkerError};
use std::sync::Arc;
use tracing::{debug, info};

/// Worker mode handler for app-server (REQ-2).
///
/// This handler processes incoming NDJSON messages from the stdio_bus daemon
/// and maintains session affinity based on the `sessionId` field.
pub struct AppServerWorkerHandler {
    config: Arc<Config>,
    #[allow(dead_code)]
    arg0_paths: Arg0DispatchPaths,
}

impl AppServerWorkerHandler {
    /// Create a new worker handler with the given configuration.
    pub fn new(config: Arc<Config>, arg0_paths: Arg0DispatchPaths) -> Self {
        Self { config, arg0_paths }
    }

    /// Get the configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }
}

#[async_trait]
impl MessageHandler for AppServerWorkerHandler {
    async fn handle(&self, msg: Message) -> Result<Option<Vec<u8>>, WorkerError> {
        let session_id = msg.routing.session_id.as_deref();
        let method = msg.routing.method.as_deref();
        let request_id = msg.routing.id.as_ref();

        debug!(
            ?session_id,
            ?method,
            ?request_id,
            is_response = msg.routing.is_response,
            "Received message"
        );

        // TODO(Task 3.3): Implement full message processing with session management
        // For now, return an error response indicating worker mode is not fully implemented
        if let Some(id) = request_id {
            let error_response = serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32603,
                    "message": "Worker mode message processing not yet implemented"
                }
            });
            return Ok(Some(serde_json::to_vec(&error_response).map_err(|e| {
                WorkerError::Handler(format!("failed to serialize error response: {e}"))
            })?));
        }

        // Notifications don't require a response
        Ok(None)
    }

    async fn on_shutdown(&self) {
        info!("App-server worker shutting down");
        // TODO(Task 3.3): Clean up sessions on shutdown
    }
}
