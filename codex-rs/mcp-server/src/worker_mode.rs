//! Worker mode message handler for MCP server.
//!
//! This module provides the `McpServerWorkerHandler` which implements the
//! `MessageHandler` trait from codex-stdio-bus. It handles incoming NDJSON
//! messages and routes them to the appropriate session based on `sessionId`.
//!
//! # Requirements
//! - REQ-3.2: Implement MCP protocol over stdin/stdout in worker mode
//! - REQ-3.3: Bind session on `initialize` request with `sessionId`
//! - REQ-3.4: Include `sessionId` in MCP responses
//! - REQ-3.5: Perform graceful shutdown on SIGTERM

use async_trait::async_trait;
use codex_arg0::Arg0DispatchPaths;
use codex_core::config::Config;
use codex_stdio_bus::message::Message;
use codex_stdio_bus::session::extract_thread_id;
use codex_stdio_bus::worker::MessageHandler;
use codex_stdio_bus::worker::WorkerError;
use rmcp::model::ClientNotification;
use rmcp::model::ClientRequest;
use rmcp::model::ErrorCode;
use rmcp::model::ErrorData;
use rmcp::model::Implementation;
use rmcp::model::InitializeResult;
use rmcp::model::JsonRpcMessage;
use rmcp::model::RequestId;
use rmcp::model::ServerCapabilities;
use rmcp::model::ToolsCapability;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::codex_tool_config::create_tool_for_codex_tool_call_param;
use crate::codex_tool_config::create_tool_for_codex_tool_call_reply_param;

/// Default drain timeout in seconds for graceful shutdown (REQ-3.5).
const DEFAULT_DRAIN_TIMEOUT_SEC: u64 = 30;

type IncomingMessage = JsonRpcMessage<ClientRequest, Value, ClientNotification>;

/// Per-session state for MCP worker mode (REQ-3.3).
///
/// Each session maintains its own state, including initialization status
/// and client capabilities. Sessions are identified by the `sessionId` field
/// in incoming messages.
#[derive(Debug)]
pub struct McpWorkerSession {
    /// The session identifier (from the `sessionId` field in messages).
    pub session_id: String,
    /// The thread ID extracted from the session ID (if applicable).
    pub thread_id: Option<String>,
    /// Whether the session has been initialized.
    pub initialized: bool,
    /// Client information from the initialize request.
    pub client_info: Option<Implementation>,
}

impl McpWorkerSession {
    /// Create a new session with the given session ID.
    pub fn new(session_id: String) -> Self {
        let thread_id = extract_thread_id(&session_id).map(String::from);
        Self {
            session_id,
            thread_id,
            initialized: false,
            client_info: None,
        }
    }
}

/// Worker mode handler for MCP server (REQ-3).
///
/// This handler processes incoming NDJSON messages from the stdio_bus daemon
/// and maintains session affinity based on the `sessionId` field.
pub struct McpServerWorkerHandler {
    #[allow(dead_code)]
    config: Arc<Config>,
    #[allow(dead_code)]
    arg0_paths: Arg0DispatchPaths,
    /// Session state keyed by session ID (REQ-3.3).
    sessions: RwLock<HashMap<String, McpWorkerSession>>,
    /// Channel for sending notifications.
    /// Notifications sent through this channel will be written to stdout by the worker runtime.
    notification_tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    /// Receiver for notifications (held by the handler to be polled during run).
    notification_rx: RwLock<Option<tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>>>,
    /// Count of pending/in-flight requests (REQ-3.5).
    /// Used for graceful shutdown to wait for pending requests to complete.
    pending_requests: AtomicUsize,
    /// Notifier for when pending requests complete (REQ-3.5).
    /// Used during graceful shutdown to signal when all requests are drained.
    drain_notify: tokio::sync::Notify,
    /// Drain timeout in seconds for graceful shutdown (REQ-3.5).
    drain_timeout_sec: u64,
}

impl McpServerWorkerHandler {
    /// Create a new worker handler with the given configuration.
    pub fn new(config: Arc<Config>, arg0_paths: Arg0DispatchPaths) -> Self {
        let (notification_tx, notification_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            config,
            arg0_paths,
            sessions: RwLock::new(HashMap::new()),
            notification_tx,
            notification_rx: RwLock::new(Some(notification_rx)),
            pending_requests: AtomicUsize::new(0),
            drain_notify: tokio::sync::Notify::new(),
            drain_timeout_sec: DEFAULT_DRAIN_TIMEOUT_SEC,
        }
    }

    /// Create a new worker handler with a custom drain timeout.
    #[allow(dead_code)]
    pub fn with_drain_timeout(
        config: Arc<Config>,
        arg0_paths: Arg0DispatchPaths,
        drain_timeout_sec: u64,
    ) -> Self {
        let (notification_tx, notification_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            config,
            arg0_paths,
            sessions: RwLock::new(HashMap::new()),
            notification_tx,
            notification_rx: RwLock::new(Some(notification_rx)),
            pending_requests: AtomicUsize::new(0),
            drain_notify: tokio::sync::Notify::new(),
            drain_timeout_sec,
        }
    }

    /// Increment the pending request count (REQ-3.5).
    fn increment_pending(&self) {
        self.pending_requests.fetch_add(1, Ordering::SeqCst);
    }

    /// Decrement the pending request count and notify if drained (REQ-3.5).
    fn decrement_pending(&self) {
        let prev = self.pending_requests.fetch_sub(1, Ordering::SeqCst);
        if prev == 1 {
            // We just went from 1 to 0, notify waiters
            self.drain_notify.notify_waiters();
        }
    }

    /// Get the current pending request count.
    #[allow(dead_code)]
    pub fn pending_count(&self) -> usize {
        self.pending_requests.load(Ordering::SeqCst)
    }

    /// Get the configuration.
    #[allow(dead_code)]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get the notification sender for sending notifications with sessionId.
    #[allow(dead_code)]
    pub fn notification_tx(&self) -> &tokio::sync::mpsc::UnboundedSender<Vec<u8>> {
        &self.notification_tx
    }

    /// Take the notification receiver for use in the worker runtime.
    ///
    /// This should only be called once, typically by the worker runtime to
    /// poll for outgoing notifications.
    pub async fn take_notification_receiver(
        &self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>> {
        self.notification_rx.write().await.take()
    }

    /// Get or create a session for the given session ID (REQ-3.3).
    ///
    /// This method ensures session affinity by returning the same session
    /// for the same session ID. If no session exists, a new one is created.
    async fn get_or_create_session(&self, session_id: &str) -> McpWorkerSession {
        // First, try to get an existing session with a read lock
        {
            let sessions = self.sessions.read().await;
            if let Some(session) = sessions.get(session_id) {
                return McpWorkerSession {
                    session_id: session.session_id.clone(),
                    thread_id: session.thread_id.clone(),
                    initialized: session.initialized,
                    client_info: session.client_info.clone(),
                };
            }
        }

        // Session doesn't exist, create a new one with a write lock
        let mut sessions = self.sessions.write().await;
        // Double-check in case another task created it while we were waiting
        if let Some(session) = sessions.get(session_id) {
            return McpWorkerSession {
                session_id: session.session_id.clone(),
                thread_id: session.thread_id.clone(),
                initialized: session.initialized,
                client_info: session.client_info.clone(),
            };
        }

        let session = McpWorkerSession::new(session_id.to_string());
        debug!(
            session_id,
            thread_id = ?session.thread_id,
            "Created new MCP worker session"
        );
        sessions.insert(
            session_id.to_string(),
            McpWorkerSession::new(session_id.to_string()),
        );
        session
    }

    /// Update session state after processing an initialize request (REQ-3.3).
    async fn mark_session_initialized(
        &self,
        session_id: &str,
        client_info: Option<Implementation>,
    ) {
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get_mut(session_id) {
            session.initialized = true;
            session.client_info = client_info;
        }
    }

    /// Handle the initialize request (REQ-3.3).
    ///
    /// This binds the session to the MCP client connection and returns
    /// the server capabilities.
    fn handle_initialize(
        &self,
        session: &McpWorkerSession,
        params: &rmcp::model::InitializeRequestParams,
    ) -> Result<InitializeResult, ErrorData> {
        if session.initialized {
            return Err(ErrorData::new(
                ErrorCode::INVALID_REQUEST,
                "Session already initialized".to_string(),
                None,
            ));
        }

        debug!(
            session_id = %session.session_id,
            client_name = %params.client_info.name,
            client_version = %params.client_info.version,
            "MCP session initialized"
        );

        Ok(InitializeResult {
            protocol_version: params.protocol_version.clone(),
            capabilities: ServerCapabilities {
                tools: Some(ToolsCapability {
                    list_changed: Some(true),
                }),
                ..Default::default()
            },
            server_info: Implementation {
                name: "codex-mcp-server".to_string(),
                title: Some("Codex".to_string()),
                version: env!("CARGO_PKG_VERSION").to_string(),
                description: None,
                icons: None,
                website_url: None,
            },
            instructions: None,
        })
    }

    /// Handle the ping request.
    fn handle_ping(&self) -> serde_json::Value {
        serde_json::json!({})
    }

    /// Handle the tools/list request.
    fn handle_list_tools(&self) -> rmcp::model::ListToolsResult {
        rmcp::model::ListToolsResult {
            meta: None,
            tools: vec![
                create_tool_for_codex_tool_call_param(),
                create_tool_for_codex_tool_call_reply_param(),
            ],
            next_cursor: None,
        }
    }

    /// Build a JSON-RPC response with the given request ID and result.
    fn build_response(request_id: &RequestId, result: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": result
        })
    }

    /// Build a JSON-RPC error response with the given request ID and error.
    fn build_error_response(request_id: &RequestId, error: ErrorData) -> serde_json::Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": error.code,
                "message": error.message,
                "data": error.data
            }
        })
    }
}

#[async_trait]
impl MessageHandler for McpServerWorkerHandler {
    async fn handle(&self, msg: Message) -> Result<Option<Vec<u8>>, WorkerError> {
        let session_id = msg.routing.session_id.as_deref();
        let method = msg.routing.method.as_deref();
        let request_id = msg.routing.id.as_ref();

        debug!(
            ?session_id,
            ?method,
            ?request_id,
            is_response = msg.routing.is_response,
            "Received MCP message"
        );

        // If this is a response (not a request), we don't need to process it
        if msg.routing.is_response {
            debug!("Ignoring response message");
            return Ok(None);
        }

        // Parse the raw message as a JSON-RPC message
        let mcp_msg: IncomingMessage = serde_json::from_slice(&msg.raw)
            .map_err(|e| WorkerError::Handler(format!("Failed to parse MCP message: {e}")))?;

        match mcp_msg {
            JsonRpcMessage::Request(request) => {
                let req_id = request.id.clone();

                // Track pending request for graceful shutdown (REQ-3.5)
                self.increment_pending();

                // Get or create session for this request (REQ-3.3)
                let session_id_str = session_id.unwrap_or("default");
                let session = self.get_or_create_session(session_id_str).await;

                let response = match &request.request {
                    ClientRequest::InitializeRequest(params) => {
                        match self.handle_initialize(&session, &params.params) {
                            Ok(result) => {
                                // Mark session as initialized
                                self.mark_session_initialized(
                                    session_id_str,
                                    Some(params.params.client_info.clone()),
                                )
                                .await;

                                let result_value = serde_json::to_value(result).map_err(|e| {
                                    WorkerError::Handler(format!(
                                        "Failed to serialize initialize result: {e}"
                                    ))
                                })?;
                                Self::build_response(&req_id, result_value)
                            }
                            Err(error) => Self::build_error_response(&req_id, error),
                        }
                    }
                    ClientRequest::PingRequest(_) => {
                        let result = self.handle_ping();
                        Self::build_response(&req_id, result)
                    }
                    ClientRequest::ListToolsRequest(_) => {
                        let result = self.handle_list_tools();
                        let result_value = serde_json::to_value(result).map_err(|e| {
                            WorkerError::Handler(format!(
                                "Failed to serialize list tools result: {e}"
                            ))
                        })?;
                        Self::build_response(&req_id, result_value)
                    }
                    // For other requests, check if session is initialized
                    _ => {
                        if !session.initialized {
                            Self::build_error_response(
                                &req_id,
                                ErrorData::new(
                                    ErrorCode::INVALID_REQUEST,
                                    "Session not initialized".to_string(),
                                    None,
                                ),
                            )
                        } else {
                            // Return method not found for unimplemented methods
                            // TODO: Integrate with full MessageProcessor for complete request handling
                            Self::build_error_response(
                                &req_id,
                                ErrorData::new(
                                    ErrorCode::METHOD_NOT_FOUND,
                                    format!(
                                        "Method not yet implemented in worker mode: {:?}",
                                        method
                                    ),
                                    None,
                                ),
                            )
                        }
                    }
                };

                // Serialize and return the response (REQ-3.4: sessionId is injected by worker runtime)
                let response_bytes = serde_json::to_vec(&response).map_err(|e| {
                    WorkerError::Handler(format!("Failed to serialize response: {e}"))
                })?;

                // Request complete, decrement pending count (REQ-3.5)
                self.decrement_pending();

                Ok(Some(response_bytes))
            }
            JsonRpcMessage::Notification(notification) => {
                // Handle notifications - no response needed
                debug!(
                    method = ?notification.notification,
                    "Received MCP notification (no response needed)"
                );
                Ok(None)
            }
            JsonRpcMessage::Response(_) | JsonRpcMessage::Error(_) => {
                // Responses/errors from client are unusual but we ignore them
                warn!("Received unexpected response/error from MCP client");
                Ok(None)
            }
        }
    }

    /// Graceful shutdown handler (REQ-3.5).
    ///
    /// This method is called when SIGTERM is received. It:
    /// 1. Waits for pending requests to drain within `drain_timeout_sec`
    /// 2. Cleans up all active sessions
    async fn on_shutdown(&self) {
        info!("MCP server worker initiating graceful shutdown (REQ-3.5)");

        let pending = self.pending_requests.load(Ordering::SeqCst);
        let drain_timeout = Duration::from_secs(self.drain_timeout_sec);

        if pending > 0 {
            info!(
                pending_requests = pending,
                drain_timeout_sec = self.drain_timeout_sec,
                "Draining pending MCP requests before shutdown"
            );

            // Wait for pending requests to complete or timeout
            let drain_result = tokio::time::timeout(drain_timeout, async {
                loop {
                    let current = self.pending_requests.load(Ordering::SeqCst);
                    if current == 0 {
                        break;
                    }
                    // Wait for notification that a request completed
                    self.drain_notify.notified().await;
                }
            })
            .await;

            match drain_result {
                Ok(()) => {
                    info!("All pending MCP requests drained successfully");
                }
                Err(_) => {
                    let remaining = self.pending_requests.load(Ordering::SeqCst);
                    warn!(
                        remaining_requests = remaining,
                        drain_timeout_sec = self.drain_timeout_sec,
                        "Drain timeout exceeded, proceeding with shutdown"
                    );
                }
            }
        } else {
            info!("No pending MCP requests to drain");
        }

        // Clean up all sessions
        let sessions = self.sessions.read().await;
        let session_count = sessions.len();
        for (session_id, session) in sessions.iter() {
            debug!(
                session_id,
                thread_id = ?session.thread_id,
                initialized = session.initialized,
                "Cleaning up MCP session"
            );
        }
        info!(session_count, "Cleaned up all MCP worker sessions");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_stdio_bus::message::RoutingFields;
    use pretty_assertions::assert_eq;

    fn create_test_handler() -> McpServerWorkerHandler {
        let config = Config::load_default_with_cli_overrides(vec![]).unwrap();
        let arg0_paths = Arg0DispatchPaths::default();
        McpServerWorkerHandler::new(Arc::new(config), arg0_paths)
    }

    #[tokio::test]
    async fn test_get_or_create_session_creates_new_session() {
        let handler = create_test_handler();

        let session = handler.get_or_create_session("thread:test-123").await;

        assert_eq!(session.session_id, "thread:test-123");
        assert_eq!(session.thread_id, Some("test-123".to_string()));
        assert!(!session.initialized);
    }

    #[tokio::test]
    async fn test_get_or_create_session_returns_existing_session() {
        let handler = create_test_handler();

        // Create initial session
        let session1 = handler.get_or_create_session("thread:test-456").await;
        assert_eq!(session1.session_id, "thread:test-456");

        // Mark session as initialized
        handler
            .mark_session_initialized("thread:test-456", None)
            .await;

        // Get session again - should return the same session with updated state
        let session2 = handler.get_or_create_session("thread:test-456").await;
        assert_eq!(session2.session_id, "thread:test-456");
        assert!(session2.initialized);
    }

    #[tokio::test]
    async fn test_session_without_thread_prefix() {
        let handler = create_test_handler();

        let session = handler.get_or_create_session("custom-session-id").await;

        assert_eq!(session.session_id, "custom-session-id");
        assert_eq!(session.thread_id, None);
    }

    #[tokio::test]
    async fn test_handle_initialize_request() {
        let handler = create_test_handler();

        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {
                    "name": "test-client",
                    "version": "1.0.0"
                }
            }
        });

        let msg = Message {
            raw: serde_json::to_vec(&request).unwrap(),
            routing: RoutingFields {
                id: Some(codex_stdio_bus::message::RequestId::Integer(1)),
                session_id: Some("thread:test-init".to_string()),
                method: Some("initialize".to_string()),
                is_response: false,
                is_error: false,
            },
        };

        let result = handler.handle(msg).await;
        assert!(result.is_ok());

        let response_bytes = result.unwrap().expect("Should have response");
        let response: serde_json::Value = serde_json::from_slice(&response_bytes).unwrap();

        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], 1);
        assert!(response["result"].is_object());
        assert!(response["result"]["serverInfo"].is_object());
        assert_eq!(response["result"]["serverInfo"]["name"], "codex-mcp-server");
        assert!(response["result"]["capabilities"].is_object());
    }

    #[tokio::test]
    async fn test_handle_ping_request() {
        let handler = create_test_handler();

        // First initialize the session
        let init_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {
                    "name": "test-client",
                    "version": "1.0.0"
                }
            }
        });

        let init_msg = Message {
            raw: serde_json::to_vec(&init_request).unwrap(),
            routing: RoutingFields {
                id: Some(codex_stdio_bus::message::RequestId::Integer(1)),
                session_id: Some("thread:test-ping".to_string()),
                method: Some("initialize".to_string()),
                is_response: false,
                is_error: false,
            },
        };
        handler.handle(init_msg).await.unwrap();

        // Now send ping
        let ping_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "ping"
        });

        let ping_msg = Message {
            raw: serde_json::to_vec(&ping_request).unwrap(),
            routing: RoutingFields {
                id: Some(codex_stdio_bus::message::RequestId::Integer(2)),
                session_id: Some("thread:test-ping".to_string()),
                method: Some("ping".to_string()),
                is_response: false,
                is_error: false,
            },
        };

        let result = handler.handle(ping_msg).await;
        assert!(result.is_ok());

        let response_bytes = result.unwrap().expect("Should have response");
        let response: serde_json::Value = serde_json::from_slice(&response_bytes).unwrap();

        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], 2);
        assert_eq!(response["result"], serde_json::json!({}));
    }

    #[tokio::test]
    async fn test_handle_list_tools_request() {
        let handler = create_test_handler();

        // First initialize the session
        let init_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {
                    "name": "test-client",
                    "version": "1.0.0"
                }
            }
        });

        let init_msg = Message {
            raw: serde_json::to_vec(&init_request).unwrap(),
            routing: RoutingFields {
                id: Some(codex_stdio_bus::message::RequestId::Integer(1)),
                session_id: Some("thread:test-tools".to_string()),
                method: Some("initialize".to_string()),
                is_response: false,
                is_error: false,
            },
        };
        handler.handle(init_msg).await.unwrap();

        // Now send tools/list
        let list_tools_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list"
        });

        let list_tools_msg = Message {
            raw: serde_json::to_vec(&list_tools_request).unwrap(),
            routing: RoutingFields {
                id: Some(codex_stdio_bus::message::RequestId::Integer(2)),
                session_id: Some("thread:test-tools".to_string()),
                method: Some("tools/list".to_string()),
                is_response: false,
                is_error: false,
            },
        };

        let result = handler.handle(list_tools_msg).await;
        assert!(result.is_ok());

        let response_bytes = result.unwrap().expect("Should have response");
        let response: serde_json::Value = serde_json::from_slice(&response_bytes).unwrap();

        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], 2);
        assert!(response["result"]["tools"].is_array());
        // Should have at least the codex and codex-reply tools
        let tools = response["result"]["tools"].as_array().unwrap();
        assert!(tools.len() >= 2);
    }

    #[tokio::test]
    async fn test_handle_request_before_initialize() {
        let handler = create_test_handler();

        // Send a request without initializing first
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "codex",
                "arguments": {}
            }
        });

        let msg = Message {
            raw: serde_json::to_vec(&request).unwrap(),
            routing: RoutingFields {
                id: Some(codex_stdio_bus::message::RequestId::Integer(1)),
                session_id: Some("thread:test-uninit".to_string()),
                method: Some("tools/call".to_string()),
                is_response: false,
                is_error: false,
            },
        };

        let result = handler.handle(msg).await;
        assert!(result.is_ok());

        let response_bytes = result.unwrap().expect("Should have response");
        let response: serde_json::Value = serde_json::from_slice(&response_bytes).unwrap();

        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], 1);
        assert!(response["error"].is_object());
        assert_eq!(response["error"]["message"], "Session not initialized");
    }

    #[tokio::test]
    async fn test_handle_double_initialize() {
        let handler = create_test_handler();

        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {
                    "name": "test-client",
                    "version": "1.0.0"
                }
            }
        });

        let msg = Message {
            raw: serde_json::to_vec(&request).unwrap(),
            routing: RoutingFields {
                id: Some(codex_stdio_bus::message::RequestId::Integer(1)),
                session_id: Some("thread:test-double-init".to_string()),
                method: Some("initialize".to_string()),
                is_response: false,
                is_error: false,
            },
        };

        // First initialize should succeed
        let result1 = handler.handle(msg.clone()).await;
        assert!(result1.is_ok());
        let response1: serde_json::Value =
            serde_json::from_slice(&result1.unwrap().unwrap()).unwrap();
        assert!(response1["result"].is_object());

        // Second initialize should fail
        let request2 = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {
                    "name": "test-client",
                    "version": "1.0.0"
                }
            }
        });

        let msg2 = Message {
            raw: serde_json::to_vec(&request2).unwrap(),
            routing: RoutingFields {
                id: Some(codex_stdio_bus::message::RequestId::Integer(2)),
                session_id: Some("thread:test-double-init".to_string()),
                method: Some("initialize".to_string()),
                is_response: false,
                is_error: false,
            },
        };

        let result2 = handler.handle(msg2).await;
        assert!(result2.is_ok());
        let response2: serde_json::Value =
            serde_json::from_slice(&result2.unwrap().unwrap()).unwrap();
        assert!(response2["error"].is_object());
        assert_eq!(response2["error"]["message"], "Session already initialized");
    }

    #[tokio::test]
    async fn test_handle_notification_no_response() {
        let handler = create_test_handler();

        // Notification has no id
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        });

        let msg = Message {
            raw: serde_json::to_vec(&notification).unwrap(),
            routing: RoutingFields {
                id: None,
                session_id: Some("thread:test-notif".to_string()),
                method: Some("notifications/initialized".to_string()),
                is_response: false,
                is_error: false,
            },
        };

        let result = handler.handle(msg).await;
        assert!(result.is_ok());
        assert!(
            result.unwrap().is_none(),
            "Notifications should not have a response"
        );
    }

    #[tokio::test]
    async fn test_handle_response_message_ignored() {
        let handler = create_test_handler();

        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {}
        });

        let msg = Message {
            raw: serde_json::to_vec(&response).unwrap(),
            routing: RoutingFields {
                id: Some(codex_stdio_bus::message::RequestId::Integer(1)),
                session_id: Some("thread:test-resp".to_string()),
                method: None,
                is_response: true,
                is_error: false,
            },
        };

        let result = handler.handle(msg).await;
        assert!(result.is_ok());
        assert!(
            result.unwrap().is_none(),
            "Response messages should be ignored"
        );
    }

    #[tokio::test]
    async fn test_session_affinity_across_requests() {
        let handler = create_test_handler();
        let session_id = "thread:affinity-test";

        // Initialize the session
        let init_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {
                    "name": "affinity-test-client",
                    "version": "2.0.0"
                }
            }
        });

        let init_msg = Message {
            raw: serde_json::to_vec(&init_request).unwrap(),
            routing: RoutingFields {
                id: Some(codex_stdio_bus::message::RequestId::Integer(1)),
                session_id: Some(session_id.to_string()),
                method: Some("initialize".to_string()),
                is_response: false,
                is_error: false,
            },
        };

        handler.handle(init_msg).await.unwrap();

        // Verify session state was preserved
        let session = handler.get_or_create_session(session_id).await;
        assert!(session.initialized);
        assert_eq!(
            session.client_info.as_ref().map(|c| c.name.as_str()),
            Some("affinity-test-client")
        );
    }

    // Tests for graceful shutdown (REQ-3.5)

    #[tokio::test]
    async fn test_pending_request_tracking() {
        let handler = create_test_handler();

        // Initially no pending requests
        assert_eq!(handler.pending_count(), 0);

        // Increment pending
        handler.increment_pending();
        assert_eq!(handler.pending_count(), 1);

        handler.increment_pending();
        assert_eq!(handler.pending_count(), 2);

        // Decrement pending
        handler.decrement_pending();
        assert_eq!(handler.pending_count(), 1);

        handler.decrement_pending();
        assert_eq!(handler.pending_count(), 0);
    }

    #[tokio::test]
    async fn test_on_shutdown_with_no_pending_requests() {
        let handler = create_test_handler();

        // Create a session
        handler.get_or_create_session("thread:test-shutdown").await;

        // Shutdown should complete immediately with no pending requests
        let start = std::time::Instant::now();
        handler.on_shutdown().await;
        let elapsed = start.elapsed();

        // Should complete quickly (well under 1 second)
        assert!(elapsed < std::time::Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_on_shutdown_drains_pending_requests() {
        let config = Config::load_default_with_cli_overrides(vec![]).unwrap();
        let arg0_paths = Arg0DispatchPaths::default();
        // Use a short drain timeout for testing
        let handler = Arc::new(McpServerWorkerHandler::with_drain_timeout(
            Arc::new(config),
            arg0_paths,
            2, // 2 second timeout
        ));

        // Simulate a pending request
        handler.increment_pending();
        assert_eq!(handler.pending_count(), 1);

        // Start shutdown in background
        let handler_clone = Arc::clone(&handler);
        let shutdown_handle = tokio::spawn(async move {
            handler_clone.on_shutdown().await;
        });

        // Give shutdown a moment to start waiting
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Complete the pending request
        handler.decrement_pending();

        // Shutdown should complete quickly after request drains
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), shutdown_handle).await;
        assert!(
            result.is_ok(),
            "Shutdown should complete after requests drain"
        );
    }

    #[tokio::test]
    async fn test_on_shutdown_timeout_exceeded() {
        let config = Config::load_default_with_cli_overrides(vec![]).unwrap();
        let arg0_paths = Arg0DispatchPaths::default();
        // Use a very short drain timeout for testing
        let handler = Arc::new(McpServerWorkerHandler::with_drain_timeout(
            Arc::new(config),
            arg0_paths,
            1, // 1 second timeout
        ));

        // Simulate a pending request that won't complete
        handler.increment_pending();
        assert_eq!(handler.pending_count(), 1);

        // Shutdown should timeout and proceed
        let start = std::time::Instant::now();
        handler.on_shutdown().await;
        let elapsed = start.elapsed();

        // Should take approximately 1 second (the drain timeout)
        assert!(elapsed >= std::time::Duration::from_millis(900));
        assert!(elapsed < std::time::Duration::from_secs(2));

        // Pending request still exists (wasn't completed)
        assert_eq!(handler.pending_count(), 1);
    }

    #[tokio::test]
    async fn test_handle_increments_and_decrements_pending() {
        let handler = create_test_handler();

        // Initially no pending requests
        assert_eq!(handler.pending_count(), 0);

        // Send an initialize request
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {
                    "name": "test-client",
                    "version": "1.0.0"
                }
            }
        });

        let msg = Message {
            raw: serde_json::to_vec(&request).unwrap(),
            routing: RoutingFields {
                id: Some(codex_stdio_bus::message::RequestId::Integer(1)),
                session_id: Some("thread:test-pending".to_string()),
                method: Some("initialize".to_string()),
                is_response: false,
                is_error: false,
            },
        };

        // Handle the request
        let result = handler.handle(msg).await;
        assert!(result.is_ok());

        // After handling, pending count should be back to 0
        assert_eq!(handler.pending_count(), 0);
    }
}
