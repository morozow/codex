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
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::JSONRPCErrorError;
use codex_app_server_protocol::RequestId;
use codex_arg0::Arg0DispatchPaths;
use codex_core::config::Config;
use codex_stdio_bus::message::Message;
use codex_stdio_bus::session::extract_thread_id;
use codex_stdio_bus::worker::{MessageHandler, WorkerError};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::error_code::INVALID_REQUEST_ERROR_CODE;
use crate::message_processor::ConnectionSessionState;

/// Per-session state for worker mode (REQ-2.5).
///
/// Each session maintains its own state, similar to how the existing app-server
/// manages per-connection state. Sessions are identified by the `sessionId` field
/// in incoming messages.
#[derive(Debug)]
pub struct WorkerSession {
    /// The session identifier (from the `sessionId` field in messages).
    pub session_id: String,
    /// The thread ID extracted from the session ID (if applicable).
    pub thread_id: Option<String>,
    /// Connection session state for message processing.
    pub connection_state: ConnectionSessionState,
}

impl WorkerSession {
    /// Create a new session with the given session ID.
    pub fn new(session_id: String) -> Self {
        let thread_id = extract_thread_id(&session_id).map(String::from);
        Self {
            session_id,
            thread_id,
            connection_state: ConnectionSessionState::default(),
        }
    }
}

/// Worker mode handler for app-server (REQ-2).
///
/// This handler processes incoming NDJSON messages from the stdio_bus daemon
/// and maintains session affinity based on the `sessionId` field.
pub struct AppServerWorkerHandler {
    #[allow(dead_code)]
    config: Arc<Config>,
    #[allow(dead_code)]
    arg0_paths: Arg0DispatchPaths,
    /// Session state keyed by session ID (REQ-2.5).
    sessions: RwLock<HashMap<String, WorkerSession>>,
}

impl AppServerWorkerHandler {
    /// Create a new worker handler with the given configuration.
    pub fn new(config: Arc<Config>, arg0_paths: Arg0DispatchPaths) -> Self {
        Self {
            config,
            arg0_paths,
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Get the configuration.
    #[allow(dead_code)]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get or create a session for the given session ID (REQ-2.5).
    ///
    /// This method ensures session affinity by returning the same session
    /// for the same session ID. If no session exists, a new one is created.
    async fn get_or_create_session(&self, session_id: &str) -> WorkerSession {
        // First, try to get an existing session with a read lock
        {
            let sessions = self.sessions.read().await;
            if let Some(session) = sessions.get(session_id) {
                return WorkerSession {
                    session_id: session.session_id.clone(),
                    thread_id: session.thread_id.clone(),
                    connection_state: session.connection_state.clone(),
                };
            }
        }

        // Session doesn't exist, create a new one with a write lock
        let mut sessions = self.sessions.write().await;
        // Double-check in case another task created it while we were waiting
        if let Some(session) = sessions.get(session_id) {
            return WorkerSession {
                session_id: session.session_id.clone(),
                thread_id: session.thread_id.clone(),
                connection_state: session.connection_state.clone(),
            };
        }

        let session = WorkerSession::new(session_id.to_string());
        debug!(
            session_id,
            thread_id = ?session.thread_id,
            "Created new worker session"
        );
        sessions.insert(
            session_id.to_string(),
            WorkerSession::new(session_id.to_string()),
        );
        session
    }

    /// Update session state after processing a request.
    async fn update_session_state(&self, session_id: &str, state: ConnectionSessionState) {
        let mut sessions = self.sessions.write().await;
        if let Some(session) = sessions.get_mut(session_id) {
            session.connection_state = state;
        }
    }

    /// Process a JSON-RPC request and return a response.
    ///
    /// This method handles the core request processing logic, delegating to
    /// the appropriate handler based on the request type.
    async fn process_request(
        &self,
        session: &mut WorkerSession,
        request: ClientRequest,
    ) -> Result<serde_json::Value, JSONRPCErrorError> {
        let request_id = request.id().clone();

        // Handle Initialize request specially
        if let ClientRequest::Initialize {
            request_id: _,
            params,
        } = &request
        {
            if session.connection_state.initialized {
                return Err(JSONRPCErrorError {
                    code: INVALID_REQUEST_ERROR_CODE,
                    message: "Already initialized".to_string(),
                    data: None,
                });
            }

            // Process initialization
            let (experimental_api_enabled, opt_out_notification_methods) =
                match &params.capabilities {
                    Some(capabilities) => (
                        capabilities.experimental_api,
                        capabilities
                            .opt_out_notification_methods
                            .clone()
                            .unwrap_or_default(),
                    ),
                    None => (false, Vec::new()),
                };

            session.connection_state.experimental_api_enabled = experimental_api_enabled;
            session.connection_state.opted_out_notification_methods =
                opt_out_notification_methods.into_iter().collect();
            session.connection_state.app_server_client_name = Some(params.client_info.name.clone());
            session.connection_state.client_version = Some(params.client_info.version.clone());
            session.connection_state.initialized = true;

            let user_agent = codex_core::default_client::get_codex_user_agent();
            let response = codex_app_server_protocol::InitializeResponse {
                user_agent,
                platform_family: std::env::consts::FAMILY.to_string(),
                platform_os: std::env::consts::OS.to_string(),
            };

            return serde_json::to_value(response).map_err(|e| JSONRPCErrorError {
                code: INVALID_REQUEST_ERROR_CODE,
                message: format!("Failed to serialize response: {e}"),
                data: None,
            });
        }

        // For non-initialize requests, check if initialized
        if !session.connection_state.initialized {
            return Err(JSONRPCErrorError {
                code: INVALID_REQUEST_ERROR_CODE,
                message: "Not initialized".to_string(),
                data: None,
            });
        }

        // For now, return an error for unimplemented methods
        // TODO: Integrate with full MessageProcessor for complete request handling
        Err(JSONRPCErrorError {
            code: -32601,
            message: format!(
                "Method not yet implemented in worker mode: request_id={request_id:?}"
            ),
            data: None,
        })
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
    fn build_error_response(request_id: &RequestId, error: JSONRPCErrorError) -> serde_json::Value {
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

        // If this is a response (not a request), we don't need to process it
        if msg.routing.is_response {
            debug!("Ignoring response message");
            return Ok(None);
        }

        // Parse the raw message as a JSON-RPC request
        let request_value: serde_json::Value = serde_json::from_slice(&msg.raw)
            .map_err(|e| WorkerError::Handler(format!("Failed to parse JSON: {e}")))?;

        // Extract the request ID from the parsed JSON (for error responses)
        let json_request_id = request_value
            .get("id")
            .cloned()
            .unwrap_or(serde_json::Value::Null);

        // If there's no request ID, this is a notification - no response needed
        if json_request_id.is_null() {
            debug!(?method, "Received notification (no response needed)");
            return Ok(None);
        }

        // Try to parse as a ClientRequest
        let client_request: ClientRequest = match serde_json::from_value(request_value.clone()) {
            Ok(req) => req,
            Err(e) => {
                warn!(error = %e, "Failed to parse ClientRequest");
                let error_response = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": json_request_id,
                    "error": {
                        "code": INVALID_REQUEST_ERROR_CODE,
                        "message": format!("Invalid request: {e}")
                    }
                });
                return Ok(Some(serde_json::to_vec(&error_response).map_err(|e| {
                    WorkerError::Handler(format!("Failed to serialize error response: {e}"))
                })?));
            }
        };

        let request_id = client_request.id().clone();

        // Get or create session for this request (REQ-2.5)
        let session_id_str = session_id.unwrap_or("default");
        let mut session = self.get_or_create_session(session_id_str).await;

        // Process the request
        let response = match self.process_request(&mut session, client_request).await {
            Ok(result) => Self::build_response(&request_id, result),
            Err(error) => Self::build_error_response(&request_id, error),
        };

        // Update session state
        self.update_session_state(session_id_str, session.connection_state)
            .await;

        // Serialize and return the response (REQ-2.6: sessionId is injected by worker runtime)
        let response_bytes = serde_json::to_vec(&response)
            .map_err(|e| WorkerError::Handler(format!("Failed to serialize response: {e}")))?;

        Ok(Some(response_bytes))
    }

    async fn on_shutdown(&self) {
        info!("App-server worker shutting down");

        // Clean up all sessions
        let sessions = self.sessions.read().await;
        let session_count = sessions.len();
        for (session_id, session) in sessions.iter() {
            debug!(
                session_id,
                thread_id = ?session.thread_id,
                initialized = session.connection_state.initialized,
                "Cleaning up session"
            );
        }
        info!(session_count, "Cleaned up all worker sessions");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_stdio_bus::message::RoutingFields;
    use pretty_assertions::assert_eq;

    fn create_test_handler() -> AppServerWorkerHandler {
        let config = Config::load_default_with_cli_overrides(vec![]).unwrap();
        let arg0_paths = Arg0DispatchPaths::default();
        AppServerWorkerHandler::new(Arc::new(config), arg0_paths)
    }

    #[tokio::test]
    async fn test_get_or_create_session_creates_new_session() {
        let handler = create_test_handler();

        let session = handler.get_or_create_session("thread:test-123").await;

        assert_eq!(session.session_id, "thread:test-123");
        assert_eq!(session.thread_id, Some("test-123".to_string()));
        assert!(!session.connection_state.initialized);
    }

    #[tokio::test]
    async fn test_get_or_create_session_returns_existing_session() {
        let handler = create_test_handler();

        // Create initial session
        let session1 = handler.get_or_create_session("thread:test-456").await;
        assert_eq!(session1.session_id, "thread:test-456");

        // Update session state
        let mut updated_state = session1.connection_state.clone();
        updated_state.initialized = true;
        handler
            .update_session_state("thread:test-456", updated_state)
            .await;

        // Get session again - should return the same session with updated state
        let session2 = handler.get_or_create_session("thread:test-456").await;
        assert_eq!(session2.session_id, "thread:test-456");
        assert!(session2.connection_state.initialized);
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
        assert!(response["result"]["userAgent"].is_string());
        assert!(response["result"]["platformFamily"].is_string());
        assert!(response["result"]["platformOs"].is_string());
    }

    #[tokio::test]
    async fn test_handle_request_before_initialize() {
        let handler = create_test_handler();

        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "thread/start",
            "params": {}
        });

        let msg = Message {
            raw: serde_json::to_vec(&request).unwrap(),
            routing: RoutingFields {
                id: Some(codex_stdio_bus::message::RequestId::Integer(1)),
                session_id: Some("thread:test-uninit".to_string()),
                method: Some("thread/start".to_string()),
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
        assert_eq!(response["error"]["message"], "Not initialized");
    }

    #[tokio::test]
    async fn test_handle_double_initialize() {
        let handler = create_test_handler();

        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
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
        assert_eq!(response2["error"]["message"], "Already initialized");
    }

    #[tokio::test]
    async fn test_handle_notification_no_response() {
        let handler = create_test_handler();

        // Notification has no id
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "some/notification",
            "params": {}
        });

        let msg = Message {
            raw: serde_json::to_vec(&notification).unwrap(),
            routing: RoutingFields {
                id: None,
                session_id: Some("thread:test-notif".to_string()),
                method: Some("some/notification".to_string()),
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
    async fn test_handle_invalid_json() {
        let handler = create_test_handler();

        let msg = Message {
            raw: b"not valid json".to_vec(),
            routing: RoutingFields {
                id: Some(codex_stdio_bus::message::RequestId::Integer(1)),
                session_id: Some("thread:test-invalid".to_string()),
                method: Some("test/method".to_string()),
                is_response: false,
                is_error: false,
            },
        };

        let result = handler.handle(msg).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), WorkerError::Handler(_)));
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
        assert!(session.connection_state.initialized);
        assert_eq!(
            session.connection_state.app_server_client_name,
            Some("affinity-test-client".to_string())
        );
        assert_eq!(
            session.connection_state.client_version,
            Some("2.0.0".to_string())
        );
    }
}
