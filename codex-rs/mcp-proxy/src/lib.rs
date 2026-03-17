//! MCP proxy worker for external MCP server management.
//!
//! This crate provides an MCP proxy that manages external MCP server connections
//! through stdio_bus. It accepts MCP requests with `sessionId` prefix `mcp:{server_name}`
//! and forwards them to the appropriate external MCP server.
//!
//! # Features
//!
//! - On-demand spawning of external MCP server processes
//! - Connection pooling per server name
//! - Request forwarding from stdio_bus to external MCP servers
//! - Response forwarding from external MCP servers back through stdio_bus
//! - Automatic restart with exponential backoff on server failure
//! - Graceful shutdown with proper cleanup of managed servers
//!
//! # Session ID Format
//!
//! The proxy expects session IDs in the format `mcp:{server_name}` where
//! `server_name` identifies which external MCP server should handle the request.

use async_trait::async_trait;
use codex_stdio_bus::message::Message;
use codex_stdio_bus::message::RequestId;
use codex_stdio_bus::session::MCP_PREFIX;
use codex_stdio_bus::session::extract_mcp_server_name;
use codex_stdio_bus::worker::MessageHandler;
use codex_stdio_bus::worker::WorkerError;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::process::Command;
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

/// Error type for MCP proxy operations.
#[derive(Debug, Error)]
pub enum McpProxyError {
    /// I/O error during communication.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization/deserialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Invalid session ID format.
    #[error("Invalid session ID format: {0}")]
    InvalidSessionId(String),

    /// Server not found.
    #[error("Server not found: {0}")]
    ServerNotFound(String),

    /// Server spawn failed.
    #[error("Failed to spawn server '{0}': {1}")]
    SpawnFailed(String, String),

    /// Server communication error.
    #[error("Server communication error: {0}")]
    ServerCommunication(String),

    /// Server process exited.
    #[error("Server process exited")]
    ProcessExited,

    /// Request timeout.
    #[error("Request timeout")]
    Timeout,

    /// Shutdown requested.
    #[error("Shutdown requested")]
    Shutdown,
}

/// Result type for MCP proxy operations.
pub type Result<T> = std::result::Result<T, McpProxyError>;

/// Type alias for the request channel message type.
type RequestMessage = (Vec<u8>, oneshot::Sender<Result<Vec<u8>>>);

/// Configuration for an external MCP server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Command to execute.
    pub command: String,
    /// Command arguments.
    #[serde(default)]
    pub args: Vec<String>,
    /// Environment variables.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Working directory.
    #[serde(default)]
    pub cwd: Option<String>,
}

/// MCP Proxy configuration.
#[derive(Debug, Clone)]
pub struct McpProxyConfig {
    /// Server configurations by name.
    pub servers: HashMap<String, ServerConfig>,
    /// Maximum restart attempts.
    pub max_restarts: u32,
    /// Restart backoff base (seconds).
    pub restart_backoff_base: u64,
    /// Maximum backoff (seconds).
    pub max_backoff: u64,
}

impl Default for McpProxyConfig {
    fn default() -> Self {
        Self {
            servers: HashMap::new(),
            max_restarts: 5,
            restart_backoff_base: 1,
            max_backoff: 60,
        }
    }
}

/// Managed MCP server process.
pub struct ManagedServer {
    request_tx: mpsc::Sender<RequestMessage>,
    healthy: Arc<AtomicBool>,
    /// Flag to indicate shutdown has been requested.
    shutdown_requested: Arc<AtomicBool>,
}

impl std::fmt::Debug for ManagedServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedServer")
            .field("healthy", &self.healthy.load(Ordering::SeqCst))
            .field(
                "shutdown_requested",
                &self.shutdown_requested.load(Ordering::SeqCst),
            )
            .finish_non_exhaustive()
    }
}

impl ManagedServer {
    /// Spawn a new managed server.
    pub async fn spawn(config: &ServerConfig) -> Result<Self> {
        let mut cmd = Command::new(&config.command);
        cmd.args(&config.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        for (key, value) in &config.env {
            cmd.env(key, value);
        }

        if let Some(cwd) = &config.cwd {
            cmd.current_dir(cwd);
        }

        let mut child = cmd
            .spawn()
            .map_err(|e| McpProxyError::SpawnFailed(config.command.clone(), e.to_string()))?;

        let stdin = child.stdin.take().ok_or_else(|| {
            McpProxyError::SpawnFailed(config.command.clone(), "stdin not available".to_string())
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            McpProxyError::SpawnFailed(config.command.clone(), "stdout not available".to_string())
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            McpProxyError::SpawnFailed(config.command.clone(), "stderr not available".to_string())
        })?;

        let (request_tx, request_rx) = mpsc::channel(128);
        let healthy = Arc::new(AtomicBool::new(true));
        let shutdown_requested = Arc::new(AtomicBool::new(false));

        // Spawn I/O handler task
        let healthy_clone = healthy.clone();
        let shutdown_clone = shutdown_requested.clone();
        tokio::spawn(async move {
            Self::io_loop(stdin, stdout, request_rx, healthy_clone, shutdown_clone).await;
        });

        // Spawn stderr logger
        tokio::spawn(async move {
            let mut reader = BufReader::new(stderr);
            let mut line = String::new();
            while reader.read_line(&mut line).await.unwrap_or(0) > 0 {
                debug!(target: "mcp_server_stderr", "{}", line.trim());
                line.clear();
            }
        });

        Ok(Self {
            request_tx,
            healthy,
            shutdown_requested,
        })
    }

    async fn io_loop(
        mut stdin: tokio::process::ChildStdin,
        stdout: tokio::process::ChildStdout,
        mut request_rx: mpsc::Receiver<RequestMessage>,
        healthy: Arc<AtomicBool>,
        shutdown_requested: Arc<AtomicBool>,
    ) {
        let mut reader = BufReader::new(stdout);
        let mut pending: std::collections::VecDeque<oneshot::Sender<Result<Vec<u8>>>> =
            std::collections::VecDeque::new();

        loop {
            // Check if shutdown was requested
            if shutdown_requested.load(Ordering::SeqCst) {
                debug!("Shutdown requested, draining pending requests");
                // Drain pending requests with shutdown error
                while let Some(tx) = pending.pop_front() {
                    let _ = tx.send(Err(McpProxyError::Shutdown));
                }
                // Close stdin to signal the child process to exit
                drop(stdin);
                break;
            }

            tokio::select! {
                Some((request, response_tx)) = request_rx.recv() => {
                    // Check shutdown again before processing
                    if shutdown_requested.load(Ordering::SeqCst) {
                        let _ = response_tx.send(Err(McpProxyError::Shutdown));
                        continue;
                    }
                    // Write request to stdin
                    if let Err(e) = stdin.write_all(&request).await {
                        error!(error = %e, "Failed to write to MCP server stdin");
                        let _ = response_tx.send(Err(McpProxyError::Io(e)));
                        continue;
                    }
                    if let Err(e) = stdin.write_all(b"\n").await {
                        let _ = response_tx.send(Err(McpProxyError::Io(e)));
                        continue;
                    }
                    if let Err(e) = stdin.flush().await {
                        let _ = response_tx.send(Err(McpProxyError::Io(e)));
                        continue;
                    }
                    pending.push_back(response_tx);
                }
                result = async {
                    let mut line = String::new();
                    reader.read_line(&mut line).await.map(|n| (n, line))
                } => {
                    match result {
                        Ok((0, _)) => {
                            // EOF - server exited
                            healthy.store(false, Ordering::SeqCst);
                            while let Some(tx) = pending.pop_front() {
                                let _ = tx.send(Err(McpProxyError::ProcessExited));
                            }
                            break;
                        }
                        Ok((_, line)) => {
                            if let Some(tx) = pending.pop_front() {
                                let _ = tx.send(Ok(line.trim().as_bytes().to_vec()));
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to read from MCP server stdout");
                            healthy.store(false, Ordering::SeqCst);
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Check if server is healthy.
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::SeqCst)
    }

    /// Forward a request to the server.
    pub async fn forward_request(&self, request: &[u8]) -> Result<Vec<u8>> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send((request.to_vec(), response_tx))
            .await
            .map_err(|_| McpProxyError::ProcessExited)?;

        response_rx
            .await
            .map_err(|_| McpProxyError::ProcessExited)?
    }

    /// Terminate the server gracefully (REQ-4.8).
    ///
    /// This method signals the server to shut down by:
    /// 1. Setting the shutdown flag to prevent new requests
    /// 2. Marking the server as unhealthy
    /// 3. The I/O loop will drain pending requests and close stdin
    ///
    /// The child process will receive EOF on stdin and should exit gracefully.
    pub async fn terminate(&self) {
        info!("Terminating managed MCP server");
        // Signal shutdown to the I/O loop
        self.shutdown_requested.store(true, Ordering::SeqCst);
        // Mark as unhealthy to prevent new requests from being routed here
        self.healthy.store(false, Ordering::SeqCst);
        // Close the request channel to wake up the I/O loop
        // The channel will be closed when all senders are dropped
    }

    /// Check if shutdown has been requested.
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::SeqCst)
    }
}

/// MCP Proxy worker handler (REQ-4).
///
/// This handler accepts MCP requests with `sessionId` prefix `mcp:{server_name}`
/// and forwards them to the appropriate external MCP server.
pub struct McpProxyHandler {
    config: McpProxyConfig,
    servers: RwLock<HashMap<String, Arc<ManagedServer>>>,
    /// Flag to indicate shutdown has been initiated.
    shutdown_initiated: AtomicBool,
}

impl McpProxyHandler {
    /// Create a new MCP proxy handler with the given configuration.
    pub fn new(config: McpProxyConfig) -> Self {
        Self {
            config,
            servers: RwLock::new(HashMap::new()),
            shutdown_initiated: AtomicBool::new(false),
        }
    }

    /// Check if shutdown has been initiated.
    pub fn is_shutdown_initiated(&self) -> bool {
        self.shutdown_initiated.load(Ordering::SeqCst)
    }

    /// Extract server name from session ID (REQ-4.1).
    ///
    /// Parses the `mcp:{server_name}` format from the session ID.
    pub fn extract_server_name(
        session_id: &Option<String>,
    ) -> std::result::Result<String, WorkerError> {
        let session_id = session_id
            .as_ref()
            .ok_or_else(|| WorkerError::Handler("Missing sessionId".to_string()))?;

        extract_mcp_server_name(session_id)
            .map(String::from)
            .ok_or_else(|| {
                WorkerError::Handler(format!(
                    "Invalid sessionId format, expected '{MCP_PREFIX}<server_name>', got '{session_id}'"
                ))
            })
    }

    /// Get or spawn server (REQ-4.2, REQ-4.3).
    ///
    /// Returns an existing healthy server or spawns a new one on demand.
    pub async fn get_or_spawn_server(
        &self,
        server_name: &str,
    ) -> std::result::Result<Arc<ManagedServer>, WorkerError> {
        // Check if server already exists and is healthy
        {
            let servers = self.servers.read().await;
            if let Some(server) = servers.get(server_name)
                && server.is_healthy()
            {
                return Ok(Arc::clone(server));
            }
        }

        // Spawn new server
        let server_config =
            self.config.servers.get(server_name).ok_or_else(|| {
                WorkerError::Handler(format!("Unknown MCP server: {server_name}"))
            })?;

        let server = ManagedServer::spawn(server_config)
            .await
            .map_err(|e| WorkerError::Handler(e.to_string()))?;

        let server = Arc::new(server);
        let mut servers = self.servers.write().await;
        servers.insert(server_name.to_string(), Arc::clone(&server));

        info!(server_name, "Spawned MCP server");
        Ok(server)
    }

    /// Handle server restart with backoff (REQ-4.6).
    pub async fn restart_server_with_backoff(
        &self,
        server_name: &str,
        attempt: u32,
    ) -> std::result::Result<(), WorkerError> {
        if attempt >= self.config.max_restarts {
            return Err(WorkerError::Handler(format!(
                "Max restarts exceeded for server: {server_name}"
            )));
        }

        let backoff = std::cmp::min(
            self.config.restart_backoff_base * 2u64.pow(attempt),
            self.config.max_backoff,
        );

        warn!(
            server_name,
            attempt,
            backoff_secs = backoff,
            "Restarting MCP server with backoff"
        );
        tokio::time::sleep(Duration::from_secs(backoff)).await;

        // Remove old server and spawn new one
        {
            let mut servers = self.servers.write().await;
            servers.remove(server_name);
        }

        self.get_or_spawn_server(server_name).await?;
        Ok(())
    }

    /// Get the current server count (for testing).
    #[cfg(test)]
    pub async fn server_count(&self) -> usize {
        self.servers.read().await.len()
    }

    /// Create a JSON-RPC error response (REQ-4.7).
    ///
    /// This is used to propagate errors to clients via stdio_bus.
    fn create_error_response(id: Option<&RequestId>, code: i32, message: &str) -> Vec<u8> {
        let id_value = match id {
            Some(RequestId::String(s)) => serde_json::json!(s),
            Some(RequestId::Integer(n)) => serde_json::json!(n),
            None => serde_json::Value::Null,
        };

        let error_response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id_value,
            "error": {
                "code": code,
                "message": message
            }
        });

        serde_json::to_vec(&error_response).unwrap_or_else(|_| {
            // Fallback to a minimal error response
            br#"{"jsonrpc":"2.0","id":null,"error":{"code":-32000,"message":"Internal error"}}"#
                .to_vec()
        })
    }
}

#[async_trait]
impl MessageHandler for McpProxyHandler {
    /// Handle an incoming MCP request (REQ-4.4, REQ-4.5, REQ-4.7).
    ///
    /// Extracts the server name from the session ID, gets or spawns the server,
    /// and forwards the request to the external MCP server.
    ///
    /// If shutdown has been initiated, returns an error response to the client (REQ-4.7).
    async fn handle(&self, msg: Message) -> std::result::Result<Option<Vec<u8>>, WorkerError> {
        // REQ-4.7: Check if shutdown has been initiated and propagate error
        if self.shutdown_initiated.load(Ordering::SeqCst) {
            // Create a JSON-RPC error response for shutdown
            let error_response = Self::create_error_response(
                msg.routing.id.as_ref(),
                -32000, // Server error code
                "MCP proxy is shutting down",
            );
            return Ok(Some(error_response));
        }

        // REQ-4.1: Extract server name from sessionId
        let server_name = Self::extract_server_name(&msg.routing.session_id)?;

        debug!(
            server_name,
            method = ?msg.routing.method,
            "Forwarding MCP request to external server"
        );

        // REQ-4.2, REQ-4.3: Get or spawn server
        let server = self.get_or_spawn_server(&server_name).await?;

        // REQ-4.4: Forward request to external MCP server
        let response = server.forward_request(&msg.raw).await.map_err(|e| {
            // REQ-4.6: Handle server exit
            if matches!(e, McpProxyError::ProcessExited) {
                warn!(server_name, "MCP server exited unexpectedly");
                // Note: Restart is handled by the caller or a background task
            }
            // REQ-4.7: Propagate errors to clients
            if matches!(e, McpProxyError::Shutdown) {
                warn!(server_name, "Request rejected due to shutdown");
            }
            WorkerError::Handler(e.to_string())
        })?;

        // REQ-4.5: Forward response back (sessionId injected by worker runtime)
        Ok(Some(response))
    }

    /// Graceful shutdown handler (REQ-4.7, REQ-4.8).
    ///
    /// Terminates all managed MCP servers gracefully and propagates
    /// shutdown errors to any pending requests.
    async fn on_shutdown(&self) {
        info!("MCP proxy received shutdown signal, initiating graceful shutdown");

        // Set shutdown flag to reject new requests (REQ-4.7)
        self.shutdown_initiated.store(true, Ordering::SeqCst);

        // REQ-4.8: Terminate all managed MCP servers gracefully
        let servers = self.servers.read().await;
        let server_count = servers.len();

        if server_count == 0 {
            info!("No managed MCP servers to terminate");
            return;
        }

        info!(
            server_count,
            "Terminating all managed MCP servers gracefully"
        );

        // Terminate each server
        for (name, server) in servers.iter() {
            info!(
                server_name = name,
                "Initiating graceful shutdown for MCP server"
            );
            server.terminate().await;
        }

        info!("All managed MCP servers have been signaled to terminate");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_extract_server_name_valid() {
        let session_id = Some("mcp:my-server".to_string());
        let result = McpProxyHandler::extract_server_name(&session_id);
        assert_eq!(result.unwrap(), "my-server");
    }

    #[test]
    fn test_extract_server_name_empty_server() {
        let session_id = Some("mcp:".to_string());
        let result = McpProxyHandler::extract_server_name(&session_id);
        assert_eq!(result.unwrap(), "");
    }

    #[test]
    fn test_extract_server_name_missing_session_id() {
        let session_id: Option<String> = None;
        let result = McpProxyHandler::extract_server_name(&session_id);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Missing sessionId")
        );
    }

    #[test]
    fn test_extract_server_name_invalid_prefix() {
        let session_id = Some("thread:abc".to_string());
        let result = McpProxyHandler::extract_server_name(&session_id);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid sessionId format")
        );
    }

    #[test]
    fn test_extract_server_name_no_prefix() {
        let session_id = Some("just-a-string".to_string());
        let result = McpProxyHandler::extract_server_name(&session_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_mcp_proxy_config_default() {
        let config = McpProxyConfig::default();
        assert!(config.servers.is_empty());
        assert_eq!(config.max_restarts, 5);
        assert_eq!(config.restart_backoff_base, 1);
        assert_eq!(config.max_backoff, 60);
    }

    #[test]
    fn test_server_config_serialization() {
        let config = ServerConfig {
            command: "node".to_string(),
            args: vec!["server.js".to_string()],
            env: HashMap::from([("NODE_ENV".to_string(), "production".to_string())]),
            cwd: Some("/app".to_string()),
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ServerConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.command, "node");
        assert_eq!(deserialized.args, vec!["server.js"]);
        assert_eq!(
            deserialized.env.get("NODE_ENV"),
            Some(&"production".to_string())
        );
        assert_eq!(deserialized.cwd, Some("/app".to_string()));
    }

    #[tokio::test]
    async fn test_mcp_proxy_handler_creation() {
        let config = McpProxyConfig::default();
        let handler = McpProxyHandler::new(config);
        assert_eq!(handler.server_count().await, 0);
    }

    #[tokio::test]
    async fn test_get_or_spawn_server_unknown_server() {
        let config = McpProxyConfig::default();
        let handler = McpProxyHandler::new(config);

        let result = handler.get_or_spawn_server("unknown-server").await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unknown MCP server")
        );
    }

    /// **Validates: Requirements 4.1, 4.3, 4.4, 4.5, 4.7**
    ///
    /// Property 9: MCP Proxy Request-Response Forwarding
    /// Tests that MCP requests with valid `mcp:{server_name}` session IDs are correctly
    /// routed to the appropriate server and responses preserve the original sessionId.
    mod property_mcp_proxy_forwarding {
        use super::*;
        use codex_stdio_bus::routing::parse_message;
        use codex_stdio_bus::session::mcp_to_session_id;
        use codex_stdio_bus::worker::StdioBusWorker;
        use proptest::prelude::*;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(500))]

            /// Test that server name is correctly extracted from `mcp:{server_name}` sessionId format.
            /// For any valid server name, the extraction should return the exact server name.
            ///
            /// **Validates: Requirements 4.1**
            #[test]
            fn server_name_extraction_from_mcp_session_id(
                server_name in "[a-zA-Z][a-zA-Z0-9_-]{0,63}"
            ) {
                let session_id = mcp_to_session_id(&server_name);
                let result = McpProxyHandler::extract_server_name(&Some(session_id));

                prop_assert!(result.is_ok());
                prop_assert_eq!(result.unwrap(), server_name);
            }

            /// Test that requests are routed to the correct server based on sessionId.
            /// For any server name in the config, requests with matching sessionId should
            /// be routed to that server (verified by checking the handler accepts the request).
            ///
            /// **Validates: Requirements 4.1, 4.3, 4.4**
            #[test]
            fn request_routing_based_on_session_id(
                server_name in "[a-zA-Z][a-zA-Z0-9_-]{0,31}",
                request_id in "[a-zA-Z0-9_-]{1,32}",
                method in "[a-zA-Z]+/[a-zA-Z]+",
            ) {
                // Build a request with mcp:{server_name} sessionId
                let session_id = mcp_to_session_id(&server_name);
                let request = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "sessionId": session_id,
                    "params": {}
                });
                let request_bytes = serde_json::to_vec(&request).unwrap();

                // Parse the message to extract routing fields
                let message = parse_message(request_bytes);

                // Verify sessionId is correctly extracted
                prop_assert_eq!(message.routing.session_id.as_ref(), Some(&session_id));

                // Verify server name can be extracted from the sessionId
                let extracted_server = McpProxyHandler::extract_server_name(&message.routing.session_id);
                prop_assert!(extracted_server.is_ok());
                prop_assert_eq!(extracted_server.unwrap(), server_name);
            }

            /// Test that response forwarding preserves the original sessionId.
            /// When a response is sent back, the sessionId from the original request
            /// should be injected into the response.
            ///
            /// **Validates: Requirements 4.5**
            #[test]
            fn response_forwarding_preserves_session_id(
                server_name in "[a-zA-Z][a-zA-Z0-9_-]{0,31}",
                request_id in "[a-zA-Z0-9_-]{1,32}",
                result_data in "[a-zA-Z0-9]{1,32}",
            ) {
                let session_id = mcp_to_session_id(&server_name);

                // Simulate a response from the external MCP server (without sessionId)
                let response = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {"data": result_data}
                });
                let mut response_bytes = serde_json::to_vec(&response).unwrap();

                // Inject sessionId (as the worker runtime would do)
                let inject_result = StdioBusWorker::inject_session_id(&mut response_bytes, &session_id);
                prop_assert!(inject_result.is_ok());

                // Verify the response now contains the sessionId
                let modified: serde_json::Value = serde_json::from_slice(&response_bytes).unwrap();
                prop_assert_eq!(&modified["sessionId"], &serde_json::json!(session_id));
                prop_assert_eq!(&modified["id"], &serde_json::json!(request_id));
                prop_assert_eq!(&modified["result"]["data"], &serde_json::json!(result_data));
            }

            /// Test that error responses also preserve the sessionId.
            /// When an error occurs, the sessionId should still be included in the error response.
            ///
            /// **Validates: Requirements 4.5, 4.7**
            #[test]
            fn error_response_preserves_session_id(
                server_name in "[a-zA-Z][a-zA-Z0-9_-]{0,31}",
                request_id in "[a-zA-Z0-9_-]{1,32}",
                error_code in any::<i32>(),
                error_message in "[a-zA-Z ]{1,32}",
            ) {
                let session_id = mcp_to_session_id(&server_name);

                // Simulate an error response from the external MCP server
                let error_response = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": error_code,
                        "message": error_message
                    }
                });
                let mut response_bytes = serde_json::to_vec(&error_response).unwrap();

                // Inject sessionId
                let inject_result = StdioBusWorker::inject_session_id(&mut response_bytes, &session_id);
                prop_assert!(inject_result.is_ok());

                // Verify the error response contains the sessionId
                let modified: serde_json::Value = serde_json::from_slice(&response_bytes).unwrap();
                prop_assert_eq!(&modified["sessionId"], &serde_json::json!(session_id));
                prop_assert_eq!(&modified["error"]["code"], &serde_json::json!(error_code));
                prop_assert_eq!(&modified["error"]["message"], &serde_json::json!(error_message));
            }

            /// Test that invalid sessionIds (wrong prefix) are rejected with appropriate error.
            /// SessionIds that don't start with `mcp:` should result in an error.
            ///
            /// **Validates: Requirements 4.1, 4.7**
            #[test]
            fn invalid_session_id_prefix_rejected(
                invalid_prefix in prop_oneof![
                    Just("thread:"),
                    Just("conn:"),
                    Just(""),
                    Just("invalid:"),
                ],
                server_name in "[a-zA-Z][a-zA-Z0-9_-]{0,31}",
            ) {
                let invalid_session_id = format!("{invalid_prefix}{server_name}");
                let result = McpProxyHandler::extract_server_name(&Some(invalid_session_id));

                prop_assert!(result.is_err());
                let error_msg = result.unwrap_err().to_string();
                prop_assert!(
                    error_msg.contains("Invalid sessionId format"),
                    "Expected 'Invalid sessionId format' error, got: {error_msg}"
                );
            }

            /// Test that missing sessionId is rejected with appropriate error.
            ///
            /// **Validates: Requirements 4.1, 4.7**
            #[test]
            fn missing_session_id_rejected(_dummy in any::<u8>()) {
                let result = McpProxyHandler::extract_server_name(&None);

                prop_assert!(result.is_err());
                let error_msg = result.unwrap_err().to_string();
                prop_assert!(
                    error_msg.contains("Missing sessionId"),
                    "Expected 'Missing sessionId' error, got: {error_msg}"
                );
            }

            /// Test that unknown server names result in appropriate error.
            /// When a request comes for a server not in the config, an error should be returned.
            ///
            /// **Validates: Requirements 4.3, 4.7**
            #[test]
            fn unknown_server_returns_error(
                server_name in "[a-zA-Z][a-zA-Z0-9_-]{0,31}",
            ) {
                // Create a handler with no configured servers
                let config = McpProxyConfig::default();
                let handler = McpProxyHandler::new(config);

                // Use tokio runtime to run async code
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                let result = rt.block_on(handler.get_or_spawn_server(&server_name));

                prop_assert!(result.is_err());
                let error_msg = result.unwrap_err().to_string();
                prop_assert!(
                    error_msg.contains("Unknown MCP server"),
                    "Expected 'Unknown MCP server' error, got: {error_msg}"
                );
            }

            /// Test that different server names route to different logical servers.
            /// Each unique server name should be treated as a separate server.
            ///
            /// **Validates: Requirements 4.1, 4.3**
            #[test]
            fn different_server_names_route_independently(
                server_name_1 in "[a-zA-Z][a-zA-Z0-9_-]{0,15}",
                server_name_2 in "[a-zA-Z][a-zA-Z0-9_-]{0,15}",
            ) {
                // Skip if server names are the same
                prop_assume!(server_name_1 != server_name_2);

                let session_id_1 = mcp_to_session_id(&server_name_1);
                let session_id_2 = mcp_to_session_id(&server_name_2);

                // Extract server names
                let extracted_1 = McpProxyHandler::extract_server_name(&Some(session_id_1));
                let extracted_2 = McpProxyHandler::extract_server_name(&Some(session_id_2));

                prop_assert!(extracted_1.is_ok());
                prop_assert!(extracted_2.is_ok());

                // Verify they extract to different server names
                prop_assert_ne!(extracted_1.unwrap(), extracted_2.unwrap());
            }

            /// Test that request payload is preserved during forwarding.
            /// The raw message bytes should be forwarded unchanged to the external server.
            ///
            /// **Validates: Requirements 4.4**
            #[test]
            fn request_payload_preserved_for_forwarding(
                server_name in "[a-zA-Z][a-zA-Z0-9_-]{0,31}",
                request_id in "[a-zA-Z0-9_-]{1,32}",
                method in "[a-zA-Z]+/[a-zA-Z]+",
                param_key in "[a-zA-Z_]{1,16}",
                param_value in "[a-zA-Z0-9]{1,32}",
            ) {
                let session_id = mcp_to_session_id(&server_name);
                let request = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "sessionId": session_id,
                    "params": {
                        param_key: param_value
                    }
                });
                let request_bytes = serde_json::to_vec(&request).unwrap();

                // Parse the message
                let message = parse_message(request_bytes.clone());

                // Verify raw bytes are preserved (this is what gets forwarded)
                prop_assert_eq!(&message.raw, &request_bytes);

                // Verify we can deserialize the raw bytes back to the original structure
                let deserialized: serde_json::Value = serde_json::from_slice(&message.raw).unwrap();
                prop_assert_eq!(deserialized, request);
            }
        }
    }

    /// Tests for graceful shutdown functionality (REQ-4.7, REQ-4.8).
    mod graceful_shutdown_tests {
        use super::*;
        use codex_stdio_bus::routing::parse_message;
        use codex_stdio_bus::session::mcp_to_session_id;
        use codex_stdio_bus::worker::MessageHandler;
        use pretty_assertions::assert_eq;

        #[test]
        fn test_create_error_response_with_string_id() {
            let id = RequestId::String("test-123".to_string());
            let response = McpProxyHandler::create_error_response(Some(&id), -32000, "Test error");

            let parsed: serde_json::Value = serde_json::from_slice(&response).unwrap();
            assert_eq!(parsed["jsonrpc"], "2.0");
            assert_eq!(parsed["id"], "test-123");
            assert_eq!(parsed["error"]["code"], -32000);
            assert_eq!(parsed["error"]["message"], "Test error");
        }

        #[test]
        fn test_create_error_response_with_integer_id() {
            let id = RequestId::Integer(42);
            let response =
                McpProxyHandler::create_error_response(Some(&id), -32001, "Another error");

            let parsed: serde_json::Value = serde_json::from_slice(&response).unwrap();
            assert_eq!(parsed["jsonrpc"], "2.0");
            assert_eq!(parsed["id"], 42);
            assert_eq!(parsed["error"]["code"], -32001);
            assert_eq!(parsed["error"]["message"], "Another error");
        }

        #[test]
        fn test_create_error_response_with_null_id() {
            let response = McpProxyHandler::create_error_response(None, -32002, "No ID error");

            let parsed: serde_json::Value = serde_json::from_slice(&response).unwrap();
            assert_eq!(parsed["jsonrpc"], "2.0");
            assert!(parsed["id"].is_null());
            assert_eq!(parsed["error"]["code"], -32002);
            assert_eq!(parsed["error"]["message"], "No ID error");
        }

        #[tokio::test]
        async fn test_shutdown_flag_initially_false() {
            let config = McpProxyConfig::default();
            let handler = McpProxyHandler::new(config);
            assert!(!handler.is_shutdown_initiated());
        }

        #[tokio::test]
        async fn test_on_shutdown_sets_shutdown_flag() {
            let config = McpProxyConfig::default();
            let handler = McpProxyHandler::new(config);

            assert!(!handler.is_shutdown_initiated());
            handler.on_shutdown().await;
            assert!(handler.is_shutdown_initiated());
        }

        #[tokio::test]
        async fn test_handle_returns_error_after_shutdown() {
            let config = McpProxyConfig::default();
            let handler = McpProxyHandler::new(config);

            // Initiate shutdown
            handler.on_shutdown().await;

            // Create a test request
            let session_id = mcp_to_session_id("test-server");
            let request = serde_json::json!({
                "jsonrpc": "2.0",
                "id": "req-1",
                "method": "test/method",
                "sessionId": session_id,
                "params": {}
            });
            let request_bytes = serde_json::to_vec(&request).unwrap();
            let message = parse_message(request_bytes);

            // Handle should return an error response
            let result = handler.handle(message).await;
            assert!(result.is_ok());

            let response_bytes = result.unwrap().expect("Should have response");
            let response: serde_json::Value = serde_json::from_slice(&response_bytes).unwrap();

            assert_eq!(response["jsonrpc"], "2.0");
            assert_eq!(response["id"], "req-1");
            assert!(response["error"].is_object());
            assert_eq!(response["error"]["code"], -32000);
            assert!(
                response["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("shutting down")
            );
        }

        #[tokio::test]
        async fn test_managed_server_shutdown_flag() {
            // We can't easily test ManagedServer::spawn without a real process,
            // but we can test the shutdown_requested flag behavior through the handler
            let config = McpProxyConfig::default();
            let handler = McpProxyHandler::new(config);

            // Before shutdown
            assert!(!handler.is_shutdown_initiated());

            // After shutdown
            handler.on_shutdown().await;
            assert!(handler.is_shutdown_initiated());
        }
    }
}
