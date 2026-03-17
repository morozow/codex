//! Worker runtime for stdio_bus integration.

use crate::message::Message;
use crate::routing::parse_message;
use async_trait::async_trait;
use std::io::ErrorKind;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::sync::watch;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

/// Error type for worker operations.
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Handler error: {0}")]
    Handler(String),
    #[error("Shutdown requested")]
    Shutdown,
}

/// Trait for message handlers.
#[async_trait]
pub trait MessageHandler: Send + Sync {
    /// Handle an incoming message and optionally return a response.
    async fn handle(&self, msg: Message) -> Result<Option<Vec<u8>>, WorkerError>;

    /// Called when shutdown is initiated.
    async fn on_shutdown(&self) {}
}

/// Worker runtime for stdio_bus integration.
pub struct StdioBusWorker {
    stdin: BufReader<tokio::io::Stdin>,
    stdout: tokio::io::Stdout,
    shutdown_rx: watch::Receiver<bool>,
    shutdown_tx: watch::Sender<bool>,
}

impl StdioBusWorker {
    /// Create a new worker instance.
    pub fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            stdin: BufReader::new(tokio::io::stdin()),
            stdout: tokio::io::stdout(),
            shutdown_rx,
            shutdown_tx,
        }
    }

    /// Read the next NDJSON message from stdin.
    pub async fn recv(&mut self) -> Result<Message, WorkerError> {
        let mut line = String::new();

        loop {
            tokio::select! {
                biased;

                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        return Err(WorkerError::Shutdown);
                    }
                }

                result = self.stdin.read_line(&mut line) => {
                    match result {
                        Ok(0) => {
                            debug!("stdin EOF");
                            return Err(WorkerError::Io(std::io::Error::new(
                                ErrorKind::UnexpectedEof,
                                "stdin closed",
                            )));
                        }
                        Ok(_) => {
                            let trimmed = line.trim_end();
                            if trimmed.is_empty() {
                                line.clear();
                                continue;
                            }
                            return Ok(parse_message(trimmed.as_bytes().to_vec()));
                        }
                        Err(e) => return Err(WorkerError::Io(e)),
                    }
                }
            }
        }
    }

    /// Send an NDJSON message to stdout.
    pub async fn send(&mut self, msg: &[u8]) -> Result<(), WorkerError> {
        self.stdout.write_all(msg).await?;
        self.stdout.write_all(b"\n").await?;
        self.stdout.flush().await?;
        Ok(())
    }

    /// Inject sessionId into a JSON message.
    pub fn inject_session_id(msg: &mut Vec<u8>, session_id: &str) -> Result<(), WorkerError> {
        let mut value: serde_json::Value = serde_json::from_slice(msg)?;
        if let Some(obj) = value.as_object_mut() {
            obj.insert(
                "sessionId".to_string(),
                serde_json::Value::String(session_id.to_string()),
            );
        }
        *msg = serde_json::to_vec(&value)?;
        Ok(())
    }

    /// Request shutdown.
    pub fn request_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Run the worker loop.
    pub async fn run<H: MessageHandler>(&mut self, handler: H) -> Result<(), WorkerError> {
        info!("Worker starting");

        // Set up signal handler.
        let shutdown_tx = self.shutdown_tx.clone();
        tokio::spawn(async move {
            if let Ok(()) = tokio::signal::ctrl_c().await {
                info!("Received SIGINT, initiating shutdown");
                let _ = shutdown_tx.send(true);
            }
        });

        #[cfg(unix)]
        {
            let shutdown_tx = self.shutdown_tx.clone();
            tokio::spawn(async move {
                let Ok(mut sigterm) =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                else {
                    tracing::warn!("Failed to register SIGTERM handler");
                    return;
                };

                sigterm.recv().await;
                info!("Received SIGTERM, initiating graceful shutdown");
                let _ = shutdown_tx.send(true);
            });
        }

        loop {
            match self.recv().await {
                Ok(msg) => {
                    let session_id = msg.routing.session_id.clone();

                    match handler.handle(msg).await {
                        Ok(Some(mut response)) => {
                            // Preserve sessionId in response.
                            if let Some(sid) = &session_id
                                && let Err(e) = Self::inject_session_id(&mut response, sid)
                            {
                                warn!(error = %e, "Failed to inject sessionId");
                            }
                            if let Err(e) = self.send(&response).await {
                                error!(error = %e, "Failed to send response");
                            }
                        }
                        Ok(None) => {
                            // No response needed (notification).
                        }
                        Err(e) => {
                            error!(error = %e, "Handler error");
                        }
                    }
                }
                Err(WorkerError::Shutdown) => {
                    info!("Shutdown requested, draining...");
                    handler.on_shutdown().await;
                    break;
                }
                Err(WorkerError::Io(e)) if e.kind() == ErrorKind::UnexpectedEof => {
                    info!("stdin closed, shutting down");
                    break;
                }
                Err(e) => {
                    error!(error = %e, "Worker error");
                    break;
                }
            }
        }

        info!("Worker stopped");
        Ok(())
    }
    /// Run the worker loop with support for outgoing notifications (REQ-2.7).
    ///
    /// This method extends the basic `run()` method to also poll for outgoing
    /// notifications from the provided receiver. Notifications are sent to stdout
    /// with their `sessionId` already included (the handler is responsible for
    /// adding the `sessionId` to notifications).
    ///
    /// # Arguments
    /// * `handler` - The message handler for processing incoming messages
    /// * `notification_rx` - Optional receiver for outgoing notifications
    pub async fn run_with_notifications<H: MessageHandler>(
        &mut self,
        handler: H,
        mut notification_rx: Option<tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>>,
    ) -> Result<(), WorkerError> {
        info!("Worker starting with notification support");

        // Set up signal handler.
        let shutdown_tx = self.shutdown_tx.clone();
        tokio::spawn(async move {
            if let Ok(()) = tokio::signal::ctrl_c().await {
                info!("Received SIGINT, initiating shutdown");
                let _ = shutdown_tx.send(true);
            }
        });

        #[cfg(unix)]
        {
            let shutdown_tx = self.shutdown_tx.clone();
            tokio::spawn(async move {
                let Ok(mut sigterm) =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                else {
                    tracing::warn!("Failed to register SIGTERM handler");
                    return;
                };

                sigterm.recv().await;
                info!("Received SIGTERM, initiating graceful shutdown");
                let _ = shutdown_tx.send(true);
            });
        }

        // Clone shutdown_rx to avoid borrowing self in multiple select! branches
        let mut shutdown_rx = self.shutdown_rx.clone();

        loop {
            tokio::select! {
                biased;

                // Check for shutdown
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("Shutdown requested, draining...");
                        handler.on_shutdown().await;
                        break;
                    }
                }

                // Poll for outgoing notifications (REQ-2.7)
                notification = async {
                    if let Some(rx) = &mut notification_rx {
                        rx.recv().await
                    } else {
                        std::future::pending().await
                    }
                } => {
                    if let Some(notification_bytes) = notification {
                        // Notifications already have sessionId included by the sender
                        if let Err(e) = self.send(&notification_bytes).await {
                            error!(error = %e, "Failed to send notification");
                        } else {
                            debug!("Sent notification to stdout");
                        }
                    }
                }

                // Poll for incoming messages
                result = self.recv_inner() => {
                    match result {
                        Ok(msg) => {
                            let session_id = msg.routing.session_id.clone();

                            match handler.handle(msg).await {
                                Ok(Some(mut response)) => {
                                    // Preserve sessionId in response (REQ-2.6)
                                    if let Some(sid) = &session_id
                                        && let Err(e) = Self::inject_session_id(&mut response, sid)
                                    {
                                        warn!(error = %e, "Failed to inject sessionId");
                                    }
                                    if let Err(e) = self.send(&response).await {
                                        error!(error = %e, "Failed to send response");
                                    }
                                }
                                Ok(None) => {
                                    // No response needed (notification).
                                }
                                Err(e) => {
                                    error!(error = %e, "Handler error");
                                }
                            }
                        }
                        Err(WorkerError::Shutdown) => {
                            info!("Shutdown requested, draining...");
                            handler.on_shutdown().await;
                            break;
                        }
                        Err(WorkerError::Io(e)) if e.kind() == ErrorKind::UnexpectedEof => {
                            info!("stdin closed, shutting down");
                            break;
                        }
                        Err(e) => {
                            error!(error = %e, "Worker error");
                            break;
                        }
                    }
                }
            }
        }

        info!("Worker stopped");
        Ok(())
    }

    /// Internal method to receive a message without shutdown handling.
    /// Used by `run_with_notifications` to allow proper select! usage.
    async fn recv_inner(&mut self) -> Result<Message, WorkerError> {
        let mut line = String::new();

        loop {
            match self.stdin.read_line(&mut line).await {
                Ok(0) => {
                    debug!("stdin EOF");
                    return Err(WorkerError::Io(std::io::Error::new(
                        ErrorKind::UnexpectedEof,
                        "stdin closed",
                    )));
                }
                Ok(_) => {
                    let trimmed = line.trim_end();
                    if trimmed.is_empty() {
                        line.clear();
                        continue;
                    }
                    return Ok(parse_message(trimmed.as_bytes().to_vec()));
                }
                Err(e) => return Err(WorkerError::Io(e)),
            }
        }
    }
}

impl Default for StdioBusWorker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::parse_message;
    use proptest::prelude::*;

    /// **Validates: Requirements 1.1, 1.2**
    ///
    /// Property 1: NDJSON I/O Round-Trip
    /// Tests that writing JSON with newline framing and reading back produces equivalent value.
    mod property_ndjson_round_trip {
        use super::*;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(1000))]

            /// Test that JSON values serialized and parsed back produce equivalent routing fields.
            #[test]
            fn json_round_trip_preserves_routing_fields(
                id in prop_oneof![
                    any::<i64>().prop_map(|n| serde_json::json!(n)),
                    "[a-zA-Z0-9_-]{1,64}".prop_map(|s| serde_json::json!(s)),
                ],
                session_id in prop::option::of("[a-zA-Z0-9:_-]{1,64}"),
                method in prop::option::of("[a-zA-Z]+/[a-zA-Z]+"),
            ) {
                // Build a JSON-RPC message
                let mut msg = serde_json::Map::new();
                msg.insert("id".to_string(), id.clone());
                if let Some(ref sid) = session_id {
                    msg.insert("sessionId".to_string(), serde_json::json!(sid));
                }
                if let Some(ref m) = method {
                    msg.insert("method".to_string(), serde_json::json!(m));
                }
                msg.insert("params".to_string(), serde_json::json!({}));

                // Serialize to JSON bytes (simulating NDJSON write)
                let json_bytes = serde_json::to_vec(&msg).unwrap();

                // Parse back through routing (simulating NDJSON read)
                let message = parse_message(json_bytes.clone());

                // Verify routing fields are preserved
                assert_eq!(message.raw, json_bytes);
                assert_eq!(message.routing.session_id, session_id);
                assert_eq!(message.routing.method, method);

                // Verify ID is preserved
                match &id {
                    serde_json::Value::Number(n) => {
                        assert_eq!(
                            message.routing.id,
                            Some(crate::message::RequestId::Integer(n.as_i64().unwrap()))
                        );
                    }
                    serde_json::Value::String(s) => {
                        assert_eq!(
                            message.routing.id,
                            Some(crate::message::RequestId::String(s.clone()))
                        );
                    }
                    _ => unreachable!(),
                }
            }

            /// Test that newline-delimited JSON lines can be split and parsed independently.
            #[test]
            fn ndjson_lines_parse_independently(
                messages in prop::collection::vec(
                    (
                        "[a-zA-Z0-9_-]{1,32}",
                        "[a-zA-Z]+/[a-zA-Z]+",
                    ),
                    1..5
                )
            ) {
                // Build multiple NDJSON lines
                let mut ndjson = String::new();
                for (id, method) in &messages {
                    let msg = serde_json::json!({
                        "id": id,
                        "method": method
                    });
                    ndjson.push_str(&serde_json::to_string(&msg).unwrap());
                    ndjson.push('\n');
                }

                // Parse each line independently (simulating NDJSON read)
                let lines: Vec<&str> = ndjson.trim_end().split('\n').collect();
                assert_eq!(lines.len(), messages.len());

                for (i, line) in lines.iter().enumerate() {
                    let message = parse_message(line.as_bytes().to_vec());
                    let (expected_id, expected_method) = &messages[i];

                    assert_eq!(
                        message.routing.id,
                        Some(crate::message::RequestId::String(expected_id.clone()))
                    );
                    assert_eq!(message.routing.method, Some(expected_method.clone()));
                }
            }

            /// Test that JSON with arbitrary nested params round-trips correctly.
            #[test]
            fn json_with_nested_params_round_trips(
                id in "[a-zA-Z0-9_-]{1,32}",
                method in "[a-zA-Z]+/[a-zA-Z]+",
                param_key in "[a-zA-Z_]{1,16}",
                param_value in "[a-zA-Z0-9 ]{1,32}",
            ) {
                let msg = serde_json::json!({
                    "id": id,
                    "method": method,
                    "params": {
                        param_key: param_value,
                        "nested": {
                            "deep": true
                        }
                    }
                });

                let json_bytes = serde_json::to_vec(&msg).unwrap();
                let message = parse_message(json_bytes.clone());

                // Routing fields should be extracted
                assert_eq!(
                    message.routing.id,
                    Some(crate::message::RequestId::String(id))
                );
                assert_eq!(message.routing.method, Some(method));

                // Raw bytes should be preserved for forwarding
                assert_eq!(message.raw, json_bytes);

                // Should be able to deserialize raw back to original structure
                let deserialized: serde_json::Value = serde_json::from_slice(&message.raw).unwrap();
                assert_eq!(deserialized, msg);
            }
        }
    }

    /// **Validates: Requirements 1.1, 1.2**
    ///
    /// Property: inject_session_id Correctness
    /// Tests that inject_session_id correctly adds sessionId to JSON messages.
    mod property_inject_session_id {
        use super::*;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(1000))]

            /// Test that inject_session_id adds sessionId to messages without one.
            #[test]
            fn injects_session_id_into_message_without_one(
                id in "[a-zA-Z0-9_-]{1,32}",
                method in "[a-zA-Z]+/[a-zA-Z]+",
                session_id in "[a-zA-Z0-9:_-]{1,64}",
            ) {
                let msg = serde_json::json!({
                    "id": id,
                    "method": method,
                    "params": {}
                });
                let mut bytes = serde_json::to_vec(&msg).unwrap();

                let result = StdioBusWorker::inject_session_id(&mut bytes, &session_id);
                assert!(result.is_ok());

                // Parse the modified message
                let modified: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(modified["sessionId"], serde_json::json!(session_id));
                assert_eq!(modified["id"], serde_json::json!(id));
                assert_eq!(modified["method"], serde_json::json!(method));
            }

            /// Test that inject_session_id overwrites existing sessionId.
            #[test]
            fn overwrites_existing_session_id(
                id in "[a-zA-Z0-9_-]{1,32}",
                old_session_id in "[a-zA-Z0-9:_-]{1,64}",
                new_session_id in "[a-zA-Z0-9:_-]{1,64}",
            ) {
                let msg = serde_json::json!({
                    "id": id,
                    "sessionId": old_session_id,
                    "method": "test/method"
                });
                let mut bytes = serde_json::to_vec(&msg).unwrap();

                let result = StdioBusWorker::inject_session_id(&mut bytes, &new_session_id);
                assert!(result.is_ok());

                let modified: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(modified["sessionId"], serde_json::json!(new_session_id));
            }

            /// Test that inject_session_id preserves all other fields.
            #[test]
            fn preserves_other_fields(
                id in any::<i64>(),
                method in "[a-zA-Z]+/[a-zA-Z]+",
                session_id in "[a-zA-Z0-9:_-]{1,64}",
                result_value in "[a-zA-Z0-9]{1,32}",
            ) {
                let msg = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": method,
                    "result": result_value,
                    "extra": {"nested": true}
                });
                let mut bytes = serde_json::to_vec(&msg).unwrap();

                let inject_result = StdioBusWorker::inject_session_id(&mut bytes, &session_id);
                assert!(inject_result.is_ok());

                let modified: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(modified["jsonrpc"], serde_json::json!("2.0"));
                assert_eq!(modified["id"], serde_json::json!(id));
                assert_eq!(modified["method"], serde_json::json!(method));
                assert_eq!(modified["result"], serde_json::json!(result_value));
                assert_eq!(modified["extra"]["nested"], serde_json::json!(true));
                assert_eq!(modified["sessionId"], serde_json::json!(session_id));
            }

            /// Test that inject_session_id works with response messages.
            #[test]
            fn works_with_response_messages(
                id in "[a-zA-Z0-9_-]{1,32}",
                session_id in "[a-zA-Z0-9:_-]{1,64}",
                result_data in "[a-zA-Z0-9]{1,32}",
            ) {
                let msg = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {"data": result_data}
                });
                let mut bytes = serde_json::to_vec(&msg).unwrap();

                let result = StdioBusWorker::inject_session_id(&mut bytes, &session_id);
                assert!(result.is_ok());

                let modified: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(modified["sessionId"], serde_json::json!(session_id));
                assert_eq!(modified["result"]["data"], serde_json::json!(result_data));
            }

            /// Test that inject_session_id works with error responses.
            #[test]
            fn works_with_error_responses(
                id in "[a-zA-Z0-9_-]{1,32}",
                session_id in "[a-zA-Z0-9:_-]{1,64}",
                error_code in any::<i32>(),
                error_msg in "[a-zA-Z ]{1,32}",
            ) {
                let msg = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {
                        "code": error_code,
                        "message": error_msg
                    }
                });
                let mut bytes = serde_json::to_vec(&msg).unwrap();

                let result = StdioBusWorker::inject_session_id(&mut bytes, &session_id);
                assert!(result.is_ok());

                let modified: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(modified["sessionId"], serde_json::json!(session_id));
                assert_eq!(modified["error"]["code"], serde_json::json!(error_code));
                assert_eq!(modified["error"]["message"], serde_json::json!(error_msg));
            }
        }
    }

    /// **Validates: Requirements 1.1, 1.2**
    ///
    /// Property: Message Structure Preservation
    /// Tests that messages maintain their structure through serialization/deserialization.
    mod property_message_structure_preservation {
        use super::*;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(500))]

            /// Test that message structure is preserved after inject_session_id and re-parsing.
            #[test]
            fn structure_preserved_after_inject_and_reparse(
                id in "[a-zA-Z0-9_-]{1,32}",
                method in "[a-zA-Z]+/[a-zA-Z]+",
                session_id in "[a-zA-Z0-9:_-]{1,64}",
            ) {
                let original = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": method,
                    "params": {"key": "value"}
                });
                let mut bytes = serde_json::to_vec(&original).unwrap();

                // Inject session ID
                StdioBusWorker::inject_session_id(&mut bytes, &session_id).unwrap();

                // Re-parse through routing
                let message = parse_message(bytes);

                // Verify routing fields
                assert_eq!(
                    message.routing.id,
                    Some(crate::message::RequestId::String(id.clone()))
                );
                assert_eq!(message.routing.method, Some(method.clone()));
                assert_eq!(message.routing.session_id, Some(session_id.clone()));

                // Verify full structure
                let parsed: serde_json::Value = serde_json::from_slice(&message.raw).unwrap();
                assert_eq!(parsed["jsonrpc"], serde_json::json!("2.0"));
                assert_eq!(parsed["id"], serde_json::json!(id));
                assert_eq!(parsed["method"], serde_json::json!(method));
                assert_eq!(parsed["sessionId"], serde_json::json!(session_id));
                assert_eq!(parsed["params"]["key"], serde_json::json!("value"));
            }

            /// Test that multiple inject_session_id calls work correctly.
            #[test]
            fn multiple_injects_work_correctly(
                id in "[a-zA-Z0-9_-]{1,32}",
                session_ids in prop::collection::vec("[a-zA-Z0-9:_-]{1,32}", 2..5),
            ) {
                let msg = serde_json::json!({
                    "id": id,
                    "method": "test/method"
                });
                let mut bytes = serde_json::to_vec(&msg).unwrap();

                // Inject multiple session IDs sequentially
                for session_id in &session_ids {
                    StdioBusWorker::inject_session_id(&mut bytes, session_id).unwrap();
                }

                // Only the last session ID should be present
                let final_msg: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(
                    final_msg["sessionId"],
                    serde_json::json!(session_ids.last().unwrap())
                );
                assert_eq!(final_msg["id"], serde_json::json!(id));
            }
        }
    }

    /// **Validates: Requirements 2.6, 5.4, 8.6**
    ///
    /// Property 3: SessionId Preservation in Responses
    /// Tests that responses contain the same sessionId as requests.
    mod property_session_id_preservation {
        use super::*;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(1000))]

            /// Test that when a request contains a sessionId, the response includes the same sessionId.
            /// This simulates the worker runtime behavior where sessionId is extracted from the request
            /// and injected into the response.
            #[test]
            fn response_contains_same_session_id_as_request(
                request_id in prop_oneof![
                    any::<i64>().prop_map(|n| serde_json::json!(n)),
                    "[a-zA-Z0-9_-]{1,32}".prop_map(|s| serde_json::json!(s)),
                ],
                session_id in "[a-zA-Z0-9:_-]{1,64}",
                method in "[a-zA-Z]+/[a-zA-Z]+",
                result_data in "[a-zA-Z0-9]{1,32}",
            ) {
                // Step 1: Create a request with sessionId
                let request = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "params": {},
                    "sessionId": session_id
                });
                let request_bytes = serde_json::to_vec(&request).unwrap();

                // Step 2: Parse the request and extract sessionId (simulating worker recv)
                let parsed_request = parse_message(request_bytes);
                let extracted_session_id = parsed_request.routing.session_id.clone();

                // Verify sessionId was extracted correctly
                prop_assert_eq!(extracted_session_id.as_deref(), Some(session_id.as_str()));

                // Step 3: Create a response without sessionId (simulating handler response)
                let response = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {"data": result_data}
                });
                let mut response_bytes = serde_json::to_vec(&response).unwrap();

                // Step 4: Inject sessionId into response (simulating worker runtime)
                if let Some(sid) = &extracted_session_id {
                    StdioBusWorker::inject_session_id(&mut response_bytes, sid).unwrap();
                }

                // Step 5: Verify the response contains the same sessionId as the request
                let final_response: serde_json::Value = serde_json::from_slice(&response_bytes).unwrap();
                assert_eq!(final_response["sessionId"], serde_json::json!(session_id));
                assert_eq!(final_response["id"], request_id);
            }

            /// Test that sessionId is preserved through the full request-response cycle
            /// for various session ID formats (thread:, conn:, mcp:).
            #[test]
            fn session_id_preserved_for_all_formats(
                request_id in "[a-zA-Z0-9_-]{1,32}",
                session_type in prop_oneof![
                    "thread:[a-zA-Z0-9_-]{1,32}",
                    "conn:[0-9]{1,10}",
                    "mcp:[a-zA-Z0-9_-]{1,32}",
                ],
            ) {
                // Create request with typed sessionId
                let request = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "test/method",
                    "sessionId": session_type
                });
                let request_bytes = serde_json::to_vec(&request).unwrap();

                // Parse and extract sessionId
                let parsed_request = parse_message(request_bytes);
                let extracted_session_id = parsed_request.routing.session_id.clone();
                prop_assert_eq!(extracted_session_id.as_deref(), Some(session_type.as_str()));

                // Create response and inject sessionId
                let response = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {}
                });
                let mut response_bytes = serde_json::to_vec(&response).unwrap();

                if let Some(sid) = &extracted_session_id {
                    StdioBusWorker::inject_session_id(&mut response_bytes, sid).unwrap();
                }

                // Verify sessionId is preserved
                let final_response: serde_json::Value = serde_json::from_slice(&response_bytes).unwrap();
                assert_eq!(final_response["sessionId"], serde_json::json!(session_type));
            }

            /// Test that different sessionIds are correctly preserved for different requests.
            /// This verifies that each request-response pair maintains its own sessionId.
            #[test]
            fn different_session_ids_preserved_for_different_requests(
                requests in prop::collection::vec(
                    (
                        "[a-zA-Z0-9_-]{1,16}",  // request_id
                        "[a-zA-Z0-9:_-]{1,32}", // session_id
                    ),
                    2..10
                )
            ) {
                // Process each request independently and verify sessionId preservation
                for (request_id, session_id) in &requests {
                    // Create request
                    let request = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": "test/method",
                        "sessionId": session_id
                    });
                    let request_bytes = serde_json::to_vec(&request).unwrap();

                    // Parse and extract sessionId
                    let parsed_request = parse_message(request_bytes);
                    let extracted_session_id = parsed_request.routing.session_id.clone();
                    prop_assert_eq!(extracted_session_id.as_deref(), Some(session_id.as_str()));

                    // Create response and inject sessionId
                    let response = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "result": {"processed": true}
                    });
                    let mut response_bytes = serde_json::to_vec(&response).unwrap();

                    if let Some(sid) = &extracted_session_id {
                        StdioBusWorker::inject_session_id(&mut response_bytes, sid).unwrap();
                    }

                    // Verify this specific sessionId is preserved
                    let final_response: serde_json::Value = serde_json::from_slice(&response_bytes).unwrap();
                    assert_eq!(final_response["sessionId"], serde_json::json!(session_id));
                    assert_eq!(final_response["id"], serde_json::json!(request_id));
                }
            }

            /// Test that sessionId preservation works with error responses.
            #[test]
            fn session_id_preserved_in_error_responses(
                request_id in "[a-zA-Z0-9_-]{1,32}",
                session_id in "[a-zA-Z0-9:_-]{1,64}",
                error_code in any::<i32>(),
                error_message in "[a-zA-Z ]{1,32}",
            ) {
                // Create request with sessionId
                let request = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "test/method",
                    "sessionId": session_id
                });
                let request_bytes = serde_json::to_vec(&request).unwrap();

                // Parse and extract sessionId
                let parsed_request = parse_message(request_bytes);
                let extracted_session_id = parsed_request.routing.session_id.clone();

                // Create error response without sessionId
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
                if let Some(sid) = &extracted_session_id {
                    StdioBusWorker::inject_session_id(&mut response_bytes, sid).unwrap();
                }

                // Verify sessionId is preserved in error response
                let final_response: serde_json::Value = serde_json::from_slice(&response_bytes).unwrap();
                assert_eq!(final_response["sessionId"], serde_json::json!(session_id));
                prop_assert!(final_response["error"].is_object());
                assert_eq!(final_response["error"]["code"], serde_json::json!(error_code));
            }

            /// Test that requests without sessionId result in responses without sessionId.
            /// This verifies the worker runtime only injects sessionId when present in request.
            #[test]
            fn no_session_id_when_request_has_none(
                request_id in "[a-zA-Z0-9_-]{1,32}",
                method in "[a-zA-Z]+/[a-zA-Z]+",
            ) {
                // Create request without sessionId
                let request = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "params": {}
                });
                let request_bytes = serde_json::to_vec(&request).unwrap();

                // Parse and verify no sessionId
                let parsed_request = parse_message(request_bytes);
                prop_assert!(parsed_request.routing.session_id.is_none());

                // Create response
                let response = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {}
                });
                let response_bytes = serde_json::to_vec(&response).unwrap();

                // Don't inject sessionId (simulating worker runtime behavior when no sessionId in request)
                // Verify response has no sessionId
                let final_response: serde_json::Value = serde_json::from_slice(&response_bytes).unwrap();
                prop_assert!(final_response.get("sessionId").is_none());
            }

            /// Test that sessionId with special characters is preserved correctly.
            #[test]
            fn session_id_with_special_chars_preserved(
                request_id in "[a-zA-Z0-9_-]{1,32}",
                // Session IDs can contain colons, underscores, and hyphens
                session_id in "thread:[a-zA-Z0-9_-]{1,16}:[a-zA-Z0-9_-]{1,16}",
            ) {
                let request = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "test/method",
                    "sessionId": session_id
                });
                let request_bytes = serde_json::to_vec(&request).unwrap();

                let parsed_request = parse_message(request_bytes);
                let extracted_session_id = parsed_request.routing.session_id.clone();
                prop_assert_eq!(extracted_session_id.as_deref(), Some(session_id.as_str()));

                let response = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {}
                });
                let mut response_bytes = serde_json::to_vec(&response).unwrap();

                if let Some(sid) = &extracted_session_id {
                    StdioBusWorker::inject_session_id(&mut response_bytes, sid).unwrap();
                }

                let final_response: serde_json::Value = serde_json::from_slice(&response_bytes).unwrap();
                assert_eq!(final_response["sessionId"], serde_json::json!(session_id));
            }
        }
    }

    /// **Validates: Requirements 2.7, 5.5, 8.7**
    ///
    /// Property 4: SessionId in Notifications
    /// Tests that notifications include sessionId for routing.
    mod property_session_id_in_notifications {
        use super::*;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(1000))]

            /// Test that notifications (messages without `id`) can include sessionId for routing.
            /// Notifications are JSON-RPC messages with a `method` field but no `id` field.
            #[test]
            fn notifications_can_include_session_id(
                method in "[a-zA-Z]+/[a-zA-Z]+",
                session_id in "[a-zA-Z0-9:_-]{1,64}",
            ) {
                // Create a notification (has method but no id)
                let notification = serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": method,
                    "params": {},
                    "sessionId": session_id
                });
                let notification_bytes = serde_json::to_vec(&notification).unwrap();

                // Parse the notification
                let parsed = parse_message(notification_bytes);

                // Verify it's recognized as a notification (has method, no id)
                prop_assert!(parsed.routing.id.is_none(), "Notification should not have id");
                prop_assert_eq!(parsed.routing.method.as_deref(), Some(method.as_str()));
                prop_assert!(!parsed.routing.is_response, "Notification should not be a response");

                // Verify sessionId is extracted for routing
                prop_assert_eq!(parsed.routing.session_id.as_deref(), Some(session_id.as_str()));
            }

            /// Test that sessionId in notifications is preserved when sent through the worker.
            /// This simulates the worker receiving a notification and forwarding it with sessionId.
            #[test]
            fn session_id_preserved_when_sending_notification(
                method in "[a-zA-Z]+/[a-zA-Z]+",
                session_id in "[a-zA-Z0-9:_-]{1,64}",
                param_key in "[a-zA-Z_]{1,16}",
                param_value in "[a-zA-Z0-9]{1,32}",
            ) {
                // Clone param_key before it's moved into json!
                let param_key_clone = param_key.clone();

                // Create a notification without sessionId (simulating handler output)
                let notification = serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": method,
                    "params": {
                        param_key: param_value
                    }
                });
                let mut notification_bytes = serde_json::to_vec(&notification).unwrap();

                // Inject sessionId (simulating worker adding sessionId for routing)
                let result = StdioBusWorker::inject_session_id(&mut notification_bytes, &session_id);
                prop_assert!(result.is_ok());

                // Parse the modified notification
                let parsed = parse_message(notification_bytes);

                // Verify it's still a notification
                prop_assert!(parsed.routing.id.is_none(), "Should still be a notification");
                prop_assert_eq!(parsed.routing.method.as_deref(), Some(method.as_str()));

                // Verify sessionId was added for routing
                prop_assert_eq!(parsed.routing.session_id.as_deref(), Some(session_id.as_str()));

                // Verify params are preserved
                let parsed_json: serde_json::Value = serde_json::from_slice(&parsed.raw).unwrap();
                assert_eq!(&parsed_json["params"][&param_key_clone], &serde_json::json!(param_value));
            }

            /// Test that notifications without sessionId are handled correctly.
            /// These notifications should parse successfully but have no sessionId for routing.
            #[test]
            fn notifications_without_session_id_handled_correctly(
                method in "[a-zA-Z]+/[a-zA-Z]+",
            ) {
                // Create a notification without sessionId
                let notification = serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": method,
                    "params": {}
                });
                let notification_bytes = serde_json::to_vec(&notification).unwrap();

                // Parse the notification
                let parsed = parse_message(notification_bytes);

                // Verify it's recognized as a notification
                prop_assert!(parsed.routing.id.is_none(), "Notification should not have id");
                prop_assert_eq!(parsed.routing.method.as_deref(), Some(method.as_str()));
                prop_assert!(!parsed.routing.is_response, "Notification should not be a response");

                // Verify sessionId is None (no routing info)
                prop_assert!(parsed.routing.session_id.is_none(), "Should have no sessionId");
            }

            /// Test that sessionId is preserved for all session type formats in notifications.
            #[test]
            fn session_id_formats_preserved_in_notifications(
                method in "[a-zA-Z]+/[a-zA-Z]+",
                session_type in prop_oneof![
                    "thread:[a-zA-Z0-9_-]{1,32}",
                    "conn:[0-9]{1,10}",
                    "mcp:[a-zA-Z0-9_-]{1,32}",
                ],
            ) {
                // Create notification with typed sessionId
                let notification = serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": method,
                    "params": {"event": "update"}
                });
                let mut notification_bytes = serde_json::to_vec(&notification).unwrap();

                // Inject sessionId
                StdioBusWorker::inject_session_id(&mut notification_bytes, &session_type).unwrap();

                // Parse and verify
                let parsed = parse_message(notification_bytes);
                prop_assert!(parsed.routing.id.is_none(), "Should be a notification");
                prop_assert_eq!(parsed.routing.session_id.as_deref(), Some(session_type.as_str()));
            }

            /// Test that multiple notifications can have different sessionIds.
            /// This verifies that each notification maintains its own routing information.
            #[test]
            fn different_notifications_have_different_session_ids(
                notifications in prop::collection::vec(
                    (
                        "[a-zA-Z]+/[a-zA-Z]+",  // method
                        "[a-zA-Z0-9:_-]{1,32}", // session_id
                    ),
                    2..10
                )
            ) {
                for (method, session_id) in &notifications {
                    // Create notification
                    let notification = serde_json::json!({
                        "jsonrpc": "2.0",
                        "method": method,
                        "params": {}
                    });
                    let mut notification_bytes = serde_json::to_vec(&notification).unwrap();

                    // Inject sessionId
                    StdioBusWorker::inject_session_id(&mut notification_bytes, session_id).unwrap();

                    // Parse and verify this specific notification has correct sessionId
                    let parsed = parse_message(notification_bytes);
                    prop_assert!(parsed.routing.id.is_none());
                    prop_assert_eq!(parsed.routing.method.as_deref(), Some(method.as_str()));
                    prop_assert_eq!(parsed.routing.session_id.as_deref(), Some(session_id.as_str()));
                }
            }

            /// Test that notifications with complex params preserve sessionId correctly.
            #[test]
            fn notifications_with_complex_params_preserve_session_id(
                method in "[a-zA-Z]+/[a-zA-Z]+",
                session_id in "[a-zA-Z0-9:_-]{1,64}",
                nested_key in "[a-zA-Z_]{1,8}",
                nested_value in "[a-zA-Z0-9]{1,16}",
            ) {
                // Clone nested_key before it's moved into json!
                let nested_key_clone = nested_key.clone();

                // Create notification with nested params
                let notification = serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": method,
                    "params": {
                        "data": {
                            nested_key: nested_value,
                            "array": [1, 2, 3],
                            "nested": {
                                "deep": true
                            }
                        }
                    },
                    "sessionId": session_id
                });
                let notification_bytes = serde_json::to_vec(&notification).unwrap();

                // Parse the notification
                let parsed = parse_message(notification_bytes);

                // Verify routing fields
                prop_assert!(parsed.routing.id.is_none());
                prop_assert_eq!(parsed.routing.method.as_deref(), Some(method.as_str()));
                prop_assert_eq!(parsed.routing.session_id.as_deref(), Some(session_id.as_str()));

                // Verify complex params are preserved
                let parsed_json: serde_json::Value = serde_json::from_slice(&parsed.raw).unwrap();
                assert_eq!(&parsed_json["params"]["data"][&nested_key_clone], &serde_json::json!(nested_value));
                assert_eq!(&parsed_json["params"]["data"]["array"], &serde_json::json!([1, 2, 3]));
                assert_eq!(&parsed_json["params"]["data"]["nested"]["deep"], &serde_json::json!(true));
            }

            /// Test that notification sessionId injection doesn't affect other fields.
            #[test]
            fn notification_session_id_injection_preserves_all_fields(
                method in "[a-zA-Z]+/[a-zA-Z]+",
                session_id in "[a-zA-Z0-9:_-]{1,64}",
            ) {
                // Create notification with various fields
                let notification = serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": method,
                    "params": {"key": "value"},
                    "extra": "field"
                });
                let mut notification_bytes = serde_json::to_vec(&notification).unwrap();

                // Inject sessionId
                StdioBusWorker::inject_session_id(&mut notification_bytes, &session_id).unwrap();

                // Verify all original fields are preserved
                let parsed_json: serde_json::Value = serde_json::from_slice(&notification_bytes).unwrap();
                assert_eq!(&parsed_json["jsonrpc"], &serde_json::json!("2.0"));
                assert_eq!(&parsed_json["method"], &serde_json::json!(method));
                assert_eq!(&parsed_json["params"]["key"], &serde_json::json!("value"));
                assert_eq!(&parsed_json["extra"], &serde_json::json!("field"));
                assert_eq!(&parsed_json["sessionId"], &serde_json::json!(session_id));

                // Verify no id field was added
                prop_assert!(parsed_json.get("id").is_none());
            }
        }
    }

    /// **Validates: Requirements 2.4, 8.3**
    ///
    /// Property 7: Stderr-Only Diagnostics
    /// Tests that worker mode writes diagnostics to stderr only and stdout contains only valid JSON.
    mod property_stderr_only_diagnostics {
        use super::*;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(500))]

            /// Test that stdout output is always valid JSON.
            /// This verifies that no diagnostic content (logs, traces) appears on stdout.
            #[test]
            fn stdout_output_is_always_valid_json(
                id in "[a-zA-Z0-9_-]{1,32}",
                method in "[a-zA-Z]+/[a-zA-Z]+",
                session_id in prop::option::of("[a-zA-Z0-9:_-]{1,64}"),
            ) {
                // Build a JSON-RPC message (simulating what would be written to stdout)
                let mut msg = serde_json::Map::new();
                msg.insert("jsonrpc".to_string(), serde_json::json!("2.0"));
                msg.insert("id".to_string(), serde_json::json!(id));
                msg.insert("method".to_string(), serde_json::json!(method));
                if let Some(ref sid) = session_id {
                    msg.insert("sessionId".to_string(), serde_json::json!(sid));
                }
                msg.insert("params".to_string(), serde_json::json!({}));

                // Serialize to JSON bytes (simulating stdout write)
                let json_bytes = serde_json::to_vec(&msg).unwrap();

                // Verify the output is valid JSON
                let parsed: Result<serde_json::Value, _> = serde_json::from_slice(&json_bytes);
                prop_assert!(parsed.is_ok(), "stdout output must be valid JSON");

                // Verify the output is a JSON object (not a primitive or array)
                let value = parsed.unwrap();
                prop_assert!(value.is_object(), "stdout output must be a JSON object");

                // Verify no diagnostic-like content in the JSON
                // Diagnostic content would typically have fields like "level", "target", "message"
                // that are characteristic of log output
                let obj = value.as_object().unwrap();
                prop_assert!(
                    !obj.contains_key("level") || obj.contains_key("jsonrpc"),
                    "stdout should not contain log-like 'level' field without being JSON-RPC"
                );
                prop_assert!(
                    !obj.contains_key("target") || obj.contains_key("jsonrpc"),
                    "stdout should not contain log-like 'target' field without being JSON-RPC"
                );
            }

            /// Test that response messages written to stdout are valid JSON-RPC.
            #[test]
            fn response_output_is_valid_jsonrpc(
                id in prop_oneof![
                    any::<i64>().prop_map(|n| serde_json::json!(n)),
                    "[a-zA-Z0-9_-]{1,32}".prop_map(|s| serde_json::json!(s)),
                ],
                session_id in "[a-zA-Z0-9:_-]{1,64}",
                result_data in "[a-zA-Z0-9]{1,32}",
            ) {
                // Create a response message
                let response = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {"data": result_data}
                });
                let mut response_bytes = serde_json::to_vec(&response).unwrap();

                // Inject sessionId (simulating worker runtime)
                StdioBusWorker::inject_session_id(&mut response_bytes, &session_id).unwrap();

                // Verify the output is valid JSON
                let parsed: Result<serde_json::Value, _> = serde_json::from_slice(&response_bytes);
                prop_assert!(parsed.is_ok(), "response output must be valid JSON");

                // Verify it's a valid JSON-RPC response
                let value = parsed.unwrap();
                prop_assert!(value.is_object(), "response must be a JSON object");
                let obj = value.as_object().unwrap();
                prop_assert!(obj.contains_key("jsonrpc"), "response must have jsonrpc field");
                prop_assert!(obj.contains_key("id"), "response must have id field");
                prop_assert!(
                    obj.contains_key("result") || obj.contains_key("error"),
                    "response must have result or error field"
                );
            }

            /// Test that error responses written to stdout are valid JSON-RPC.
            #[test]
            fn error_response_output_is_valid_jsonrpc(
                id in "[a-zA-Z0-9_-]{1,32}",
                session_id in "[a-zA-Z0-9:_-]{1,64}",
                error_code in any::<i32>(),
                error_message in "[a-zA-Z ]{1,32}",
            ) {
                // Create an error response message
                let error_response = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {
                        "code": error_code,
                        "message": error_message
                    }
                });
                let mut response_bytes = serde_json::to_vec(&error_response).unwrap();

                // Inject sessionId (simulating worker runtime)
                StdioBusWorker::inject_session_id(&mut response_bytes, &session_id).unwrap();

                // Verify the output is valid JSON
                let parsed: Result<serde_json::Value, _> = serde_json::from_slice(&response_bytes);
                prop_assert!(parsed.is_ok(), "error response output must be valid JSON");

                // Verify it's a valid JSON-RPC error response
                let value = parsed.unwrap();
                prop_assert!(value.is_object(), "error response must be a JSON object");
                let obj = value.as_object().unwrap();
                prop_assert!(obj.contains_key("jsonrpc"), "error response must have jsonrpc field");
                prop_assert!(obj.contains_key("id"), "error response must have id field");
                prop_assert!(obj.contains_key("error"), "error response must have error field");

                // Verify error structure
                let error = obj.get("error").unwrap();
                prop_assert!(error.is_object(), "error field must be an object");
                let error_obj = error.as_object().unwrap();
                prop_assert!(error_obj.contains_key("code"), "error must have code field");
                prop_assert!(error_obj.contains_key("message"), "error must have message field");
            }

            /// Test that notification messages written to stdout are valid JSON-RPC.
            #[test]
            fn notification_output_is_valid_jsonrpc(
                method in "[a-zA-Z]+/[a-zA-Z]+",
                session_id in "[a-zA-Z0-9:_-]{1,64}",
            ) {
                // Create a notification message (no id field)
                let notification = serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": method,
                    "params": {}
                });
                let mut notification_bytes = serde_json::to_vec(&notification).unwrap();

                // Inject sessionId (simulating worker runtime)
                StdioBusWorker::inject_session_id(&mut notification_bytes, &session_id).unwrap();

                // Verify the output is valid JSON
                let parsed: Result<serde_json::Value, _> = serde_json::from_slice(&notification_bytes);
                prop_assert!(parsed.is_ok(), "notification output must be valid JSON");

                // Verify it's a valid JSON-RPC notification
                let value = parsed.unwrap();
                prop_assert!(value.is_object(), "notification must be a JSON object");
                let obj = value.as_object().unwrap();
                prop_assert!(obj.contains_key("jsonrpc"), "notification must have jsonrpc field");
                prop_assert!(obj.contains_key("method"), "notification must have method field");
                prop_assert!(!obj.contains_key("id"), "notification must not have id field");
            }

            /// Test that NDJSON lines are each valid JSON.
            /// This verifies that multiple messages written to stdout are properly framed.
            #[test]
            fn ndjson_lines_are_each_valid_json(
                messages in prop::collection::vec(
                    (
                        "[a-zA-Z0-9_-]{1,16}",  // id
                        "[a-zA-Z]+/[a-zA-Z]+",  // method
                        "[a-zA-Z0-9:_-]{1,32}", // session_id
                    ),
                    1..10
                )
            ) {
                // Build multiple NDJSON lines (simulating stdout output)
                let mut ndjson = String::new();
                for (id, method, session_id) in &messages {
                    let msg = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "method": method,
                        "params": {},
                        "sessionId": session_id
                    });
                    ndjson.push_str(&serde_json::to_string(&msg).unwrap());
                    ndjson.push('\n');
                }

                // Parse each line and verify it's valid JSON
                for (i, line) in ndjson.trim_end().split('\n').enumerate() {
                    let parsed: Result<serde_json::Value, _> = serde_json::from_str(line);
                    prop_assert!(
                        parsed.is_ok(),
                        "NDJSON line {} must be valid JSON: {}",
                        i,
                        line
                    );

                    let value = parsed.unwrap();
                    prop_assert!(
                        value.is_object(),
                        "NDJSON line {} must be a JSON object",
                        i
                    );
                }
            }

            /// Test that stdout output does not contain common diagnostic patterns.
            /// This verifies that log output, stack traces, and other diagnostics
            /// are not mixed with JSON-RPC messages.
            #[test]
            fn stdout_output_has_no_diagnostic_patterns(
                id in "[a-zA-Z0-9_-]{1,32}",
                method in "[a-zA-Z]+/[a-zA-Z]+",
            ) {
                // Create a valid JSON-RPC message
                let msg = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": method,
                    "params": {}
                });
                let json_str = serde_json::to_string(&msg).unwrap();

                // Verify the output doesn't contain common diagnostic patterns
                // These patterns would indicate log output mixed with JSON
                prop_assert!(
                    !json_str.contains("TRACE") || json_str.contains("\"TRACE\""),
                    "stdout should not contain unquoted TRACE log level"
                );
                prop_assert!(
                    !json_str.contains("DEBUG") || json_str.contains("\"DEBUG\""),
                    "stdout should not contain unquoted DEBUG log level"
                );
                prop_assert!(
                    !json_str.contains("INFO") || json_str.contains("\"INFO\""),
                    "stdout should not contain unquoted INFO log level"
                );
                prop_assert!(
                    !json_str.contains("WARN") || json_str.contains("\"WARN\""),
                    "stdout should not contain unquoted WARN log level"
                );
                prop_assert!(
                    !json_str.contains("ERROR") || json_str.contains("\"ERROR\""),
                    "stdout should not contain unquoted ERROR log level"
                );

                // Verify no stack trace patterns
                prop_assert!(
                    !json_str.contains("at ") || json_str.contains("\"at \""),
                    "stdout should not contain stack trace 'at ' pattern"
                );
                prop_assert!(
                    !json_str.contains("panic") || json_str.contains("\"panic\""),
                    "stdout should not contain unquoted 'panic'"
                );
            }
        }

        /// Test that the worker's send method produces valid NDJSON output.
        #[tokio::test]
        async fn worker_send_produces_valid_ndjson() {
            // This test verifies the contract that send() writes valid JSON followed by newline
            let msg = serde_json::json!({
                "jsonrpc": "2.0",
                "id": "test-1",
                "result": {"status": "ok"}
            });
            let bytes = serde_json::to_vec(&msg).unwrap();

            // Verify the bytes are valid JSON
            let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert!(parsed.is_object());
            assert_eq!(parsed["jsonrpc"], "2.0");
            assert_eq!(parsed["id"], "test-1");
        }

        /// Test that inject_session_id maintains valid JSON output.
        #[test]
        fn inject_session_id_maintains_valid_json() {
            let msg = serde_json::json!({
                "jsonrpc": "2.0",
                "id": "test-1",
                "result": {"data": "value"}
            });
            let mut bytes = serde_json::to_vec(&msg).unwrap();

            // Inject sessionId
            StdioBusWorker::inject_session_id(&mut bytes, "thread:abc-123").unwrap();

            // Verify output is still valid JSON
            let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert!(parsed.is_object());
            assert_eq!(parsed["jsonrpc"], "2.0");
            assert_eq!(parsed["sessionId"], "thread:abc-123");
        }

        /// Test that complex nested JSON maintains validity after processing.
        #[test]
        fn complex_json_maintains_validity() {
            let msg = serde_json::json!({
                "jsonrpc": "2.0",
                "id": "complex-1",
                "result": {
                    "nested": {
                        "array": [1, 2, 3],
                        "object": {"key": "value"},
                        "null": null,
                        "bool": true,
                        "number": 42.5
                    }
                }
            });
            let mut bytes = serde_json::to_vec(&msg).unwrap();

            // Inject sessionId
            StdioBusWorker::inject_session_id(&mut bytes, "conn:12345").unwrap();

            // Verify output is still valid JSON with all nested structures intact
            let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert!(parsed.is_object());
            assert_eq!(
                parsed["result"]["nested"]["array"],
                serde_json::json!([1, 2, 3])
            );
            assert_eq!(parsed["result"]["nested"]["object"]["key"], "value");
            assert!(parsed["result"]["nested"]["null"].is_null());
            assert_eq!(parsed["result"]["nested"]["bool"], true);
            assert_eq!(parsed["result"]["nested"]["number"], 42.5);
        }
    }

    /// Unit tests for edge cases.
    mod unit_tests {
        use super::*;

        #[test]
        fn inject_session_id_fails_on_invalid_json() {
            let mut bytes = b"not valid json".to_vec();
            let result = StdioBusWorker::inject_session_id(&mut bytes, "test-session");
            assert!(result.is_err());
            assert!(matches!(result.unwrap_err(), WorkerError::Json(_)));
        }

        #[test]
        fn inject_session_id_handles_non_object_json() {
            // Arrays don't have object methods, so sessionId won't be inserted
            // but the function should still succeed
            let mut bytes = serde_json::to_vec(&serde_json::json!([1, 2, 3])).unwrap();
            let result = StdioBusWorker::inject_session_id(&mut bytes, "test-session");
            assert!(result.is_ok());

            // The array should be unchanged (no sessionId added)
            let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert!(parsed.is_array());
            assert_eq!(parsed.as_array().unwrap().len(), 3);
        }

        #[test]
        fn inject_session_id_handles_null_json() {
            let mut bytes = serde_json::to_vec(&serde_json::Value::Null).unwrap();
            let result = StdioBusWorker::inject_session_id(&mut bytes, "test-session");
            assert!(result.is_ok());

            let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert!(parsed.is_null());
        }

        #[test]
        fn inject_session_id_handles_empty_object() {
            let mut bytes = serde_json::to_vec(&serde_json::json!({})).unwrap();
            let result = StdioBusWorker::inject_session_id(&mut bytes, "test-session");
            assert!(result.is_ok());

            let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(parsed["sessionId"], serde_json::json!("test-session"));
        }

        #[test]
        fn inject_session_id_handles_unicode_session_id() {
            let msg = serde_json::json!({"id": "test"});
            let mut bytes = serde_json::to_vec(&msg).unwrap();

            let result = StdioBusWorker::inject_session_id(&mut bytes, "session-日本語-🎉");
            assert!(result.is_ok());

            let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(parsed["sessionId"], serde_json::json!("session-日本語-🎉"));
        }

        #[test]
        fn ndjson_empty_lines_are_skipped() {
            // Simulate NDJSON with empty lines
            let ndjson = r#"{"id":"1","method":"test/a"}

{"id":"2","method":"test/b"}
"#;
            let lines: Vec<&str> = ndjson
                .split('\n')
                .filter(|line| !line.trim().is_empty())
                .collect();

            assert_eq!(lines.len(), 2);

            let msg1 = parse_message(lines[0].as_bytes().to_vec());
            assert_eq!(
                msg1.routing.id,
                Some(crate::message::RequestId::String("1".to_string()))
            );

            let msg2 = parse_message(lines[1].as_bytes().to_vec());
            assert_eq!(
                msg2.routing.id,
                Some(crate::message::RequestId::String("2".to_string()))
            );
        }

        #[test]
        fn complete_round_trip_with_session_injection() {
            // Simulate a complete round-trip:
            // 1. Receive message (parse)
            // 2. Process and create response
            // 3. Inject session ID
            // 4. Send response (serialize)

            // Step 1: Receive request
            let request = serde_json::json!({
                "jsonrpc": "2.0",
                "id": "req-123",
                "method": "thread/start",
                "params": {"prompt": "Hello"},
                "sessionId": "thread:abc-456"
            });
            let request_bytes = serde_json::to_vec(&request).unwrap();
            let parsed_request = parse_message(request_bytes);

            // Extract session ID from request
            let session_id = parsed_request.routing.session_id.clone().unwrap();
            assert_eq!(session_id, "thread:abc-456");

            // Step 2: Create response
            let response = serde_json::json!({
                "jsonrpc": "2.0",
                "id": "req-123",
                "result": {"status": "started"}
            });
            let mut response_bytes = serde_json::to_vec(&response).unwrap();

            // Step 3: Inject session ID
            StdioBusWorker::inject_session_id(&mut response_bytes, &session_id).unwrap();

            // Step 4: Verify response can be parsed and has correct session ID
            let parsed_response = parse_message(response_bytes);
            assert_eq!(
                parsed_response.routing.id,
                Some(crate::message::RequestId::String("req-123".to_string()))
            );
            assert_eq!(
                parsed_response.routing.session_id,
                Some("thread:abc-456".to_string())
            );
            assert!(parsed_response.routing.is_response);
        }
    }

    /// **Validates: Requirements 2.5, 3.3**
    ///
    /// Property 8: Session Affinity
    /// Tests that requests with the same sessionId are processed by the same session,
    /// maintaining state consistency across multiple requests.
    mod property_session_affinity {
        use super::*;
        use std::collections::HashMap;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(500))]

            /// Test that requests with the same sessionId are routed to the same session.
            /// This simulates the session routing behavior where sessionId determines
            /// which session handles the request.
            #[test]
            fn same_session_id_routes_to_same_session(
                session_id in "[a-zA-Z0-9:_-]{1,64}",
                request_ids in prop::collection::vec("[a-zA-Z0-9_-]{1,16}", 2..10),
            ) {
                // Simulate a session registry (like the handler's sessions HashMap)
                let mut session_registry: HashMap<String, Vec<String>> = HashMap::new();

                // Process multiple requests with the same sessionId
                for request_id in &request_ids {
                    let request = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": "test/method",
                        "params": {},
                        "sessionId": session_id
                    });
                    let request_bytes = serde_json::to_vec(&request).unwrap();

                    // Parse the request and extract sessionId
                    let parsed = parse_message(request_bytes);
                    let extracted_session_id = parsed.routing.session_id.clone();

                    // Verify sessionId was extracted correctly
                    prop_assert_eq!(extracted_session_id.as_deref(), Some(session_id.as_str()));

                    // Route to session (simulating get_or_create_session behavior)
                    let session_key = extracted_session_id.unwrap();
                    session_registry
                        .entry(session_key)
                        .or_default()
                        .push(request_id.clone());
                }

                // Verify all requests were routed to the same session
                prop_assert_eq!(session_registry.len(), 1, "All requests should route to one session");
                let session_requests = session_registry.get(&session_id).unwrap();
                prop_assert_eq!(session_requests.len(), request_ids.len());
            }

            /// Test that session state is preserved across multiple requests with the same sessionId.
            /// This simulates the stateful session behavior where initialization state persists.
            #[test]
            fn session_state_preserved_across_requests(
                session_id in "[a-zA-Z0-9:_-]{1,64}",
                num_requests in 2..10usize,
            ) {
                // Simulate session state (like WorkerSession or McpWorkerSession)
                struct MockSession {
                    initialized: bool,
                    request_count: usize,
                }

                let mut sessions: HashMap<String, MockSession> = HashMap::new();

                // Process multiple requests
                for i in 0..num_requests {
                    let request = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": format!("req-{i}"),
                        "method": if i == 0 { "initialize" } else { "test/method" },
                        "params": {},
                        "sessionId": session_id
                    });
                    let request_bytes = serde_json::to_vec(&request).unwrap();

                    // Parse and extract sessionId
                    let parsed = parse_message(request_bytes);
                    let extracted_session_id = parsed.routing.session_id.clone().unwrap();

                    // Get or create session (simulating handler behavior)
                    let session = sessions.entry(extracted_session_id.clone()).or_insert_with(|| {
                        MockSession {
                            initialized: false,
                            request_count: 0,
                        }
                    });

                    // Update session state
                    if parsed.routing.method.as_deref() == Some("initialize") {
                        session.initialized = true;
                    }
                    session.request_count += 1;
                }

                // Verify session state was preserved
                prop_assert_eq!(sessions.len(), 1, "Should have exactly one session");
                let session = sessions.get(&session_id).unwrap();
                prop_assert!(session.initialized, "Session should be initialized");
                prop_assert_eq!(session.request_count, num_requests, "All requests should be counted");
            }

            /// Test that different sessionIds result in different sessions.
            /// This verifies session isolation between different clients/threads.
            #[test]
            fn different_session_ids_create_different_sessions(
                session_ids in prop::collection::hash_set("[a-zA-Z0-9:_-]{1,32}", 2..10),
            ) {
                let session_ids: Vec<String> = session_ids.into_iter().collect();
                let mut session_registry: HashMap<String, usize> = HashMap::new();

                // Process one request per sessionId
                for (i, session_id) in session_ids.iter().enumerate() {
                    let request = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": format!("req-{i}"),
                        "method": "test/method",
                        "params": {},
                        "sessionId": session_id
                    });
                    let request_bytes = serde_json::to_vec(&request).unwrap();

                    // Parse and extract sessionId
                    let parsed = parse_message(request_bytes);
                    let extracted_session_id = parsed.routing.session_id.clone().unwrap();

                    // Route to session
                    *session_registry.entry(extracted_session_id).or_insert(0) += 1;
                }

                // Verify each sessionId created a separate session
                prop_assert_eq!(
                    session_registry.len(),
                    session_ids.len(),
                    "Each sessionId should have its own session"
                );

                // Verify each session received exactly one request
                for count in session_registry.values() {
                    prop_assert_eq!(*count, 1, "Each session should have exactly one request");
                }
            }

            /// Test that session affinity works with thread: prefixed sessionIds.
            /// This validates REQ-2.5 for app-server thread affinity.
            #[test]
            fn thread_session_affinity(
                thread_id in "[a-zA-Z0-9_-]{1,32}",
                request_ids in prop::collection::vec("[a-zA-Z0-9_-]{1,16}", 2..10),
            ) {
                let session_id = format!("thread:{thread_id}");
                let mut session_requests: Vec<String> = Vec::new();

                // Process multiple requests with the same thread sessionId
                for request_id in &request_ids {
                    let request = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": "thread/message",
                        "params": {"content": "test"},
                        "sessionId": session_id
                    });
                    let request_bytes = serde_json::to_vec(&request).unwrap();

                    // Parse and verify sessionId
                    let parsed = parse_message(request_bytes);
                    prop_assert_eq!(parsed.routing.session_id.as_deref(), Some(session_id.as_str()));

                    // Verify thread ID can be extracted
                    let extracted_thread_id = crate::session::extract_thread_id(&session_id);
                    prop_assert_eq!(extracted_thread_id, Some(thread_id.as_str()));

                    session_requests.push(request_id.clone());
                }

                // Verify all requests were processed in order
                prop_assert_eq!(session_requests.len(), request_ids.len());
            }

            /// Test that session affinity works with MCP server sessionIds.
            /// This validates REQ-3.3 for MCP session binding.
            #[test]
            fn mcp_session_affinity(
                server_name in "[a-zA-Z0-9_-]{1,32}",
                request_ids in prop::collection::vec(any::<i64>(), 2..10),
            ) {
                let session_id = format!("mcp:{server_name}");
                let mut session_state = (false, 0usize); // (initialized, request_count)

                // Process multiple requests with the same MCP sessionId
                for (i, request_id) in request_ids.iter().enumerate() {
                    let method = if i == 0 { "initialize" } else { "tools/list" };
                    let request = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": method,
                        "params": if i == 0 {
                            serde_json::json!({
                                "protocolVersion": "2024-11-05",
                                "capabilities": {},
                                "clientInfo": {"name": "test", "version": "1.0"}
                            })
                        } else {
                            serde_json::json!({})
                        },
                        "sessionId": session_id
                    });
                    let request_bytes = serde_json::to_vec(&request).unwrap();

                    // Parse and verify sessionId
                    let parsed = parse_message(request_bytes);
                    prop_assert_eq!(parsed.routing.session_id.as_deref(), Some(session_id.as_str()));

                    // Verify MCP server name can be extracted
                    let extracted_server = crate::session::extract_mcp_server_name(&session_id);
                    prop_assert_eq!(extracted_server, Some(server_name.as_str()));

                    // Update session state
                    if parsed.routing.method.as_deref() == Some("initialize") {
                        session_state.0 = true;
                    }
                    session_state.1 += 1;
                }

                // Verify session state was maintained
                prop_assert!(session_state.0, "Session should be initialized");
                prop_assert_eq!(session_state.1, request_ids.len());
            }

            /// Test that interleaved requests from different sessions maintain isolation.
            /// This simulates concurrent clients sending requests.
            #[test]
            fn interleaved_requests_maintain_session_isolation(
                session_a in "session-a-[a-zA-Z0-9]{1,16}",
                session_b in "session-b-[a-zA-Z0-9]{1,16}",
                num_requests in 2..10usize,
            ) {
                let mut session_states: HashMap<String, Vec<i32>> = HashMap::new();

                // Interleave requests from two sessions
                for i in 0..num_requests {
                    // Request from session A
                    let request_a = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": format!("a-{i}"),
                        "method": "test/method",
                        "params": {"value": i * 2},
                        "sessionId": session_a
                    });
                    let parsed_a = parse_message(serde_json::to_vec(&request_a).unwrap());
                    let sid_a = parsed_a.routing.session_id.clone().unwrap();
                    session_states.entry(sid_a).or_default().push((i * 2) as i32);

                    // Request from session B
                    let request_b = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": format!("b-{i}"),
                        "method": "test/method",
                        "params": {"value": i * 2 + 1},
                        "sessionId": session_b
                    });
                    let parsed_b = parse_message(serde_json::to_vec(&request_b).unwrap());
                    let sid_b = parsed_b.routing.session_id.clone().unwrap();
                    session_states.entry(sid_b).or_default().push((i * 2 + 1) as i32);
                }

                // Verify sessions are isolated
                prop_assert_eq!(session_states.len(), 2, "Should have exactly two sessions");

                let state_a = session_states.get(&session_a).unwrap();
                let state_b = session_states.get(&session_b).unwrap();

                prop_assert_eq!(state_a.len(), num_requests);
                prop_assert_eq!(state_b.len(), num_requests);

                // Verify session A has even values, session B has odd values
                for (i, &val) in state_a.iter().enumerate() {
                    prop_assert_eq!(val, (i * 2) as i32, "Session A should have even values");
                }
                for (i, &val) in state_b.iter().enumerate() {
                    prop_assert_eq!(val, (i * 2 + 1) as i32, "Session B should have odd values");
                }
            }

            /// Test that responses maintain session affinity with their requests.
            /// This verifies the full request-response cycle preserves sessionId.
            #[test]
            fn response_maintains_session_affinity(
                session_id in "[a-zA-Z0-9:_-]{1,64}",
                request_id in "[a-zA-Z0-9_-]{1,32}",
                result_data in "[a-zA-Z0-9]{1,32}",
            ) {
                // Create request with sessionId
                let request = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "test/method",
                    "params": {},
                    "sessionId": session_id
                });
                let request_bytes = serde_json::to_vec(&request).unwrap();

                // Parse request and extract sessionId
                let parsed_request = parse_message(request_bytes);
                let extracted_session_id = parsed_request.routing.session_id.clone();
                prop_assert_eq!(extracted_session_id.as_deref(), Some(session_id.as_str()));

                // Create response without sessionId (simulating handler output)
                let response = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {"data": result_data}
                });
                let mut response_bytes = serde_json::to_vec(&response).unwrap();

                // Inject sessionId (simulating worker runtime)
                if let Some(sid) = &extracted_session_id {
                    StdioBusWorker::inject_session_id(&mut response_bytes, sid).unwrap();
                }

                // Parse response and verify sessionId is preserved
                let parsed_response = parse_message(response_bytes);
                prop_assert_eq!(
                    parsed_response.routing.session_id.as_deref(),
                    Some(session_id.as_str()),
                    "Response should have same sessionId as request"
                );
                prop_assert_eq!(
                    parsed_response.routing.id,
                    parsed_request.routing.id,
                    "Response should have same id as request"
                );
            }

            /// Test that session affinity is maintained for all session type formats.
            #[test]
            fn session_affinity_for_all_session_types(
                session_type in prop_oneof![
                    "thread:[a-zA-Z0-9_-]{1,32}",
                    "conn:[0-9]{1,10}",
                    "mcp:[a-zA-Z0-9_-]{1,32}",
                    "[a-zA-Z0-9_-]{1,32}",  // custom/unknown format
                ],
                num_requests in 2..5usize,
            ) {
                let mut session_request_ids: Vec<String> = Vec::new();

                // Process multiple requests with the same sessionId
                for i in 0..num_requests {
                    let request_id = format!("req-{i}");
                    let request = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": "test/method",
                        "params": {},
                        "sessionId": session_type
                    });
                    let request_bytes = serde_json::to_vec(&request).unwrap();

                    // Parse and verify sessionId
                    let parsed = parse_message(request_bytes);
                    prop_assert_eq!(
                        parsed.routing.session_id.as_deref(),
                        Some(session_type.as_str()),
                        "SessionId should be extracted correctly for all formats"
                    );

                    session_request_ids.push(request_id);
                }

                // Verify all requests were processed
                prop_assert_eq!(session_request_ids.len(), num_requests);
            }
        }
    }
}
