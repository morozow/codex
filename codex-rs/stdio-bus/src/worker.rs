//! Worker runtime for stdio_bus integration.

use crate::message::Message;
use crate::routing::parse_message;
use async_trait::async_trait;
use std::io::ErrorKind;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

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
}
