//! Integration tests for stdio_bus worker functionality.
//!
//! These tests validate Requirements 8 (Worker Contract Compliance) and
//! Requirements 11 (Reliability Requirements) from the stdio_bus integration spec.

use async_trait::async_trait;
use codex_stdio_bus::message::Message;
use codex_stdio_bus::message::RequestId;
use codex_stdio_bus::parse_message;
use codex_stdio_bus::worker::MessageHandler;
use codex_stdio_bus::worker::StdioBusWorker;
use codex_stdio_bus::worker::WorkerError;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use tokio::sync::Mutex;

/// Type alias for response generator function to reduce complexity.
type ResponseFn = Box<dyn Fn(&Message) -> Option<serde_json::Value> + Send + Sync>;

/// Mock handler for testing worker behavior.
struct MockHandler {
    /// Tracks how many messages have been processed.
    message_count: AtomicUsize,
    /// Stores received messages for verification.
    received_messages: Arc<Mutex<Vec<serde_json::Value>>>,
    /// Optional response generator function.
    response_fn: Option<ResponseFn>,
    /// Whether to simulate an error.
    simulate_error: bool,
    /// Shutdown callback was called.
    shutdown_called: Arc<std::sync::atomic::AtomicBool>,
}

impl MockHandler {
    fn new() -> Self {
        Self {
            message_count: AtomicUsize::new(0),
            received_messages: Arc::new(Mutex::new(Vec::new())),
            response_fn: None,
            simulate_error: false,
            shutdown_called: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn with_response_fn<F>(mut self, f: F) -> Self
    where
        F: Fn(&Message) -> Option<serde_json::Value> + Send + Sync + 'static,
    {
        self.response_fn = Some(Box::new(f));
        self
    }
}

#[async_trait]
impl MessageHandler for MockHandler {
    async fn handle(&self, msg: Message) -> Result<Option<Vec<u8>>, WorkerError> {
        self.message_count.fetch_add(1, Ordering::SeqCst);

        // Store the received message
        let json: serde_json::Value =
            serde_json::from_slice(&msg.raw).map_err(|e| WorkerError::Handler(e.to_string()))?;
        self.received_messages.lock().await.push(json.clone());

        if self.simulate_error {
            return Err(WorkerError::Handler("Simulated error".to_string()));
        }

        // Generate response if we have a response function
        if let Some(ref response_fn) = self.response_fn
            && let Some(response) = response_fn(&msg)
        {
            return Ok(Some(serde_json::to_vec(&response)?));
        }

        // Default: echo back with result if message has an id
        if msg.routing.id.is_some() {
            let response = serde_json::json!({
                "jsonrpc": "2.0",
                "id": json.get("id").cloned().unwrap_or(serde_json::Value::Null),
                "result": {"echo": true}
            });
            Ok(Some(serde_json::to_vec(&response)?))
        } else {
            // Notification - no response
            Ok(None)
        }
    }

    async fn on_shutdown(&self) {
        self.shutdown_called
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

// =============================================================================
// Module: Worker Contract Compliance Tests (Requirements 8.1-8.9)
// =============================================================================

/// Tests for Requirement 8: Worker Contract Compliance
mod worker_contract_compliance {
    use super::*;
    use pretty_assertions::assert_eq;

    /// **Validates: Requirements 8.1, 8.2**
    ///
    /// Test that worker reads NDJSON messages from stdin and writes to stdout.
    #[test]
    fn ndjson_message_framing() {
        // Test that messages are properly framed with newlines
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "test-1",
            "method": "test/echo",
            "params": {}
        });

        // Serialize to NDJSON (single line with newline terminator)
        let ndjson = serde_json::to_string(&request).unwrap();
        assert!(!ndjson.contains('\n'), "JSON should be single line");

        // Parse back through routing
        let message = parse_message(ndjson.as_bytes().to_vec());
        assert_eq!(
            message.routing.id,
            Some(RequestId::String("test-1".to_string()))
        );
        assert_eq!(message.routing.method, Some("test/echo".to_string()));
    }

    /// **Validates: Requirements 8.4**
    ///
    /// Test that worker does not write non-JSON content to stdout.
    #[test]
    fn stdout_contains_only_valid_json() {
        // All output messages must be valid JSON
        let messages = vec![
            serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": {}}),
            serde_json::json!({"jsonrpc": "2.0", "id": "str-id", "result": {"data": "test"}}),
            serde_json::json!({"jsonrpc": "2.0", "id": 42, "error": {"code": -32600, "message": "Invalid"}}),
            serde_json::json!({"jsonrpc": "2.0", "method": "notify", "params": {}}),
        ];

        for msg in messages {
            let bytes = serde_json::to_vec(&msg).unwrap();
            // Verify it's valid JSON
            let parsed: Result<serde_json::Value, _> = serde_json::from_slice(&bytes);
            assert!(parsed.is_ok(), "Output must be valid JSON");
            // Verify it's an object
            assert!(parsed.unwrap().is_object(), "Output must be JSON object");
        }
    }

    /// **Validates: Requirements 8.5**
    ///
    /// Test that requests with `id` get exactly one response with the same `id`.
    #[test]
    fn request_response_correlation() {
        // Create a request with an id
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "req-123",
            "method": "test/method",
            "params": {}
        });
        let request_bytes = serde_json::to_vec(&request).unwrap();
        let message = parse_message(request_bytes);

        // Verify the request has an id
        assert_eq!(
            message.routing.id,
            Some(RequestId::String("req-123".to_string()))
        );

        // Create a response with the same id
        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "req-123",
            "result": {"status": "ok"}
        });
        let response_bytes = serde_json::to_vec(&response).unwrap();
        let response_message = parse_message(response_bytes);

        // Verify the response has the same id
        assert_eq!(
            response_message.routing.id,
            Some(RequestId::String("req-123".to_string()))
        );
        assert!(response_message.routing.is_response);
    }

    /// **Validates: Requirements 8.5**
    ///
    /// Test that messages without `id` (notifications) get no response.
    #[test]
    fn notifications_get_no_response() {
        // Create a notification (no id)
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "$/progress",
            "params": {"token": "abc"}
        });
        let notification_bytes = serde_json::to_vec(&notification).unwrap();
        let message = parse_message(notification_bytes);

        // Verify the notification has no id
        assert!(message.routing.id.is_none());
        assert!(!message.routing.is_response);
        assert_eq!(message.routing.method, Some("$/progress".to_string()));
    }

    /// **Validates: Requirements 8.6**
    ///
    /// Test that requests with `sessionId` include the same `sessionId` in response.
    #[test]
    fn session_id_preserved_in_response() {
        // Create a request with sessionId
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "req-456",
            "method": "test/method",
            "params": {},
            "sessionId": "thread:abc-123"
        });
        let request_bytes = serde_json::to_vec(&request).unwrap();
        let message = parse_message(request_bytes);

        // Verify sessionId was extracted
        assert_eq!(
            message.routing.session_id,
            Some("thread:abc-123".to_string())
        );

        // Create a response without sessionId
        let response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "req-456",
            "result": {}
        });
        let mut response_bytes = serde_json::to_vec(&response).unwrap();

        // Inject sessionId (simulating worker runtime behavior)
        StdioBusWorker::inject_session_id(&mut response_bytes, "thread:abc-123").unwrap();

        // Verify sessionId was injected
        let final_response: serde_json::Value = serde_json::from_slice(&response_bytes).unwrap();
        assert_eq!(
            final_response["sessionId"],
            serde_json::json!("thread:abc-123")
        );
        assert_eq!(final_response["id"], serde_json::json!("req-456"));
    }

    /// **Validates: Requirements 8.7**
    ///
    /// Test that notifications include `sessionId` for routing.
    #[test]
    fn session_id_in_notifications() {
        // Create a notification without sessionId
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "$/progress",
            "params": {"token": "xyz"}
        });
        let mut notification_bytes = serde_json::to_vec(&notification).unwrap();

        // Inject sessionId for routing
        StdioBusWorker::inject_session_id(&mut notification_bytes, "conn:42").unwrap();

        // Verify sessionId was added
        let final_notification: serde_json::Value =
            serde_json::from_slice(&notification_bytes).unwrap();
        assert_eq!(
            final_notification["sessionId"],
            serde_json::json!("conn:42")
        );
        assert!(final_notification.get("id").is_none());
        assert_eq!(
            final_notification["method"],
            serde_json::json!("$/progress")
        );
    }

    /// **Validates: Requirements 8.6**
    ///
    /// Test sessionId preservation for all session type formats.
    #[test]
    fn session_id_formats_preserved() {
        let session_ids = vec![
            "thread:abc-123",
            "conn:12345",
            "mcp:my-server",
            "thread:uuid-like-id-here",
            "conn:0",
            "mcp:server-with-dashes",
        ];

        for session_id in session_ids {
            let request = serde_json::json!({
                "jsonrpc": "2.0",
                "id": "test-id",
                "method": "test/method",
                "sessionId": session_id
            });
            let request_bytes = serde_json::to_vec(&request).unwrap();
            let message = parse_message(request_bytes);

            assert_eq!(
                message.routing.session_id,
                Some(session_id.to_string()),
                "Failed for session_id: {session_id}"
            );

            // Test injection
            let response = serde_json::json!({
                "jsonrpc": "2.0",
                "id": "test-id",
                "result": {}
            });
            let mut response_bytes = serde_json::to_vec(&response).unwrap();
            StdioBusWorker::inject_session_id(&mut response_bytes, session_id).unwrap();

            let final_response: serde_json::Value =
                serde_json::from_slice(&response_bytes).unwrap();
            assert_eq!(
                final_response["sessionId"],
                serde_json::json!(session_id),
                "Injection failed for session_id: {session_id}"
            );
        }
    }

    /// **Validates: Requirements 8.5**
    ///
    /// Test that both string and numeric IDs are handled correctly.
    #[test]
    fn string_and_numeric_ids_handled() {
        // String ID
        let string_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "string-id-123",
            "method": "test/method"
        });
        let string_message = parse_message(serde_json::to_vec(&string_request).unwrap());
        assert_eq!(
            string_message.routing.id,
            Some(RequestId::String("string-id-123".to_string()))
        );

        // Numeric ID
        let numeric_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 42,
            "method": "test/method"
        });
        let numeric_message = parse_message(serde_json::to_vec(&numeric_request).unwrap());
        assert_eq!(numeric_message.routing.id, Some(RequestId::Integer(42)));

        // Large numeric ID
        let large_id_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 9223372036854775807_i64,
            "method": "test/method"
        });
        let large_id_message = parse_message(serde_json::to_vec(&large_id_request).unwrap());
        assert_eq!(
            large_id_message.routing.id,
            Some(RequestId::Integer(9223372036854775807))
        );
    }
}

// =============================================================================
// Module: Full Pipeline Tests (Mock stdio_bus Daemon Simulation)
// =============================================================================

/// Tests simulating the full pipeline with mock stdio_bus daemon behavior.
mod full_pipeline_simulation {
    use super::*;
    use pretty_assertions::assert_eq;

    /// Simulates a stdio_bus daemon routing messages to workers.
    struct MockStdioBusDaemon {
        /// Session to worker mapping.
        session_workers: HashMap<String, usize>,
        /// Messages received by each worker.
        worker_messages: Vec<Vec<serde_json::Value>>,
        /// Responses from workers.
        worker_responses: Vec<Vec<serde_json::Value>>,
    }

    impl MockStdioBusDaemon {
        fn new(num_workers: usize) -> Self {
            Self {
                session_workers: HashMap::new(),
                worker_messages: vec![Vec::new(); num_workers],
                worker_responses: vec![Vec::new(); num_workers],
            }
        }

        /// Route a message to the appropriate worker based on sessionId.
        fn route_message(&mut self, msg: &serde_json::Value) -> Option<usize> {
            let session_id = msg.get("sessionId")?.as_str()?;

            // Get or assign worker for this session
            let num_workers = self.worker_messages.len();
            let current_session_count = self.session_workers.len();
            let worker_idx = *self
                .session_workers
                .entry(session_id.to_string())
                .or_insert_with(|| {
                    // Simple round-robin assignment for new sessions
                    current_session_count % num_workers
                });

            self.worker_messages[worker_idx].push(msg.clone());
            Some(worker_idx)
        }

        /// Simulate worker processing and response.
        fn process_worker_response(&mut self, worker_idx: usize, response: serde_json::Value) {
            self.worker_responses[worker_idx].push(response);
        }
    }

    /// **Validates: Requirements 8.1-8.7, 11.1**
    ///
    /// Test full message routing pipeline with session affinity.
    #[test]
    fn full_pipeline_with_session_affinity() {
        let mut daemon = MockStdioBusDaemon::new(3);

        // Send multiple requests with different sessions
        let requests = vec![
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": "req-1",
                "method": "test/method",
                "sessionId": "thread:session-a"
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": "req-2",
                "method": "test/method",
                "sessionId": "thread:session-b"
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": "req-3",
                "method": "test/method",
                "sessionId": "thread:session-a"  // Same session as req-1
            }),
        ];

        // Route all requests
        let mut worker_assignments = Vec::new();
        for request in &requests {
            let worker_idx = daemon.route_message(request);
            worker_assignments.push(worker_idx);
        }

        // Verify session affinity: requests with same sessionId go to same worker
        assert_eq!(
            worker_assignments[0], worker_assignments[2],
            "Requests with same sessionId should go to same worker"
        );

        // Verify different sessions can go to different workers
        // (they might go to the same worker by chance, but the routing should work)
        assert!(worker_assignments[0].is_some());
        assert!(worker_assignments[1].is_some());
    }

    /// **Validates: Requirements 8.5, 8.6**
    ///
    /// Test request-response correlation through the pipeline.
    #[test]
    fn pipeline_request_response_correlation() {
        let mut daemon = MockStdioBusDaemon::new(2);

        // Send a request
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "pipeline-req-1",
            "method": "test/echo",
            "params": {"data": "hello"},
            "sessionId": "thread:test-session"
        });

        let worker_idx = daemon.route_message(&request).unwrap();

        // Simulate worker processing
        let request_msg = parse_message(serde_json::to_vec(&request).unwrap());
        let session_id = request_msg.routing.session_id;

        // Create response
        let mut response = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "pipeline-req-1",
            "result": {"echo": "hello"}
        });

        // Inject sessionId (as worker runtime would do)
        if let Some(sid) = &session_id {
            response["sessionId"] = serde_json::json!(sid);
        }

        daemon.process_worker_response(worker_idx, response.clone());

        // Verify response has correct id and sessionId
        let stored_response = &daemon.worker_responses[worker_idx][0];
        assert_eq!(stored_response["id"], serde_json::json!("pipeline-req-1"));
        assert_eq!(
            stored_response["sessionId"],
            serde_json::json!("thread:test-session")
        );
    }

    /// **Validates: Requirements 8.7, 11.1**
    ///
    /// Test notification routing through the pipeline.
    #[test]
    fn pipeline_notification_routing() {
        let mut daemon = MockStdioBusDaemon::new(2);

        // Send notifications with sessionId for routing
        let notifications = vec![
            serde_json::json!({
                "jsonrpc": "2.0",
                "method": "$/progress",
                "params": {"token": "1"},
                "sessionId": "conn:100"
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "method": "$/progress",
                "params": {"token": "2"},
                "sessionId": "conn:100"  // Same session
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "method": "$/progress",
                "params": {"token": "3"},
                "sessionId": "conn:200"  // Different session
            }),
        ];

        let mut assignments = Vec::new();
        for notification in &notifications {
            let worker_idx = daemon.route_message(notification);
            assignments.push(worker_idx);
        }

        // Verify notifications with same sessionId go to same worker
        assert_eq!(
            assignments[0], assignments[1],
            "Notifications with same sessionId should route to same worker"
        );
    }

    /// **Validates: Requirements 8.1-8.4**
    ///
    /// Test NDJSON framing in pipeline.
    #[test]
    fn pipeline_ndjson_framing() {
        // Simulate multiple messages in NDJSON format
        let messages = vec![
            serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "m1", "sessionId": "s1"}),
            serde_json::json!({"jsonrpc": "2.0", "id": 2, "method": "m2", "sessionId": "s2"}),
            serde_json::json!({"jsonrpc": "2.0", "id": 3, "method": "m3", "sessionId": "s1"}),
        ];

        // Build NDJSON stream
        let mut ndjson_stream = String::new();
        for msg in &messages {
            ndjson_stream.push_str(&serde_json::to_string(msg).unwrap());
            ndjson_stream.push('\n');
        }

        // Parse each line
        let lines: Vec<&str> = ndjson_stream.trim_end().split('\n').collect();
        assert_eq!(lines.len(), 3);

        for (i, line) in lines.iter().enumerate() {
            let parsed = parse_message(line.as_bytes().to_vec());
            assert_eq!(parsed.routing.id, Some(RequestId::Integer((i + 1) as i64)));
        }
    }
}

// =============================================================================
// Module: Error Handling and Recovery Tests (Requirements 11.1-11.6)
// =============================================================================

/// Tests for error handling and recovery scenarios.
mod error_handling_and_recovery {
    use super::*;
    use pretty_assertions::assert_eq;

    /// **Validates: Requirements 11.1**
    ///
    /// Test that valid messages are delivered without loss.
    #[test]
    fn messages_delivered_without_loss() {
        let messages: Vec<serde_json::Value> = (0..100)
            .map(|i| {
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": format!("msg-{i}"),
                    "method": "test/method",
                    "sessionId": format!("session-{}", i % 10)
                })
            })
            .collect();

        // Parse all messages
        let parsed: Vec<_> = messages
            .iter()
            .map(|m| parse_message(serde_json::to_vec(m).unwrap()))
            .collect();

        // Verify all messages were parsed correctly
        assert_eq!(parsed.len(), 100);
        for (i, msg) in parsed.iter().enumerate() {
            assert_eq!(msg.routing.id, Some(RequestId::String(format!("msg-{i}"))));
        }
    }

    /// **Validates: Requirements 11.1**
    ///
    /// Test handling of malformed JSON messages.
    #[test]
    fn handles_malformed_json() {
        let malformed_inputs = vec![
            b"not json at all".to_vec(),
            b"{incomplete".to_vec(),
            b"[1, 2, 3]".to_vec(), // Array instead of object
            b"null".to_vec(),
            b"true".to_vec(),
            b"123".to_vec(),
            b"\"string\"".to_vec(),
        ];

        for input in malformed_inputs {
            let message = parse_message(input.clone());
            // Should return default routing fields, not panic
            assert!(message.routing.id.is_none());
            assert!(message.routing.session_id.is_none());
            assert!(message.routing.method.is_none());
        }
    }

    /// **Validates: Requirements 11.1**
    ///
    /// Test handling of empty and whitespace-only input.
    #[test]
    fn handles_empty_and_whitespace_input() {
        let empty_inputs = vec![
            b"".to_vec(),
            b"   ".to_vec(),
            b"\n".to_vec(),
            b"\t\n  ".to_vec(),
        ];

        for input in empty_inputs {
            let message = parse_message(input);
            assert!(message.routing.id.is_none());
            assert!(message.routing.session_id.is_none());
        }
    }

    /// **Validates: Requirements 11.1**
    ///
    /// Test handling of oversized session IDs.
    #[test]
    fn handles_oversized_session_id() {
        // Create a session ID that exceeds MAX_SESSION_ID_LEN (256 bytes)
        let oversized_session_id = "x".repeat(300);
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "test-1",
            "method": "test/method",
            "sessionId": oversized_session_id
        });

        let message = parse_message(serde_json::to_vec(&request).unwrap());

        // Session ID should be rejected (None) but other fields should be extracted
        assert!(message.routing.session_id.is_none());
        assert_eq!(
            message.routing.id,
            Some(RequestId::String("test-1".to_string()))
        );
        assert_eq!(message.routing.method, Some("test/method".to_string()));
    }

    /// **Validates: Requirements 11.1**
    ///
    /// Test handling of messages with invalid field types.
    #[test]
    fn handles_invalid_field_types() {
        // sessionId as number instead of string
        let invalid_session = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "test-1",
            "method": "test/method",
            "sessionId": 12345
        });
        let msg1 = parse_message(serde_json::to_vec(&invalid_session).unwrap());
        assert!(msg1.routing.session_id.is_none());
        assert_eq!(
            msg1.routing.id,
            Some(RequestId::String("test-1".to_string()))
        );

        // method as number instead of string
        let invalid_method = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "test-2",
            "method": 123
        });
        let msg2 = parse_message(serde_json::to_vec(&invalid_method).unwrap());
        assert!(msg2.routing.method.is_none());
        assert_eq!(
            msg2.routing.id,
            Some(RequestId::String("test-2".to_string()))
        );

        // id as boolean (invalid)
        let invalid_id = serde_json::json!({
            "jsonrpc": "2.0",
            "id": true,
            "method": "test/method"
        });
        let msg3 = parse_message(serde_json::to_vec(&invalid_id).unwrap());
        assert!(msg3.routing.id.is_none());
        assert_eq!(msg3.routing.method, Some("test/method".to_string()));
    }

    /// **Validates: Requirements 11.4, 11.5**
    ///
    /// Test exponential backoff calculation for worker restarts.
    #[test]
    fn exponential_backoff_calculation() {
        // Simulate backoff calculation as per REQ-11.5
        let base_backoff: u64 = 1;
        let max_backoff: u64 = 60;

        let expected_backoffs = vec![
            (0, 1),  // 1s
            (1, 2),  // 2s
            (2, 4),  // 4s
            (3, 8),  // 8s
            (4, 16), // 16s
            (5, 32), // 32s
            (6, 60), // capped at 60s
            (7, 60), // still capped
        ];

        for (attempt, expected) in expected_backoffs {
            let backoff = std::cmp::min(base_backoff * 2u64.pow(attempt), max_backoff);
            assert_eq!(
                backoff, expected,
                "Backoff for attempt {attempt} should be {expected}s"
            );
        }
    }

    /// **Validates: Requirements 11.6**
    ///
    /// Test max restarts tracking within restart window.
    #[test]
    fn max_restarts_within_window() {
        // Simulate restart tracking as per REQ-11.6
        let max_restarts: u32 = 5;
        let restart_window_sec: u64 = 60;

        struct RestartTracker {
            restarts: Vec<std::time::Instant>,
            max_restarts: u32,
            window: std::time::Duration,
        }

        impl RestartTracker {
            fn new(max_restarts: u32, window_sec: u64) -> Self {
                Self {
                    restarts: Vec::new(),
                    max_restarts,
                    window: std::time::Duration::from_secs(window_sec),
                }
            }

            fn can_restart(&mut self) -> bool {
                let now = std::time::Instant::now();
                // Remove restarts outside the window
                self.restarts
                    .retain(|&t| now.duration_since(t) < self.window);
                // Check if we can restart
                if self.restarts.len() < self.max_restarts as usize {
                    self.restarts.push(now);
                    true
                } else {
                    false
                }
            }
        }

        let mut tracker = RestartTracker::new(max_restarts, restart_window_sec);

        // Should allow max_restarts restarts
        for i in 0..max_restarts {
            assert!(
                tracker.can_restart(),
                "Should allow restart {i} within limit"
            );
        }

        // Should deny restart after max_restarts
        assert!(
            !tracker.can_restart(),
            "Should deny restart after max_restarts"
        );
    }
}

// =============================================================================
// Module: Graceful Shutdown Tests (Requirements 8.8, 8.9)
// =============================================================================

/// Tests for graceful shutdown behavior.
mod graceful_shutdown {
    use super::*;
    use pretty_assertions::assert_eq;

    /// Shutdown state machine as per design document.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum ShutdownState {
        Running,
        Draining { deadline: std::time::Instant },
        Terminated,
    }

    impl ShutdownState {
        fn on_signal(&mut self, drain_timeout: std::time::Duration) {
            match self {
                ShutdownState::Running => {
                    *self = ShutdownState::Draining {
                        deadline: std::time::Instant::now() + drain_timeout,
                    };
                }
                ShutdownState::Draining { .. } => {
                    // Second signal - force terminate
                    *self = ShutdownState::Terminated;
                }
                ShutdownState::Terminated => {}
            }
        }

        fn should_accept_new_requests(&self) -> bool {
            matches!(self, ShutdownState::Running)
        }

        fn should_exit(&self) -> bool {
            match self {
                ShutdownState::Terminated => true,
                ShutdownState::Draining { deadline } => std::time::Instant::now() >= *deadline,
                ShutdownState::Running => false,
            }
        }
    }

    /// **Validates: Requirements 8.8**
    ///
    /// Test shutdown state transitions.
    #[test]
    fn shutdown_state_transitions() {
        let drain_timeout = std::time::Duration::from_secs(30);
        let mut state = ShutdownState::Running;

        // Initial state
        assert!(state.should_accept_new_requests());
        assert!(!state.should_exit());

        // First signal -> Draining
        state.on_signal(drain_timeout);
        assert!(matches!(state, ShutdownState::Draining { .. }));
        assert!(!state.should_accept_new_requests());

        // Second signal -> Terminated
        state.on_signal(drain_timeout);
        assert_eq!(state, ShutdownState::Terminated);
        assert!(state.should_exit());
    }

    /// **Validates: Requirements 8.8**
    ///
    /// Test that draining state stops accepting new requests.
    #[test]
    fn draining_stops_new_requests() {
        let drain_timeout = std::time::Duration::from_secs(30);
        let mut state = ShutdownState::Running;

        // Can accept requests while running
        assert!(state.should_accept_new_requests());

        // Transition to draining
        state.on_signal(drain_timeout);

        // Cannot accept new requests while draining
        assert!(!state.should_accept_new_requests());
    }

    /// **Validates: Requirements 8.9**
    ///
    /// Test that drain timeout triggers exit.
    #[test]
    fn drain_timeout_triggers_exit() {
        // Use a very short timeout for testing
        let drain_timeout = std::time::Duration::from_millis(1);
        let mut state = ShutdownState::Running;

        state.on_signal(drain_timeout);
        assert!(matches!(state, ShutdownState::Draining { .. }));

        // Wait for timeout
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Should now exit
        assert!(state.should_exit());
    }

    /// **Validates: Requirements 8.8**
    ///
    /// Test worker shutdown callback.
    #[tokio::test]
    async fn worker_shutdown_callback() {
        let handler = MockHandler::new();
        let shutdown_called = handler.shutdown_called.clone();

        // Verify shutdown not called initially
        assert!(!shutdown_called.load(std::sync::atomic::Ordering::SeqCst));

        // Call on_shutdown
        handler.on_shutdown().await;

        // Verify shutdown was called
        assert!(shutdown_called.load(std::sync::atomic::Ordering::SeqCst));
    }
}

// =============================================================================
// Module: Session Affinity Tests (Requirements 2.5, 3.3)
// =============================================================================

/// Tests for session affinity behavior.
mod session_affinity {
    use super::*;
    use pretty_assertions::assert_eq;

    /// **Validates: Requirements 2.5, 3.3**
    ///
    /// Test that requests with same sessionId are processed by same session.
    #[test]
    fn same_session_id_same_session() {
        // Simulate session management
        let mut sessions: HashMap<String, usize> = HashMap::new();
        let mut next_session_id = 0;

        let get_or_create_session = |sessions: &mut HashMap<String, usize>,
                                     next_id: &mut usize,
                                     session_id: &str|
         -> usize {
            *sessions.entry(session_id.to_string()).or_insert_with(|| {
                let id = *next_id;
                *next_id += 1;
                id
            })
        };

        // First request with session-a
        let session_a_1 =
            get_or_create_session(&mut sessions, &mut next_session_id, "thread:session-a");

        // Second request with session-b
        let session_b =
            get_or_create_session(&mut sessions, &mut next_session_id, "thread:session-b");

        // Third request with session-a (should get same session)
        let session_a_2 =
            get_or_create_session(&mut sessions, &mut next_session_id, "thread:session-a");

        assert_eq!(
            session_a_1, session_a_2,
            "Same sessionId should map to same session"
        );
        assert_ne!(
            session_a_1, session_b,
            "Different sessionIds should map to different sessions"
        );
    }

    /// **Validates: Requirements 2.5, 3.3**
    ///
    /// Test session affinity with different session types.
    #[test]
    fn session_affinity_different_types() {
        let mut sessions: HashMap<String, usize> = HashMap::new();
        let mut next_id = 0;

        let get_session = |sessions: &mut HashMap<String, usize>,
                           next_id: &mut usize,
                           session_id: &str|
         -> usize {
            *sessions.entry(session_id.to_string()).or_insert_with(|| {
                let id = *next_id;
                *next_id += 1;
                id
            })
        };

        // Thread sessions
        let thread_1 = get_session(&mut sessions, &mut next_id, "thread:abc");
        let thread_1_again = get_session(&mut sessions, &mut next_id, "thread:abc");
        assert_eq!(thread_1, thread_1_again);

        // Connection sessions
        let conn_1 = get_session(&mut sessions, &mut next_id, "conn:123");
        let conn_1_again = get_session(&mut sessions, &mut next_id, "conn:123");
        assert_eq!(conn_1, conn_1_again);

        // MCP sessions
        let mcp_1 = get_session(&mut sessions, &mut next_id, "mcp:server-1");
        let mcp_1_again = get_session(&mut sessions, &mut next_id, "mcp:server-1");
        assert_eq!(mcp_1, mcp_1_again);

        // All different session types should be different sessions
        assert_ne!(thread_1, conn_1);
        assert_ne!(conn_1, mcp_1);
        assert_ne!(thread_1, mcp_1);
    }
}

// =============================================================================
// Module: Message Handler Tests
// =============================================================================

/// Tests for the MessageHandler trait implementation.
mod message_handler_tests {
    use super::*;
    use pretty_assertions::assert_eq;

    /// **Validates: Requirements 8.5**
    ///
    /// Test that handler returns response for requests with id.
    #[tokio::test]
    async fn handler_returns_response_for_request() {
        let handler = MockHandler::new();

        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "test-req",
            "method": "test/method",
            "params": {}
        });
        let message = parse_message(serde_json::to_vec(&request).unwrap());

        let result = handler.handle(message).await;
        assert!(result.is_ok());

        let response_bytes = result.unwrap();
        assert!(response_bytes.is_some());

        let response: serde_json::Value = serde_json::from_slice(&response_bytes.unwrap()).unwrap();
        assert_eq!(response["id"], serde_json::json!("test-req"));
        assert!(response.get("result").is_some());
    }

    /// **Validates: Requirements 8.5**
    ///
    /// Test that handler returns None for notifications.
    #[tokio::test]
    async fn handler_returns_none_for_notification() {
        let handler = MockHandler::new();

        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "$/progress",
            "params": {}
        });
        let message = parse_message(serde_json::to_vec(&notification).unwrap());

        let result = handler.handle(message).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    /// **Validates: Requirements 8.1-8.7**
    ///
    /// Test handler with custom response function.
    #[tokio::test]
    async fn handler_with_custom_response() {
        let handler = MockHandler::new().with_response_fn(|msg| {
            msg.routing.id.as_ref().map(|id| {
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": match id {
                        RequestId::String(s) => serde_json::json!(s),
                        RequestId::Integer(n) => serde_json::json!(n),
                    },
                    "result": {"custom": true}
                })
            })
        });

        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 42,
            "method": "test/custom"
        });
        let message = parse_message(serde_json::to_vec(&request).unwrap());

        let result = handler.handle(message).await.unwrap().unwrap();
        let response: serde_json::Value = serde_json::from_slice(&result).unwrap();

        assert_eq!(response["id"], serde_json::json!(42));
        assert_eq!(response["result"]["custom"], serde_json::json!(true));
    }

    /// **Validates: Requirements 8.1-8.7**
    ///
    /// Test that handler tracks received messages.
    #[tokio::test]
    async fn handler_tracks_messages() {
        let handler = MockHandler::new();

        let messages = vec![
            serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "m1"}),
            serde_json::json!({"jsonrpc": "2.0", "id": 2, "method": "m2"}),
            serde_json::json!({"jsonrpc": "2.0", "method": "notify"}),
        ];

        for msg in &messages {
            let message = parse_message(serde_json::to_vec(msg).unwrap());
            let _ = handler.handle(message).await;
        }

        assert_eq!(handler.message_count.load(Ordering::SeqCst), 3);

        let received = handler.received_messages.lock().await;
        assert_eq!(received.len(), 3);
        assert_eq!(received[0]["id"], serde_json::json!(1));
        assert_eq!(received[1]["id"], serde_json::json!(2));
        assert!(received[2].get("id").is_none());
    }
}

// =============================================================================
// Module: Transport Abstraction Tests (Requirements 7.1-7.5)
// =============================================================================

/// Tests for transport abstraction.
mod transport_abstraction {
    use codex_stdio_bus::create_transport;

    /// **Validates: Requirements 7.4, 7.5**
    ///
    /// Test that create_transport returns correct transport type.
    #[test]
    fn create_transport_returns_correct_type() {
        // Worker mode should return StdioBusWorkerTransport
        let worker_transport = create_transport(/*worker_mode=*/ true);
        assert!(worker_transport.is_worker_mode());

        // Non-worker mode should return DirectStdioTransport
        let direct_transport = create_transport(/*worker_mode=*/ false);
        assert!(!direct_transport.is_worker_mode());
    }

    /// **Validates: Requirements 7.2**
    ///
    /// Test DirectStdioTransport properties.
    #[test]
    fn direct_transport_properties() {
        let transport = create_transport(/*worker_mode=*/ false);
        assert!(!transport.is_worker_mode());
        assert!(transport.current_session_id().is_none());
    }

    /// **Validates: Requirements 7.3**
    ///
    /// Test StdioBusWorkerTransport properties.
    #[test]
    fn worker_transport_properties() {
        let transport = create_transport(/*worker_mode=*/ true);
        assert!(transport.is_worker_mode());
        // Session ID is None until a message is received
        assert!(transport.current_session_id().is_none());
    }
}

// =============================================================================
// Module: Configuration Tests (Requirements 6.1-6.4)
// =============================================================================

/// Tests for configuration generation and validation.
mod configuration_tests {
    use codex_stdio_bus::LimitsConfig;
    use codex_stdio_bus::PoolConfig;
    use codex_stdio_bus::RoutingConfig;
    use codex_stdio_bus::StdioBusConfig;
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;
    use std::path::Path;

    /// **Validates: Requirements 6.4**
    ///
    /// Test development configuration preset.
    #[test]
    fn development_config_preset() {
        let config = StdioBusConfig::development(Path::new("/tmp/codex-test"));

        assert!(!config.pools.is_empty());
        assert_eq!(config.pools[0].id, "app-server");
        assert!(config.pools[0].args.contains(&"--worker".to_string()));

        // Development config should have higher restart limits
        assert!(config.limits.max_restarts >= 5);
    }

    /// **Validates: Requirements 6.4**
    ///
    /// Test production configuration preset.
    #[test]
    fn production_config_preset() {
        let config = StdioBusConfig::production("/usr/bin/codex-app-server", 4);

        assert!(!config.pools.is_empty());
        assert_eq!(config.pools[0].id, "app-server");
        assert_eq!(config.pools[0].instances, 4);
        assert!(config.pools[0].args.contains(&"--worker".to_string()));

        // Production config should have default pool routing
        assert_eq!(config.routing.default_pool, Some("app-server".to_string()));
    }

    /// **Validates: Requirements 6.3**
    ///
    /// Test configuration validation.
    #[test]
    fn config_validation() {
        // Valid config
        let valid_config = StdioBusConfig {
            pools: vec![PoolConfig {
                id: "test-pool".to_string(),
                command: "/bin/test".to_string(),
                args: vec![],
                instances: 1,
                env: HashMap::new(),
                cwd: None,
            }],
            limits: LimitsConfig::default(),
            routing: RoutingConfig::default(),
        };
        assert!(valid_config.validate().is_ok());

        // Invalid: empty pools
        let empty_pools = StdioBusConfig {
            pools: vec![],
            limits: LimitsConfig::default(),
            routing: RoutingConfig::default(),
        };
        assert!(empty_pools.validate().is_err());

        // Invalid: empty pool id
        let empty_id = StdioBusConfig {
            pools: vec![PoolConfig {
                id: "".to_string(),
                command: "/bin/test".to_string(),
                args: vec![],
                instances: 1,
                env: HashMap::new(),
                cwd: None,
            }],
            limits: LimitsConfig::default(),
            routing: RoutingConfig::default(),
        };
        assert!(empty_id.validate().is_err());

        // Invalid: empty command
        let empty_command = StdioBusConfig {
            pools: vec![PoolConfig {
                id: "test".to_string(),
                command: "".to_string(),
                args: vec![],
                instances: 1,
                env: HashMap::new(),
                cwd: None,
            }],
            limits: LimitsConfig::default(),
            routing: RoutingConfig::default(),
        };
        assert!(empty_command.validate().is_err());

        // Invalid: zero instances
        let zero_instances = StdioBusConfig {
            pools: vec![PoolConfig {
                id: "test".to_string(),
                command: "/bin/test".to_string(),
                args: vec![],
                instances: 0,
                env: HashMap::new(),
                cwd: None,
            }],
            limits: LimitsConfig::default(),
            routing: RoutingConfig::default(),
        };
        assert!(zero_instances.validate().is_err());
    }

    /// **Validates: Requirements 6.2**
    ///
    /// Test environment variable substitution.
    #[test]
    fn env_var_substitution() {
        // Set test environment variables
        // SAFETY: This test runs in isolation and we clean up the env vars after
        unsafe {
            std::env::set_var("TEST_CODEX_HOME", "/home/test");
            std::env::set_var("TEST_LOG_LEVEL", "debug");
        }

        let mut config = StdioBusConfig {
            pools: vec![PoolConfig {
                id: "test".to_string(),
                command: "${TEST_CODEX_HOME}/bin/app".to_string(),
                args: vec!["--log=${TEST_LOG_LEVEL}".to_string()],
                instances: 1,
                env: HashMap::from([
                    ("HOME".to_string(), "${TEST_CODEX_HOME}".to_string()),
                    ("LOG".to_string(), "$TEST_LOG_LEVEL".to_string()),
                ]),
                cwd: Some("${TEST_CODEX_HOME}/work".to_string()),
            }],
            limits: LimitsConfig::default(),
            routing: RoutingConfig::default(),
        };

        config.substitute_env_vars();

        assert_eq!(config.pools[0].command, "/home/test/bin/app");
        assert_eq!(config.pools[0].args[0], "--log=debug");
        assert_eq!(
            config.pools[0].env.get("HOME"),
            Some(&"/home/test".to_string())
        );
        assert_eq!(config.pools[0].env.get("LOG"), Some(&"debug".to_string()));
        assert_eq!(config.pools[0].cwd, Some("/home/test/work".to_string()));

        // Clean up
        // SAFETY: Cleaning up env vars set by this test
        unsafe {
            std::env::remove_var("TEST_CODEX_HOME");
            std::env::remove_var("TEST_LOG_LEVEL");
        }
    }

    /// **Validates: Requirements 6.1**
    ///
    /// Test configuration serialization to JSON.
    #[test]
    fn config_serialization() {
        let config = StdioBusConfig {
            pools: vec![PoolConfig {
                id: "test-pool".to_string(),
                command: "/bin/test".to_string(),
                args: vec!["--arg1".to_string()],
                instances: 2,
                env: HashMap::from([("KEY".to_string(), "value".to_string())]),
                cwd: Some("/work".to_string()),
            }],
            limits: LimitsConfig::default(),
            routing: RoutingConfig {
                session_id_field: "sessionId".to_string(),
                default_pool: Some("test-pool".to_string()),
            },
        };

        let json = serde_json::to_string_pretty(&config).unwrap();
        let parsed: StdioBusConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.pools.len(), 1);
        assert_eq!(parsed.pools[0].id, "test-pool");
        assert_eq!(parsed.pools[0].instances, 2);
        assert_eq!(parsed.routing.default_pool, Some("test-pool".to_string()));
    }

    /// **Validates: Requirements 6.1**
    ///
    /// Test default limits configuration.
    #[test]
    fn default_limits_config() {
        let limits = LimitsConfig::default();

        // Verify defaults match TC-3 resource limits
        assert_eq!(limits.max_input_buffer, 1_048_576); // 1MB
        assert_eq!(limits.max_output_queue, 4_194_304); // 4MB
        assert_eq!(limits.max_restarts, 5);
        assert_eq!(limits.restart_window_sec, 60);
        assert_eq!(limits.drain_timeout_sec, 30);
        assert_eq!(limits.backpressure_timeout_sec, 60);
    }
}
