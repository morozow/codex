//! Routing field extraction from JSON-RPC messages.

use crate::message::Message;
use crate::message::RequestId;
use crate::message::RoutingFields;
use serde_json::Value;
use tracing::debug;
use tracing::warn;

/// Maximum session ID length in bytes.
pub const MAX_SESSION_ID_LEN: usize = 256;
/// Maximum request ID length in bytes.
pub const MAX_REQUEST_ID_LEN: usize = 128;

/// Extract routing fields from raw JSON bytes.
///
/// This function performs minimal parsing to extract only routing-relevant
/// fields without deserializing the entire message.
pub fn extract_routing_fields(raw: &[u8]) -> RoutingFields {
    let value: Value = match serde_json::from_slice(raw) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "Failed to parse JSON for routing extraction");
            return RoutingFields::default();
        }
    };

    let obj = match value.as_object() {
        Some(o) => o,
        None => return RoutingFields::default(),
    };

    let id = obj.get("id").and_then(RequestId::from_value);

    let session_id = obj
        .get("sessionId")
        .and_then(Value::as_str)
        .filter(|s| s.len() <= MAX_SESSION_ID_LEN)
        .map(String::from);

    let method = obj.get("method").and_then(Value::as_str).map(String::from);

    // Detect response by presence of result or error.
    let is_response = obj.contains_key("result") || obj.contains_key("error");
    let is_error = obj.contains_key("error");

    debug!(
        ?id,
        ?session_id,
        ?method,
        is_response,
        "Extracted routing fields"
    );

    RoutingFields {
        id,
        session_id,
        method,
        is_response,
        is_error,
    }
}

/// Create a Message from raw bytes with routing extraction.
pub fn parse_message(raw: Vec<u8>) -> Message {
    let routing = extract_routing_fields(&raw);
    Message { raw, routing }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// **Validates: Requirements 1.3, 1.4, 1.5**
    ///
    /// Property 2: Routing Field Extraction
    /// Tests extraction of `id` (string and numeric), `sessionId`, and `method` fields.
    mod property_routing_field_extraction {
        use super::*;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(1000))]

            /// Test that string IDs are correctly extracted.
            #[test]
            fn extracts_string_id(id in "[a-zA-Z0-9_-]{1,64}") {
                let msg = serde_json::json!({
                    "id": id,
                    "method": "test/method"
                });
                let raw = serde_json::to_vec(&msg).unwrap();
                let routing = extract_routing_fields(&raw);

                assert_eq!(routing.id, Some(RequestId::String(id)));
            }

            /// Test that numeric IDs are correctly extracted.
            #[test]
            fn extracts_numeric_id(id in any::<i64>()) {
                let msg = serde_json::json!({
                    "id": id,
                    "method": "test/method"
                });
                let raw = serde_json::to_vec(&msg).unwrap();
                let routing = extract_routing_fields(&raw);

                assert_eq!(routing.id, Some(RequestId::Integer(id)));
            }

            /// Test that sessionId is correctly extracted.
            #[test]
            fn extracts_session_id(session_id in "[a-zA-Z0-9:_-]{1,64}") {
                let msg = serde_json::json!({
                    "id": "test-1",
                    "sessionId": session_id,
                    "method": "test/method"
                });
                let raw = serde_json::to_vec(&msg).unwrap();
                let routing = extract_routing_fields(&raw);

                assert_eq!(routing.session_id, Some(session_id));
            }

            /// Test that method is correctly extracted.
            #[test]
            fn extracts_method(method in "[a-zA-Z]+/[a-zA-Z]+") {
                let msg = serde_json::json!({
                    "id": "test-1",
                    "method": method
                });
                let raw = serde_json::to_vec(&msg).unwrap();
                let routing = extract_routing_fields(&raw);

                assert_eq!(routing.method, Some(method));
            }

            /// Test that all routing fields are correctly extracted together.
            #[test]
            fn extracts_all_fields_together(
                id in prop_oneof![
                    any::<i64>().prop_map(RequestId::Integer),
                    "[a-zA-Z0-9_-]{1,64}".prop_map(RequestId::String),
                ],
                session_id in "[a-zA-Z0-9:_-]{1,64}",
                method in "[a-zA-Z]+/[a-zA-Z]+",
            ) {
                let mut msg = serde_json::Map::new();
                match &id {
                    RequestId::Integer(n) => {
                        msg.insert("id".to_string(), serde_json::Value::Number((*n).into()));
                    }
                    RequestId::String(s) => {
                        msg.insert("id".to_string(), serde_json::Value::String(s.clone()));
                    }
                }
                msg.insert("sessionId".to_string(), serde_json::Value::String(session_id.clone()));
                msg.insert("method".to_string(), serde_json::Value::String(method.clone()));

                let raw = serde_json::to_vec(&msg).unwrap();
                let routing = extract_routing_fields(&raw);

                assert_eq!(routing.id, Some(id));
                assert_eq!(routing.session_id, Some(session_id));
                assert_eq!(routing.method, Some(method));
            }

            /// Test that session IDs exceeding MAX_SESSION_ID_LEN are rejected.
            #[test]
            fn rejects_oversized_session_id(
                session_id in "[a-zA-Z0-9]{257,300}"
            ) {
                let msg = serde_json::json!({
                    "id": "test-1",
                    "sessionId": session_id,
                    "method": "test/method"
                });
                let raw = serde_json::to_vec(&msg).unwrap();
                let routing = extract_routing_fields(&raw);

                // Session ID should be None because it exceeds MAX_SESSION_ID_LEN
                assert_eq!(routing.session_id, None);
            }
        }
    }

    /// **Validates: Requirements 1.4**
    ///
    /// Property: Response Detection
    /// Tests detection of response messages by presence of `result` or `error` fields.
    mod property_response_detection {
        use super::*;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(500))]

            /// Test that messages with `result` field are detected as responses.
            #[test]
            fn detects_result_response(result in any::<i64>()) {
                let msg = serde_json::json!({
                    "id": "test-1",
                    "result": result
                });
                let raw = serde_json::to_vec(&msg).unwrap();
                let routing = extract_routing_fields(&raw);

                assert!(routing.is_response);
                assert!(!routing.is_error);
            }

            /// Test that messages with `error` field are detected as error responses.
            #[test]
            fn detects_error_response(error_code in any::<i32>(), error_msg in "[a-zA-Z ]{1,50}") {
                let msg = serde_json::json!({
                    "id": "test-1",
                    "error": {
                        "code": error_code,
                        "message": error_msg
                    }
                });
                let raw = serde_json::to_vec(&msg).unwrap();
                let routing = extract_routing_fields(&raw);

                assert!(routing.is_response);
                assert!(routing.is_error);
            }

            /// Test that messages without `result` or `error` are not responses.
            #[test]
            fn detects_non_response(method in "[a-zA-Z]+/[a-zA-Z]+") {
                let msg = serde_json::json!({
                    "id": "test-1",
                    "method": method,
                    "params": {}
                });
                let raw = serde_json::to_vec(&msg).unwrap();
                let routing = extract_routing_fields(&raw);

                assert!(!routing.is_response);
                assert!(!routing.is_error);
            }
        }
    }

    /// **Validates: Requirements 1.3**
    ///
    /// Property: Invalid JSON Handling
    /// Tests that invalid JSON returns default routing fields.
    mod property_invalid_json_handling {
        use super::*;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(200))]

            /// Test that invalid JSON bytes return default routing fields.
            #[test]
            fn handles_invalid_json(garbage in prop::collection::vec(any::<u8>(), 1..100)) {
                // Skip if the garbage happens to be valid JSON
                if serde_json::from_slice::<serde_json::Value>(&garbage).is_ok() {
                    return Ok(());
                }

                let routing = extract_routing_fields(&garbage);

                assert_eq!(routing.id, None);
                assert_eq!(routing.session_id, None);
                assert_eq!(routing.method, None);
                assert!(!routing.is_response);
                assert!(!routing.is_error);
            }
        }

        #[test]
        fn handles_empty_input() {
            let routing = extract_routing_fields(&[]);

            assert_eq!(routing.id, None);
            assert_eq!(routing.session_id, None);
            assert_eq!(routing.method, None);
            assert!(!routing.is_response);
            assert!(!routing.is_error);
        }

        #[test]
        fn handles_truncated_json() {
            let routing = extract_routing_fields(b"{\"id\": \"test");

            assert_eq!(routing.id, None);
            assert_eq!(routing.session_id, None);
            assert_eq!(routing.method, None);
            assert!(!routing.is_response);
            assert!(!routing.is_error);
        }
    }

    /// **Validates: Requirements 1.3**
    ///
    /// Property: Non-Object JSON Handling
    /// Tests that non-object JSON values return default routing fields.
    mod property_non_object_json_handling {
        use super::*;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(200))]

            /// Test that JSON arrays return default routing fields.
            #[test]
            fn handles_json_array(values in prop::collection::vec(any::<i64>(), 0..10)) {
                let msg = serde_json::Value::Array(
                    values.into_iter().map(|v| serde_json::json!(v)).collect()
                );
                let raw = serde_json::to_vec(&msg).unwrap();
                let routing = extract_routing_fields(&raw);

                assert_eq!(routing.id, None);
                assert_eq!(routing.session_id, None);
                assert_eq!(routing.method, None);
                assert!(!routing.is_response);
                assert!(!routing.is_error);
            }

            /// Test that JSON primitives return default routing fields.
            #[test]
            fn handles_json_primitive(value in any::<i64>()) {
                let raw = serde_json::to_vec(&value).unwrap();
                let routing = extract_routing_fields(&raw);

                assert_eq!(routing.id, None);
                assert_eq!(routing.session_id, None);
                assert_eq!(routing.method, None);
                assert!(!routing.is_response);
                assert!(!routing.is_error);
            }

            /// Test that JSON strings return default routing fields.
            #[test]
            fn handles_json_string(value in "[a-zA-Z0-9]{1,50}") {
                let raw = serde_json::to_vec(&value).unwrap();
                let routing = extract_routing_fields(&raw);

                assert_eq!(routing.id, None);
                assert_eq!(routing.session_id, None);
                assert_eq!(routing.method, None);
                assert!(!routing.is_response);
                assert!(!routing.is_error);
            }
        }

        #[test]
        fn handles_json_null() {
            let raw = serde_json::to_vec(&serde_json::Value::Null).unwrap();
            let routing = extract_routing_fields(&raw);

            assert_eq!(routing.id, None);
            assert_eq!(routing.session_id, None);
            assert_eq!(routing.method, None);
            assert!(!routing.is_response);
            assert!(!routing.is_error);
        }

        #[test]
        fn handles_json_boolean() {
            let raw = serde_json::to_vec(&true).unwrap();
            let routing = extract_routing_fields(&raw);

            assert_eq!(routing.id, None);
            assert_eq!(routing.session_id, None);
            assert_eq!(routing.method, None);
            assert!(!routing.is_response);
            assert!(!routing.is_error);
        }
    }

    /// Unit tests for specific edge cases.
    mod unit_tests {
        use super::*;

        #[test]
        fn extracts_fields_from_complete_request() {
            let msg = serde_json::json!({
                "jsonrpc": "2.0",
                "id": "req-123",
                "method": "thread/start",
                "params": { "prompt": "Hello" },
                "sessionId": "thread:abc-456"
            });
            let raw = serde_json::to_vec(&msg).unwrap();
            let routing = extract_routing_fields(&raw);

            assert_eq!(routing.id, Some(RequestId::String("req-123".to_string())));
            assert_eq!(routing.session_id, Some("thread:abc-456".to_string()));
            assert_eq!(routing.method, Some("thread/start".to_string()));
            assert!(!routing.is_response);
            assert!(!routing.is_error);
        }

        #[test]
        fn extracts_fields_from_success_response() {
            let msg = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 42,
                "result": { "status": "ok" },
                "sessionId": "conn:12345"
            });
            let raw = serde_json::to_vec(&msg).unwrap();
            let routing = extract_routing_fields(&raw);

            assert_eq!(routing.id, Some(RequestId::Integer(42)));
            assert_eq!(routing.session_id, Some("conn:12345".to_string()));
            assert_eq!(routing.method, None);
            assert!(routing.is_response);
            assert!(!routing.is_error);
        }

        #[test]
        fn extracts_fields_from_error_response() {
            let msg = serde_json::json!({
                "jsonrpc": "2.0",
                "id": "err-1",
                "error": {
                    "code": -32600,
                    "message": "Invalid Request"
                },
                "sessionId": "mcp:server-1"
            });
            let raw = serde_json::to_vec(&msg).unwrap();
            let routing = extract_routing_fields(&raw);

            assert_eq!(routing.id, Some(RequestId::String("err-1".to_string())));
            assert_eq!(routing.session_id, Some("mcp:server-1".to_string()));
            assert_eq!(routing.method, None);
            assert!(routing.is_response);
            assert!(routing.is_error);
        }

        #[test]
        fn handles_notification_without_id() {
            let msg = serde_json::json!({
                "jsonrpc": "2.0",
                "method": "$/progress",
                "params": { "token": "abc" },
                "sessionId": "thread:xyz"
            });
            let raw = serde_json::to_vec(&msg).unwrap();
            let routing = extract_routing_fields(&raw);

            assert_eq!(routing.id, None);
            assert_eq!(routing.session_id, Some("thread:xyz".to_string()));
            assert_eq!(routing.method, Some("$/progress".to_string()));
            assert!(!routing.is_response);
            assert!(!routing.is_error);
        }

        #[test]
        fn handles_missing_optional_fields() {
            let msg = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1
            });
            let raw = serde_json::to_vec(&msg).unwrap();
            let routing = extract_routing_fields(&raw);

            assert_eq!(routing.id, Some(RequestId::Integer(1)));
            assert_eq!(routing.session_id, None);
            assert_eq!(routing.method, None);
            assert!(!routing.is_response);
            assert!(!routing.is_error);
        }

        #[test]
        fn ignores_non_string_session_id() {
            let msg = serde_json::json!({
                "id": "test-1",
                "sessionId": 12345,
                "method": "test/method"
            });
            let raw = serde_json::to_vec(&msg).unwrap();
            let routing = extract_routing_fields(&raw);

            assert_eq!(routing.id, Some(RequestId::String("test-1".to_string())));
            assert_eq!(routing.session_id, None);
            assert_eq!(routing.method, Some("test/method".to_string()));
        }

        #[test]
        fn ignores_non_string_method() {
            let msg = serde_json::json!({
                "id": "test-1",
                "method": 123
            });
            let raw = serde_json::to_vec(&msg).unwrap();
            let routing = extract_routing_fields(&raw);

            assert_eq!(routing.id, Some(RequestId::String("test-1".to_string())));
            assert_eq!(routing.method, None);
        }

        #[test]
        fn ignores_invalid_id_types() {
            // Test with boolean ID
            let msg = serde_json::json!({
                "id": true,
                "method": "test/method"
            });
            let raw = serde_json::to_vec(&msg).unwrap();
            let routing = extract_routing_fields(&raw);

            assert_eq!(routing.id, None);
            assert_eq!(routing.method, Some("test/method".to_string()));

            // Test with array ID
            let msg = serde_json::json!({
                "id": [1, 2, 3],
                "method": "test/method"
            });
            let raw = serde_json::to_vec(&msg).unwrap();
            let routing = extract_routing_fields(&raw);

            assert_eq!(routing.id, None);
            assert_eq!(routing.method, Some("test/method".to_string()));

            // Test with object ID
            let msg = serde_json::json!({
                "id": {"nested": "object"},
                "method": "test/method"
            });
            let raw = serde_json::to_vec(&msg).unwrap();
            let routing = extract_routing_fields(&raw);

            assert_eq!(routing.id, None);
            assert_eq!(routing.method, Some("test/method".to_string()));
        }

        #[test]
        fn parse_message_creates_message_with_routing() {
            let msg = serde_json::json!({
                "id": "msg-1",
                "method": "test/echo",
                "sessionId": "thread:test"
            });
            let raw = serde_json::to_vec(&msg).unwrap();
            let message = parse_message(raw.clone());

            assert_eq!(message.raw, raw);
            assert_eq!(
                message.routing.id,
                Some(RequestId::String("msg-1".to_string()))
            );
            assert_eq!(message.routing.session_id, Some("thread:test".to_string()));
            assert_eq!(message.routing.method, Some("test/echo".to_string()));
        }
    }
}
