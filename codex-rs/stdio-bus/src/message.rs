//! Message types and parsing for stdio_bus protocol.

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

/// Raw message with extracted routing information.
#[derive(Debug, Clone)]
pub struct Message {
    /// Original JSON bytes for zero-copy forwarding.
    pub raw: Vec<u8>,
    /// Extracted routing fields.
    pub routing: RoutingFields,
}

/// Routing fields extracted from JSON-RPC messages.
#[derive(Debug, Clone, Default)]
pub struct RoutingFields {
    /// Request/response correlation ID (string or numeric).
    pub id: Option<RequestId>,
    /// Session affinity identifier.
    pub session_id: Option<String>,
    /// JSON-RPC method name.
    pub method: Option<String>,
    /// True if message is a response (has result or error).
    pub is_response: bool,
    /// True if message is an error response.
    pub is_error: bool,
}

/// Request ID supporting both string and numeric values.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    String(String),
    Integer(i64),
}

impl RequestId {
    /// Parse from JSON value.
    pub fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::String(s) => Some(RequestId::String(s.clone())),
            Value::Number(n) => n.as_i64().map(RequestId::Integer),
            _ => None,
        }
    }
}
