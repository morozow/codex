//! Session ID mapping utilities.

/// Session ID prefix for thread-based sessions.
pub const THREAD_PREFIX: &str = "thread:";
/// Session ID prefix for connection-based sessions.
pub const CONN_PREFIX: &str = "conn:";
/// Session ID prefix for MCP server sessions.
pub const MCP_PREFIX: &str = "mcp:";

/// Map a Codex thread ID to a session ID.
pub fn thread_to_session_id(thread_id: &str) -> String {
    format!("{THREAD_PREFIX}{thread_id}")
}

/// Map a connection ID to a session ID.
pub fn conn_to_session_id(connection_id: u64) -> String {
    format!("{CONN_PREFIX}{connection_id}")
}

/// Map an MCP server name to a session ID.
pub fn mcp_to_session_id(server_name: &str) -> String {
    format!("{MCP_PREFIX}{server_name}")
}

/// Extract the server name from an MCP session ID.
pub fn extract_mcp_server_name(session_id: &str) -> Option<&str> {
    session_id.strip_prefix(MCP_PREFIX)
}

/// Extract the thread ID from a thread session ID.
pub fn extract_thread_id(session_id: &str) -> Option<&str> {
    session_id.strip_prefix(THREAD_PREFIX)
}

/// Determine the session type from a session ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionType {
    Thread(String),
    Connection(u64),
    Mcp(String),
    Unknown(String),
}

impl SessionType {
    pub fn from_session_id(session_id: &str) -> Self {
        if let Some(thread_id) = session_id.strip_prefix(THREAD_PREFIX) {
            SessionType::Thread(thread_id.to_string())
        } else if let Some(conn_id) = session_id.strip_prefix(CONN_PREFIX) {
            conn_id
                .parse()
                .map(SessionType::Connection)
                .unwrap_or_else(|_| SessionType::Unknown(session_id.to_string()))
        } else if let Some(server_name) = session_id.strip_prefix(MCP_PREFIX) {
            SessionType::Mcp(server_name.to_string())
        } else {
            SessionType::Unknown(session_id.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// **Validates: Requirements 5.1, 5.2, 5.3**
    ///
    /// Property 5: Session ID Mapping Formats
    /// Tests round-trip mapping and extraction for all session types.
    mod property_tests {
        use super::*;

        proptest! {
            /// Round-trip property for thread IDs: thread_to_session_id followed by
            /// extract_thread_id returns the original thread ID.
            ///
            /// **Validates: Requirements 5.1**
            #[test]
            fn thread_id_round_trip(thread_id in "[a-zA-Z0-9_-]{1,64}") {
                let session_id = thread_to_session_id(&thread_id);
                let extracted = extract_thread_id(&session_id);
                prop_assert_eq!(extracted, Some(thread_id.as_str()));
            }

            /// Round-trip property for connection IDs: conn_to_session_id followed by
            /// SessionType::from_session_id returns the original connection ID.
            ///
            /// **Validates: Requirements 5.2**
            #[test]
            fn connection_id_round_trip(conn_id: u64) {
                let session_id = conn_to_session_id(conn_id);
                let session_type = SessionType::from_session_id(&session_id);
                prop_assert_eq!(session_type, SessionType::Connection(conn_id));
            }

            /// Round-trip property for MCP server names: mcp_to_session_id followed by
            /// extract_mcp_server_name returns the original server name.
            ///
            /// **Validates: Requirements 5.3**
            #[test]
            fn mcp_server_name_round_trip(server_name in "[a-zA-Z0-9_-]{1,64}") {
                let session_id = mcp_to_session_id(&server_name);
                let extracted = extract_mcp_server_name(&session_id);
                prop_assert_eq!(extracted, Some(server_name.as_str()));
            }

            /// SessionType::from_session_id correctly parses thread session IDs.
            ///
            /// **Validates: Requirements 5.1**
            #[test]
            fn session_type_parses_thread(thread_id in "[a-zA-Z0-9_-]{1,64}") {
                let session_id = thread_to_session_id(&thread_id);
                let session_type = SessionType::from_session_id(&session_id);
                prop_assert_eq!(session_type, SessionType::Thread(thread_id));
            }

            /// SessionType::from_session_id correctly parses MCP session IDs.
            ///
            /// **Validates: Requirements 5.3**
            #[test]
            fn session_type_parses_mcp(server_name in "[a-zA-Z0-9_-]{1,64}") {
                let session_id = mcp_to_session_id(&server_name);
                let session_type = SessionType::from_session_id(&session_id);
                prop_assert_eq!(session_type, SessionType::Mcp(server_name));
            }

            /// Unknown session IDs (without recognized prefix) are handled correctly.
            ///
            /// **Validates: Requirements 5.1, 5.2, 5.3**
            #[test]
            fn unknown_session_ids_handled(
                // Generate strings that don't start with any known prefix
                unknown_id in "[a-zA-Z0-9_-]{1,64}"
                    .prop_filter("must not start with known prefix", |s| {
                        !s.starts_with(THREAD_PREFIX)
                            && !s.starts_with(CONN_PREFIX)
                            && !s.starts_with(MCP_PREFIX)
                    })
            ) {
                let session_type = SessionType::from_session_id(&unknown_id);
                prop_assert_eq!(session_type, SessionType::Unknown(unknown_id));
            }

            /// Invalid connection IDs (non-numeric after prefix) are handled as Unknown.
            ///
            /// **Validates: Requirements 5.2**
            #[test]
            fn invalid_connection_id_handled(
                invalid_conn in "[a-zA-Z_-]{1,32}"
            ) {
                let session_id = format!("{CONN_PREFIX}{invalid_conn}");
                let session_type = SessionType::from_session_id(&session_id);
                prop_assert_eq!(session_type, SessionType::Unknown(session_id));
            }
        }
    }

    /// Unit tests for specific edge cases.
    mod unit_tests {
        use super::*;
        use pretty_assertions::assert_eq;

        #[test]
        fn thread_prefix_is_correct() {
            assert_eq!(THREAD_PREFIX, "thread:");
        }

        #[test]
        fn conn_prefix_is_correct() {
            assert_eq!(CONN_PREFIX, "conn:");
        }

        #[test]
        fn mcp_prefix_is_correct() {
            assert_eq!(MCP_PREFIX, "mcp:");
        }

        #[test]
        fn thread_to_session_id_formats_correctly() {
            assert_eq!(thread_to_session_id("abc123"), "thread:abc123");
            assert_eq!(thread_to_session_id(""), "thread:");
        }

        #[test]
        fn conn_to_session_id_formats_correctly() {
            assert_eq!(conn_to_session_id(0), "conn:0");
            assert_eq!(conn_to_session_id(12345), "conn:12345");
            assert_eq!(conn_to_session_id(u64::MAX), format!("conn:{}", u64::MAX));
        }

        #[test]
        fn mcp_to_session_id_formats_correctly() {
            assert_eq!(mcp_to_session_id("my-server"), "mcp:my-server");
            assert_eq!(mcp_to_session_id(""), "mcp:");
        }

        #[test]
        fn extract_thread_id_works() {
            assert_eq!(extract_thread_id("thread:abc"), Some("abc"));
            assert_eq!(extract_thread_id("thread:"), Some(""));
            assert_eq!(extract_thread_id("conn:123"), None);
            assert_eq!(extract_thread_id("mcp:server"), None);
            assert_eq!(extract_thread_id("unknown"), None);
        }

        #[test]
        fn extract_mcp_server_name_works() {
            assert_eq!(extract_mcp_server_name("mcp:my-server"), Some("my-server"));
            assert_eq!(extract_mcp_server_name("mcp:"), Some(""));
            assert_eq!(extract_mcp_server_name("thread:abc"), None);
            assert_eq!(extract_mcp_server_name("conn:123"), None);
            assert_eq!(extract_mcp_server_name("unknown"), None);
        }

        #[test]
        fn session_type_from_session_id_parses_all_types() {
            assert_eq!(
                SessionType::from_session_id("thread:abc123"),
                SessionType::Thread("abc123".to_string())
            );
            assert_eq!(
                SessionType::from_session_id("conn:42"),
                SessionType::Connection(42)
            );
            assert_eq!(
                SessionType::from_session_id("mcp:my-server"),
                SessionType::Mcp("my-server".to_string())
            );
            assert_eq!(
                SessionType::from_session_id("unknown-format"),
                SessionType::Unknown("unknown-format".to_string())
            );
        }

        #[test]
        fn session_type_handles_edge_cases() {
            // Empty values after prefix
            assert_eq!(
                SessionType::from_session_id("thread:"),
                SessionType::Thread("".to_string())
            );
            assert_eq!(
                SessionType::from_session_id("conn:0"),
                SessionType::Connection(0)
            );
            assert_eq!(
                SessionType::from_session_id("mcp:"),
                SessionType::Mcp("".to_string())
            );

            // Invalid connection ID (non-numeric)
            assert_eq!(
                SessionType::from_session_id("conn:not-a-number"),
                SessionType::Unknown("conn:not-a-number".to_string())
            );

            // Connection ID overflow (larger than u64::MAX)
            let overflow_id = format!("conn:{}0", u64::MAX);
            assert_eq!(
                SessionType::from_session_id(&overflow_id),
                SessionType::Unknown(overflow_id)
            );
        }
    }
}
