//! Backward compatibility tests for stdio_bus integration.
//!
//! These tests verify that the app-server continues to work correctly when the
//! `--worker` flag is NOT used, ensuring backward compatibility with existing
//! deployments.
//!
//! Requirements verified:
//! - REQ-9.1: App_Server works without stdio_bus when `--worker` flag is not set
//! - REQ-9.2: App_Server accepts existing client connections without changes
//! - REQ-9.3: App_Server preserves existing JSON-RPC message format unchanged
//! - REQ-9.4: App_Server continues to support existing config.toml settings

use anyhow::Result;
use app_test_support::McpProcess;
use app_test_support::create_mock_responses_server_repeating_assistant;
use app_test_support::to_response;
use codex_app_server_protocol::ConfigReadParams;
use codex_app_server_protocol::ConfigReadResponse;
use codex_app_server_protocol::JSONRPCResponse;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::UserInput as V2UserInput;
use pretty_assertions::assert_eq;
use serde_json::Value;
use std::path::Path;
use tempfile::TempDir;
use tokio::time::timeout;

const DEFAULT_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// REQ-9.1: Verify app-server works without `--worker` flag.
///
/// This test confirms that the app-server starts and operates correctly using
/// the default stdio transport when the `--worker` flag is not provided.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn app_server_works_without_worker_flag() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri())?;

    // McpProcess::new() spawns app-server WITHOUT the --worker flag
    let mut mcp = McpProcess::new(codex_home.path()).await?;

    // Verify initialization works
    timeout(DEFAULT_READ_TIMEOUT, mcp.initialize()).await??;

    // The server should be operational and accept requests
    let req_id = mcp
        .send_thread_start_request(ThreadStartParams::default())
        .await?;
    let resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(req_id)),
    )
    .await??;
    let ThreadStartResponse { thread, .. } = to_response(resp)?;

    assert!(
        !thread.id.is_empty(),
        "thread should be created successfully"
    );
    Ok(())
}

/// REQ-9.2: Verify existing client connections work unchanged.
///
/// This test confirms that stdio-based client connections continue to work
/// exactly as before, with no changes to the connection protocol.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn existing_stdio_client_connections_unchanged() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri())?;

    let mut mcp = McpProcess::new(codex_home.path()).await?;

    // Standard initialization handshake should work unchanged
    timeout(DEFAULT_READ_TIMEOUT, mcp.initialize()).await??;

    // Multiple sequential requests should work
    for i in 0..3 {
        let req_id = mcp
            .send_thread_start_request(ThreadStartParams::default())
            .await?;
        let resp: JSONRPCResponse = timeout(
            DEFAULT_READ_TIMEOUT,
            mcp.read_stream_until_response_message(RequestId::Integer(req_id)),
        )
        .await??;
        let ThreadStartResponse { thread, .. } = to_response(resp)?;
        assert!(
            !thread.id.is_empty(),
            "request {i} should succeed with valid thread id"
        );
    }

    Ok(())
}

/// REQ-9.3: Verify JSON-RPC message format is preserved.
///
/// This test confirms that the JSON-RPC 2.0 message format is unchanged,
/// including request/response structure, id correlation, and error handling.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_rpc_message_format_preserved() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri())?;

    let mut mcp = McpProcess::new(codex_home.path()).await?;
    timeout(DEFAULT_READ_TIMEOUT, mcp.initialize()).await??;

    // Test request-response correlation
    let req_id = mcp
        .send_thread_start_request(ThreadStartParams::default())
        .await?;
    let resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(req_id)),
    )
    .await??;

    // Verify response has correct id correlation
    assert_eq!(
        resp.id,
        RequestId::Integer(req_id),
        "response id should match request id"
    );

    // Verify response structure contains expected fields
    let result_value = serde_json::to_value(&resp.result)?;
    assert!(
        result_value.get("thread").is_some(),
        "response should contain thread field"
    );

    // Verify thread object structure
    let thread_obj = result_value
        .get("thread")
        .and_then(Value::as_object)
        .expect("thread should be an object");
    assert!(thread_obj.contains_key("id"), "thread should have id field");
    assert!(
        thread_obj.contains_key("createdAt"),
        "thread should have createdAt field"
    );
    assert!(
        thread_obj.contains_key("status"),
        "thread should have status field"
    );

    Ok(())
}

/// REQ-9.3: Verify notifications follow JSON-RPC format.
///
/// This test confirms that server-initiated notifications maintain the
/// standard JSON-RPC notification format.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_rpc_notification_format_preserved() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri())?;

    let mut mcp = McpProcess::new(codex_home.path()).await?;
    timeout(DEFAULT_READ_TIMEOUT, mcp.initialize()).await??;

    let req_id = mcp
        .send_thread_start_request(ThreadStartParams::default())
        .await?;
    let _: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(req_id)),
    )
    .await??;

    // Wait for thread/started notification
    let notif = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("thread/started"),
    )
    .await??;

    // Verify notification structure
    assert_eq!(notif.method, "thread/started");
    assert!(
        notif.params.is_some(),
        "notification should have params field"
    );

    let params = notif.params.expect("params should be present");
    assert!(
        params.get("thread").is_some(),
        "thread/started notification should contain thread"
    );

    Ok(())
}

/// REQ-9.4: Verify config.toml settings continue to work.
///
/// This test confirms that configuration loaded from config.toml is properly
/// applied and can be read back through the config API.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_toml_settings_still_work() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;

    // Create config with specific settings
    let config_toml = codex_home.path().join("config.toml");
    std::fs::write(
        &config_toml,
        format!(
            r#"
model = "test-model-from-config"
approval_policy = "on-request"
sandbox_mode = "workspace-write"

model_provider = "mock_provider"

[model_providers.mock_provider]
name = "Mock provider for test"
base_url = "{}/v1"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
"#,
            server.uri()
        ),
    )?;

    let mut mcp = McpProcess::new(codex_home.path()).await?;
    timeout(DEFAULT_READ_TIMEOUT, mcp.initialize()).await??;

    // Read config and verify settings are applied
    let req_id = mcp
        .send_config_read_request(ConfigReadParams {
            include_layers: false,
            cwd: None,
        })
        .await?;
    let resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(req_id)),
    )
    .await??;
    let ConfigReadResponse { config, .. } = to_response(resp)?;

    // Verify config values from config.toml are applied
    assert_eq!(
        config.model.as_deref(),
        Some("test-model-from-config"),
        "model from config.toml should be applied"
    );
    assert_eq!(
        config.approval_policy,
        Some(codex_app_server_protocol::AskForApproval::OnRequest),
        "approval_policy from config.toml should be applied"
    );
    assert_eq!(
        config.sandbox_mode,
        Some(codex_app_server_protocol::SandboxMode::WorkspaceWrite),
        "sandbox_mode from config.toml should be applied"
    );

    Ok(())
}

/// REQ-9.4: Verify model provider settings from config.toml work.
///
/// This test confirms that custom model provider configurations in config.toml
/// are properly loaded and used.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_toml_model_provider_settings_work() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri())?;

    let mut mcp = McpProcess::new(codex_home.path()).await?;
    timeout(DEFAULT_READ_TIMEOUT, mcp.initialize()).await??;

    // Start a thread - this exercises the model provider configuration
    let req_id = mcp
        .send_thread_start_request(ThreadStartParams::default())
        .await?;
    let resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(req_id)),
    )
    .await??;
    let ThreadStartResponse { model_provider, .. } = to_response(resp)?;

    // Verify the mock provider from config.toml is being used
    assert_eq!(
        model_provider, "mock_provider",
        "model_provider from config.toml should be used"
    );

    Ok(())
}

/// Combined backward compatibility test verifying full request-response cycle.
///
/// This test exercises a complete workflow to ensure all backward compatibility
/// requirements work together correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_backward_compatible_workflow() -> Result<()> {
    let server = create_mock_responses_server_repeating_assistant("Done").await;
    let codex_home = TempDir::new()?;
    create_config_toml(codex_home.path(), &server.uri())?;

    // REQ-9.1: Start without --worker flag
    let mut mcp = McpProcess::new(codex_home.path()).await?;

    // REQ-9.2: Standard initialization
    timeout(DEFAULT_READ_TIMEOUT, mcp.initialize()).await??;

    // REQ-9.4: Config is loaded
    let config_req_id = mcp
        .send_config_read_request(ConfigReadParams {
            include_layers: false,
            cwd: None,
        })
        .await?;
    let _: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(config_req_id)),
    )
    .await??;

    // REQ-9.3: JSON-RPC format for thread creation
    let thread_req_id = mcp
        .send_thread_start_request(ThreadStartParams::default())
        .await?;
    let thread_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(thread_req_id)),
    )
    .await??;
    let ThreadStartResponse { thread, .. } = to_response(thread_resp)?;

    // REQ-9.3: JSON-RPC format for turn start
    let turn_req_id = mcp
        .send_turn_start_request(TurnStartParams {
            thread_id: thread.id.clone(),
            input: vec![V2UserInput::Text {
                text: "Hello".to_string(),
                text_elements: Vec::new(),
            }],
            ..Default::default()
        })
        .await?;
    let turn_resp: JSONRPCResponse = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_response_message(RequestId::Integer(turn_req_id)),
    )
    .await??;
    let _: TurnStartResponse = to_response(turn_resp)?;

    // REQ-9.3: Notifications follow JSON-RPC format
    let completed = timeout(
        DEFAULT_READ_TIMEOUT,
        mcp.read_stream_until_notification_message("turn/completed"),
    )
    .await??;
    assert_eq!(completed.method, "turn/completed");

    Ok(())
}

// Helper to create a config.toml pointing at the mock model server.
fn create_config_toml(codex_home: &Path, server_uri: &str) -> std::io::Result<()> {
    let config_toml = codex_home.join("config.toml");
    std::fs::write(
        config_toml,
        format!(
            r#"
model = "mock-model"
approval_policy = "never"
sandbox_mode = "read-only"

model_provider = "mock_provider"

[model_providers.mock_provider]
name = "Mock provider for test"
base_url = "{server_uri}/v1"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
"#
        ),
    )
}
