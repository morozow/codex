//! Worker mode entry point for stdio_bus integration.
//!
//! This module provides the entry point for running the app-server as a stdio_bus
//! worker. In worker mode, the app-server reads NDJSON messages from stdin and
//! writes responses to stdout. Session affinity is maintained via the `sessionId`
//! field in messages. Diagnostic output is written to stderr only.
//!
//! # Requirements
//! - REQ-2.2: Read NDJSON messages from stdin
//! - REQ-2.3: Write NDJSON responses to stdout
//! - REQ-2.4: Write diagnostic output to stderr only

use codex_arg0::Arg0DispatchPaths;
use codex_cloud_requirements::cloud_requirements_loader;
use codex_core::AuthManager;
use codex_core::config::Config;
use codex_core::config::ConfigBuilder;
use codex_core::config_loader::LoaderOverrides;
use codex_stdio_bus::worker::StdioBusWorker;
use codex_utils_cli::CliConfigOverrides;
use std::io::ErrorKind;
use std::io::Result as IoResult;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod handler;

pub use handler::AppServerWorkerHandler;

/// Initialize tracing to write to stderr only (REQ-2.4).
///
/// In worker mode, stdout is reserved for NDJSON messages to the stdio_bus daemon.
/// All diagnostic output (logs, traces) must go to stderr.
fn init_worker_tracing() {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(EnvFilter::from_default_env());

    // Try to initialize the subscriber. If it fails (e.g., already initialized),
    // we continue anyway since tracing is optional for correctness.
    let _ = tracing_subscriber::registry().with(fmt_layer).try_init();
}

/// Run the app-server in stdio_bus worker mode.
///
/// In worker mode, the app-server reads NDJSON messages from stdin and writes
/// responses to stdout. Session affinity is maintained via the `sessionId` field
/// in messages. Diagnostic output is written to stderr only.
///
/// This mode is designed for use with the stdio_bus daemon, which manages
/// multiple worker instances and routes messages based on session ID.
///
/// # Requirements
/// - REQ-2.1: Support `--worker` CLI flag to enable worker mode
/// - REQ-2.2: Read NDJSON messages from stdin
/// - REQ-2.3: Write NDJSON responses to stdout
/// - REQ-2.4: Write diagnostic output to stderr only
pub async fn run_worker_mode(
    arg0_paths: Arg0DispatchPaths,
    cli_config_overrides: CliConfigOverrides,
    loader_overrides: LoaderOverrides,
) -> IoResult<()> {
    // Initialize tracing to stderr only (REQ-2.4)
    init_worker_tracing();

    info!("Starting app-server in worker mode");

    // Parse CLI overrides
    let cli_kv_overrides = cli_config_overrides.parse_overrides().map_err(|e| {
        std::io::Error::new(
            ErrorKind::InvalidInput,
            format!("error parsing -c overrides: {e}"),
        )
    })?;

    // Load cloud requirements for configuration
    let cloud_requirements = match ConfigBuilder::default()
        .cli_overrides(cli_kv_overrides.clone())
        .loader_overrides(loader_overrides.clone())
        .build()
        .await
    {
        Ok(config) => {
            let auth_manager = AuthManager::shared(
                config.codex_home.clone(),
                false,
                config.cli_auth_credentials_store_mode,
            );
            cloud_requirements_loader(auth_manager, config.chatgpt_base_url, config.codex_home)
        }
        Err(err) => {
            error!(error = %err, "Failed to preload config for cloud requirements");
            codex_core::config_loader::CloudRequirementsLoader::default()
        }
    };

    // Load configuration
    let config = match ConfigBuilder::default()
        .cli_overrides(cli_kv_overrides.clone())
        .loader_overrides(loader_overrides)
        .cloud_requirements(cloud_requirements)
        .build()
        .await
    {
        Ok(config) => config,
        Err(err) => {
            error!(error = %err, "Failed to load config, using defaults");
            Config::load_default_with_cli_overrides(cli_kv_overrides).map_err(|e| {
                std::io::Error::new(
                    ErrorKind::InvalidData,
                    format!("error loading default config: {e}"),
                )
            })?
        }
    };

    info!(
        codex_home = %config.codex_home.display(),
        "Configuration loaded"
    );

    // Create worker and handler
    let mut worker = StdioBusWorker::new();
    let handler = AppServerWorkerHandler::new(Arc::new(config), arg0_paths);

    // Run worker loop (REQ-2.2, REQ-2.3, REQ-2.8)
    worker
        .run(handler)
        .await
        .map_err(|e| std::io::Error::other(format!("worker error: {e}")))?;

    info!("App-server worker mode exiting");
    Ok(())
}
