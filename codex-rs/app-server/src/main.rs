use clap::Parser;
use codex_app_server::AppServerTransport;
use codex_app_server::run_main_with_transport;
use codex_app_server::run_worker_mode;
use codex_arg0::Arg0DispatchPaths;
use codex_arg0::arg0_dispatch_or_else;
use codex_core::config_loader::LoaderOverrides;
use codex_utils_cli::CliConfigOverrides;
use std::path::PathBuf;

// Debug-only test hook: lets integration tests point the server at a temporary
// managed config file without writing to /etc.
const MANAGED_CONFIG_PATH_ENV_VAR: &str = "CODEX_APP_SERVER_MANAGED_CONFIG_PATH";

#[derive(Debug, Parser)]
struct AppServerArgs {
    /// Run in stdio_bus worker mode. When set, the app-server reads NDJSON
    /// messages from stdin and writes responses to stdout, with session
    /// affinity via sessionId field.
    #[arg(long)]
    worker: bool,

    /// Transport endpoint URL. Supported values: `stdio://` (default),
    /// `ws://IP:PORT`. Ignored in worker mode.
    #[arg(
        long = "listen",
        value_name = "URL",
        default_value = AppServerTransport::DEFAULT_LISTEN_URL
    )]
    listen: AppServerTransport,
}

fn main() -> anyhow::Result<()> {
    arg0_dispatch_or_else(|arg0_paths: Arg0DispatchPaths| async move {
        let args = AppServerArgs::parse();
        let managed_config_path = managed_config_path_from_debug_env();
        let loader_overrides = LoaderOverrides {
            managed_config_path,
            ..Default::default()
        };

        if args.worker {
            // REQ-2.1: Worker mode - run as stdio_bus worker
            run_worker_mode(arg0_paths, CliConfigOverrides::default(), loader_overrides).await?;
        } else {
            // REQ-9.1: Backward compatibility - existing behavior
            run_main_with_transport(
                arg0_paths,
                CliConfigOverrides::default(),
                loader_overrides,
                false,
                args.listen,
            )
            .await?;
        }
        Ok(())
    })
}

fn managed_config_path_from_debug_env() -> Option<PathBuf> {
    #[cfg(debug_assertions)]
    {
        if let Ok(value) = std::env::var(MANAGED_CONFIG_PATH_ENV_VAR) {
            return if value.is_empty() {
                None
            } else {
                Some(PathBuf::from(value))
            };
        }
    }

    None
}
