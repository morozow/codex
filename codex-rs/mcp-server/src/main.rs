use clap::Parser;
use codex_arg0::Arg0DispatchPaths;
use codex_arg0::arg0_dispatch_or_else;
use codex_mcp_server::run_main;
use codex_mcp_server::run_worker_mode;
use codex_utils_cli::CliConfigOverrides;

/// MCP server CLI arguments.
#[derive(Debug, Parser)]
struct McpServerArgs {
    /// Run in stdio_bus worker mode. When set, the MCP server reads NDJSON
    /// messages from stdin and writes responses to stdout, with session
    /// affinity via sessionId field.
    #[arg(long)]
    worker: bool,
}

fn main() -> anyhow::Result<()> {
    arg0_dispatch_or_else(|arg0_paths: Arg0DispatchPaths| async move {
        let args = McpServerArgs::parse();

        if args.worker {
            // REQ-3.1: Worker mode - run as stdio_bus worker
            run_worker_mode(arg0_paths, CliConfigOverrides::default()).await?;
        } else {
            // REQ-9.5: Backward compatibility - existing behavior
            run_main(arg0_paths, CliConfigOverrides::default()).await?;
        }
        Ok(())
    })
}
