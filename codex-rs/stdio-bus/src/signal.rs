//! Platform-specific signal handling for graceful shutdown.

use tokio::sync::watch;
use tracing::info;

/// Graceful shutdown state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownState {
    /// Normal operation.
    Running,
    /// Shutdown initiated, draining in progress.
    Draining,
    /// Shutdown complete.
    Stopped,
}

/// Signal handler for graceful shutdown.
pub struct SignalHandler {
    shutdown_tx: watch::Sender<ShutdownState>,
    shutdown_rx: watch::Receiver<ShutdownState>,
}

impl SignalHandler {
    /// Create a new signal handler.
    pub fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(ShutdownState::Running);
        Self {
            shutdown_tx,
            shutdown_rx,
        }
    }

    /// Get a receiver for shutdown state changes.
    pub fn subscribe(&self) -> watch::Receiver<ShutdownState> {
        self.shutdown_rx.clone()
    }

    /// Request shutdown (transition to Draining state).
    pub fn request_shutdown(&self) {
        let _ = self.shutdown_tx.send(ShutdownState::Draining);
    }

    /// Mark shutdown as complete.
    pub fn complete_shutdown(&self) {
        let _ = self.shutdown_tx.send(ShutdownState::Stopped);
    }

    /// Check if shutdown has been requested.
    pub fn is_shutdown_requested(&self) -> bool {
        *self.shutdown_rx.borrow() != ShutdownState::Running
    }

    /// Install platform-specific signal handlers.
    ///
    /// This spawns background tasks that listen for SIGINT and SIGTERM
    /// and trigger graceful shutdown when received.
    pub fn install(&self) {
        // SIGINT (Ctrl+C) handler
        let shutdown_tx = self.shutdown_tx.clone();
        tokio::spawn(async move {
            if let Ok(()) = tokio::signal::ctrl_c().await {
                info!("Received SIGINT, initiating graceful shutdown");
                let _ = shutdown_tx.send(ShutdownState::Draining);
            }
        });

        // SIGTERM handler (Unix only)
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
                let _ = shutdown_tx.send(ShutdownState::Draining);
            });
        }
    }
}

impl Default for SignalHandler {
    fn default() -> Self {
        Self::new()
    }
}
