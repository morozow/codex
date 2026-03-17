//! Transport abstraction for message I/O.

use crate::message::Message;
use crate::worker::WorkerError;
use async_trait::async_trait;

/// Transport trait for abstracting message I/O.
#[async_trait]
pub trait Transport: Send + Sync {
    /// Receive the next message.
    async fn recv(&mut self) -> Result<Message, WorkerError>;

    /// Send a message.
    async fn send(&mut self, msg: &[u8]) -> Result<(), WorkerError>;

    /// Check if running in worker mode.
    fn is_worker_mode(&self) -> bool;

    /// Get the current session ID (if any).
    fn current_session_id(&self) -> Option<&str>;
}

/// Direct stdio transport (current behavior).
///
/// This transport reads/writes directly to stdin/stdout without
/// session ID handling, maintaining backward compatibility.
pub struct DirectStdioTransport {
    stdin: tokio::io::BufReader<tokio::io::Stdin>,
    stdout: tokio::io::Stdout,
}

impl DirectStdioTransport {
    pub fn new() -> Self {
        Self {
            stdin: tokio::io::BufReader::new(tokio::io::stdin()),
            stdout: tokio::io::stdout(),
        }
    }
}

impl Default for DirectStdioTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transport for DirectStdioTransport {
    async fn recv(&mut self) -> Result<Message, WorkerError> {
        use tokio::io::AsyncBufReadExt;
        let mut line = String::new();
        let n = self.stdin.read_line(&mut line).await?;
        if n == 0 {
            return Err(WorkerError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "stdin closed",
            )));
        }
        Ok(crate::routing::parse_message(
            line.trim_end().as_bytes().to_vec(),
        ))
    }

    async fn send(&mut self, msg: &[u8]) -> Result<(), WorkerError> {
        use tokio::io::AsyncWriteExt;
        self.stdout.write_all(msg).await?;
        self.stdout.write_all(b"\n").await?;
        self.stdout.flush().await?;
        Ok(())
    }

    fn is_worker_mode(&self) -> bool {
        false
    }

    fn current_session_id(&self) -> Option<&str> {
        None
    }
}

/// stdio_bus worker transport.
///
/// This transport handles session ID extraction and injection
/// for stdio_bus worker mode.
pub struct StdioBusWorkerTransport {
    inner: crate::worker::StdioBusWorker,
    current_session_id: Option<String>,
}

impl StdioBusWorkerTransport {
    pub fn new() -> Self {
        Self {
            inner: crate::worker::StdioBusWorker::new(),
            current_session_id: None,
        }
    }
}

impl Default for StdioBusWorkerTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transport for StdioBusWorkerTransport {
    async fn recv(&mut self) -> Result<Message, WorkerError> {
        let msg = self.inner.recv().await?;
        self.current_session_id = msg.routing.session_id.clone();
        Ok(msg)
    }

    async fn send(&mut self, msg: &[u8]) -> Result<(), WorkerError> {
        let mut msg = msg.to_vec();
        if let Some(sid) = &self.current_session_id {
            crate::worker::StdioBusWorker::inject_session_id(&mut msg, sid)?;
        }
        self.inner.send(&msg).await
    }

    fn is_worker_mode(&self) -> bool {
        true
    }

    fn current_session_id(&self) -> Option<&str> {
        self.current_session_id.as_deref()
    }
}

/// Create the appropriate transport based on worker mode flag.
pub fn create_transport(worker_mode: bool) -> Box<dyn Transport> {
    if worker_mode {
        Box::new(StdioBusWorkerTransport::new())
    } else {
        Box::new(DirectStdioTransport::new())
    }
}
