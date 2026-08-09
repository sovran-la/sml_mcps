//! Stdio Transport
//!
//! Communication over stdin/stdout for local MCP servers.

use crate::transport::Transport;
use crate::types::{JsonRpcMessage, Result};
use std::io::{self, BufRead, Write};

/// Stdio transport - reads from stdin, writes to stdout
pub struct StdioTransport {
    stdin: io::Stdin,
    stdout: io::Stdout,
}

impl StdioTransport {
    pub fn new() -> Self {
        Self {
            stdin: io::stdin(),
            stdout: io::stdout(),
        }
    }
}

impl Default for StdioTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl Transport for StdioTransport {
    fn read(&mut self) -> Result<JsonRpcMessage> {
        let mut line = String::new();
        self.stdin.lock().read_line(&mut line)?;

        if line.is_empty() {
            return Err(crate::types::McpError::TransportClosed);
        }

        JsonRpcMessage::parse(&line)
    }

    fn write(&mut self, message: &JsonRpcMessage) -> Result<()> {
        let mut handle = self.stdout.lock();
        serde_json::to_writer(&mut handle, message)?;
        writeln!(handle)?;
        handle.flush()?;
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        // Nothing to close for stdio
        Ok(())
    }

    fn close_write(&mut self) -> Result<()> {
        // Can't half-close stdout without ending the process; no-op. The bridge
        // shutting down means main is about to return and the process exits.
        Ok(())
    }

    fn try_clone_writer(&self) -> Option<Box<dyn Transport>> {
        // stdin/stdout are process-global handles; a fresh StdioTransport
        // writes to the same stdout. The bridge only ever calls write() on it.
        Some(Box::new(StdioTransport::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::JsonRpcMessage;

    #[test]
    fn test_message_roundtrip() {
        // Test that we can serialize and deserialize messages
        let msg = JsonRpcMessage::request(1i64, "test/method", None);
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: JsonRpcMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, parsed);
    }

    #[test]
    fn test_stdio_transport_new() {
        let _transport = StdioTransport::new();
        // Just verify it creates without panic
    }

    #[test]
    fn test_stdio_transport_default() {
        let _transport = StdioTransport::default();
        // Verify Default trait works
    }

    #[test]
    fn test_stdio_transport_close() {
        let mut transport = StdioTransport::new();
        // Close should succeed (it's a no-op for stdio)
        assert!(transport.close().is_ok());
    }

    // Note: We can't easily test read() and write() with real stdin/stdout in unit tests.
    // Those are covered by integration tests (running the examples).
    // The HTTP transport tests provide coverage for the Transport trait methods.
}
