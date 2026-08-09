//! Unix Socket Transport
//!
//! Communication over a Unix domain socket. Same newline-delimited JSON-RPC
//! wire format as [`StdioTransport`](crate::StdioTransport) - the only
//! difference is the underlying stream.
//!
//! Two constructors cover both sides of a connection:
//! - [`UnixTransport::connect`] - client/shim side, dials an existing daemon socket.
//! - [`UnixTransport::from_stream`] - server/daemon side, wraps a stream handed
//!   back by [`UnixListener::accept`](std::os::unix::net::UnixListener::accept).
//!
//! Unix-only: the whole module is gated behind `#[cfg(unix)]`, no feature flag.

use crate::transport::Transport;
use crate::types::{JsonRpcMessage, Result};
use std::io::{BufRead, BufReader, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::Path;

/// Transport over a Unix domain socket stream.
///
/// Holds a buffered reader and a cloned write handle to the same socket, so
/// reads and writes are independent (the standard split-stream pattern).
pub struct UnixTransport {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
}

impl UnixTransport {
    /// Connect to an existing daemon socket (shim/client side).
    ///
    /// Fails if the socket file doesn't exist or no daemon is listening
    /// (`ConnectionRefused`).
    pub fn connect(path: impl AsRef<Path>) -> Result<Self> {
        let stream = UnixStream::connect(path)?;
        Self::try_from_stream(stream)
    }

    /// Wrap an already-accepted stream (daemon/server side).
    ///
    /// Panics only on file-descriptor exhaustion (`try_clone` failure), which
    /// is a process-wide catastrophic condition rather than a per-connection
    /// error. The infallible signature keeps the accept loop ergonomic.
    pub fn from_stream(stream: UnixStream) -> Self {
        Self::try_from_stream(stream)
            .expect("UnixTransport::from_stream: failed to clone UnixStream (fd exhaustion?)")
    }

    /// Fallible inner constructor shared by [`connect`](Self::connect) and
    /// [`from_stream`](Self::from_stream).
    fn try_from_stream(stream: UnixStream) -> Result<Self> {
        let writer = stream.try_clone()?;
        Ok(Self {
            reader: BufReader::new(stream),
            writer,
        })
    }
}

impl Transport for UnixTransport {
    fn read(&mut self) -> Result<JsonRpcMessage> {
        let mut line = String::new();
        match self.reader.read_line(&mut line) {
            Ok(_) => {}
            // Treat an abrupt peer disconnect as a clean close so the server
            // loop breaks instead of bubbling up an error.
            Err(e) if is_disconnect(&e) => return Err(crate::types::McpError::TransportClosed),
            Err(e) => return Err(e.into()),
        }

        // EOF: read_line returned 0 bytes, line stays empty.
        if line.is_empty() {
            return Err(crate::types::McpError::TransportClosed);
        }

        JsonRpcMessage::parse(&line)
    }

    fn write(&mut self, message: &JsonRpcMessage) -> Result<()> {
        serde_json::to_writer(&mut self.writer, message)?;
        writeln!(self.writer)?;
        self.writer.flush()?;
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        // Best-effort shutdown; ignore NotConnected (already closed).
        match self.writer.shutdown(Shutdown::Both) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotConnected => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn close_write(&mut self) -> Result<()> {
        // Half-close: shut down only the write direction so the peer reads EOF
        // while we can still read anything still in flight.
        match self.writer.shutdown(Shutdown::Write) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotConnected => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn try_clone_writer(&self) -> Option<Box<dyn Transport>> {
        // Clone the underlying fd so the bridge's other direction has an
        // independent write handle to the same socket connection.
        self.writer
            .try_clone()
            .ok()
            .map(|s| Box::new(UnixTransport::from_stream(s)) as Box<dyn Transport>)
    }
}

/// True if an IO error means the peer went away (so we should treat it as a
/// clean transport close rather than a hard error).
fn is_disconnect(e: &std::io::Error) -> bool {
    use std::io::ErrorKind::*;
    matches!(
        e.kind(),
        UnexpectedEof | ConnectionReset | ConnectionAborted | BrokenPipe
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{JsonRpcMessage, McpError};

    #[test]
    fn test_roundtrip_through_pair() {
        let (a, b) = UnixStream::pair().unwrap();
        let mut client = UnixTransport::from_stream(a);
        let mut server = UnixTransport::from_stream(b);

        let msg = JsonRpcMessage::request(1i64, "tools/list", None);
        client.write(&msg).unwrap();

        let received = server.read().unwrap();
        assert_eq!(received, msg);
    }

    #[test]
    fn test_bidirectional() {
        let (a, b) = UnixStream::pair().unwrap();
        let mut client = UnixTransport::from_stream(a);
        let mut server = UnixTransport::from_stream(b);

        // client -> server
        let req = JsonRpcMessage::request(7i64, "ping", None);
        client.write(&req).unwrap();
        assert_eq!(server.read().unwrap(), req);

        // server -> client
        let resp = JsonRpcMessage::response(7i64, serde_json::json!({"ok": true}));
        server.write(&resp).unwrap();
        assert_eq!(client.read().unwrap(), resp);
    }

    #[test]
    fn test_multiple_messages_framing() {
        // Several messages written back-to-back must each read out as one
        // message (newline framing intact).
        let (a, b) = UnixStream::pair().unwrap();
        let mut client = UnixTransport::from_stream(a);
        let mut server = UnixTransport::from_stream(b);

        for i in 0..5i64 {
            let msg = JsonRpcMessage::request(i, "ping", None);
            client.write(&msg).unwrap();
        }
        for i in 0..5i64 {
            let got = server.read().unwrap();
            assert_eq!(got, JsonRpcMessage::request(i, "ping", None));
        }
    }

    #[test]
    fn test_large_message() {
        let (a, b) = UnixStream::pair().unwrap();
        let mut client = UnixTransport::from_stream(a);

        // ~256KB payload to exercise buffering across multiple reads. It exceeds
        // the socket send buffer, so the read must happen concurrently or the
        // write blocks once the buffer fills - read on a separate thread.
        let big = "x".repeat(256 * 1024);
        let msg = JsonRpcMessage::request(
            1i64,
            "tools/call",
            Some(serde_json::json!({ "name": "echo", "arguments": { "message": big } })),
        );

        let expected = msg.clone();
        let reader = std::thread::spawn(move || {
            let mut server = UnixTransport::from_stream(b);
            server.read().unwrap()
        });

        client.write(&msg).unwrap();
        assert_eq!(reader.join().unwrap(), expected);
    }

    #[test]
    fn test_close_detection_on_drop() {
        let (a, b) = UnixStream::pair().unwrap();
        let mut server = UnixTransport::from_stream(b);

        // Drop the client end -> server read should see EOF -> TransportClosed.
        drop(a);
        let result = server.read();
        assert!(matches!(result, Err(McpError::TransportClosed)));
    }

    #[test]
    fn test_close_detection_on_explicit_close() {
        let (a, b) = UnixStream::pair().unwrap();
        let mut client = UnixTransport::from_stream(a);
        let mut server = UnixTransport::from_stream(b);

        client.close().unwrap();
        let result = server.read();
        assert!(matches!(result, Err(McpError::TransportClosed)));
    }

    #[test]
    fn test_close_is_idempotent() {
        let (a, _b) = UnixStream::pair().unwrap();
        let mut client = UnixTransport::from_stream(a);
        assert!(client.close().is_ok());
        // Second close should not error (NotConnected is swallowed).
        assert!(client.close().is_ok());
    }

    #[test]
    fn test_connect_refused_on_missing_socket() {
        // No listener at this path -> connect should fail, not panic.
        let path = std::env::temp_dir().join("sml_mcps_nonexistent_test.sock");
        let _ = std::fs::remove_file(&path);
        let result = UnixTransport::connect(&path);
        assert!(result.is_err());
    }
}
