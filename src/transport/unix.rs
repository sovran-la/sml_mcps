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
//! The framing, the split reader/writer and the deadlines are
//! [`SocketTransport`]'s, shared with [`TcpTransport`](crate::TcpTransport):
//! what is here is the dialing, and the name.
//!
//! Unix-only: the whole module is gated behind `#[cfg(unix)]`, no feature flag.

use crate::transport::Transport;
use crate::transport::socket_stream::SocketTransport;
use crate::types::{JsonRpcMessage, Result};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

/// Transport over a Unix domain socket stream.
///
/// Holds a buffered reader and a cloned write handle to the same socket, so
/// reads and writes are independent (the standard split-stream pattern).
pub struct UnixTransport(SocketTransport<UnixStream>);

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
    /// Panics on file-descriptor exhaustion (`try_clone` failure). Use
    /// [`try_from_stream`](Self::try_from_stream) anywhere the caller can carry
    /// on without this connection - an accept loop that must keep listening, a
    /// proxy that can report the failure - and keep this one for the top of a
    /// program, where there is nothing to carry on to.
    pub fn from_stream(stream: UnixStream) -> Self {
        Self::try_from_stream(stream)
            .expect("UnixTransport::from_stream: failed to clone UnixStream (fd exhaustion?)")
    }

    /// [`from_stream`](Self::from_stream) with the failure handed back.
    ///
    /// The clone is a second descriptor for the same socket, so this fails
    /// exactly when the process has run out of them - which is a condition a
    /// server is expected to survive, not one it should die of.
    pub fn try_from_stream(stream: UnixStream) -> Result<Self> {
        Ok(Self(SocketTransport::try_from_stream(stream)?))
    }
}

impl Transport for UnixTransport {
    fn read(&mut self) -> Result<JsonRpcMessage> {
        self.0.read()
    }

    fn write(&mut self, message: &JsonRpcMessage) -> Result<()> {
        self.0.write(message)
    }

    fn close(&mut self) -> Result<()> {
        self.0.close()
    }

    fn close_write(&mut self) -> Result<()> {
        self.0.close_write()
    }

    fn try_clone_writer(&self) -> Option<Box<dyn Transport>> {
        self.0.try_clone_writer()
    }

    /// Supported: the deadline rides on the socket's own `SO_RCVTIMEO`.
    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<bool> {
        self.0.set_read_timeout(timeout)
    }

    fn set_max_message_bytes(&mut self, max: usize) {
        self.0.set_max_message_bytes(max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{JsonRpcMessage, McpError};
    use std::time::Instant;

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
    fn test_read_timeout_expires_and_then_recovers() {
        let (a, b) = UnixStream::pair().unwrap();
        let mut client = UnixTransport::from_stream(a);
        let mut server = UnixTransport::from_stream(b);

        assert!(
            server
                .set_read_timeout(Some(Duration::from_millis(50)))
                .unwrap()
        );

        let started = Instant::now();
        assert!(matches!(server.read(), Err(McpError::Timeout(_))));
        assert!(started.elapsed() >= Duration::from_millis(40));
        assert!(started.elapsed() < Duration::from_secs(2));

        // The connection is still usable; a timeout is not a close.
        let msg = JsonRpcMessage::request(1i64, "ping", None);
        client.write(&msg).unwrap();
        assert_eq!(server.read().unwrap(), msg);
    }

    #[test]
    fn test_clearing_the_timeout_restores_blocking_reads() {
        let (a, b) = UnixStream::pair().unwrap();
        let mut server = UnixTransport::from_stream(b);

        server
            .set_read_timeout(Some(Duration::from_millis(20)))
            .unwrap();
        assert!(matches!(server.read(), Err(McpError::Timeout(_))));

        server.set_read_timeout(None).unwrap();

        // With no deadline the read blocks until the peer writes, rather than
        // expiring again.
        let writer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(120));
            let mut client = UnixTransport::from_stream(a);
            client
                .write(&JsonRpcMessage::request(9i64, "ping", None))
                .unwrap();
            // Keep the peer alive until the read lands.
            std::thread::sleep(Duration::from_millis(200));
        });

        assert_eq!(
            server.read().unwrap(),
            JsonRpcMessage::request(9i64, "ping", None)
        );
        writer.join().unwrap();
    }

    #[test]
    fn test_a_timeout_does_not_lose_a_partial_message() {
        // Half a message, then a pause longer than the deadline, then the rest.
        // Dropping the first half would desync framing permanently.
        let (a, b) = UnixStream::pair().unwrap();
        let mut server = UnixTransport::from_stream(b);

        let msg = JsonRpcMessage::request(5i64, "ping", None);
        let line = format!("{}\n", serde_json::to_string(&msg).unwrap());
        let (head, tail) = line.split_at(14);
        let (head, tail) = (head.to_string(), tail.to_string());

        let writer = std::thread::spawn(move || {
            use std::io::Write as _;
            let mut a = a;
            a.write_all(head.as_bytes()).unwrap();
            a.flush().unwrap();
            std::thread::sleep(Duration::from_millis(150));
            a.write_all(tail.as_bytes()).unwrap();
            a.flush().unwrap();
            std::thread::sleep(Duration::from_millis(200));
        });

        server
            .set_read_timeout(Some(Duration::from_millis(40)))
            .unwrap();
        assert!(matches!(server.read(), Err(McpError::Timeout(_))));

        server
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        assert_eq!(server.read().unwrap(), msg, "the first half was lost");

        writer.join().unwrap();
    }

    #[test]
    fn test_disconnect_after_a_timeout_is_still_a_clean_close() {
        // Regression guard: clearing a deadline on a socket whose peer has gone
        // fails with EINVAL on macOS. If that error escaped, a normal hangup
        // would look like an IO failure and take the server loop down with it.
        let (a, b) = UnixStream::pair().unwrap();
        let mut server = UnixTransport::from_stream(b);

        server
            .set_read_timeout(Some(Duration::from_millis(20)))
            .unwrap();
        assert!(matches!(server.read(), Err(McpError::Timeout(_))));

        drop(a);
        server.set_read_timeout(None).unwrap();
        assert!(matches!(server.read(), Err(McpError::TransportClosed)));
    }

    #[test]
    fn test_timeouts_are_supported_on_a_unix_socket() {
        let (_a, b) = UnixStream::pair().unwrap();
        let mut server = UnixTransport::from_stream(b);
        assert!(
            server
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap()
        );
        assert!(server.set_read_timeout(None).unwrap());
    }

    #[test]
    fn test_connect_refused_on_missing_socket() {
        // No listener at this path -> connect should fail, not panic.
        let path = std::env::temp_dir().join("sml_mcps_nonexistent_test.sock");
        let _ = std::fs::remove_file(&path);
        let result = UnixTransport::connect(&path);
        assert!(result.is_err());
    }

    #[test]
    fn test_try_from_stream_hands_the_failure_back() {
        // The fallible constructor is what `try_clone_writer` goes through, so
        // that a process out of descriptors gets a `None` rather than the panic
        // `from_stream` is entitled to. Here the clone succeeds; what matters
        // is that the result is one the caller can look at.
        let (a, b) = UnixStream::pair().unwrap();
        let mut client = UnixTransport::try_from_stream(a).expect("a pair can be cloned");
        let mut server = UnixTransport::try_from_stream(b).unwrap();

        let msg = JsonRpcMessage::request(1i64, "ping", None);
        client.write(&msg).unwrap();
        assert_eq!(server.read().unwrap(), msg);
    }

    #[test]
    fn test_try_clone_writer_writes_to_the_same_socket() {
        let (a, b) = UnixStream::pair().unwrap();
        let client = UnixTransport::from_stream(a);
        let mut server = UnixTransport::from_stream(b);

        let mut cloned = client.try_clone_writer().expect("a unix socket can split");
        let msg = JsonRpcMessage::request(4i64, "ping", None);
        cloned.write(&msg).unwrap();

        assert_eq!(server.read().unwrap(), msg);
    }

    #[test]
    fn test_the_original_still_writes_after_its_writer_is_cloned() {
        // Independent handles, not a handover: the bridge writes through the
        // clone on one thread while the original is still in use on another.
        let (a, b) = UnixStream::pair().unwrap();
        let mut client = UnixTransport::from_stream(a);
        let mut server = UnixTransport::from_stream(b);

        let mut cloned = client.try_clone_writer().unwrap();
        let first = JsonRpcMessage::request(1i64, "ping", None);
        let second = JsonRpcMessage::request(2i64, "ping", None);

        cloned.write(&first).unwrap();
        client.write(&second).unwrap();

        assert_eq!(server.read().unwrap(), first);
        assert_eq!(
            server.read().unwrap(),
            second,
            "two handles on one socket must not lose or interleave a message"
        );
    }

    #[test]
    fn test_a_cloned_writer_can_be_cloned_again() {
        // The bridge holds the clone as a `Box<dyn Transport>` and asks it for
        // another; a clone that could not be split would end the chain.
        let (a, b) = UnixStream::pair().unwrap();
        let client = UnixTransport::from_stream(a);
        let mut server = UnixTransport::from_stream(b);

        let cloned = client.try_clone_writer().unwrap();
        let mut again = cloned
            .try_clone_writer()
            .expect("a clone is still a socket");

        let msg = JsonRpcMessage::request(7i64, "ping", None);
        again.write(&msg).unwrap();
        assert_eq!(server.read().unwrap(), msg);
    }
}
