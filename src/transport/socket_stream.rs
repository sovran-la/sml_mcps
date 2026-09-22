//! The half of a socket transport that is not about which socket it is.
//!
//! [`UnixTransport`](crate::UnixTransport) and [`TcpTransport`](crate::TcpTransport)
//! differ in how they are dialed and in nothing else: the same
//! newline-delimited JSON-RPC framing, the same split into an owned reader and
//! a cloned writer, the same `SO_RCVTIMEO`-backed deadlines, the same
//! half-close on teardown. That shared body lives here once, over whatever
//! stream can answer the three questions in [`SocketStream`].
//!
//! Applications can implement SocketStream for authenticated duplex streams.

use crate::transport::Transport;
use crate::transport::line::{DeadlineRead, LineReader};
use crate::types::{JsonRpcMessage, Result};
use std::io::{Read, Write};
use std::net::Shutdown;
use std::time::{Duration, Instant};

/// A connected socket a [`SocketTransport`] can be built on.
///
/// Three capabilities, all of which `std`'s stream types already have as
/// inherent methods:
///
/// - **cloning**, for the independent write handle
///   [`Transport::try_clone_writer`] hands out,
/// - **read timeouts**, which are what let a server pump its transport instead
///   of blocking in [`read`](Transport::read) forever,
/// - **half-close**, so a peer can be told the conversation is over in one
///   direction without losing what is still in flight in the other.
///
/// `Sync` as well as `Send` because [`Transport`] requires it, and a transport
/// is only as `Sync` as what it holds.
pub trait SocketStream: Read + Write + Send + Sync + Sized + 'static {
    /// A second descriptor for the same connection.
    fn try_clone(&self) -> std::io::Result<Self>;

    /// Apply `dur` to subsequent reads, or `None` to block indefinitely.
    fn set_read_timeout(&self, dur: Option<Duration>) -> std::io::Result<()>;

    /// Shut down one or both directions of the connection.
    fn shutdown(&self, how: Shutdown) -> std::io::Result<()>;
}

#[cfg(unix)]
impl SocketStream for std::os::unix::net::UnixStream {
    fn try_clone(&self) -> std::io::Result<Self> {
        // Explicitly the inherent method: the trait's has the same name, and
        // `self.try_clone()` inside its own impl is how that becomes a loop.
        std::os::unix::net::UnixStream::try_clone(self)
    }

    fn set_read_timeout(&self, dur: Option<Duration>) -> std::io::Result<()> {
        std::os::unix::net::UnixStream::set_read_timeout(self, dur)
    }

    fn shutdown(&self, how: Shutdown) -> std::io::Result<()> {
        std::os::unix::net::UnixStream::shutdown(self, how)
    }
}

impl SocketStream for std::net::TcpStream {
    fn try_clone(&self) -> std::io::Result<Self> {
        std::net::TcpStream::try_clone(self)
    }

    fn set_read_timeout(&self, dur: Option<Duration>) -> std::io::Result<()> {
        std::net::TcpStream::set_read_timeout(self, dur)
    }

    fn shutdown(&self, how: Shutdown) -> std::io::Result<()> {
        std::net::TcpStream::shutdown(self, how)
    }
}

/// A socket whose reads honor a deadline via `SO_RCVTIMEO`.
struct TimedStream<S> {
    stream: S,
}

impl<S: SocketStream> Read for TimedStream<S> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buf)
    }
}

impl<S: SocketStream> DeadlineRead for TimedStream<S> {
    fn set_deadline(&mut self, deadline: Option<Instant>) -> std::io::Result<()> {
        // The socket reads a zero timeout as "block forever", so an already
        // passed deadline has to round up rather than down. A nanosecond is the
        // smallest thing that still means "expire".
        let remaining = deadline.map(|deadline| {
            deadline
                .saturating_duration_since(Instant::now())
                .max(Duration::from_nanos(1))
        });
        self.stream.set_read_timeout(remaining)
    }
}

/// Newline-delimited JSON-RPC over a connected socket.
///
/// Holds a buffered reader and a cloned write handle to the same socket, so
/// reads and writes are independent (the standard split-stream pattern).
pub struct SocketTransport<S: SocketStream> {
    reader: LineReader<TimedStream<S>>,
    writer: S,
    /// Applied to the next read; `None` blocks indefinitely.
    read_timeout: Option<Duration>,
}

impl<S: SocketStream> SocketTransport<S> {
    /// Wrap an already-connected socket.
    ///
    /// The clone is a second descriptor for the same socket, so this fails
    /// exactly when the process has run out of them - which is a condition a
    /// server is expected to survive, not one it should die of.
    pub fn try_from_stream(stream: S) -> Result<Self> {
        let writer = stream.try_clone()?;
        Ok(Self {
            reader: LineReader::new(TimedStream { stream }),
            writer,
            read_timeout: None,
        })
    }
}

impl<S: SocketStream> Transport for SocketTransport<S> {
    fn read(&mut self) -> Result<JsonRpcMessage> {
        self.reader.read_message(self.read_timeout)
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
        // Clone the underlying descriptor so the bridge's other direction has
        // an independent write handle to the same connection.
        //
        // Both clones are fallible and both are answered with `None`: a
        // process that ran out of descriptors between the two is the exact
        // condition this method exists to report, not one to panic on.
        let stream = self.writer.try_clone().ok()?;
        let transport = SocketTransport::try_from_stream(stream).ok()?;

        Some(Box::new(transport))
    }

    /// Supported: the deadline rides on the socket's own `SO_RCVTIMEO`.
    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<bool> {
        self.read_timeout = timeout;
        Ok(true)
    }

    fn set_max_message_bytes(&mut self, max: usize) {
        self.reader.set_limit(max);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::McpError;
    use std::net::{TcpListener, TcpStream};

    /// A connected TCP pair on loopback, both ends already wrapped.
    ///
    /// The generic body is exercised over Unix sockets by `unix.rs` and over
    /// TCP by `tcp.rs`; what belongs here is the handful of properties that are
    /// the *generic's* own - the ones a second stream type must not break.
    fn tcp_pair() -> (SocketTransport<TcpStream>, SocketTransport<TcpStream>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();

        (
            SocketTransport::try_from_stream(client).unwrap(),
            SocketTransport::try_from_stream(server).unwrap(),
        )
    }

    #[test]
    fn test_a_message_survives_the_round_trip() {
        let (mut client, mut server) = tcp_pair();

        let msg = JsonRpcMessage::request(1i64, "tools/list", None);
        client.write(&msg).unwrap();

        assert_eq!(server.read().unwrap(), msg);
    }

    #[test]
    fn test_the_writer_is_an_independent_handle() {
        // What `Bridge` and a task worker both lean on: writing through the
        // clone while the original is still in use, without losing or
        // interleaving a message.
        let (mut client, mut server) = tcp_pair();

        let mut cloned = client.try_clone_writer().expect("a socket can split");
        let first = JsonRpcMessage::request(1i64, "ping", None);
        let second = JsonRpcMessage::request(2i64, "ping", None);

        cloned.write(&first).unwrap();
        client.write(&second).unwrap();

        assert_eq!(server.read().unwrap(), first);
        assert_eq!(server.read().unwrap(), second);
    }

    #[test]
    fn test_a_deadline_expires_and_the_connection_survives_it() {
        let (mut client, mut server) = tcp_pair();

        assert!(
            server
                .set_read_timeout(Some(Duration::from_millis(50)))
                .unwrap(),
            "a socket transport honors deadlines"
        );
        assert!(matches!(server.read(), Err(McpError::Timeout(_))));

        // A timeout is a wait that ended, not a connection that did.
        server.set_read_timeout(None).unwrap();
        let msg = JsonRpcMessage::request(3i64, "ping", None);
        client.write(&msg).unwrap();
        assert_eq!(server.read().unwrap(), msg);
    }

    #[test]
    fn test_a_half_close_is_an_eof_the_peer_can_still_answer_through() {
        let (mut client, mut server) = tcp_pair();

        client.close_write().unwrap();

        // The peer sees end of stream...
        assert!(matches!(server.read(), Err(McpError::TransportClosed)));
        // ...and can still write back, which is the whole difference between
        // this and `close`.
        let msg = JsonRpcMessage::response(1i64, serde_json::json!({ "ok": true }));
        server.write(&msg).unwrap();
        assert_eq!(client.read().unwrap(), msg);
    }

    #[test]
    fn test_closing_twice_is_not_an_error() {
        let (mut client, _server) = tcp_pair();
        assert!(client.close().is_ok());
        assert!(client.close().is_ok(), "NotConnected is not a failure");
    }

    #[test]
    fn test_the_message_ceiling_is_pushed_down_to_the_reader() {
        let (mut client, mut server) = tcp_pair();
        server.set_max_message_bytes(64);

        let too_big = JsonRpcMessage::request(
            1i64,
            "tools/call",
            Some(serde_json::json!({ "name": "echo", "text": "x".repeat(512) })),
        );
        client.write(&too_big).unwrap();

        assert!(
            matches!(server.read(), Err(McpError::InvalidMessage(_))),
            "a message over the ceiling is refused, not absorbed"
        );

        // And framing resynchronizes on the next one.
        let fits = JsonRpcMessage::request(2i64, "ping", None);
        client.write(&fits).unwrap();
        assert_eq!(server.read().unwrap(), fits);
    }
}
