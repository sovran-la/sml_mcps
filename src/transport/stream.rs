//! A transport over a stream this crate did not open.
//!
//! [`UnixTransport`](crate::UnixTransport) and [`TcpTransport`](crate::TcpTransport)
//! know what they are holding: a descriptor they can clone for an independent
//! writer, and a socket they can put a receive timeout on. A stream that
//! arrived already wrapped - a TLS session, a compressed pipe, a test double -
//! is one object with one state machine, and neither of those is available
//! through it.
//!
//! [`StreamTransport`] is the honest version of that: the same
//! newline-delimited JSON-RPC framing over anything `Read + Write + Send`,
//! reporting plainly that it cannot split and cannot deadline. It is what
//! makes a [`Listener`](crate::Listener) implementable without this crate
//! knowing anything about TLS - the application accepts, handshakes, and hands
//! the finished stream in.
//!
//! ```no_run
//! use sml_mcps::StreamTransport;
//! use std::net::TcpStream;
//!
//! # fn handshake(s: TcpStream) -> TcpStream { s }
//! let tcp = TcpStream::connect("127.0.0.1:7743").unwrap();
//! let transport = StreamTransport::new(handshake(tcp));
//! ```
//!
//! # What the server does with one
//!
//! [`Server::start`](crate::Server::start) copes with both limitations, the
//! same way it does for the HTTP transport: writes go through the read handle,
//! and task workers may not make server-initiated requests. Tools, resources,
//! prompts, notifications and logging are unaffected. If a connection needs
//! elicitation or sampling *from inside a task*, give it a transport that can
//! split.

use crate::transport::Transport;
use crate::transport::line::{DeadlineRead, LineReader};
use crate::types::{JsonRpcMessage, Result};
use std::io::{Read, Write};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Anything with two directions that can move between threads.
trait ReadWrite: Read + Write + Send {}
impl<T: Read + Write + Send> ReadWrite for T {}

/// The stream, as [`LineReader`] wants to see it.
struct Source {
    stream: Box<dyn ReadWrite>,
}

impl Read for Source {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.stream.read(buf)
    }
}

impl DeadlineRead for Source {
    /// Refused, loudly.
    ///
    /// Unreachable in practice: [`StreamTransport::set_read_timeout`] answers
    /// `false` and [`read`](StreamTransport::read) never asks for a deadline,
    /// so nothing arms one. Should that ever stop being true, an error is the
    /// failure to have - the alternative is a read that silently ignores its
    /// deadline and blocks forever.
    fn set_deadline(&mut self, _deadline: Option<Instant>) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "a StreamTransport cannot put a deadline on a stream it did not open",
        ))
    }
}

/// Newline-delimited JSON-RPC over a stream the caller already owns.
///
/// See the [module docs](self) for what it can and cannot do.
pub struct StreamTransport {
    /// Never actually locked: every method here takes `&mut self`, so there is
    /// nothing to contend with and the guard is taken with
    /// [`Mutex::get_mut`], which compiles to nothing.
    ///
    /// It is here for the type system. [`Transport`] requires `Sync`, and a
    /// `Box<dyn Read + Write + Send>` is not - wrapping it is what makes the
    /// bound true without an `unsafe impl` that a later edit could quietly
    /// invalidate.
    inner: Mutex<LineReader<Source>>,
}

impl StreamTransport {
    /// Wrap a stream that is ready to carry messages.
    ///
    /// Whatever setup the stream needs - a TLS handshake, an authentication
    /// exchange, a proxy header - happens before this: from here on the bytes
    /// are JSON-RPC lines.
    pub fn new<S: Read + Write + Send + 'static>(stream: S) -> Self {
        Self {
            inner: Mutex::new(LineReader::new(Source {
                stream: Box::new(stream),
            })),
        }
    }

    /// The reader, which also owns the stream that writes go through.
    ///
    /// A `Mutex` is only poisoned by a panic while it was held, and this one is
    /// never held - but taking the inner value either way costs a branch and
    /// removes a panic path.
    fn reader(&mut self) -> &mut LineReader<Source> {
        self.inner.get_mut().unwrap_or_else(|e| e.into_inner())
    }
}

impl Transport for StreamTransport {
    fn read(&mut self) -> Result<JsonRpcMessage> {
        // Always untimed: see `Source::set_deadline`.
        self.reader().read_message(None)
    }

    fn write(&mut self, message: &JsonRpcMessage) -> Result<()> {
        let stream = &mut self.reader().source_mut().stream;
        serde_json::to_writer(&mut *stream, message)?;
        writeln!(stream)?;
        stream.flush()?;
        Ok(())
    }

    /// Flushes and gives up the stream's contents; the stream itself closes
    /// when this transport is dropped.
    ///
    /// There is no `shutdown` to call: half-closing is a socket operation, and
    /// what is underneath here may be several layers away from one.
    fn close(&mut self) -> Result<()> {
        let stream = &mut self.reader().source_mut().stream;
        // A peer that has already gone is not a failure to report - the
        // conversation is over either way, which is what `close` was asked to
        // arrange.
        let _ = stream.flush();
        Ok(())
    }

    /// Not supported: an opaque stream is one object, and handing out a second
    /// writer to it would mean two threads interleaving bytes mid-message.
    fn try_clone_writer(&self) -> Option<Box<dyn Transport>> {
        None
    }

    /// Not supported: `SO_RCVTIMEO` belongs to a socket, and this may be many
    /// layers above one.
    fn set_read_timeout(&mut self, _timeout: Option<Duration>) -> Result<bool> {
        Ok(false)
    }

    fn set_max_message_bytes(&mut self, max: usize) {
        self.reader().set_limit(max);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::McpError;
    use std::net::{TcpListener, TcpStream};

    /// A connected TCP pair, both ends wrapped as opaque streams.
    ///
    /// A plain `TcpStream` stands in for a TLS-wrapped one: this transport
    /// cannot tell the difference, which is the point of it.
    fn pair() -> (StreamTransport, StreamTransport) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();

        (StreamTransport::new(client), StreamTransport::new(server))
    }

    #[test]
    fn test_a_message_survives_the_round_trip() {
        let (mut client, mut server) = pair();

        let msg = JsonRpcMessage::request(1i64, "tools/list", None);
        client.write(&msg).unwrap();

        assert_eq!(server.read().unwrap(), msg);
    }

    #[test]
    fn test_both_directions_work_on_one_stream() {
        // The whole reason the writer reaches through the reader: there is one
        // object, and it has to carry traffic each way.
        let (mut client, mut server) = pair();

        let req = JsonRpcMessage::request(7i64, "ping", None);
        client.write(&req).unwrap();
        assert_eq!(server.read().unwrap(), req);

        let resp = JsonRpcMessage::response(7i64, serde_json::json!({ "ok": true }));
        server.write(&resp).unwrap();
        assert_eq!(client.read().unwrap(), resp);
    }

    #[test]
    fn test_messages_written_back_to_back_read_out_one_at_a_time() {
        let (mut client, mut server) = pair();

        for i in 0..5i64 {
            client
                .write(&JsonRpcMessage::request(i, "ping", None))
                .unwrap();
        }
        for i in 0..5i64 {
            assert_eq!(
                server.read().unwrap(),
                JsonRpcMessage::request(i, "ping", None)
            );
        }
    }

    #[test]
    fn test_a_message_larger_than_the_socket_buffer_arrives_whole() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = StreamTransport::new(TcpStream::connect(addr).unwrap());

        let msg = JsonRpcMessage::request(
            1i64,
            "tools/call",
            Some(serde_json::json!({ "text": "x".repeat(256 * 1024) })),
        );

        let expected = msg.clone();
        let reader = std::thread::spawn(move || {
            let (server, _) = listener.accept().unwrap();
            StreamTransport::new(server).read().unwrap()
        });

        client.write(&msg).unwrap();
        assert_eq!(reader.join().unwrap(), expected);
    }

    #[test]
    fn test_a_hung_up_peer_is_a_clean_close() {
        let (client, mut server) = pair();

        drop(client);

        assert!(matches!(server.read(), Err(McpError::TransportClosed)));
    }

    #[test]
    fn test_it_says_it_cannot_split() {
        // Not a detail: `Server::start` reads this to decide whether a task
        // worker may make server-initiated requests. Answering `Some` with a
        // handle that was really the same object would deadlock instead.
        let (client, _server) = pair();

        assert!(client.try_clone_writer().is_none());
    }

    #[test]
    fn test_it_says_it_cannot_deadline() {
        let (mut client, _server) = pair();

        assert!(
            !client
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap(),
            "claiming a deadline it cannot honor would hang the pump that trusted it"
        );
        assert!(!client.set_read_timeout(None).unwrap());
    }

    #[test]
    fn test_the_message_ceiling_is_honored() {
        let (mut client, mut server) = pair();
        server.set_max_message_bytes(64);

        client
            .write(&JsonRpcMessage::request(
                1i64,
                "tools/call",
                Some(serde_json::json!({ "text": "x".repeat(512) })),
            ))
            .unwrap();
        assert!(matches!(server.read(), Err(McpError::InvalidMessage(_))));

        // Framing resynchronizes: the connection survives an oversized message.
        let fits = JsonRpcMessage::request(2i64, "ping", None);
        client.write(&fits).unwrap();
        assert_eq!(server.read().unwrap(), fits);
    }

    #[test]
    fn test_close_flushes_rather_than_failing() {
        let (mut client, server) = pair();

        assert!(client.close().is_ok());

        // And a peer that has already gone is still not an error: `close` is
        // called on teardown paths that have nothing to do about a failure.
        drop(server);
        assert!(client.close().is_ok());
    }

    #[test]
    fn test_it_carries_a_stream_that_is_not_a_socket() {
        // The generic promise, held to: an in-memory duplex is `Read + Write +
        // Send` and nothing more, and it works.
        struct Duplex {
            incoming: std::io::Cursor<Vec<u8>>,
            outgoing: Vec<u8>,
        }
        impl Read for Duplex {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                self.incoming.read(buf)
            }
        }
        impl Write for Duplex {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.outgoing.write(buf)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let incoming = JsonRpcMessage::request(1i64, "ping", None);
        let line = format!("{}\n", serde_json::to_string(&incoming).unwrap());
        let mut transport = StreamTransport::new(Duplex {
            incoming: std::io::Cursor::new(line.into_bytes()),
            outgoing: Vec::new(),
        });

        assert_eq!(transport.read().unwrap(), incoming);
        transport
            .write(&JsonRpcMessage::response(1i64, serde_json::json!({})))
            .unwrap();
        // Nothing left to read once the cursor is spent.
        assert!(matches!(transport.read(), Err(McpError::TransportClosed)));
    }

    #[test]
    fn test_a_deadline_pushed_down_anyway_is_refused_not_ignored() {
        // `set_deadline` is unreachable through the public surface. If it ever
        // stops being, the failure has to be visible rather than a read that
        // quietly waits forever.
        let mut source = Source {
            stream: Box::new(std::io::Cursor::new(Vec::new())),
        };

        let err = source.set_deadline(Some(Instant::now())).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
    }
}
