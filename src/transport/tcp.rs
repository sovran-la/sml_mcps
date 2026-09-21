//! TCP Transport
//!
//! Communication over a TCP stream. Same newline-delimited JSON-RPC wire
//! format as [`UnixTransport`](crate::UnixTransport) - the only difference is
//! the underlying stream, and both are the same
//! [`SocketTransport`](crate::transport::socket_stream::SocketTransport)
//! underneath.
//!
//! Three constructors cover both sides of a connection:
//! - [`TcpTransport::connect`] - client side, dials a listening daemon.
//! - [`TcpTransport::connect_timeout`] - the same with a deadline per resolved
//!   address, for a host that drops rather than refuses.
//! - [`TcpTransport::from_stream`] - server side, wraps a stream handed back by
//!   [`TcpListener::accept`](std::net::TcpListener::accept).
//!
//! This is what the *remote shim* rides on: a binary launched with
//! `--connect <host:port>` dials a daemon on another machine - one serving
//! through a [`TcpSocketListener`](crate::TcpSocketListener) - and proxies
//! stdio to it. See [`Server::serve_daemon_with`](crate::Server::serve_daemon_with).
//!
//! **This is not an encrypted transport.** A daemon that listens on TCP is
//! reachable by anything that can route to it, and this carries whatever it is
//! given in the clear - every tool call, every result. Bind it to an interface
//! only the people you trust can reach (a Tailscale address, a loopback behind
//! an SSH tunnel), or - what [`Listener`](crate::Listener) exists for - do the
//! accepting yourself, wrap the stream in TLS, and hand the result in as a
//! [`StreamTransport`](crate::StreamTransport).

use crate::transport::Transport;
use crate::transport::socket_stream::SocketTransport;
use crate::types::{JsonRpcMessage, Result};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

/// Transport over a TCP stream.
///
/// Holds a buffered reader and a cloned write handle to the same socket, so
/// reads and writes are independent (the standard split-stream pattern).
pub struct TcpTransport(SocketTransport<TcpStream>);

impl TcpTransport {
    /// Connect to a listening server (client side).
    pub fn connect(addr: impl ToSocketAddrs) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        Self::try_from_stream(stream)
    }

    /// [`connect`](Self::connect), giving up on each resolved address after
    /// `timeout`.
    ///
    /// `TcpStream::connect` has no deadline of its own: a host that is routed
    /// but not answering - a machine that is off, a firewall that drops rather
    /// than refuses - holds the caller for the kernel's SYN retry schedule,
    /// which is minutes. A shim dialing a daemon on another machine needs to
    /// say "unreachable" well before that.
    ///
    /// A name that resolves to several addresses is tried in order, each with
    /// its own `timeout`, and the last failure is the one reported. A name that
    /// resolves to nothing at all is an error too, not a silent success.
    pub fn connect_timeout(addr: impl ToSocketAddrs, timeout: Duration) -> Result<Self> {
        let mut last_error: Option<std::io::Error> = None;
        for candidate in addr.to_socket_addrs()? {
            match TcpStream::connect_timeout(&candidate, timeout) {
                Ok(stream) => return Self::try_from_stream(stream),
                Err(error) => last_error = Some(error),
            }
        }

        Err(last_error
            .unwrap_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "the address resolved to nothing",
                )
            })
            .into())
    }

    /// Wrap an already-accepted stream (server side).
    ///
    /// Panics on file-descriptor exhaustion (`try_clone` failure). Use
    /// [`try_from_stream`](Self::try_from_stream) anywhere the caller can carry
    /// on without this connection - an accept loop that must keep listening -
    /// and keep this one for the top of a program, where there is nothing to
    /// carry on to.
    pub fn from_stream(stream: TcpStream) -> Self {
        Self::try_from_stream(stream)
            .expect("TcpTransport::from_stream: failed to clone TcpStream (fd exhaustion?)")
    }

    /// [`from_stream`](Self::from_stream) with the failure handed back.
    pub fn try_from_stream(stream: TcpStream) -> Result<Self> {
        // One JSON-RPC message per line, each written and flushed on its own,
        // is precisely the traffic Nagle's algorithm delays: small writes with
        // a reply expected before the next one. Best-effort, because a socket
        // that will not take the option is still a usable socket.
        let _ = stream.set_nodelay(true);

        Ok(Self(SocketTransport::try_from_stream(stream)?))
    }
}

impl Transport for TcpTransport {
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
    use crate::types::McpError;
    use std::net::TcpListener;
    use std::time::Instant;

    /// A connected pair on loopback: client end, server end.
    fn pair() -> (TcpTransport, TcpTransport) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpTransport::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();

        (client, TcpTransport::from_stream(server))
    }

    #[test]
    fn test_roundtrip_through_a_connected_pair() {
        let (mut client, mut server) = pair();

        let msg = JsonRpcMessage::request(1i64, "tools/list", None);
        client.write(&msg).unwrap();

        assert_eq!(server.read().unwrap(), msg);
    }

    #[test]
    fn test_bidirectional() {
        let (mut client, mut server) = pair();

        let req = JsonRpcMessage::request(7i64, "ping", None);
        client.write(&req).unwrap();
        assert_eq!(server.read().unwrap(), req);

        let resp = JsonRpcMessage::response(7i64, serde_json::json!({ "ok": true }));
        server.write(&resp).unwrap();
        assert_eq!(client.read().unwrap(), resp);
    }

    #[test]
    fn test_multiple_messages_framing() {
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
    fn test_large_message() {
        // Bigger than any socket buffer, so the read has to happen while the
        // write is still going or the writer blocks.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpTransport::connect(addr).unwrap();

        let big = "x".repeat(256 * 1024);
        let msg = JsonRpcMessage::request(
            1i64,
            "tools/call",
            Some(serde_json::json!({ "name": "echo", "arguments": { "message": big } })),
        );

        let expected = msg.clone();
        let reader = std::thread::spawn(move || {
            let (server, _) = listener.accept().unwrap();
            TcpTransport::from_stream(server).read().unwrap()
        });

        client.write(&msg).unwrap();
        assert_eq!(reader.join().unwrap(), expected);
    }

    #[test]
    fn test_close_detection_on_drop() {
        let (client, mut server) = pair();

        drop(client);
        assert!(matches!(server.read(), Err(McpError::TransportClosed)));
    }

    #[test]
    fn test_close_is_idempotent() {
        let (mut client, _server) = pair();
        assert!(client.close().is_ok());
        assert!(client.close().is_ok());
    }

    #[test]
    fn test_read_timeout_expires_and_then_recovers() {
        let (mut client, mut server) = pair();

        assert!(
            server
                .set_read_timeout(Some(Duration::from_millis(50)))
                .unwrap()
        );

        let started = Instant::now();
        assert!(matches!(server.read(), Err(McpError::Timeout(_))));
        assert!(started.elapsed() >= Duration::from_millis(40));
        assert!(started.elapsed() < Duration::from_secs(2));

        let msg = JsonRpcMessage::request(1i64, "ping", None);
        client.write(&msg).unwrap();
        assert_eq!(server.read().unwrap(), msg);
    }

    #[test]
    fn test_a_timeout_does_not_lose_a_partial_message() {
        // Half a message, a pause longer than the deadline, then the rest.
        // Dropping the first half would desync framing permanently.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut writer_stream = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();
        let mut server = TcpTransport::from_stream(server);

        let msg = JsonRpcMessage::request(5i64, "ping", None);
        let line = format!("{}\n", serde_json::to_string(&msg).unwrap());
        let (head, tail) = line.split_at(14);
        let (head, tail) = (head.to_string(), tail.to_string());

        let writer = std::thread::spawn(move || {
            use std::io::Write as _;
            writer_stream.write_all(head.as_bytes()).unwrap();
            writer_stream.flush().unwrap();
            std::thread::sleep(Duration::from_millis(150));
            writer_stream.write_all(tail.as_bytes()).unwrap();
            writer_stream.flush().unwrap();
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
    fn test_try_clone_writer_writes_to_the_same_socket() {
        let (client, mut server) = pair();

        let mut cloned = client.try_clone_writer().expect("a tcp socket can split");
        let msg = JsonRpcMessage::request(4i64, "ping", None);
        cloned.write(&msg).unwrap();

        assert_eq!(server.read().unwrap(), msg);
    }

    #[test]
    fn test_a_cloned_writer_can_be_cloned_again() {
        // The bridge holds the clone as a `Box<dyn Transport>` and asks it for
        // another; a clone that could not be split would end the chain.
        let (client, mut server) = pair();

        let mut again = client
            .try_clone_writer()
            .unwrap()
            .try_clone_writer()
            .expect("a clone is still a socket");

        let msg = JsonRpcMessage::request(7i64, "ping", None);
        again.write(&msg).unwrap();
        assert_eq!(server.read().unwrap(), msg);
    }

    #[test]
    fn test_connect_refused_when_nobody_is_listening() {
        // Bind, learn the port, drop the listener: an address nothing answers
        // on. `connect` has to fail rather than panic.
        let addr = {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap()
        };

        assert!(TcpTransport::connect(addr).is_err());
    }

    #[test]
    fn test_connect_timeout_reaches_a_listener() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let mut client = TcpTransport::connect_timeout(addr, Duration::from_secs(5)).unwrap();
        let (server, _) = listener.accept().unwrap();
        let mut server = TcpTransport::from_stream(server);

        let msg = JsonRpcMessage::request(1i64, "ping", None);
        client.write(&msg).unwrap();
        assert_eq!(server.read().unwrap(), msg);
    }

    #[test]
    fn test_connect_timeout_resolves_a_name() {
        // `localhost`, not a literal: the whole point of taking `ToSocketAddrs`
        // is that a MagicDNS name works, and this is the one name every box
        // resolves. It may resolve to both `::1` and `127.0.0.1`; whichever
        // order they come in, one of them is the listener and the other is a
        // refusal that must not end the attempt.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let mut client =
            TcpTransport::connect_timeout(format!("localhost:{port}"), Duration::from_secs(5))
                .unwrap();
        let (server, _) = listener.accept().unwrap();
        let mut server = TcpTransport::from_stream(server);

        let msg = JsonRpcMessage::request(1i64, "ping", None);
        client.write(&msg).unwrap();
        assert_eq!(server.read().unwrap(), msg);
    }

    #[test]
    fn test_connect_timeout_is_refused_when_nobody_is_listening() {
        let addr = {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap()
        };

        let started = Instant::now();
        let Err(err) = TcpTransport::connect_timeout(addr, Duration::from_secs(5)) else {
            panic!("nothing is listening there");
        };
        assert!(matches!(err, McpError::Io(_)), "{err:?}");
        // A refusal is immediate; the timeout is for silence, not for this.
        assert!(started.elapsed() < Duration::from_secs(4));
    }

    #[test]
    fn test_connect_timeout_reports_a_name_that_does_not_resolve() {
        // `.invalid` is reserved (RFC 2606) precisely so it never resolves.
        let Err(err) =
            TcpTransport::connect_timeout("no-such-host.invalid:7211", Duration::from_secs(5))
        else {
            panic!("a reserved-invalid name must not resolve");
        };
        assert!(matches!(err, McpError::Io(_)), "{err:?}");
    }

    #[test]
    fn test_connect_timeout_gives_up_on_silence() {
        // 10.255.255.1 is in a private range this test box is not routed to,
        // so a SYN there is dropped rather than refused - the case the
        // deadline exists for. If the network *does* answer (a refusal, or a
        // listener), that is still an outcome within the deadline, which is all
        // the assertion needs.
        let started = Instant::now();
        let _ = TcpTransport::connect_timeout("10.255.255.1:9", Duration::from_millis(300));
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "connect_timeout should not wait out the kernel's SYN retries"
        );
    }

    #[test]
    fn test_nagle_is_off_on_both_sides() {
        // Not cosmetic: with Nagle on, a request whose reply the peer is
        // waiting for can sit in the kernel for tens of milliseconds.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();

        let client_stream = client.try_clone().unwrap();
        let server_stream = server.try_clone().unwrap();
        let _client = TcpTransport::from_stream(client);
        let _server = TcpTransport::from_stream(server);

        assert!(client_stream.nodelay().unwrap());
        assert!(server_stream.nodelay().unwrap());
    }
}
