//! Where a daemon's connections come from.
//!
//! [`UnixServer`](crate::UnixServer) used to mean one Unix socket, because
//! that is what it accepted on. [`Listener`] is the seam that lets the same
//! accept loop, the same connection ceiling and the same idle clock serve
//! connections arriving from anywhere: the daemon's own socket, a TCP port,
//! or something the application accepted and wrapped itself.
//!
//! Adding one is [`UnixServer::listener`](crate::UnixServer::listener) or
//! [`DaemonOptions::listener`](crate::DaemonOptions::listener). A daemon that
//! adds none behaves exactly as it did.
//!
//! Unix-only, like the daemon it feeds.

use crate::transport::{TcpTransport, Transport, UnixTransport};
use crate::types::{McpError, Result};
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::time::Duration;

/// How long [`TcpSocketListener::wake`] will wait to reach itself.
///
/// Waking is a connection to our own listening address, and the daemon is
/// shutting down when it happens - so the useful failure is a quick one.
/// Loopback answers in microseconds; anything that has not answered in a
/// second is not going to.
const WAKE_TIMEOUT: Duration = Duration::from_secs(1);

/// A source of client connections for a daemon.
///
/// # Implementing one
///
/// The reason this trait exists rather than a list of socket types: a
/// connection that needs work before it can carry messages - a TLS handshake,
/// an authentication exchange, a proxy header - does that work inside
/// [`accept`](Self::accept), and hands back a transport over the finished
/// stream. That is how a daemon gets mTLS without this crate having any idea
/// what TLS is:
///
/// ```no_run
/// use sml_mcps::{Listener, StreamTransport, Transport};
/// use std::io;
/// use std::net::{SocketAddr, TcpListener, TcpStream};
///
/// # struct Acceptor;
/// # impl Acceptor { fn handshake(&self, s: TcpStream) -> io::Result<TcpStream> { Ok(s) } }
/// struct MtlsListener {
///     tcp: TcpListener,
///     addr: SocketAddr,
///     acceptor: Acceptor,
/// }
///
/// impl Listener for MtlsListener {
///     fn accept(&self) -> io::Result<Box<dyn Transport>> {
///         let (stream, _peer) = self.tcp.accept()?;
///         // A client without a valid certificate fails here, and the accept
///         // loop treats that as one connection lost - not a daemon down.
///         let secured = self.acceptor.handshake(stream)?;
///         Ok(Box::new(StreamTransport::new(secured)))
///     }
///
///     fn wake(&self) -> io::Result<()> {
///         TcpStream::connect(self.addr).map(|_| ())
///     }
///
///     fn describe(&self) -> String {
///         format!("mtls://{}", self.addr)
///     }
/// }
/// ```
///
/// # The contract
///
/// - [`accept`](Self::accept) blocks. It is called in a loop on a thread of
///   its own, and returning an error is not fatal: the daemon backs off
///   briefly and calls it again.
/// - [`wake`](Self::wake) **must** make a thread parked in `accept` return,
///   because the daemon joins its accept threads on the way out. A listener
///   that cannot be woken is a daemon that cannot exit. Connecting to yourself
///   is the usual way, and is what both built-in listeners do.
/// - Both are called from threads other than the one that built the listener,
///   hence `Send + Sync`.
pub trait Listener: Send + Sync + 'static {
    /// Block until a client connects, and hand back a transport speaking to
    /// it.
    ///
    /// Everything the connection needs before it can carry JSON-RPC belongs in
    /// here.
    fn accept(&self) -> io::Result<Box<dyn Transport>>;

    /// Make a thread parked in [`accept`](Self::accept) return promptly.
    ///
    /// Called once per listener when the daemon is shutting down, after the
    /// shutdown has been flagged - so a connection this produces is observed,
    /// refused, and the loop ends. Best-effort: the daemon ignores the result,
    /// because there is nothing useful to do about a listener that will not
    /// answer its own knock.
    fn wake(&self) -> io::Result<()>;

    /// How this listener names itself in the daemon's startup line.
    fn describe(&self) -> String;
}

/// The daemon's own Unix socket, as a [`Listener`].
///
/// Deliberately not public: binding the socket, refusing a path this user does
/// not own, clearing a stale one, writing the PID file and unlinking on exit
/// all belong to [`UnixServer`](crate::UnixServer), and none of it would be
/// improved by being duplicated out here.
pub(crate) struct UnixSocketListener {
    listener: UnixListener,
    path: PathBuf,
}

impl UnixSocketListener {
    /// Wrap a listener already bound at `path`.
    pub(crate) fn new(listener: UnixListener, path: impl Into<PathBuf>) -> Self {
        Self {
            listener,
            path: path.into(),
        }
    }
}

impl Listener for UnixSocketListener {
    fn accept(&self) -> io::Result<Box<dyn Transport>> {
        let (stream, _addr) = self.listener.accept()?;
        // Fallibly, because the descriptor the clone needs is the one the
        // process has run out of when this fails - and losing one connection
        // is the right cost for that, not losing the daemon.
        let transport = UnixTransport::try_from_stream(stream).map_err(io::Error::other)?;

        Ok(Box::new(transport))
    }

    fn wake(&self) -> io::Result<()> {
        UnixStream::connect(&self.path).map(|_| ())
    }

    fn describe(&self) -> String {
        format!("unix:{}", self.path.display())
    }
}

/// A TCP port, as a [`Listener`].
///
/// **In the clear.** Everything a client sends and everything the daemon
/// answers crosses the network unencrypted and unauthenticated - anything that
/// can route to the address gets a session, and an MCP session runs tools as
/// the daemon's user. Bind it somewhere only people you already trust can
/// reach, or implement [`Listener`] yourself and wrap the streams in TLS.
///
/// ```no_run
/// use sml_mcps::{DaemonOptions, TcpSocketListener};
/// use std::time::Duration;
///
/// # fn main() -> sml_mcps::Result<()> {
/// let options = DaemonOptions::new("/run/my-daemon/server.sock")
///     .idle_timeout(Duration::from_secs(300))
///     .listener(TcpSocketListener::bind("127.0.0.1:7743")?);
/// # let _ = options;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct TcpSocketListener {
    listener: TcpListener,
    /// The address actually bound, which is not always the one asked for -
    /// port 0 means "any", and [`local_addr`](Self::local_addr) is how the
    /// caller finds out which.
    addr: SocketAddr,
}

impl TcpSocketListener {
    /// Bind `addr` and listen on it.
    ///
    /// Binding here rather than inside the daemon is deliberate: a port
    /// already in use is reported to whoever ran the command, before
    /// `--daemon` has forked away from the terminal that could have shown it.
    pub fn bind(addr: impl ToSocketAddrs) -> Result<Self> {
        let listener = TcpListener::bind(addr)
            .map_err(|e| McpError::Internal(format!("failed to bind a TCP listener: {}", e)))?;

        Self::from_listener(listener)
    }

    /// Take over a listener that is already bound.
    ///
    /// For a daemon handed its socket by the service manager that started it -
    /// launchd, systemd - rather than binding one itself.
    pub fn from_listener(listener: TcpListener) -> Result<Self> {
        let addr = listener.local_addr().map_err(|e| {
            McpError::Internal(format!("a TCP listener with no local address: {}", e))
        })?;

        Ok(Self { listener, addr })
    }

    /// The address this listener is actually bound to.
    ///
    /// Worth asking after `bind("127.0.0.1:0")`, which is how a test gets a
    /// port nobody else has.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Listener for TcpSocketListener {
    fn accept(&self) -> io::Result<Box<dyn Transport>> {
        let (stream, _peer) = self.listener.accept()?;
        let transport = TcpTransport::try_from_stream(stream).map_err(io::Error::other)?;

        Ok(Box::new(transport))
    }

    fn wake(&self) -> io::Result<()> {
        TcpStream::connect_timeout(&connectable(self.addr), WAKE_TIMEOUT).map(|_| ())
    }

    fn describe(&self) -> String {
        format!("tcp://{}", self.addr)
    }
}

/// A bound address, as somewhere to connect *to*.
///
/// `0.0.0.0` and `::` say "every interface" to `bind` and mean nothing to
/// `connect`; the address that reaches a listener on all of them is loopback,
/// of the same family.
fn connectable(addr: SocketAddr) -> SocketAddr {
    if !addr.ip().is_unspecified() {
        return addr;
    }

    let loopback = match addr.ip() {
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
    };

    SocketAddr::new(loopback, addr.port())
}

/// Where a Unix listener for tests goes, unique per call.
#[cfg(test)]
fn temp_socket_path(tag: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};

    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::SeqCst);

    std::env::temp_dir().join(format!(
        "sml_mcps_listener_{}_{}_{}.sock",
        std::process::id(),
        n,
        tag
    ))
}

/// Whether `path` names something on disk, symlinks included.
#[cfg(test)]
fn exists(path: &std::path::Path) -> bool {
    std::fs::symlink_metadata(path).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::JsonRpcMessage;
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    // ---- TCP -------------------------------------------------------------

    #[test]
    fn test_binding_port_zero_reports_the_port_it_got() {
        let listener = TcpSocketListener::bind("127.0.0.1:0").unwrap();

        let addr = listener.local_addr();
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_ne!(addr.port(), 0, "port 0 is a request, not an answer");
    }

    #[test]
    fn test_a_port_already_taken_is_reported_not_swallowed() {
        let held = TcpSocketListener::bind("127.0.0.1:0").unwrap();

        let error = TcpSocketListener::bind(held.local_addr())
            .expect_err("two listeners cannot hold one port");

        assert!(
            error.to_string().contains("failed to bind"),
            "the caller has to be able to tell binding apart from anything \
             else that went wrong: {error}"
        );
    }

    #[test]
    fn test_accept_hands_back_a_transport_that_speaks_to_the_client() {
        let listener = TcpSocketListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr();

        let client = thread::spawn(move || {
            let mut client = TcpTransport::connect(addr).unwrap();
            client
                .write(&JsonRpcMessage::request(1i64, "ping", None))
                .unwrap();
            client.read().unwrap()
        });

        let mut accepted = listener.accept().unwrap();
        assert_eq!(
            accepted.read().unwrap(),
            JsonRpcMessage::request(1i64, "ping", None)
        );

        let answer = JsonRpcMessage::response(1i64, serde_json::json!({ "ok": true }));
        accepted.write(&answer).unwrap();
        assert_eq!(client.join().unwrap(), answer);
    }

    #[test]
    fn test_waking_a_tcp_listener_returns_a_parked_accept() {
        // The property the daemon's whole shutdown path rests on: a thread
        // blocked in `accept` has to come back when asked, or the join at the
        // end of `run` never finishes.
        let listener = Arc::new(TcpSocketListener::bind("127.0.0.1:0").unwrap());

        let parked = {
            let listener = listener.clone();
            thread::spawn(move || listener.accept().map(|_| ()))
        };

        // Give the thread time to actually be inside `accept`, so this is not
        // a race the wake wins by arriving first.
        thread::sleep(Duration::from_millis(100));
        assert!(!parked.is_finished(), "the accept should be blocked");

        listener.wake().unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        while !parked.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(parked.is_finished(), "wake must return a parked accept");
        parked.join().unwrap().unwrap();
    }

    #[test]
    fn test_a_tcp_listener_describes_its_address() {
        let listener = TcpSocketListener::bind("127.0.0.1:0").unwrap();
        let described = listener.describe();

        assert!(described.starts_with("tcp://127.0.0.1:"), "{described}");
        assert!(described.ends_with(&listener.local_addr().port().to_string()));
    }

    #[test]
    fn test_from_listener_adopts_a_socket_someone_else_bound() {
        let raw = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = raw.local_addr().unwrap();

        let adopted = TcpSocketListener::from_listener(raw).unwrap();

        assert_eq!(adopted.local_addr(), addr, "the same socket, not a new one");
    }

    #[test]
    fn test_an_unspecified_bind_address_is_woken_on_loopback() {
        // `0.0.0.0` is an instruction to `bind` and gibberish to `connect`, so
        // waking one has to be aimed at an address that actually reaches it.
        let any_v4: SocketAddr = "0.0.0.0:7743".parse().unwrap();
        assert_eq!(
            connectable(any_v4),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7743)
        );

        let any_v6: SocketAddr = "[::]:7743".parse().unwrap();
        assert_eq!(
            connectable(any_v6),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 7743)
        );

        // And an address that names an interface is left exactly alone.
        let specific: SocketAddr = "127.0.0.1:7743".parse().unwrap();
        assert_eq!(connectable(specific), specific);
        let specific_v6: SocketAddr = "[::1]:7743".parse().unwrap();
        assert_eq!(connectable(specific_v6), specific_v6);
    }

    #[test]
    fn test_waking_a_listener_bound_to_every_interface_works() {
        // The unit above says which address is chosen; this says the choice is
        // the right one, against a real listener.
        let listener = TcpSocketListener::bind("0.0.0.0:0").unwrap();
        assert!(listener.local_addr().ip().is_unspecified());

        listener.wake().expect("a wake must reach its own listener");
        listener.accept().expect("and be accepted");
    }

    // ---- Unix ------------------------------------------------------------

    #[test]
    fn test_the_unix_listener_accepts_and_describes_its_path() {
        let path = temp_socket_path("accept");
        let listener = UnixSocketListener::new(UnixListener::bind(&path).unwrap(), &path);

        assert_eq!(listener.describe(), format!("unix:{}", path.display()));

        let client_path = path.clone();
        let client = thread::spawn(move || {
            let mut client = UnixTransport::connect(&client_path).unwrap();
            client
                .write(&JsonRpcMessage::request(2i64, "ping", None))
                .unwrap();
        });

        let mut accepted = listener.accept().unwrap();
        assert_eq!(
            accepted.read().unwrap(),
            JsonRpcMessage::request(2i64, "ping", None)
        );

        client.join().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_waking_a_unix_listener_returns_a_parked_accept() {
        let path = temp_socket_path("wake");
        let listener = Arc::new(UnixSocketListener::new(
            UnixListener::bind(&path).unwrap(),
            &path,
        ));

        let parked = {
            let listener = listener.clone();
            thread::spawn(move || listener.accept().map(|_| ()))
        };

        thread::sleep(Duration::from_millis(100));
        assert!(!parked.is_finished(), "the accept should be blocked");

        listener.wake().unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        while !parked.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(parked.is_finished(), "wake must return a parked accept");
        parked.join().unwrap().unwrap();

        assert!(exists(&path), "waking must not unlink the socket");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_waking_a_unix_socket_that_has_gone_is_an_error_not_a_panic() {
        // What the daemon's teardown does if cleanup somehow got there first.
        // It ignores the result; what matters is that there is one.
        let path = temp_socket_path("gone");
        let listener = UnixSocketListener::new(UnixListener::bind(&path).unwrap(), &path);
        std::fs::remove_file(&path).unwrap();

        assert!(listener.wake().is_err());
    }

    #[test]
    fn test_a_listener_is_usable_behind_a_trait_object() {
        // The daemon holds `Arc<dyn Listener>`; a trait that could not be made
        // into one would not be the seam it is meant to be.
        let path = temp_socket_path("dyn");
        let unix: Arc<dyn Listener> = Arc::new(UnixSocketListener::new(
            UnixListener::bind(&path).unwrap(),
            &path,
        ));
        let tcp: Arc<dyn Listener> = Arc::new(TcpSocketListener::bind("127.0.0.1:0").unwrap());

        let listeners = [unix, tcp];
        let described: Vec<String> = listeners.iter().map(|l| l.describe()).collect();

        assert!(described[0].starts_with("unix:"));
        assert!(described[1].starts_with("tcp://"));

        let _ = std::fs::remove_file(&path);
    }
}
