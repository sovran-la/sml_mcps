//! One daemon, several listeners, and an application that can refuse to let it
//! go.
//!
//! Everything here goes through the public API, because that is how the
//! consumer this exists for reaches it. Two halves:
//!
//! - `UnixServer` driven in-process: connections arriving from a Unix socket, a
//!   TCP port, and a listener written in this file that hands the daemon a
//!   `StreamTransport` over a stream it accepted itself - which is what an mTLS
//!   front door looks like from the crate's side.
//! - The `multi_listener` example driven as a real detached daemon, for the
//!   parts that only exist in a process of its own: `serve_daemon_with`, the
//!   idle veto surviving the client that asked for it, and a signal beating
//!   that veto.

#![cfg(unix)]

use serde_json::Value;
use sml_mcps::{
    CallToolResult, DaemonOptions, JsonRpcMessage, Listener, McpError, Result, Server,
    ServerConfig, StreamTransport, TcpSocketListener, TcpTransport, Tool, ToolEnv, Transport,
    UnixServer, UnixTransport,
};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

// ---- scaffolding ----------------------------------------------------------

/// Unique temp socket path per call.
fn temp_socket() -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::SeqCst);

    std::env::temp_dir().join(format!("sml_mcps_multi_{}_{}.sock", std::process::id(), n))
}

/// A TCP port held open until the caller is ready to give it away.
///
/// Only the tests that spawn a *child* need this: a port has to be named on a
/// command line before the child exists to bind it, and the gap between
/// choosing one and the child binding it is a gap another test could fill.
/// Holding the listener until the moment of the spawn closes that gap for
/// everything running in this binary; an in-process listener does not need it
/// at all, and binds `:0` itself.
fn reserve_port() -> (SocketAddr, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    (addr, listener)
}

/// Per-connection context over state the whole daemon shares.
struct Ctx {
    conn_id: String,
    counter: Arc<AtomicI64>,
}

/// Bumps the shared counter - proof that two listeners are one daemon.
struct IncrementTool;
impl Tool<Ctx> for IncrementTool {
    fn name(&self) -> &str {
        "increment"
    }
    fn description(&self) -> &str {
        "Increment the shared counter"
    }
    fn schema(&self) -> Value {
        serde_json::json!({ "type": "object", "properties": {} })
    }
    fn execute(&self, _a: Value, ctx: &mut Ctx, _e: &ToolEnv) -> Result<CallToolResult> {
        let v = ctx.counter.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(CallToolResult::text(format!("{}", v)))
    }
}

/// Reports this connection's id - proof that contexts are per-connection.
struct WhoamiTool;
impl Tool<Ctx> for WhoamiTool {
    fn name(&self) -> &str {
        "whoami"
    }
    fn description(&self) -> &str {
        "Return this connection's id"
    }
    fn schema(&self) -> Value {
        serde_json::json!({ "type": "object", "properties": {} })
    }
    fn execute(&self, _a: Value, ctx: &mut Ctx, _e: &ToolEnv) -> Result<CallToolResult> {
        Ok(CallToolResult::text(ctx.conn_id.clone()))
    }
}

/// A listener that accepts and wraps the stream itself.
///
/// A plain `TcpStream` stands in for a TLS-wrapped one: the daemon is handed a
/// `Box<dyn Transport>` and has no way to tell the difference, which is exactly
/// the property the mTLS consumer is relying on.
struct WrappedListener {
    listener: TcpListener,
    addr: SocketAddr,
}

impl WrappedListener {
    /// On a loopback port the system picks, which nothing else can be holding.
    fn bind_any() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        Self { listener, addr }
    }
}

impl Listener for WrappedListener {
    fn accept(&self) -> io::Result<Box<dyn Transport>> {
        let (stream, _peer) = self.listener.accept()?;

        Ok(Box::new(StreamTransport::new(stream)))
    }

    fn wake(&self) -> io::Result<()> {
        TcpStream::connect(self.addr).map(|_| ())
    }

    fn describe(&self) -> String {
        format!("wrapped://{}", self.addr)
    }
}

fn config() -> ServerConfig {
    ServerConfig {
        name: "multi-listener-test".into(),
        version: "1.0.0".into(),
        ..Default::default()
    }
}

/// Run a daemon on `socket`, configured by `build`, on a thread of its own.
fn spawn<B>(socket: &Path, counter: Arc<AtomicI64>, build: B) -> thread::JoinHandle<Result<()>>
where
    B: FnOnce(UnixServer<Ctx>) -> UnixServer<Ctx> + Send + 'static,
{
    let socket = socket.to_path_buf();
    thread::spawn(move || {
        let server = UnixServer::new(config()).with_tools(|s: &mut Server<Ctx>| {
            s.add_tool(IncrementTool)?;
            s.add_tool(WhoamiTool)?;
            Ok(())
        });

        build(server).serve(&socket, move |conn_id| Ctx {
            conn_id: conn_id.to_string(),
            counter: counter.clone(),
        })
    })
}

/// Connect over the Unix socket, retrying until the daemon is listening.
fn unix_client(socket: &Path) -> UnixTransport {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match UnixTransport::connect(socket) {
            Ok(t) => return t,
            Err(e) if Instant::now() >= deadline => {
                panic!("could not reach {}: {}", socket.display(), e)
            }
            Err(_) => thread::sleep(Duration::from_millis(20)),
        }
    }
}

/// Connect over TCP, retrying until the daemon is listening.
fn tcp_client(addr: SocketAddr) -> TcpTransport {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match TcpTransport::connect(addr) {
            Ok(t) => return t,
            Err(e) if Instant::now() >= deadline => panic!("could not reach {}: {}", addr, e),
            Err(_) => thread::sleep(Duration::from_millis(20)),
        }
    }
}

/// Connect over TCP and wrap the stream the way the listener's side does.
fn wrapped_client(addr: SocketAddr) -> StreamTransport {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match TcpStream::connect(addr) {
            Ok(s) => return StreamTransport::new(s),
            Err(e) if Instant::now() >= deadline => panic!("could not reach {}: {}", addr, e),
            Err(_) => thread::sleep(Duration::from_millis(20)),
        }
    }
}

/// Send `initialize` over any transport and assert it was answered.
fn initialize<T: Transport>(t: &mut T) {
    let request = JsonRpcMessage::request(
        1i64,
        "initialize",
        Some(serde_json::json!({
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": { "name": "multi-listener-test", "version": "1.0" }
        })),
    );
    t.write(&request).unwrap();
    assert!(matches!(t.read().unwrap(), JsonRpcMessage::Response(_)));
}

/// Call a tool over any transport and hand back its first text content.
fn call_tool<T: Transport>(t: &mut T, id: i64, name: &str) -> String {
    t.write(&JsonRpcMessage::request(
        id,
        "tools/call",
        Some(serde_json::json!({ "name": name, "arguments": {} })),
    ))
    .unwrap();

    loop {
        match t.read().unwrap() {
            JsonRpcMessage::Response(r) => {
                let result = r
                    .result
                    .unwrap_or_else(|| panic!("`{name}`: {:?}", r.error));
                return result["content"][0]["text"]
                    .as_str()
                    .expect("text content")
                    .to_string();
            }
            JsonRpcMessage::Notification(_) => continue,
            other => panic!("expected a response to `{name}`, got {other:?}"),
        }
    }
}

/// Wait for a thread to finish, and say whether it did.
fn finished(handle: &thread::JoinHandle<Result<()>>, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if handle.is_finished() {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    false
}

/// Wait for a path to appear.
fn wait_for_file(path: &Path, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        thread::sleep(Duration::from_millis(25));
    }
    false
}

/// Wait for a path to go away.
fn wait_for_removal(path: &Path, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if !path.exists() {
            return true;
        }
        thread::sleep(Duration::from_millis(25));
    }
    false
}

// ---- several listeners, one daemon ----------------------------------------

#[test]
fn test_a_unix_socket_and_a_tcp_port_serve_the_same_daemon() {
    let socket = temp_socket();
    let counter = Arc::new(AtomicI64::new(0));
    let tcp = TcpSocketListener::bind("127.0.0.1:0").unwrap();
    let addr = tcp.local_addr();

    let daemon = spawn(&socket, counter, move |s| {
        s.idle_timeout(Duration::from_secs(30)).listener(tcp)
    });

    let mut over_unix = unix_client(&socket);
    let mut over_tcp = tcp_client(addr);
    initialize(&mut over_unix);
    initialize(&mut over_tcp);

    // Two connections, told apart...
    let unix_id = call_tool(&mut over_unix, 2, "whoami");
    let tcp_id = call_tool(&mut over_tcp, 2, "whoami");
    assert!(unix_id.starts_with("conn-"), "{unix_id}");
    assert!(tcp_id.starts_with("conn-"), "{tcp_id}");
    assert_ne!(unix_id, tcp_id);

    // ...over one piece of state, which is what makes it one daemon and not
    // two that happen to share a binary.
    assert_eq!(call_tool(&mut over_unix, 3, "increment"), "1");
    assert_eq!(call_tool(&mut over_tcp, 3, "increment"), "2");
    assert_eq!(call_tool(&mut over_unix, 4, "increment"), "3");

    drop(over_unix);
    drop(over_tcp);
    drop(daemon);
    let _ = std::fs::remove_file(&socket);
}

#[test]
fn test_a_listener_that_wraps_its_own_streams_is_served_like_any_other() {
    // The mTLS path, with the handshake replaced by nothing: what the daemon
    // gets is a `StreamTransport` over a stream this test accepted, and it
    // serves tools over it exactly as it does over its own socket.
    let socket = temp_socket();
    let counter = Arc::new(AtomicI64::new(0));
    let wrapped = WrappedListener::bind_any();
    let addr = wrapped.addr;

    let daemon = spawn(&socket, counter, move |s| {
        s.idle_timeout(Duration::from_secs(30)).listener(wrapped)
    });

    let mut over_unix = unix_client(&socket);
    initialize(&mut over_unix);
    assert_eq!(call_tool(&mut over_unix, 2, "increment"), "1");

    let mut over_wrapped = wrapped_client(addr);
    initialize(&mut over_wrapped);
    assert!(call_tool(&mut over_wrapped, 2, "whoami").starts_with("conn-"));
    assert_eq!(
        call_tool(&mut over_wrapped, 3, "increment"),
        "2",
        "a wrapped stream reaches the same daemon as the socket"
    );

    drop(over_unix);
    drop(over_wrapped);
    drop(daemon);
    let _ = std::fs::remove_file(&socket);
}

#[test]
fn test_three_listeners_at_once() {
    // Unix, plain TCP and a hand-wrapped stream, all up together - the shape
    // the consumer actually deploys.
    let socket = temp_socket();
    let counter = Arc::new(AtomicI64::new(0));
    let tcp = TcpSocketListener::bind("127.0.0.1:0").unwrap();
    let tcp_addr = tcp.local_addr();
    let wrapped = WrappedListener::bind_any();
    let wrapped_addr = wrapped.addr;

    let daemon = spawn(&socket, counter, move |s| {
        s.idle_timeout(Duration::from_secs(30))
            .listener(tcp)
            .listener(wrapped)
    });

    let mut a = unix_client(&socket);
    let mut b = tcp_client(tcp_addr);
    let mut c = wrapped_client(wrapped_addr);
    initialize(&mut a);
    initialize(&mut b);
    initialize(&mut c);

    assert_eq!(call_tool(&mut a, 2, "increment"), "1");
    assert_eq!(call_tool(&mut b, 2, "increment"), "2");
    assert_eq!(call_tool(&mut c, 2, "increment"), "3");

    let ids = [
        call_tool(&mut a, 3, "whoami"),
        call_tool(&mut b, 3, "whoami"),
        call_tool(&mut c, 3, "whoami"),
    ];
    let mut unique = ids.to_vec();
    unique.sort();
    unique.dedup();
    assert_eq!(
        unique.len(),
        3,
        "three connections, three contexts: {ids:?}"
    );

    drop((a, b, c));
    drop(daemon);
    let _ = std::fs::remove_file(&socket);
}

#[test]
fn test_the_connection_ceiling_is_the_daemon_s_and_not_each_listener_s() {
    // Threads and descriptors belong to the process, so a ceiling a peer could
    // multiply by opening a second kind of connection would not be one.
    let socket = temp_socket();
    let counter = Arc::new(AtomicI64::new(0));
    let tcp = TcpSocketListener::bind("127.0.0.1:0").unwrap();
    let addr = tcp.local_addr();

    let daemon = spawn(&socket, counter, move |s| {
        s.idle_timeout(Duration::from_secs(30))
            .max_connections(1)
            .listener(tcp)
    });

    let mut held = unix_client(&socket);
    initialize(&mut held);

    let mut refused = tcp_client(addr);
    let _ = refused.write(&JsonRpcMessage::request(1i64, "tools/list", None));
    assert!(
        matches!(refused.read(), Err(McpError::TransportClosed)),
        "the only slot is taken, whichever door the next client comes through"
    );

    // And the connection holding the slot is untouched by the refusal.
    assert_eq!(call_tool(&mut held, 2, "increment"), "1");

    drop(held);
    drop(daemon);
    let _ = std::fs::remove_file(&socket);
}

#[test]
fn test_a_client_on_a_second_listener_cancels_the_idle_countdown() {
    // The idle clock counts clients, not Unix clients.
    let socket = temp_socket();
    let counter = Arc::new(AtomicI64::new(0));
    let tcp = TcpSocketListener::bind("127.0.0.1:0").unwrap();
    let addr = tcp.local_addr();

    let daemon = spawn(&socket, counter, move |s| {
        s.idle_timeout(Duration::from_millis(250)).listener(tcp)
    });

    let mut over_tcp = tcp_client(addr);
    initialize(&mut over_tcp);

    // Well past the idle window, with the only client on TCP.
    thread::sleep(Duration::from_millis(700));
    assert_eq!(
        call_tool(&mut over_tcp, 2, "increment"),
        "1",
        "a TCP client is a client"
    );
    assert!(socket.exists(), "the daemon should still be up");

    drop(over_tcp);
    drop(daemon);
    let _ = std::fs::remove_file(&socket);
}

#[test]
fn test_an_idle_exit_stops_every_listener_and_returns() {
    // `serve` joins its accept threads, so a listener that could not be woken
    // would hang here forever. This is the test that would catch it.
    let socket = temp_socket();
    let counter = Arc::new(AtomicI64::new(0));
    let tcp = TcpSocketListener::bind("127.0.0.1:0").unwrap();
    let tcp_addr = tcp.local_addr();
    let wrapped = WrappedListener::bind_any();
    let wrapped_addr = wrapped.addr;

    let daemon = spawn(&socket, counter, move |s| {
        s.idle_timeout(Duration::from_millis(200))
            .listener(tcp)
            .listener(wrapped)
    });

    // One client, so the daemon is definitely up before it is left alone.
    drop(unix_client(&socket));

    assert!(
        finished(&daemon, Duration::from_secs(10)),
        "an idle daemon with extra listeners must still return from `serve`"
    );
    assert!(daemon.join().unwrap().is_ok());
    assert!(!socket.exists(), "and clean up after itself");

    // The ports are free again, which is what "stopped" has to mean.
    assert!(TcpListener::bind(tcp_addr).is_ok());
    assert!(TcpListener::bind(wrapped_addr).is_ok());
}

// ---- the shutdown guard ---------------------------------------------------

#[test]
fn test_a_busy_application_keeps_an_idle_daemon_alive() {
    let socket = temp_socket();
    let counter = Arc::new(AtomicI64::new(0));
    let busy = Arc::new(AtomicBool::new(true));

    let daemon = spawn(&socket, counter, {
        let busy = busy.clone();
        move |s| {
            s.idle_timeout(Duration::from_millis(150))
                .busy_check(move || busy.load(Ordering::SeqCst))
        }
    });

    // A client comes and goes, which starts the countdown.
    drop(unix_client(&socket));

    // Several windows later the daemon is still there and still serving.
    thread::sleep(Duration::from_millis(700));
    assert!(!daemon.is_finished(), "the veto should have held");
    let mut client = unix_client(&socket);
    initialize(&mut client);
    assert_eq!(call_tool(&mut client, 2, "increment"), "1");
    drop(client);

    // The work finishes, and the next window ends the daemon.
    busy.store(false, Ordering::SeqCst);
    assert!(
        finished(&daemon, Duration::from_secs(10)),
        "a daemon that is no longer busy has to be allowed to leave"
    );
    assert!(daemon.join().unwrap().is_ok());
    assert!(!socket.exists());
}

#[test]
fn test_without_a_busy_check_an_idle_daemon_leaves_as_it_always_did() {
    // The default path, unchanged: no predicate, no veto, one window.
    let socket = temp_socket();
    let counter = Arc::new(AtomicI64::new(0));

    let daemon = spawn(&socket, counter, |s| {
        s.idle_timeout(Duration::from_millis(150))
    });

    drop(unix_client(&socket));

    assert!(finished(&daemon, Duration::from_secs(10)));
    assert!(daemon.join().unwrap().is_ok());
    assert!(!socket.exists());
}

// ---- a real daemon, through serve_daemon_with -----------------------------

#[test]
fn test_daemon_options_carry_what_they_were_given() {
    // The options object is the public way into all of this, so what it holds
    // has to be visible from outside the crate - including, for anyone
    // debugging a daemon that came up wrong, which listeners it was handed.
    let tcp = TcpSocketListener::bind("127.0.0.1:0").unwrap();
    let port = tcp.local_addr().port();

    let options = DaemonOptions::new("/tmp/sml_mcps_options_test.sock")
        .idle_timeout(Duration::from_secs(300))
        .max_connections(8)
        .listener(tcp)
        .busy_check(|| true);

    let shown = format!("{options:?}");
    assert!(shown.contains("/tmp/sml_mcps_options_test.sock"), "{shown}");
    assert!(shown.contains("300s"), "{shown}");
    assert!(shown.contains('8'), "{shown}");
    assert!(
        shown.contains(&format!("tcp://127.0.0.1:{port}")),
        "{shown}"
    );
    assert!(shown.contains("busy_check: true"), "{shown}");

    // And the defaults are the daemon this crate has always had.
    let bare = format!("{:?}", DaemonOptions::new("/tmp/x.sock"));
    assert!(bare.contains("idle_timeout: None"), "{bare}");
    assert!(bare.contains("max_connections: None"), "{bare}");
    assert!(bare.contains("listeners: []"), "{bare}");
    assert!(bare.contains("busy_check: false"), "{bare}");
}

/// The `multi_listener` example, built once for the whole test binary.
static EXAMPLE_BINARY: std::sync::LazyLock<PathBuf> = std::sync::LazyLock::new(|| {
    let status = Command::new(env!("CARGO"))
        .args(["build", "--example", "multi_listener"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("failed to run cargo build");
    assert!(
        status.success(),
        "cargo build --example multi_listener failed"
    );

    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("target");
    path.push("debug");
    path.push("examples");
    path.push("multi_listener");
    assert!(path.exists(), "example binary not found at {path:?}");
    path
});

/// PID file path for a socket (the crate's own convention).
fn pid_path(socket: &Path) -> PathBuf {
    let mut p = socket.to_path_buf();
    p.set_extension("pid");
    p
}

fn read_pid(path: &Path) -> Option<i32> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

fn alive(pid: i32) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}

/// Kill the daemon and remove what it left behind, whatever the test did.
struct DaemonGuard {
    socket: PathBuf,
    pid_file: PathBuf,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        if let Some(pid) = read_pid(&self.pid_file) {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        let _ = std::fs::remove_file(&self.socket);
        let _ = std::fs::remove_file(&self.pid_file);
    }
}

/// Start the example as a detached daemon and wait for its socket.
fn start_example(socket: &Path, args: &[String], reserved: Vec<TcpListener>) -> DaemonGuard {
    let guard = DaemonGuard {
        socket: socket.to_path_buf(),
        pid_file: pid_path(socket),
    };

    // Held only so that nothing else in this binary could take them; the child
    // needs them free.
    drop(reserved);

    let status = Command::new(&*EXAMPLE_BINARY)
        .args(["--daemon", "--socket", socket.to_str().unwrap()])
        .args(args)
        .status()
        .expect("failed to spawn the example daemon");
    assert!(status.success(), "the daemon launcher should exit 0");

    assert!(
        wait_for_file(socket, Duration::from_secs(10)),
        "the daemon never bound {}",
        socket.display()
    );
    assert!(wait_for_file(&guard.pid_file, Duration::from_secs(10)));

    guard
}

#[test]
fn test_serve_daemon_with_detaches_and_serves_every_listener() {
    let socket = temp_socket();
    let (tcp_addr, tcp_held) = reserve_port();
    let (wrapped_addr, wrapped_held) = reserve_port();

    let _guard = start_example(
        &socket,
        &[
            "--tcp".into(),
            tcp_addr.to_string(),
            "--wrapped".into(),
            wrapped_addr.to_string(),
            "--idle-ms".into(),
            "30000".into(),
        ],
        vec![tcp_held, wrapped_held],
    );

    let mut over_unix = unix_client(&socket);
    let mut over_tcp = tcp_client(tcp_addr);
    let mut over_wrapped = wrapped_client(wrapped_addr);
    initialize(&mut over_unix);
    initialize(&mut over_tcp);
    initialize(&mut over_wrapped);

    // One counter behind all three doors.
    assert_eq!(call_tool(&mut over_unix, 2, "increment"), "counter = 1");
    assert_eq!(call_tool(&mut over_tcp, 2, "increment"), "counter = 2");
    assert_eq!(call_tool(&mut over_wrapped, 2, "increment"), "counter = 3");
}

#[test]
fn test_work_that_outlives_its_client_keeps_the_daemon_up() {
    // The motivating case, end to end in a process of its own: a client asks
    // for something, hangs up, and the daemon must not idle out from under the
    // work it was left holding.
    let socket = temp_socket();
    let _guard = start_example(&socket, &["--idle-ms".into(), "200".into()], Vec::new());

    {
        let mut client = unix_client(&socket);
        initialize(&mut client);
        assert_eq!(call_tool(&mut client, 2, "busy"), "1 job(s) running");
        // The client leaves. The job does not.
    }

    thread::sleep(Duration::from_millis(900));
    assert!(
        socket.exists(),
        "the daemon idled out while it still had work"
    );

    // Finish the job, and the next window takes the daemon with it.
    {
        let mut client = unix_client(&socket);
        initialize(&mut client);
        assert_eq!(call_tool(&mut client, 2, "idle"), "0 job(s) running");
    }

    assert!(
        wait_for_removal(&socket, Duration::from_secs(10)),
        "a daemon with nothing left to do should have exited"
    );
}

#[test]
fn test_a_signal_is_not_vetoable() {
    // The guard is on the daemon's own decision to leave, not on the
    // operator's. `busy` is asserted for the whole test and SIGTERM still
    // wins.
    let socket = temp_socket();
    let guard = start_example(&socket, &["--idle-ms".into(), "200".into()], Vec::new());

    {
        let mut client = unix_client(&socket);
        initialize(&mut client);
        assert_eq!(call_tool(&mut client, 2, "busy"), "1 job(s) running");
    }

    let pid = read_pid(&guard.pid_file).expect("the daemon should have published a PID");
    assert!(alive(pid));

    assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);

    assert!(
        wait_for_removal(&socket, Duration::from_secs(10)),
        "SIGTERM must not be vetoable"
    );
    assert!(wait_for_removal(&guard.pid_file, Duration::from_secs(10)));

    let deadline = Instant::now() + Duration::from_secs(10);
    while alive(pid) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(25));
    }
    assert!(!alive(pid), "the daemon should have exited");
}
