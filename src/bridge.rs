//! Bridge - transparent MCP proxy (the shim)
//!
//! A [`Bridge`] pumps JSON-RPC messages between two transports without
//! inspecting them. The shim binary wires a [`StdioTransport`](crate::StdioTransport)
//! (talking to the MCP client) to a [`UnixTransport`](crate::UnixTransport)
//! (talking to the daemon):
//!
//! ```ignore
//! fn main() -> sml_mcps::Result<()> {
//!     let upstream = Bridge::auto_start(
//!         "/tmp/myapp/server.sock",
//!         "myapp-server",
//!         &["--daemon"],
//!     )?;
//!     Bridge::run(StdioTransport::new(), upstream)
//! }
//! ```
//!
//! [`Bridge::auto_start`] connects to an existing daemon, or spawns one and
//! waits for it to come up - including stale socket / dead-PID recovery.
//!
//! [`Server::serve_daemon`](crate::Server::serve_daemon) wires this up for you,
//! including deciding from `argv` whether this process is the shim or the
//! daemon it proxies to.
//!
//! Unix-only: gated behind `#[cfg(unix)]`.

use crate::server::{error_id, is_malformed};
use crate::transport::{Transport, UnixTransport, pid_path_for};
use crate::types::{JsonRpcMessage, McpError, Result};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// How long to wait for a possibly-wedged daemon (live PID, socket present but
/// not yet accepting) before treating it as dead and restarting.
const STARTUP_GRACE: Duration = Duration::from_secs(2);

/// How long to wait for a freshly-spawned daemon to start listening.
const READY_TIMEOUT: Duration = Duration::from_secs(10);

/// How many times to ask a socket whether anyone is home before unlinking it.
///
/// One refusal is not proof: a daemon whose listen backlog is momentarily full
/// also refuses. Deleting its socket on that basis would cut off a healthy
/// server, so the answer has to be consistent.
const PROBE_ATTEMPTS: u32 = 3;

/// Gap between probes, long enough for a backlog to drain.
const PROBE_GAP: Duration = Duration::from_millis(50);

/// Transparent bidirectional MCP proxy.
pub struct Bridge;

impl Bridge {
    /// Connect to a daemon, auto-starting it if needed.
    ///
    /// 1. Try connecting to `socket_path` - if a daemon is up, done.
    /// 2. Otherwise reconcile socket + PID-file state:
    ///    - live PID, socket present -> daemon may be starting; wait briefly.
    ///    - dead PID -> stale; remove socket + PID file and restart.
    ///    - socket but no PID file -> probe it; remove and restart only if
    ///      nothing is listening.
    /// 3. Spawn `daemon_bin` with `daemon_args` (expected to daemonize).
    /// 4. Poll the socket until the daemon is listening, then connect.
    ///
    /// A socket is never unlinked without first confirming nothing answers on
    /// it, so a daemon that comes up in the middle of this keeps its socket -
    /// and gets connected to - rather than being orphaned from its own path.
    ///
    /// `daemon_bin` must double-fork (e.g. via
    /// [`UnixServer::serve_daemon`](crate::UnixServer::serve_daemon)) so the
    /// spawned process exits promptly once the detached daemon is running.
    pub fn auto_start(
        socket_path: impl AsRef<Path>,
        daemon_bin: &str,
        daemon_args: &[&str],
    ) -> Result<UnixTransport> {
        auto_start_inner(
            socket_path.as_ref(),
            daemon_bin,
            daemon_args,
            STARTUP_GRACE,
            READY_TIMEOUT,
        )
    }

    /// Pump messages between `client` and `upstream` until the upstream side
    /// closes.
    ///
    /// Full duplex: spawns one thread per direction. Each thread owns an
    /// independent read source and write sink (via
    /// [`Transport::try_clone_writer`]), so neither blocks the other.
    ///
    /// Teardown drains cleanly: when the client hangs up, the request pump
    /// half-closes the upstream ([`Transport::close_write`]) so the daemon sees
    /// EOF and flushes its remaining responses; the response pump relays those,
    /// then ends when the daemon closes. `run` returns when that response pump
    /// finishes - i.e. once the upstream connection is fully drained and closed.
    ///
    /// Both transports must support [`Transport::try_clone_writer`] (stdio and
    /// unix do); a transport that doesn't yields an error.
    pub fn run<A: Transport + 'static, B: Transport + 'static>(
        client: A,
        upstream: B,
    ) -> Result<()> {
        let mut client = client;
        let mut upstream = upstream;

        // Shared, because each direction answers unreadable input on the sink
        // the *other* direction owns.
        let client_sink = SharedWriter::new(client.try_clone_writer().ok_or_else(|| {
            McpError::Internal("client transport cannot be split for proxying".into())
        })?);
        let upstream_sink = SharedWriter::new(upstream.try_clone_writer().ok_or_else(|| {
            McpError::Internal("upstream transport cannot be split for proxying".into())
        })?);

        // client -> upstream (requests). Detached: ends when the client hits
        // EOF, at which point it half-closes the upstream so the daemon can
        // finish up. If the daemon vanished while the client is still talking,
        // this thread outlives `run` and dies when the process exits.
        let mut to_upstream = upstream_sink.clone();
        let mut to_client = client_sink.clone();
        let _c2u = thread::spawn(move || {
            let _ = pump(&mut client, &mut to_upstream, &mut to_client);
            let _ = to_upstream.close_write();
        });

        // upstream -> client (responses). Joined: its completion means the
        // upstream connection has drained and closed - the definitive
        // end-of-session signal.
        let mut to_client = client_sink;
        let mut to_upstream = upstream_sink;
        let result = pump(&mut upstream, &mut to_client, &mut to_upstream);
        let _ = to_client.close_write();
        result
    }

    /// Bridge stdio (stdin/stdout) to a Unix transport.
    ///
    /// This is the complete shim binary in one call: connects CC's stdio
    /// transport to an upstream daemon.
    ///
    /// # Example
    /// ```ignore
    /// let upstream = Bridge::auto_start("server.sock", "my-daemon", &["--daemon"])?;
    /// Bridge::run_stdio(upstream)?;
    /// ```
    pub fn run_stdio(upstream: UnixTransport) -> Result<()> {
        Self::run(crate::StdioTransport::new(), upstream)
    }
}

/// A write sink both pump directions can reach.
///
/// One lock per sink keeps two threads from interleaving halves of two
/// messages into one unparseable line - the same invariant
/// `Server::write_message` maintains now that task workers write too.
#[derive(Clone)]
struct SharedWriter(Arc<Mutex<Box<dyn Transport>>>);

impl SharedWriter {
    fn new(writer: Box<dyn Transport>) -> Self {
        Self(Arc::new(Mutex::new(writer)))
    }

    fn with<T>(&self, f: impl FnOnce(&mut dyn Transport) -> Result<T>) -> Result<T> {
        let mut guard = self
            .0
            .lock()
            .map_err(|_| McpError::Internal("Bridge writer lock poisoned".into()))?;
        f(guard.as_mut())
    }
}

impl Transport for SharedWriter {
    /// Write-only by construction: the reads belong to the two owned handles.
    fn read(&mut self) -> Result<JsonRpcMessage> {
        Err(McpError::Internal(
            "a bridge write handle cannot be read from".into(),
        ))
    }

    fn write(&mut self, message: &JsonRpcMessage) -> Result<()> {
        self.with(|writer| writer.write(message))
    }

    fn close(&mut self) -> Result<()> {
        self.with(|writer| writer.close())
    }

    fn close_write(&mut self) -> Result<()> {
        self.with(|writer| writer.close_write())
    }
}

/// Forward every message from `reader` to `writer` until end-of-stream.
///
/// `back` is the way home to whoever sent the unreadable thing: a message the
/// shim cannot parse is answered with a JSON-RPC error on the reader's own side
/// and the pump continues, exactly as the server does. Tearing down instead
/// meant one malformed line from the client half-closed the upstream, the
/// daemon saw EOF, the response pump ended, and `Bridge::run` returned
/// `Ok(())` - the session gone, with no error written and no diagnostic. The
/// 8 MiB frame cap gave that the same trigger for a *legitimate* message the
/// shim used to forward verbatim.
///
/// Teardown is reserved for the connection actually failing.
fn pump(
    reader: &mut dyn Transport,
    writer: &mut dyn Transport,
    back: &mut dyn Transport,
) -> Result<()> {
    loop {
        match reader.read() {
            Ok(msg) => writer.write(&msg)?,
            Err(McpError::TransportClosed) => return Ok(()),
            Err(e) if is_malformed(&e) => {
                let answer = JsonRpcMessage::error(error_id(&e), e.to_jsonrpc_error());
                // A shim that cannot even report the problem has lost the
                // connection it would report it on; let the next read say so.
                if back.write(&answer).is_err() {
                    return Ok(());
                }
            }
            Err(e) => return Err(e),
        }
    }
}

/// Testable core of [`Bridge::auto_start`] with injectable timeouts.
fn auto_start_inner(
    socket_path: &Path,
    daemon_bin: &str,
    daemon_args: &[&str],
    startup_grace: Duration,
    ready_timeout: Duration,
) -> Result<UnixTransport> {
    // Fast path: a daemon is already accepting connections.
    if let Ok(t) = UnixTransport::connect(socket_path) {
        return Ok(t);
    }

    let pid_path = pid_path_for(socket_path);

    if socket_path.exists() {
        // Socket file present but the connect above failed. Whichever way that
        // is explained, the socket is only ever removed by `clear_socket`,
        // which refuses to unlink one that turns out to be live.
        match read_pid_file(&pid_path) {
            Some(pid) if process_alive(pid) => {
                // Daemon may be mid-startup or wedged. Give it a moment.
                if let Some(t) = wait_for_socket(socket_path, startup_grace) {
                    return Ok(t);
                }
                // Still not accepting -> treat as dead. Clear and restart.
                if let Some(t) = clear_socket(socket_path, Some(&pid_path)) {
                    return Ok(t);
                }
            }
            Some(_) => {
                // PID file points at a dead process -> stale.
                if let Some(t) = clear_socket(socket_path, Some(&pid_path)) {
                    return Ok(t);
                }
            }
            None => {
                // Socket with no PID file. That is what an orphan looks like -
                // and also what a `UnixServer::serve` daemon looks like, since
                // only `serve_daemon` writes a PID file. Probing tells them
                // apart; assuming orphan here used to unlink a live server's
                // socket, leaving it listening on a path nobody could reach.
                if let Some(t) = clear_socket(socket_path, None) {
                    return Ok(t);
                }
            }
        }
    } else if let Some(pid) = read_pid_file(&pid_path) {
        // No socket yet, but a PID file exists.
        if process_alive(pid) {
            // Daemon may still be binding; wait before deciding to respawn.
            if let Some(t) = wait_for_socket(socket_path, startup_grace) {
                return Ok(t);
            }
        }
        // Dead, or alive-but-never-bound: clear the stale PID file.
        remove_quietly(&pid_path);
    }

    // Spawn the daemon and wait for it to come up.
    spawn_daemon(daemon_bin, daemon_args)?;

    wait_for_socket(socket_path, ready_timeout).ok_or_else(|| {
        McpError::Internal(format!(
            "daemon `{}` did not start listening on {} within {:?}",
            daemon_bin,
            socket_path.display(),
            ready_timeout
        ))
    })
}

/// Read a PID from a PID file (trimmed integer), or `None` if absent/garbage.
fn read_pid_file(path: &Path) -> Option<i32> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// True if a process with `pid` exists (via `kill(pid, 0)`).
///
/// `kill` returns 0 if a signal could be delivered; `EPERM` means the process
/// exists but we lack permission to signal it (still alive); `ESRCH` means no
/// such process.
fn process_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    // SAFETY: kill with signal 0 performs only an existence/permission check.
    unsafe {
        if libc::kill(pid, 0) == 0 {
            return true;
        }
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

/// What a socket path turned out to be.
enum Probe {
    /// Something is listening, and this is the connection to it.
    Live(UnixTransport),
    /// The path is not a reachable socket: nothing bound, not a socket, or
    /// gone. Safe to unlink.
    Dead,
    /// Neither could be established - a permission problem, or a listener under
    /// enough pressure to refuse. Not safe to unlink.
    Unknown,
}

/// Ask a socket path what it is, insisting on a consistent answer.
///
/// A single `ECONNREFUSED` is not enough to condemn a socket: on the BSDs a
/// listener with a full backlog refuses exactly the same way a socket with no
/// listener does. Repeating the probe separates the two, because a backlog
/// drains and an abandoned socket does not.
fn probe_socket(path: &Path, attempts: u32, gap: Duration) -> Probe {
    let mut verdict = Probe::Unknown;

    for attempt in 0..attempts {
        if attempt > 0 {
            thread::sleep(gap);
        }
        match UnixStream::connect(path) {
            Ok(stream) => return Probe::Live(UnixTransport::from_stream(stream)),
            Err(e) if is_unreachable(&e) => verdict = Probe::Dead,
            // Something else is wrong. Say so and stop guessing - one
            // inconclusive answer outranks any number of confident ones.
            Err(_) => return Probe::Unknown,
        }
    }

    verdict
}

/// Does this connect error mean "no daemon owns this path"?
///
/// `ECONNREFUSED` is a socket nobody is bound to, `ENOENT` is a path that is
/// already gone, and `ENOTSOCK` is a leftover regular file sitting where the
/// socket belongs. Anything else - `EACCES`, `EAGAIN` from a saturated backlog
/// - proves nothing.
fn is_unreachable(e: &std::io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::ECONNREFUSED) | Some(libc::ENOENT) | Some(libc::ENOTSOCK)
    )
}

/// Remove a socket that has proven itself dead, and its PID file with it.
///
/// Returns a live connection instead if the socket answers while being
/// examined, which is the whole point: a daemon that finished binding a moment
/// after the first connect attempt must not have its socket deleted out from
/// under it. That leaves it listening on an inode with no name, unreachable to
/// every client, until it idles out.
fn clear_socket(socket_path: &Path, pid_path: Option<&Path>) -> Option<UnixTransport> {
    match probe_socket(socket_path, PROBE_ATTEMPTS, PROBE_GAP) {
        Probe::Live(transport) => return Some(transport),
        Probe::Dead => {
            remove_quietly(socket_path);
            if let Some(pid) = pid_path {
                remove_quietly(pid);
            }
        }
        // Leave a socket we cannot account for alone. Spawning over it is the
        // daemon's call to make, not ours.
        Probe::Unknown => {}
    }
    None
}

/// Poll-connect to the socket until success or the timeout elapses.
fn wait_for_socket(path: &Path, timeout: Duration) -> Option<UnixTransport> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(t) = UnixTransport::connect(path) {
            return Some(t);
        }
        if Instant::now() >= deadline {
            return None;
        }
        thread::sleep(Duration::from_millis(25));
    }
}

/// Spawn the daemon process with stdio detached, then reap it.
///
/// The launched process is expected to double-fork and `_exit` once the
/// detached daemon is running, so `wait()` returns promptly. Stdio is routed
/// to `/dev/null` so the daemon's startup output can't corrupt the shim's own
/// stdout (which carries MCP traffic).
fn spawn_daemon(bin: &str, args: &[&str]) -> Result<()> {
    use std::process::{Command, Stdio};

    let mut child = Command::new(bin)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| McpError::Internal(format!("failed to spawn daemon `{}`: {}", bin, e)))?;

    let _ = child.wait();
    Ok(())
}

/// Remove a file, ignoring errors (it may already be gone).
fn remove_quietly(path: &Path) {
    let _ = std::fs::remove_file(path);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::{Server, ServerConfig, Tool, ToolEnv};
    use crate::transport::{UnixServer, UnixTransport};
    use crate::types::{CallToolResult, JsonRpcMessage, RequestId, Result as McpResult};
    use serde_json::Value;
    use std::os::unix::net::UnixStream;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_path(suffix: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::SeqCst);
        let pid = std::process::id();
        std::env::temp_dir().join(format!("sml_mcps_bridge_{}_{}_{}", pid, n, suffix))
    }

    /// Create a socket file that is definitely bound to nobody.
    ///
    /// Binding and dropping a listener leaves the file behind, which is what a
    /// crashed daemon leaves too. Teardown is not instantaneous though: under
    /// load, a connect issued immediately afterwards occasionally still
    /// succeeds. Since "nothing is listening" is this test's *premise* and not
    /// its subject, wait for it to actually hold.
    fn dead_socket(suffix: &str) -> PathBuf {
        let path = temp_path(suffix);
        {
            let _listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        }

        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if UnixStream::connect(&path).is_err() {
                return path;
            }
            thread::sleep(Duration::from_millis(5));
        }
        panic!("socket at {} never stopped accepting", path.display());
    }

    // ---- process_alive / pid file ---------------------------------------

    #[test]
    fn test_process_alive_self() {
        let me = unsafe { libc::getpid() };
        assert!(process_alive(me));
    }

    #[test]
    fn test_process_alive_dead() {
        // PID 0 and absurdly-high PIDs should not be alive.
        assert!(!process_alive(0));
        assert!(!process_alive(-1));
        // 0x7fff_fffe is extremely unlikely to be a live PID.
        assert!(!process_alive(2_147_483_646));
    }

    #[test]
    fn test_read_pid_file_roundtrip() {
        let path = temp_path("pid");
        std::fs::write(&path, "12345\n").unwrap();
        assert_eq!(read_pid_file(&path), Some(12345));
        let _ = std::fs::remove_file(&path);

        // Missing file -> None.
        assert_eq!(read_pid_file(Path::new("/no/such/pid/file")), None);

        // Garbage -> None.
        let path2 = temp_path("pid2");
        std::fs::write(&path2, "not-a-number").unwrap();
        assert_eq!(read_pid_file(&path2), None);
        let _ = std::fs::remove_file(&path2);
    }

    // ---- Bridge::run -----------------------------------------------------

    #[test]
    fn test_run_proxies_both_directions() {
        // Two socket pairs: one stands in for the client connection, the other
        // for the upstream/daemon connection. The bridge sits in the middle.
        let (client_bridge, client_peer) = UnixStream::pair().unwrap();
        let (upstream_bridge, upstream_peer) = UnixStream::pair().unwrap();

        let bridge = thread::spawn(move || {
            Bridge::run(
                UnixTransport::from_stream(client_bridge),
                UnixTransport::from_stream(upstream_bridge),
            )
        });

        // Act as the client and the daemon via the peer ends.
        let mut client = UnixTransport::from_stream(client_peer);
        let mut daemon = UnixTransport::from_stream(upstream_peer);

        // client -> upstream
        let req = JsonRpcMessage::request(1i64, "tools/call", None);
        client.write(&req).unwrap();
        assert_eq!(daemon.read().unwrap(), req);

        // upstream -> client (response)
        let resp = JsonRpcMessage::response(1i64, serde_json::json!({ "ok": true }));
        daemon.write(&resp).unwrap();
        assert_eq!(client.read().unwrap(), resp);

        // A notification followed by another request, to confirm full duplex
        // isn't gated on alternation.
        let note = JsonRpcMessage::notification("notifications/message", None);
        daemon.write(&note).unwrap();
        assert_eq!(client.read().unwrap(), note);

        let req2 = JsonRpcMessage::request(2i64, "ping", None);
        client.write(&req2).unwrap();
        assert_eq!(daemon.read().unwrap(), req2);

        // Tear down both ends: the client hangs up (half-closing the upstream)
        // and the daemon closes, ending the response pump so the bridge returns.
        drop(client);
        drop(daemon);
        let joined = bridge.join().unwrap();
        assert!(joined.is_ok());
    }

    #[test]
    fn test_a_malformed_message_is_answered_and_the_shim_carries_on() {
        // C1 taught the server to answer unreadable input and keep going; the
        // shim in front of it was not taught the same thing. One malformed line
        // ended the request pump, which half-closed the upstream, which made
        // the daemon EOF, which ended the response pump - so `run` returned
        // `Ok(())` with the session gone, nothing written, and no diagnostic.
        let (client_bridge, client_peer) = UnixStream::pair().unwrap();
        let (upstream_bridge, upstream_peer) = UnixStream::pair().unwrap();

        let daemon = thread::spawn(move || {
            let mut d = UnixTransport::from_stream(upstream_peer);
            while let Ok(JsonRpcMessage::Request(req)) = d.read() {
                d.write(&JsonRpcMessage::response(req.id, serde_json::json!({})))
                    .unwrap();
            }
        });

        let bridge = thread::spawn(move || {
            Bridge::run(
                UnixTransport::from_stream(client_bridge),
                UnixTransport::from_stream(upstream_bridge),
            )
        });

        let mut raw = client_peer.try_clone().unwrap();
        let mut client = UnixTransport::from_stream(client_peer);

        client
            .write(&JsonRpcMessage::request(1i64, "ping", None))
            .unwrap();
        assert!(matches!(
            client.read().unwrap(),
            JsonRpcMessage::Response(_)
        ));

        // Garbage in. The answer comes back from the shim itself.
        {
            use std::io::Write;
            raw.write_all(b"{not json\n").unwrap();
            raw.flush().unwrap();
        }
        let JsonRpcMessage::Response(answered) = client.read().unwrap() else {
            panic!("the shim must answer, not hang up");
        };
        assert_eq!(answered.error.unwrap().code, -32700);

        // And the session is still usable, which is the whole point.
        client
            .write(&JsonRpcMessage::request(2i64, "ping", None))
            .unwrap();
        let JsonRpcMessage::Response(second) = client.read().unwrap() else {
            panic!("expected a response");
        };
        assert_eq!(second.id, RequestId::Number(2));

        drop(client);
        drop(raw);
        let _ = bridge.join().unwrap();
        daemon.join().unwrap();
    }

    #[test]
    fn test_an_oversized_message_does_not_kill_the_shim() {
        // S9's 8 MiB cap silently changed the bridge's contract: a message the
        // shim used to forward verbatim now surfaces as `InvalidMessage`, which
        // was fatal. It must be answerable like any other unreadable input.
        let (client_bridge, client_peer) = UnixStream::pair().unwrap();
        let (upstream_bridge, upstream_peer) = UnixStream::pair().unwrap();

        let daemon = thread::spawn(move || {
            let mut d = UnixTransport::from_stream(upstream_peer);
            while let Ok(JsonRpcMessage::Request(req)) = d.read() {
                d.write(&JsonRpcMessage::response(req.id, serde_json::json!({})))
                    .unwrap();
            }
        });

        let mut client_transport = UnixTransport::from_stream(client_bridge);
        // A small ceiling, so the test does not have to push 8 MiB to make the
        // point.
        client_transport.set_max_message_bytes(256);

        let bridge = thread::spawn(move || {
            Bridge::run(
                client_transport,
                UnixTransport::from_stream(upstream_bridge),
            )
        });

        let mut raw = client_peer.try_clone().unwrap();
        let mut client = UnixTransport::from_stream(client_peer);

        {
            use std::io::Write;
            let huge = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": { "big": "x".repeat(1024) },
            });
            raw.write_all(huge.to_string().as_bytes()).unwrap();
            raw.write_all(b"\n").unwrap();
            raw.flush().unwrap();
        }

        let JsonRpcMessage::Response(answered) = client.read().unwrap() else {
            panic!("the shim must answer, not hang up");
        };
        let error = answered.error.unwrap();
        assert_eq!(error.code, -32600);
        assert!(error.message.contains("256 byte limit"), "{error:?}");

        // Still alive.
        client
            .write(&JsonRpcMessage::request(2i64, "ping", None))
            .unwrap();
        assert!(matches!(
            client.read().unwrap(),
            JsonRpcMessage::Response(_)
        ));

        drop(client);
        drop(raw);
        let _ = bridge.join().unwrap();
        daemon.join().unwrap();
    }

    #[test]
    fn test_run_drains_responses_after_client_eof() {
        // Models the shim teardown: the client sends requests then hits EOF; the
        // daemon's already-queued responses must still reach the client before
        // the bridge exits. A full close (instead of half-close) would drop them.
        let (client_bridge, client_peer) = UnixStream::pair().unwrap();
        let (upstream_bridge, upstream_peer) = UnixStream::pair().unwrap();

        // Fake daemon: reply to each request; on read EOF, close (drop).
        let daemon = thread::spawn(move || {
            let mut d = UnixTransport::from_stream(upstream_peer);
            while let Ok(JsonRpcMessage::Request(req)) = d.read() {
                let resp =
                    JsonRpcMessage::response(req.id, serde_json::json!({ "echo": req.method }));
                d.write(&resp).unwrap();
            }
            // d drops here -> upstream closes -> bridge's response pump ends.
        });

        let bridge = thread::spawn(move || {
            Bridge::run(
                UnixTransport::from_stream(client_bridge),
                UnixTransport::from_stream(upstream_bridge),
            )
        });

        let mut client = UnixTransport::from_stream(client_peer);

        // Fire three requests, then signal stdin-EOF via a write half-close.
        for i in 1..=3i64 {
            client
                .write(&JsonRpcMessage::request(i, "ping", None))
                .unwrap();
        }
        client.close_write().unwrap();

        // All three responses must still arrive (drained, not dropped).
        for i in 1..=3i64 {
            match client.read().unwrap() {
                JsonRpcMessage::Response(r) => assert_eq!(r.id, RequestId::Number(i)),
                other => panic!("expected response, got {:?}", other),
            }
        }

        let joined = bridge.join().unwrap();
        assert!(joined.is_ok());
        daemon.join().unwrap();
    }

    #[test]
    fn test_run_shuts_down_when_upstream_closes() {
        let (client_bridge, _client_peer) = UnixStream::pair().unwrap();
        let (upstream_bridge, upstream_peer) = UnixStream::pair().unwrap();

        let bridge = thread::spawn(move || {
            Bridge::run(
                UnixTransport::from_stream(client_bridge),
                UnixTransport::from_stream(upstream_bridge),
            )
        });

        // Drop the daemon side -> upstream read hits EOF -> bridge returns.
        drop(upstream_peer);
        let joined = bridge.join().unwrap();
        assert!(joined.is_ok());
    }

    // ---- auto_start ------------------------------------------------------

    struct PingContext;
    struct PingTool;
    impl Tool<PingContext> for PingTool {
        fn name(&self) -> &str {
            "ping_tool"
        }
        fn description(&self) -> &str {
            "ping"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn execute(
            &self,
            _a: Value,
            _c: &mut PingContext,
            _e: &ToolEnv,
        ) -> McpResult<CallToolResult> {
            Ok(CallToolResult::text("pong"))
        }
    }

    /// Start a `UnixServer` on `sock` and block until it is accepting.
    ///
    /// Waiting here rather than inside the test is what makes the auto_start
    /// tests deterministic: whether the daemon is up is setup, not the thing
    /// under test, so it gets a generous deadline and a hard failure.
    fn start_ping_daemon(sock: &Path) -> thread::JoinHandle<McpResult<()>> {
        let sock_for_server = sock.to_path_buf();
        let handle = thread::spawn(move || {
            UnixServer::new(ServerConfig::default())
                .idle_timeout(Duration::from_secs(30))
                .with_tools(|s: &mut Server<PingContext>| {
                    s.add_tool(PingTool)?;
                    Ok(())
                })
                .serve(&sock_for_server, |_conn_id| PingContext)
        });

        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if UnixStream::connect(sock).is_ok() {
                return handle;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("daemon never started listening on {}", sock.display());
    }

    #[test]
    fn test_auto_start_connects_to_running_daemon() {
        let sock = temp_path("sock");
        let _server = start_ping_daemon(&sock);

        // One call, no retry loop. The daemon is known to be up, so the fast
        // path must take it; "false" as daemon_bin fails if it is ever spawned.
        let mut transport = auto_start_inner(
            &sock,
            "false",
            &[],
            Duration::from_millis(50),
            Duration::from_millis(200),
        )
        .expect("auto_start should connect to the running daemon");

        // Drive a request through to prove it's a live connection.
        let req = JsonRpcMessage::request(1i64, "ping", None);
        transport.write(&req).unwrap();
        let resp = transport.read().unwrap();
        if let JsonRpcMessage::Response(r) = resp {
            assert_eq!(r.id, RequestId::Number(1));
        } else {
            panic!("expected response");
        }

        let _ = std::fs::remove_file(&sock);
    }

    #[test]
    fn test_auto_start_never_unlinks_a_live_socket() {
        // Regression guard for the race that made the test above flake: a
        // `serve` daemon writes no PID file, so a socket with no PID file used
        // to be read as an orphan and deleted - even while the daemon was
        // listening on it. Everything after that failed with ENOENT.
        let sock = temp_path("sock");
        let _server = start_ping_daemon(&sock);

        for _ in 0..10 {
            let transport = auto_start_inner(
                &sock,
                "false",
                &[],
                Duration::from_millis(50),
                Duration::from_millis(200),
            );
            assert!(transport.is_ok(), "auto_start should keep connecting");
            assert!(sock.exists(), "a live daemon's socket must survive");
        }

        // And the daemon is still reachable by a plain connect.
        assert!(UnixTransport::connect(&sock).is_ok());
        let _ = std::fs::remove_file(&sock);
    }

    #[test]
    fn test_clear_socket_returns_a_live_daemon_instead_of_deleting_it() {
        // Same guarantee, exercised directly: the cleanup path itself refuses
        // to unlink a socket that answers.
        let sock = temp_path("sock");
        let _server = start_ping_daemon(&sock);

        let recovered = clear_socket(&sock, None);
        assert!(
            recovered.is_some(),
            "a live socket must come back connected"
        );
        assert!(sock.exists(), "a live socket must not be removed");

        let _ = std::fs::remove_file(&sock);
    }

    #[test]
    fn test_probe_socket_classifies_each_state() {
        // Live.
        let live = temp_path("sock");
        let _server = start_ping_daemon(&live);
        assert!(matches!(
            probe_socket(&live, PROBE_ATTEMPTS, Duration::ZERO),
            Probe::Live(_)
        ));
        let _ = std::fs::remove_file(&live);

        // Bound and then abandoned: the file outlives the listener.
        let stale = dead_socket("sock");
        assert!(matches!(
            probe_socket(&stale, PROBE_ATTEMPTS, Duration::ZERO),
            Probe::Dead
        ));
        let _ = std::fs::remove_file(&stale);

        // A regular file squatting on the path (ENOTSOCK).
        let orphan = temp_path("sock");
        std::fs::write(&orphan, b"not a socket").unwrap();
        assert!(matches!(
            probe_socket(&orphan, PROBE_ATTEMPTS, Duration::ZERO),
            Probe::Dead
        ));
        let _ = std::fs::remove_file(&orphan);

        // Nothing there at all (ENOENT).
        let missing = temp_path("sock");
        assert!(matches!(
            probe_socket(&missing, PROBE_ATTEMPTS, Duration::ZERO),
            Probe::Dead
        ));
    }

    #[test]
    fn test_probe_socket_will_not_condemn_what_it_cannot_reach() {
        // A path too long for `sockaddr_un` fails before the kernel is asked,
        // so there is no errno to read and no way to know what is there. The
        // verdict has to be Unknown - and Unknown never deletes.
        let long = temp_path(&"x".repeat(120));
        std::fs::write(&long, b"something").unwrap();

        assert!(matches!(
            probe_socket(&long, PROBE_ATTEMPTS, Duration::ZERO),
            Probe::Unknown
        ));
        assert!(clear_socket(&long, None).is_none());
        assert!(long.exists(), "an unexplained path must be left alone");

        let _ = std::fs::remove_file(&long);
    }

    #[test]
    fn test_only_conclusive_errors_condemn_a_socket() {
        use std::io::Error;

        // Proof that nothing owns the path.
        for errno in [libc::ECONNREFUSED, libc::ENOENT, libc::ENOTSOCK] {
            assert!(is_unreachable(&Error::from_raw_os_error(errno)), "{errno}");
        }

        // Proof of nothing. EACCES is a socket we are not allowed to reach;
        // EAGAIN on a unix socket is a listener whose backlog is full - both
        // belong to daemons that are very much alive.
        for errno in [libc::EACCES, libc::EAGAIN, libc::EINTR, libc::ETIMEDOUT] {
            assert!(!is_unreachable(&Error::from_raw_os_error(errno)), "{errno}");
        }

        // An error raised before any syscall carries no errno at all.
        assert!(!is_unreachable(&Error::other("no syscall happened")));
    }

    #[test]
    fn test_probe_socket_needs_a_consistent_answer() {
        // A socket that refuses once and accepts on a later attempt is a
        // daemon under load, not a corpse. `probe_socket` returns Live because
        // any successful connect ends the probe.
        let sock = temp_path("sock");

        // Nothing listening yet -> the first probe attempt refuses.
        let sock_for_server = sock.clone();
        let binder = thread::spawn(move || {
            thread::sleep(Duration::from_millis(60));
            let listener = std::os::unix::net::UnixListener::bind(&sock_for_server).unwrap();
            // Hold it open long enough for the probe to find it.
            thread::sleep(Duration::from_millis(500));
            drop(listener);
        });

        // Bind lands ~60ms in; probing across ~200ms of attempts must find it.
        let verdict = probe_socket(&sock, 6, Duration::from_millis(40));
        assert!(
            matches!(verdict, Probe::Live(_)),
            "a daemon that binds mid-probe must not be condemned"
        );

        drop(verdict);
        binder.join().unwrap();
        let _ = std::fs::remove_file(&sock);
    }

    #[test]
    fn test_clear_socket_removes_a_confirmed_dead_socket_and_its_pid_file() {
        let sock = dead_socket("sock");
        let pid = pid_path_for(&sock);
        std::fs::write(&pid, "2147483646\n").unwrap();

        assert!(clear_socket(&sock, Some(&pid)).is_none());
        assert!(!sock.exists(), "a dead socket should be removed");
        assert!(!pid.exists(), "its PID file should go with it");
    }

    #[test]
    fn test_auto_start_removes_orphaned_socket() {
        let sock = temp_path("sock");
        // Orphaned socket file: present, nothing listening, no PID file.
        std::fs::write(&sock, b"orphan").unwrap();
        assert!(sock.exists());

        // Spawn "true" as the "daemon": it exits immediately without binding,
        // so auto_start removes the orphan, spawns, then times out waiting.
        let result = auto_start_inner(
            &sock,
            "true",
            &[],
            Duration::from_millis(50),
            Duration::from_millis(150),
        );

        // The orphaned socket must have been detected and removed (and "true"
        // didn't recreate it).
        assert!(!sock.exists(), "orphaned socket should be removed");
        // And since "true" never binds, the overall call times out.
        assert!(result.is_err());
    }

    #[test]
    fn test_auto_start_clears_stale_pid_without_socket() {
        let sock = temp_path("sock");
        let pid_path = pid_path_for(&sock);
        // Dead PID, no socket.
        std::fs::write(&pid_path, "2147483646\n").unwrap();
        assert!(pid_path.exists());

        let result = auto_start_inner(
            &sock,
            "true",
            &[],
            Duration::from_millis(50),
            Duration::from_millis(150),
        );

        // Stale PID file cleared; "true" doesn't bind so we still time out.
        assert!(!pid_path.exists(), "stale PID file should be removed");
        assert!(result.is_err());
        let _ = std::fs::remove_file(&sock);
        let _ = std::fs::remove_file(&pid_path);
    }

    #[test]
    fn test_auto_start_spawns_and_connects_via_helper_binary() {
        // End-to-end: spawn a helper that runs a UnixServer in the foreground,
        // and verify auto_start brings it up and connects. Uses `cargo` to run
        // a throwaway? No - instead use a real server started slightly late to
        // exercise the post-spawn wait path deterministically without a binary.
        let sock = temp_path("sock");
        let sock_for_server = sock.clone();

        // Start the server ~150ms after auto_start begins polling, so the
        // wait_for_socket loop has to spin at least once.
        let server = thread::spawn(move || {
            thread::sleep(Duration::from_millis(150));
            UnixServer::new(ServerConfig::default())
                .idle_timeout(Duration::from_secs(5))
                .with_tools(|s: &mut Server<PingContext>| {
                    s.add_tool(PingTool)?;
                    Ok(())
                })
                .serve(&sock_for_server, |_conn_id| PingContext)
        });

        // "true" is a no-op spawn; the real readiness comes from the server
        // thread above. This exercises spawn_daemon + wait_for_socket.
        let transport = auto_start_inner(
            &sock,
            "true",
            &[],
            Duration::from_millis(50),
            Duration::from_secs(5),
        );
        assert!(
            transport.is_ok(),
            "auto_start should connect once server is up"
        );

        drop(transport);
        let _ = server.join();
        let _ = std::fs::remove_file(&sock);
    }

    // ---- multi-bridge integration ----------------------------------------

    /// Context that tracks its connection identity.
    struct BridgeTestContext {
        conn_id: String,
    }

    /// Returns the conn_id so we can verify each bridge reaches its own
    /// daemon-side context.
    struct BridgeWhoamiTool;
    impl Tool<BridgeTestContext> for BridgeWhoamiTool {
        fn name(&self) -> &str {
            "whoami"
        }
        fn description(&self) -> &str {
            "Return connection id"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn execute(
            &self,
            _a: Value,
            ctx: &mut BridgeTestContext,
            _e: &ToolEnv,
        ) -> McpResult<CallToolResult> {
            Ok(CallToolResult::text(ctx.conn_id.clone()))
        }
    }

    /// Helper: send initialize handshake through a raw UnixTransport.
    fn bridge_initialize(t: &mut UnixTransport, id: i64) {
        let req = JsonRpcMessage::request(
            id,
            "initialize",
            Some(serde_json::json!({
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": { "name": "test", "version": "1.0" }
            })),
        );
        t.write(&req).unwrap();
        let resp = t.read().unwrap();
        assert!(matches!(resp, JsonRpcMessage::Response(_)));
    }

    /// Helper: call a tool through a raw UnixTransport and return the text.
    fn bridge_call_tool(t: &mut UnixTransport, id: i64, name: &str, args: Value) -> String {
        let req = JsonRpcMessage::request(
            id,
            "tools/call",
            Some(serde_json::json!({ "name": name, "arguments": args })),
        );
        t.write(&req).unwrap();
        let resp = t.read().unwrap();
        match resp {
            JsonRpcMessage::Response(r) => {
                let result = r.result.expect("expected result");
                result["content"][0]["text"]
                    .as_str()
                    .expect("text content")
                    .to_string()
            }
            other => panic!("expected response, got {:?}", other),
        }
    }

    #[test]
    fn test_multi_bridge_concurrent_no_crosstalk() {
        // End-to-end: 3 bridges (simulating 3 CC terminals with shims) all
        // connected to the same daemon, sending interleaved requests.
        // Verifies each bridge gets responses routed to the correct client.
        let sock = temp_path("sock");
        let sock_for_server = sock.clone();

        // Stand up the daemon.
        let _server = thread::spawn(move || {
            UnixServer::new(ServerConfig::default())
                .idle_timeout(Duration::from_secs(10))
                .with_tools(|s: &mut Server<BridgeTestContext>| {
                    s.add_tool(BridgeWhoamiTool)?;
                    Ok(())
                })
                .serve(&sock_for_server, |conn_id| BridgeTestContext {
                    conn_id: conn_id.to_string(),
                })
        });

        // Wait for the daemon to be ready.
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if UnixTransport::connect(&sock).is_ok() {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }

        // Spin up 3 bridges. Each one gets a UnixStream::pair() for the
        // "client" side and a real connection to the daemon for the upstream.
        let mut clients = Vec::new();
        let mut bridge_handles = Vec::new();

        for _ in 0..3 {
            let (client_bridge, client_peer) = UnixStream::pair().unwrap();
            let upstream = UnixTransport::connect(&sock).unwrap();

            let handle = thread::spawn(move || {
                Bridge::run(UnixTransport::from_stream(client_bridge), upstream)
            });

            clients.push(UnixTransport::from_stream(client_peer));
            bridge_handles.push(handle);
        }

        // Initialize all three through their bridges.
        for client in clients.iter_mut() {
            bridge_initialize(client, 1);
        }

        // Each bridge should get a unique conn_id from the daemon.
        let mut conn_ids: Vec<String> = Vec::new();
        for client in clients.iter_mut() {
            conn_ids.push(bridge_call_tool(client, 2, "whoami", serde_json::json!({})));
        }
        assert_ne!(conn_ids[0], conn_ids[1]);
        assert_ne!(conn_ids[1], conn_ids[2]);
        assert_ne!(conn_ids[0], conn_ids[2]);

        // Interleave requests in a different order and verify each bridge
        // still gets its own conn_id back — no cross-talk.
        for round in 3..=5i64 {
            // Reverse order each round to stress the routing.
            let order: Vec<usize> = if round % 2 == 0 {
                vec![0, 1, 2]
            } else {
                vec![2, 1, 0]
            };
            for &i in &order {
                let got = bridge_call_tool(&mut clients[i], round, "whoami", serde_json::json!({}));
                assert_eq!(
                    got, conn_ids[i],
                    "bridge {} got conn_id {} but expected {} (round {})",
                    i, got, conn_ids[i], round
                );
            }
        }

        // Clean shutdown: drop all clients, bridges will see EOF and exit.
        drop(clients);
        for h in bridge_handles {
            let _ = h.join();
        }
        let _ = std::fs::remove_file(&sock);
    }
}
