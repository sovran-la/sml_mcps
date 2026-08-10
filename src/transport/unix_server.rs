//! Unix Socket Server (daemon listener)
//!
//! Mirrors [`HttpServer`](crate::HttpServer) but over a Unix domain socket.
//! Binds a [`UnixListener`], accepts connections, and spawns one
//! `std::thread` per connection - connection counts are single digits (a
//! handful of client sessions sharing one daemon), so thread-per-connection
//! is the right amount of machinery.
//!
//! Three ways to run:
//! - [`UnixServer::serve`] - foreground, blocks (debugging/development).
//! - [`UnixServer::serve_daemon`] - double-fork daemonize, then serve.
//! - both honor [`UnixServer::idle_timeout`] - exit after N idle seconds so a
//!   model-heavy daemon doesn't linger forever after the last client leaves.
//!
//! For the whole pattern - this, the shim in front of it, and the `argv`
//! dispatch that picks between them - in one call, see
//! [`Server::serve_daemon`](crate::Server::serve_daemon).
//!
//! Unix-only: gated behind `#[cfg(unix)]`, no feature flag.

use crate::server::{Server, ServerConfig};
use crate::transport::UnixTransport;
use crate::types::{McpError, Result};
use std::io::{self, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

/// Setup function type for configuring tools on each connection.
type SetupFn<C> = Box<dyn Fn(&mut Server<C>) -> Result<()> + Send + Sync>;

// ---- signal handling -----------------------------------------------------

/// Write end of the self-pipe used by the signal handler. -1 when inactive.
static SIGNAL_WRITE_FD: AtomicI32 = AtomicI32::new(-1);

/// Signal handler: writes a byte to the self-pipe so the signal watcher
/// thread can trigger a clean shutdown. Only uses async-signal-safe ops.
extern "C" fn shutdown_signal_handler(_sig: libc::c_int) {
    let fd = SIGNAL_WRITE_FD.load(Ordering::Relaxed);
    if fd >= 0 {
        unsafe {
            libc::write(fd, b"x" as *const u8 as *const libc::c_void, 1);
        }
    }
}

/// Create a pipe for signal delivery. Returns (read_fd, write_fd).
fn create_signal_pipe() -> Result<(i32, i32)> {
    let mut fds = [0i32; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } == -1 {
        return Err(io::Error::last_os_error().into());
    }
    Ok((fds[0], fds[1]))
}

/// Install SIGTERM/SIGINT handlers and spawn a watcher thread that triggers
/// the existing shutdown mechanism (set `state.shutdown`, self-connect to
/// unblock accept). Returns a handle to the watcher thread.
///
/// On drop/cleanup: close the pipe and reset the global fd, which causes
/// the watcher thread to exit naturally.
fn install_signal_handlers(
    shared: &Arc<(Mutex<ConnState>, Condvar)>,
    socket_path: &Path,
) -> Result<SignalGuard> {
    let (sig_read, sig_write) = create_signal_pipe()?;

    // Publish the write fd so the signal handler can reach it.
    SIGNAL_WRITE_FD.store(sig_write, Ordering::SeqCst);

    // Register handlers. Save previous handlers for restoration.
    let prev_term;
    let prev_int;
    unsafe {
        prev_term = libc::signal(
            libc::SIGTERM,
            shutdown_signal_handler as *const () as libc::sighandler_t,
        );
        prev_int = libc::signal(
            libc::SIGINT,
            shutdown_signal_handler as *const () as libc::sighandler_t,
        );
    }

    let shared_clone = shared.clone();
    let socket_clone = socket_path.to_path_buf();

    let watcher = thread::Builder::new()
        .name("signal-watcher".into())
        .spawn(move || {
            // Block until the signal handler writes, or the pipe closes.
            let mut buf = [0u8; 1];
            let n = unsafe { libc::read(sig_read, buf.as_mut_ptr() as *mut libc::c_void, 1) };
            unsafe {
                libc::close(sig_read);
            }

            if n > 0 {
                // Signal received — trigger clean shutdown.
                let (lock, cv) = &*shared_clone;
                if let Ok(mut state) = lock.lock() {
                    state.shutdown = true;
                    cv.notify_all();
                }
                // Unblock accept() via self-connect (same trick as idle_watcher).
                let _ = UnixStream::connect(&socket_clone);
            }
        })
        .map_err(|e| McpError::Internal(format!("failed to spawn signal watcher: {}", e)))?;

    Ok(SignalGuard {
        write_fd: sig_write,
        prev_term,
        prev_int,
        watcher: Some(watcher),
    })
}

/// RAII guard that restores signal handlers and cleans up the pipe on drop.
struct SignalGuard {
    write_fd: i32,
    prev_term: libc::sighandler_t,
    prev_int: libc::sighandler_t,
    watcher: Option<thread::JoinHandle<()>>,
}

impl Drop for SignalGuard {
    fn drop(&mut self) {
        // Restore previous signal handlers.
        unsafe {
            libc::signal(libc::SIGTERM, self.prev_term);
            libc::signal(libc::SIGINT, self.prev_int);
        }

        // Close the write end — if the watcher is still blocked on read(),
        // it'll get EOF and exit cleanly.
        SIGNAL_WRITE_FD.store(-1, Ordering::SeqCst);
        unsafe {
            libc::close(self.write_fd);
        }

        if let Some(w) = self.watcher.take() {
            let _ = w.join();
        }
    }
}

/// Monotonic source of per-connection identifiers.
static CONN_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Shared connection bookkeeping, guarded by a `Mutex` and paired with a
/// `Condvar` so the idle watcher can sleep until something changes.
#[derive(Default)]
struct ConnState {
    /// Number of currently-live connections.
    active: usize,
    /// Bumped on every accept. Lets the idle watcher tell "still idle" from
    /// "a new client arrived and left during my timeout window".
    generation: u64,
    /// Set when the server should stop accepting and exit.
    shutdown: bool,
}

/// Decrements the active-connection count when a connection thread exits,
/// even on panic (RAII). Without this an unwinding handler would leave the
/// daemon thinking a client is still connected and never idle-out.
struct ActiveGuard(Arc<(Mutex<ConnState>, Condvar)>);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        let (lock, cv) = &*self.0;
        if let Ok(mut state) = lock.lock() {
            state.active = state.active.saturating_sub(1);
            cv.notify_all();
        }
    }
}

/// High-level Unix-socket MCP daemon.
///
/// # Example
/// ```ignore
/// use std::time::Duration;
/// UnixServer::new(config)
///     .idle_timeout(Duration::from_secs(300))
///     .with_tools(|s| {
///         s.add_tool(EchoTool)?;
///         Ok(())
///     })
///     .serve_daemon("/tmp/myapp/server.sock", |conn_id| {
///         AppContext::new(conn_id)
///     })?;
/// ```
pub struct UnixServer<C> {
    config: ServerConfig,
    setup: Option<SetupFn<C>>,
    idle_timeout: Option<Duration>,
}

impl<C: Send + Sync + 'static> UnixServer<C> {
    /// Create a new Unix server with the given configuration.
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config,
            setup: None,
            idle_timeout: None,
        }
    }

    /// Configure tools via a setup closure.
    ///
    /// The closure runs once per connection to set up a fresh [`Server`].
    pub fn with_tools<F>(mut self, setup: F) -> Self
    where
        F: Fn(&mut Server<C>) -> Result<()> + Send + Sync + 'static,
    {
        self.setup = Some(Box::new(setup));
        self
    }

    /// How long to stay alive after the last connection disconnects.
    ///
    /// Default: `None` (run forever). Recommended for model-heavy daemons so
    /// closing and reopening a terminal a few seconds apart doesn't reload
    /// multi-GB state.
    pub fn idle_timeout(mut self, duration: Duration) -> Self {
        self.idle_timeout = Some(duration);
        self
    }

    /// Run in the foreground (for debugging/development).
    ///
    /// Binds the socket, accepts connections, and blocks. The `context_factory`
    /// receives a unique `conn_id` per connection.
    pub fn serve<F>(self, socket_path: impl AsRef<Path>, context_factory: F) -> Result<()>
    where
        F: Fn(&str) -> C + Send + Sync + 'static,
    {
        let socket_path = socket_path.as_ref().to_path_buf();
        self.run(socket_path, None, context_factory)
    }

    /// Daemonize, then serve.
    ///
    /// Double-forks, detaches via `setsid`, redirects stdio to `/dev/null`,
    /// writes a PID file next to the socket (`server.sock` -> `server.pid`),
    /// then binds the socket and enters the accept loop. The original process
    /// prints the first child's PID and returns to the shell.
    pub fn serve_daemon<F>(self, socket_path: impl AsRef<Path>, context_factory: F) -> Result<()>
    where
        F: Fn(&str) -> C + Send + Sync + 'static,
    {
        let socket_path = socket_path.as_ref().to_path_buf();
        let pid_path = pid_path_for(&socket_path);

        // Forks twice; only the detached grandchild returns here.
        daemonize_fork()?;

        self.run(socket_path, Some(pid_path), context_factory)
    }

    /// Shared bind + accept-loop driver for both `serve` and `serve_daemon`.
    fn run<F>(
        self,
        socket_path: PathBuf,
        pid_path: Option<PathBuf>,
        context_factory: F,
    ) -> Result<()>
    where
        F: Fn(&str) -> C + Send + Sync + 'static,
    {
        let UnixServer {
            config,
            setup,
            idle_timeout,
        } = self;

        // Clear a stale socket (or refuse if a live server owns it).
        prepare_socket_path(&socket_path)?;

        let listener = UnixListener::bind(&socket_path).map_err(|e| {
            McpError::Internal(format!("failed to bind {}: {}", socket_path.display(), e))
        })?;

        let setup = setup.map(Arc::new);
        let factory = Arc::new(context_factory);
        let shared: Arc<(Mutex<ConnState>, Condvar)> =
            Arc::new((Mutex::new(ConnState::default()), Condvar::new()));

        // Signal handling: SIGTERM/SIGINT trigger a clean shutdown via the
        // same self-connect mechanism the idle watcher uses. The SignalGuard
        // restores previous handlers on drop. Installed BEFORE the PID file
        // is written so that signals are handled from the moment external
        // processes can discover the daemon's PID.
        let _signal_guard = install_signal_handlers(&shared, &socket_path)?;

        // Daemon mode: now that we own the socket and signal handlers are
        // installed, publish the PID and detach stdio. Writing the PID after
        // bind means the file only exists when we actually hold the socket.
        if let Some(ref pid) = pid_path {
            write_pid_file(pid)?;
            redirect_stdio_to_devnull()?;
        }

        eprintln!(
            "MCP Unix server `{}` listening on {}",
            config.name,
            socket_path.display()
        );

        // Optional idle watcher: exits the daemon after `idle_timeout` of zero
        // connections. Wakes the accept loop by self-connecting the socket.
        let watcher = idle_timeout.map(|timeout| {
            let shared = shared.clone();
            let socket_path = socket_path.clone();
            thread::spawn(move || idle_watcher(shared, timeout, socket_path))
        });

        let result = accept_loop(&listener, &shared, &config, &setup, &factory);

        // Make sure the watcher stops even if we exited for a non-idle reason.
        {
            let (lock, cv) = &*shared;
            if let Ok(mut state) = lock.lock() {
                state.shutdown = true;
                cv.notify_all();
            }
        }
        if let Some(w) = watcher {
            let _ = w.join();
        }

        cleanup(&socket_path, pid_path.as_deref());
        result
    }
}

/// Main accept loop. Returns `Ok(())` on a clean shutdown break, `Err` on an
/// accept-level failure.
fn accept_loop<C, F>(
    listener: &UnixListener,
    shared: &Arc<(Mutex<ConnState>, Condvar)>,
    config: &ServerConfig,
    setup: &Option<Arc<SetupFn<C>>>,
    factory: &Arc<F>,
) -> Result<()>
where
    C: Send + Sync + 'static,
    F: Fn(&str) -> C + Send + Sync + 'static,
{
    loop {
        let (stream, _addr) = match listener.accept() {
            Ok(pair) => pair,
            Err(e) => return Err(McpError::Internal(format!("accept failed: {}", e))),
        };

        // Register the connection. If a shutdown was requested (the idle
        // watcher self-connected to wake us), stop here and drop this stream.
        {
            let (lock, cv) = &**shared;
            let mut state = lock
                .lock()
                .map_err(|_| McpError::Internal("conn state lock poisoned".into()))?;
            if state.shutdown {
                break;
            }
            state.active += 1;
            state.generation += 1;
            cv.notify_all();
        }

        let conn_id = format!("conn-{}", CONN_COUNTER.fetch_add(1, Ordering::SeqCst));
        let cfg = config.clone();
        let setup = setup.clone();
        let factory = factory.clone();
        let shared_thread = shared.clone();

        let spawn = thread::Builder::new()
            .name(conn_id.clone())
            .spawn(move || handle_connection(stream, conn_id, cfg, setup, factory, shared_thread));

        if let Err(e) = spawn {
            eprintln!("failed to spawn connection thread: {}", e);
            // We incremented `active` above but no ActiveGuard will run - undo it.
            let (lock, cv) = &**shared;
            if let Ok(mut state) = lock.lock() {
                state.active = state.active.saturating_sub(1);
                cv.notify_all();
            }
        }
    }
    Ok(())
}

/// Per-connection worker: build a fresh server + context, then run the
/// blocking MCP loop until the client disconnects.
fn handle_connection<C, F>(
    stream: UnixStream,
    conn_id: String,
    config: ServerConfig,
    setup: Option<Arc<SetupFn<C>>>,
    factory: Arc<F>,
    shared: Arc<(Mutex<ConnState>, Condvar)>,
) where
    C: Send + Sync + 'static,
    F: Fn(&str) -> C,
{
    // Decrements `active` on any exit path, including panic.
    let _guard = ActiveGuard(shared);

    let context = factory(&conn_id);
    let mut server: Server<C> = Server::new(config);

    if let Some(setup) = setup.as_ref() {
        if let Err(e) = setup(&mut server) {
            eprintln!("[{}] tool setup failed: {}", conn_id, e);
            return;
        }
    }

    let transport = UnixTransport::from_stream(stream);
    if let Err(e) = server.start(transport, context) {
        eprintln!("[{}] connection closed: {}", conn_id, e);
    }
}

/// Idle watcher thread. Sleeps until the daemon has been idle (zero
/// connections) for `timeout`, then requests shutdown and wakes the accept
/// loop. A new connection arriving during the window cancels the countdown.
fn idle_watcher(shared: Arc<(Mutex<ConnState>, Condvar)>, timeout: Duration, socket_path: PathBuf) {
    let (lock, cv) = &*shared;
    loop {
        let mut state = match lock.lock() {
            Ok(g) => g,
            Err(_) => return,
        };

        // Wait until idle (or shutdown).
        while state.active > 0 && !state.shutdown {
            state = match cv.wait(state) {
                Ok(g) => g,
                Err(_) => return,
            };
        }
        if state.shutdown {
            return;
        }

        // Idle now. Remember the generation, then wait out the timeout. If a
        // connection arrives it bumps `generation` and notifies, waking us early.
        let gen_at_idle = state.generation;
        let (mut state, res) = match cv.wait_timeout(state, timeout) {
            Ok(pair) => pair,
            Err(_) => return,
        };

        if state.shutdown {
            return;
        }
        if res.timed_out() && state.active == 0 && state.generation == gen_at_idle {
            // Genuinely idle for the whole window - shut down.
            state.shutdown = true;
            cv.notify_all();
            drop(state);
            // Unblock the accept loop's blocking `accept()` so it observes the
            // shutdown flag and returns.
            let _ = UnixStream::connect(&socket_path);
            return;
        }
        // Otherwise activity arrived; loop and re-evaluate from the top.
    }
}

/// Compute the PID-file path for a socket path: replace the extension with
/// `pid` (e.g. `server.sock` -> `server.pid`, `server` -> `server.pid`).
pub(crate) fn pid_path_for(socket_path: &Path) -> PathBuf {
    let mut p = socket_path.to_path_buf();
    p.set_extension("pid");
    p
}

/// Remove the socket file and, if present, the PID file. Best-effort.
fn cleanup(socket_path: &Path, pid_path: Option<&Path>) {
    let _ = std::fs::remove_file(socket_path);
    if let Some(pid) = pid_path {
        let _ = std::fs::remove_file(pid);
    }
}

/// Clear the socket path before binding. If a live server is already
/// listening, refuse (don't clobber it); if it's a stale socket file, remove it.
fn prepare_socket_path(path: &Path) -> Result<()> {
    if path.exists() {
        match UnixStream::connect(path) {
            Ok(_) => {
                return Err(McpError::Internal(format!(
                    "socket {} is already in use by a live server",
                    path.display()
                )));
            }
            Err(_) => {
                // Nothing listening - stale socket, safe to remove.
                std::fs::remove_file(path)?;
            }
        }
    }
    Ok(())
}

/// Write the current process PID to `path`.
pub(crate) fn write_pid_file(path: &Path) -> Result<()> {
    let pid = unsafe { libc::getpid() };
    std::fs::write(path, format!("{}\n", pid))?;
    Ok(())
}

/// Double-fork + setsid to detach from the controlling terminal.
///
/// 1. `fork` - the original parent prints the child PID and `_exit`s.
/// 2. `setsid` - the child becomes a session leader (no controlling tty).
/// 3. `fork` again - the grandchild can never reacquire a terminal.
///
/// Only the final grandchild returns `Ok(())`; everyone else `_exit`s.
fn daemonize_fork() -> Result<()> {
    // SAFETY: fork/setsid/_exit are async-signal-safe libc calls; we run them
    // before spawning any threads, so there is no multi-thread fork hazard.
    unsafe {
        match libc::fork() {
            -1 => return Err(io::Error::last_os_error().into()),
            0 => {} // child continues
            child => {
                // Original parent: report the child PID and exit immediately
                // without running destructors / flushing inherited buffers.
                let mut out = io::stdout();
                let _ = writeln!(out, "{}", child);
                let _ = out.flush();
                libc::_exit(0);
            }
        }

        if libc::setsid() == -1 {
            return Err(io::Error::last_os_error().into());
        }

        match libc::fork() {
            -1 => return Err(io::Error::last_os_error().into()),
            0 => {} // grandchild continues - this is the daemon
            _ => libc::_exit(0),
        }
    }

    Ok(())
}

/// Point stdin/stdout/stderr at `/dev/null` so the detached daemon holds no
/// terminal file descriptors.
fn redirect_stdio_to_devnull() -> Result<()> {
    use std::fs::OpenOptions;
    use std::os::unix::io::AsRawFd;

    let devnull = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/null")?;
    let fd = devnull.as_raw_fd();

    // SAFETY: dup2 onto the three standard fds; `fd` is a valid open
    // descriptor for the duration of the calls.
    unsafe {
        if libc::dup2(fd, libc::STDIN_FILENO) == -1
            || libc::dup2(fd, libc::STDOUT_FILENO) == -1
            || libc::dup2(fd, libc::STDERR_FILENO) == -1
        {
            return Err(io::Error::last_os_error().into());
        }
    }
    // `devnull` drops here, closing the original fd; the three dup2'd copies
    // keep the underlying open file description alive.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::{Server, Tool, ToolEnv};
    use crate::transport::{Transport, UnixTransport};
    use crate::types::{CallToolResult, JsonRpcMessage, RequestId};
    use serde_json::Value;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;
    use std::time::Instant;

    // ---- test scaffolding ------------------------------------------------

    /// Unique temp socket path per test, avoiding collisions across runs.
    fn temp_socket() -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::SeqCst);
        let pid = std::process::id();
        std::env::temp_dir().join(format!("sml_mcps_unix_test_{}_{}.sock", pid, n))
    }

    /// Per-connection context: remembers its own conn_id and shares a
    /// conn-keyed map so tests can prove isolation.
    struct TestContext {
        conn_id: String,
        store: Arc<StdMutex<HashMap<String, i64>>>,
    }

    /// Returns the conn_id this connection's context was built with.
    struct WhoamiTool;
    impl Tool<TestContext> for WhoamiTool {
        fn name(&self) -> &str {
            "whoami"
        }
        fn description(&self) -> &str {
            "Return this connection's id"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn execute(
            &self,
            _a: Value,
            ctx: &mut TestContext,
            _e: &ToolEnv,
        ) -> Result<CallToolResult> {
            Ok(CallToolResult::text(ctx.conn_id.clone()))
        }
    }

    /// Writes a value into the shared store keyed by this connection's id.
    struct SetTool;
    impl Tool<TestContext> for SetTool {
        fn name(&self) -> &str {
            "set"
        }
        fn description(&self) -> &str {
            "Set this connection's stored value"
        }
        fn schema(&self) -> Value {
            serde_json::json!({
                "type": "object",
                "properties": { "value": { "type": "integer" } },
                "required": ["value"]
            })
        }
        fn execute(&self, a: Value, ctx: &mut TestContext, _e: &ToolEnv) -> Result<CallToolResult> {
            let v = a.get("value").and_then(|v| v.as_i64()).unwrap_or(0);
            ctx.store.lock().unwrap().insert(ctx.conn_id.clone(), v);
            Ok(CallToolResult::text(format!("set {}", v)))
        }
    }

    /// Reads this connection's stored value (0 if unset). Proves isolation:
    /// connection B never sees the value connection A set under its own id.
    struct GetTool;
    impl Tool<TestContext> for GetTool {
        fn name(&self) -> &str {
            "get"
        }
        fn description(&self) -> &str {
            "Get this connection's stored value"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn execute(
            &self,
            _a: Value,
            ctx: &mut TestContext,
            _e: &ToolEnv,
        ) -> Result<CallToolResult> {
            let v = ctx
                .store
                .lock()
                .unwrap()
                .get(&ctx.conn_id)
                .copied()
                .unwrap_or(0);
            Ok(CallToolResult::text(format!("{}", v)))
        }
    }

    fn test_config() -> ServerConfig {
        ServerConfig {
            name: "unix-test".into(),
            version: "1.0.0".into(),
            instructions: None,
            ..Default::default()
        }
    }

    /// Spawn a server on `path` with the standard test toolset and a shared store.
    fn spawn_server(
        path: &Path,
        idle: Option<Duration>,
        store: Arc<StdMutex<HashMap<String, i64>>>,
    ) -> thread::JoinHandle<Result<()>> {
        let path = path.to_path_buf();
        thread::spawn(move || {
            let mut server =
                UnixServer::new(test_config()).with_tools(|s: &mut Server<TestContext>| {
                    s.add_tool(WhoamiTool)?;
                    s.add_tool(SetTool)?;
                    s.add_tool(GetTool)?;
                    Ok(())
                });
            if let Some(d) = idle {
                server = server.idle_timeout(d);
            }
            server.serve(&path, move |conn_id| TestContext {
                conn_id: conn_id.to_string(),
                store: store.clone(),
            })
        })
    }

    /// Connect a client transport, retrying until the server is listening.
    fn connect_retry(path: &Path) -> UnixTransport {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match UnixTransport::connect(path) {
                Ok(t) => return t,
                Err(_) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(20));
                }
                Err(e) => panic!("could not connect to {}: {}", path.display(), e),
            }
        }
    }

    /// Send a `tools/call` and return the response's first text content.
    fn call_tool(t: &mut UnixTransport, id: i64, name: &str, args: Value) -> String {
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

    /// Send `initialize` and assert success (handshake before tool calls).
    fn initialize(t: &mut UnixTransport, id: i64) {
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

    // ---- unit tests ------------------------------------------------------

    #[test]
    fn test_pid_path_for() {
        assert_eq!(
            pid_path_for(Path::new("/x/server.sock")),
            PathBuf::from("/x/server.pid")
        );
        assert_eq!(
            pid_path_for(Path::new("/x/server")),
            PathBuf::from("/x/server.pid")
        );
    }

    #[test]
    fn test_write_pid_file_and_cleanup() {
        let sock = temp_socket();
        let pid = pid_path_for(&sock);

        write_pid_file(&pid).unwrap();
        assert!(pid.exists());
        let contents = std::fs::read_to_string(&pid).unwrap();
        let written: i32 = contents.trim().parse().unwrap();
        assert_eq!(written, unsafe { libc::getpid() });

        // cleanup removes both socket (absent here) and pid file.
        cleanup(&sock, Some(&pid));
        assert!(!pid.exists());
    }

    #[test]
    fn test_prepare_socket_path_removes_stale() {
        let sock = temp_socket();
        // Create a plain file at the socket path (no listener) -> stale.
        std::fs::write(&sock, b"stale").unwrap();
        assert!(sock.exists());

        prepare_socket_path(&sock).unwrap();
        assert!(!sock.exists());
    }

    #[test]
    fn test_prepare_socket_path_refuses_live() {
        let sock = temp_socket();
        let _listener = UnixListener::bind(&sock).unwrap();

        // A live listener owns the path -> prepare must refuse, not remove.
        let result = prepare_socket_path(&sock);
        assert!(result.is_err());
        assert!(sock.exists());

        let _ = std::fs::remove_file(&sock);
    }

    // ---- integration tests ----------------------------------------------

    #[test]
    fn test_serve_initialize_list_call() {
        let sock = temp_socket();
        let store = Arc::new(StdMutex::new(HashMap::new()));
        let _server = spawn_server(&sock, Some(Duration::from_secs(10)), store);

        let mut client = connect_retry(&sock);
        initialize(&mut client, 1);

        // tools/list
        let req = JsonRpcMessage::request(2, "tools/list", None);
        client.write(&req).unwrap();
        let resp = client.read().unwrap();
        if let JsonRpcMessage::Response(r) = resp {
            let names: Vec<String> = r.result.unwrap()["tools"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["name"].as_str().unwrap().to_string())
                .collect();
            assert!(names.contains(&"whoami".to_string()));
            assert!(names.contains(&"set".to_string()));
            assert!(names.contains(&"get".to_string()));
        } else {
            panic!("expected response");
        }

        // tools/call
        let who = call_tool(&mut client, 3, "whoami", serde_json::json!({}));
        assert!(who.starts_with("conn-"));

        let _ = std::fs::remove_file(&sock);
    }

    #[test]
    fn test_multi_client_no_crosstalk() {
        let sock = temp_socket();
        let store = Arc::new(StdMutex::new(HashMap::new()));
        let _server = spawn_server(&sock, Some(Duration::from_secs(10)), store);

        let mut a = connect_retry(&sock);
        let mut b = connect_retry(&sock);
        let mut c = connect_retry(&sock);
        initialize(&mut a, 1);
        initialize(&mut b, 1);
        initialize(&mut c, 1);

        let ida = call_tool(&mut a, 2, "whoami", serde_json::json!({}));
        let idb = call_tool(&mut b, 2, "whoami", serde_json::json!({}));
        let idc = call_tool(&mut c, 2, "whoami", serde_json::json!({}));

        // Each connection got a distinct id.
        assert_ne!(ida, idb);
        assert_ne!(idb, idc);
        assert_ne!(ida, idc);

        // Interleaved calls don't cross wires.
        assert_eq!(call_tool(&mut a, 3, "whoami", serde_json::json!({})), ida);
        assert_eq!(call_tool(&mut c, 3, "whoami", serde_json::json!({})), idc);
        assert_eq!(call_tool(&mut b, 3, "whoami", serde_json::json!({})), idb);

        let _ = std::fs::remove_file(&sock);
    }

    #[test]
    fn test_conn_id_isolation() {
        let sock = temp_socket();
        let store = Arc::new(StdMutex::new(HashMap::new()));
        let _server = spawn_server(&sock, Some(Duration::from_secs(10)), store);

        let mut a = connect_retry(&sock);
        let mut b = connect_retry(&sock);
        initialize(&mut a, 1);
        initialize(&mut b, 1);

        // A stores 5 under its own conn_id.
        assert_eq!(
            call_tool(&mut a, 2, "set", serde_json::json!({ "value": 5 })),
            "set 5"
        );

        // B reads its own slot -> still 0 (never sees A's write).
        assert_eq!(call_tool(&mut b, 2, "get", serde_json::json!({})), "0");

        // A reads back its own slot -> 5.
        assert_eq!(call_tool(&mut a, 3, "get", serde_json::json!({})), "5");

        let _ = std::fs::remove_file(&sock);
    }

    #[test]
    fn test_idle_timeout_exits() {
        let sock = temp_socket();
        let store = Arc::new(StdMutex::new(HashMap::new()));
        let server = spawn_server(&sock, Some(Duration::from_millis(300)), store);

        {
            let mut client = connect_retry(&sock);
            initialize(&mut client, 1);
            // client drops here -> connection closes -> daemon goes idle
        }

        // After the idle window the server should exit and remove the socket.
        let deadline = Instant::now() + Duration::from_secs(5);
        while sock.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(25));
        }
        assert!(!sock.exists(), "socket should be removed after idle exit");

        // serve() should have returned Ok.
        let joined = server.join().unwrap();
        assert!(joined.is_ok());
    }

    #[test]
    fn test_idle_timeout_cancelled_by_reconnect() {
        let sock = temp_socket();
        let store = Arc::new(StdMutex::new(HashMap::new()));
        let _server = spawn_server(&sock, Some(Duration::from_millis(500)), store);

        {
            let mut client = connect_retry(&sock);
            initialize(&mut client, 1);
            // drops -> idle countdown starts
        }

        // Reconnect well before the 500ms window elapses.
        thread::sleep(Duration::from_millis(150));
        let mut client2 =
            UnixTransport::connect(&sock).expect("reconnect before idle timeout should succeed");
        initialize(&mut client2, 1);

        // Past the original window: server is still alive because the reconnect
        // cancelled the countdown.
        thread::sleep(Duration::from_millis(500));
        let who = call_tool(&mut client2, 2, "whoami", serde_json::json!({}));
        assert!(who.starts_with("conn-"));
        assert!(sock.exists());

        let _ = std::fs::remove_file(&sock);
    }

    #[test]
    fn test_request_response_id_preserved() {
        let sock = temp_socket();
        let store = Arc::new(StdMutex::new(HashMap::new()));
        let _server = spawn_server(&sock, Some(Duration::from_secs(10)), store);

        let mut client = connect_retry(&sock);
        initialize(&mut client, 1);

        let req = JsonRpcMessage::request(99, "ping", None);
        client.write(&req).unwrap();
        let resp = client.read().unwrap();
        if let JsonRpcMessage::Response(r) = resp {
            assert_eq!(r.id, RequestId::Number(99));
        } else {
            panic!("expected response");
        }

        let _ = std::fs::remove_file(&sock);
    }
}
