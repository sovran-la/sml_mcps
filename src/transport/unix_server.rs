//! Daemon listener (a Unix socket, and anything else it is given)
//!
//! Mirrors [`HttpServer`](crate::HttpServer) but over a Unix domain socket.
//! Binds a [`UnixListener`], accepts connections, and spawns one
//! `std::thread` per connection - connection counts are single digits (a
//! handful of client sessions sharing one daemon), so thread-per-connection
//! is the right amount of machinery. There is still a ceiling on how many of
//! them a peer can ask for: see [`UnixServer::max_connections`].
//!
//! Three ways to run:
//! - [`UnixServer::serve`] - foreground, blocks (debugging/development).
//! - [`UnixServer::serve_daemon`] - double-fork daemonize, then serve.
//! - both honor [`UnixServer::idle_timeout`] - exit after N idle seconds so a
//!   model-heavy daemon doesn't linger forever after the last client leaves.
//!
//! Two things can be added to that:
//! - [`UnixServer::listener`] - more places connections may arrive from, on
//!   equal terms with the Unix socket: same connection ceiling, same idle
//!   clock, same per-connection server. See [`Listener`].
//! - [`UnixServer::busy_check`] - a veto on idle exit, for a daemon whose work
//!   outlives the client session that asked for it.
//!
//! For the whole pattern - this, the shim in front of it, and the `argv`
//! dispatch that picks between them - in one call, see
//! [`Server::serve_daemon`](crate::Server::serve_daemon).
//!
//! Unix-only: gated behind `#[cfg(unix)]`, no feature flag.

use crate::server::{Server, ServerConfig};
use crate::socket::{SocketLock, ensure_private_parent_dir, owned_by_current_user};
use crate::transport::daemon_state::{
    ActiveGuard, Admission, BusyCheck, ConnState, Listeners, Shared, admit, describe, idle_watcher,
    release, request_shutdown, shutdown_requested,
};
use crate::transport::signals::install_signal_handlers;
use crate::transport::{Listener, Transport, UnixSocketListener};
use crate::types::{McpError, Result};
use std::io::{self, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

/// Setup function type for configuring tools on each connection.
type SetupFn<C> = Box<dyn Fn(&mut Server<C>) -> Result<()> + Send + Sync>;

/// Live connections allowed at once, by default.
///
/// Each one costs a thread and two descriptors, so this is the ceiling on what
/// a client that reconnects in a loop can make this process hold. Well above
/// the handful of sessions a daemon really sees, and well below the descriptor
/// limit a shell hands out - the point is that the number is ours and not the
/// peer's. Lower than the HTTP server's because a local daemon serves the
/// people logged into one machine, not the internet.
pub const DEFAULT_MAX_CONNECTIONS: usize = 64;

/// How long to wait after an `accept` that failed, or a thread the OS would
/// not give us, before trying again.
///
/// Long enough that a descriptor shortage cannot become a hot loop, short
/// enough that a daemon recovers the moment there is room again.
const ACCEPT_RETRY: Duration = Duration::from_millis(10);

/// Monotonic source of per-connection identifiers.
static CONN_COUNTER: AtomicU64 = AtomicU64::new(0);

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
///     .serve_daemon(user_socket_path("myapp", "server.sock"), |conn_id| {
///         AppContext::new(conn_id)
///     })?;
/// ```
pub struct UnixServer<C> {
    config: ServerConfig,
    setup: Option<SetupFn<C>>,
    idle_timeout: Option<Duration>,
    max_connections: usize,
    /// Listeners beyond the Unix socket. Empty is the default and is exactly
    /// the daemon this type has always been.
    listeners: Listeners,
    /// The application's veto on idle exit. `None` means the idle clock is the
    /// only thing consulted, which is what it always was.
    busy_check: Option<BusyCheck>,
}

impl<C: Send + Sync + 'static> UnixServer<C> {
    /// Create a new Unix server with the given configuration.
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config,
            setup: None,
            idle_timeout: None,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            listeners: Vec::new(),
            busy_check: None,
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
    ///
    /// "Idle" means no live connections, on any listener. A daemon whose work
    /// carries on after the client that asked for it has gone wants
    /// [`busy_check`](Self::busy_check) as well.
    pub fn idle_timeout(mut self, duration: Duration) -> Self {
        self.idle_timeout = Some(duration);
        self
    }

    /// How many connections may be live at once.
    ///
    /// Default: [`DEFAULT_MAX_CONNECTIONS`]. A connection over the ceiling is
    /// closed as soon as it is accepted, which a client sees as an immediate
    /// end-of-file and can retry; the daemon says nothing on the socket,
    /// because the accept loop is the one thread that must never wait on a
    /// peer's reading pace.
    ///
    /// One ceiling for the daemon, counted across every listener.
    ///
    /// Zero is raised to one - a daemon that serves nobody is not a
    /// configuration anyone means.
    pub fn max_connections(mut self, connections: usize) -> Self {
        self.max_connections = connections.max(1);
        self
    }

    /// Accept connections from here as well as from the Unix socket.
    ///
    /// May be called more than once. Every listener shares this daemon's
    /// connection ceiling, its idle clock, and its per-connection server
    /// setup - the only thing that differs is where the bytes came from.
    ///
    /// The listener is given a thread of its own; the Unix socket keeps the
    /// calling thread, so [`serve`](Self::serve) blocks exactly as before.
    ///
    /// ```ignore
    /// UnixServer::new(config)
    ///     .listener(TcpSocketListener::bind("127.0.0.1:7743")?)
    ///     .serve(socket_path, |conn_id| AppContext::new(conn_id))
    /// ```
    ///
    /// See [`Listener`] for what implementing one costs - it is how a daemon
    /// gets mTLS without this crate having a TLS dependency.
    pub fn listener(self, listener: impl Listener) -> Self {
        self.shared_listener(Arc::new(listener))
    }

    /// [`listener`](Self::listener) for one that is already shared.
    ///
    /// What [`DaemonOptions`](crate::DaemonOptions) hands over, having boxed
    /// the listener when it was given one.
    pub(crate) fn shared_listener(mut self, listener: Arc<dyn Listener>) -> Self {
        self.listeners.push(listener);
        self
    }

    /// Let the application veto idle exit while it still has work in hand.
    ///
    /// [`idle_timeout`](Self::idle_timeout) counts connections, which is the
    /// right measure for a daemon that only works while a client is asking it
    /// to. A daemon supervising something that outlives the session - a child
    /// process, a background job - would idle out from under it. With a busy
    /// check installed, the daemon exits only when it has been idle for the
    /// whole window **and** `is_busy` answers `false`.
    ///
    /// ```ignore
    /// let agents = registry.clone();
    /// UnixServer::new(config)
    ///     .idle_timeout(Duration::from_secs(300))
    ///     .busy_check(move || agents.any_running())
    /// ```
    ///
    /// The details worth knowing:
    ///
    /// - It is asked only at the moment the daemon is about to exit for being
    ///   idle, and only then. A veto costs one more idle window: the watcher
    ///   re-arms and asks again after `idle_timeout`, so the poll interval is
    ///   that timeout and not a second knob.
    /// - **SIGTERM and SIGINT are not vetoable.** "Stop now" means now; this
    ///   is a guard on the daemon's own decision to leave, not on the
    ///   operator's.
    /// - It runs on the idle watcher's thread with no lock of ours held, so it
    ///   may take whatever locks it likes - but it should be quick, since the
    ///   idle decision waits on it.
    /// - Without [`idle_timeout`](Self::idle_timeout) there is no idle exit to
    ///   veto, and this is never called.
    /// - A panic inside it kills the watcher thread, which leaves the daemon
    ///   up for good. That is the safe direction to fail - the work survives,
    ///   and an operator can still signal it - but it is a bug in the
    ///   predicate either way.
    pub fn busy_check<F>(self, is_busy: F) -> Self
    where
        F: Fn() -> bool + Send + Sync + 'static,
    {
        self.shared_busy_check(Arc::new(is_busy))
    }

    /// [`busy_check`](Self::busy_check) for a predicate that is already shared.
    pub(crate) fn shared_busy_check(mut self, is_busy: BusyCheck) -> Self {
        self.busy_check = Some(is_busy);
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
            max_connections,
            listeners: extra,
            busy_check,
        } = self;

        // The directory the socket goes in, private to this user, before
        // anything is bound in it. A daemon driven through `UnixServer`
        // directly gets the same treatment `Server::serve_daemon` arranges.
        ensure_private_parent_dir(&socket_path)?;

        // The claim on the socket path, held until this server has stopped
        // serving and unlinked its own socket (`cleanup` runs before `claim`
        // drops). Everything between here and `bind` - the staleness probe,
        // the unlink - happens under it, so no other server can misread this
        // one mid-bind as stale and delete its socket. See [`SocketLock`].
        let claim = claim_socket_path(&socket_path)?;

        // Clear a stale socket (or refuse if a live server owns it).
        prepare_socket_path(&socket_path, &claim)?;

        let bound = UnixListener::bind(&socket_path).map_err(|e| {
            McpError::Internal(format!("failed to bind {}: {}", socket_path.display(), e))
        })?;

        // Primary first: see `Listeners`.
        let mut listeners: Listeners = Vec::with_capacity(extra.len() + 1);
        listeners.push(Arc::new(UnixSocketListener::new(bound, &socket_path)));
        listeners.extend(extra);

        let setup = setup.map(Arc::new);
        let factory = Arc::new(context_factory);
        let shared: Shared = Arc::new((Mutex::new(ConnState::default()), Condvar::new()));

        // Signal handling: SIGTERM/SIGINT flag the shutdown and wake every
        // listener, through the same path the idle watcher uses. The guard
        // restores previous handlers on drop. Installed BEFORE the PID file
        // is written so that signals are handled from the moment external
        // processes can discover the daemon's PID.
        let _signal_guard = {
            let shared = shared.clone();
            let listeners = listeners.clone();
            install_signal_handlers(move || request_shutdown(&shared, &listeners))?
        };

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
            describe(&listeners)
        );

        // Optional idle watcher: exits the daemon after `idle_timeout` of zero
        // connections, unless `busy_check` says otherwise.
        let watcher = idle_timeout.map(|timeout| {
            let shared = shared.clone();
            let listeners = listeners.clone();
            thread::spawn(move || idle_watcher(shared, timeout, listeners, busy_check))
        });

        let result = serve_listeners(
            &listeners,
            &shared,
            max_connections,
            &config,
            &setup,
            &factory,
        );

        // The idle watcher is waiting on the same flag `serve_listeners` has
        // already set on its way back here, whatever ended the daemon - so
        // this only has to wait for it.
        if let Some(w) = watcher {
            let _ = w.join();
        }

        cleanup(&socket_path, pid_path.as_deref());
        result
    }
}

/// Accept on every listener until the daemon is asked to stop.
///
/// The first listener keeps this thread - so `serve` blocks, and a daemon with
/// no extra listeners spawns nothing it did not spawn before - and every other
/// one gets a thread. Returns when the primary loop does, once the rest have
/// been woken and joined.
fn serve_listeners<C, F>(
    listeners: &Listeners,
    shared: &Shared,
    max_connections: usize,
    config: &ServerConfig,
    setup: &Option<Arc<SetupFn<C>>>,
    factory: &Arc<F>,
) -> Result<()>
where
    C: Send + Sync + 'static,
    F: Fn(&str) -> C + Send + Sync + 'static,
{
    let (primary, extra) = listeners
        .split_first()
        .expect("a daemon always has its Unix socket");

    let mut accepting = Vec::with_capacity(extra.len());
    let mut refused = None;

    for listener in extra {
        let name = listener.describe();
        let listener = listener.clone();
        let shared = shared.clone();
        let config = config.clone();
        let setup = setup.clone();
        let factory = factory.clone();
        let reported = name.clone();

        let spawned = thread::Builder::new()
            .name(format!("accept-{}", name))
            .spawn(move || {
                let outcome = accept_loop(
                    &*listener,
                    &shared,
                    max_connections,
                    &config,
                    &setup,
                    &factory,
                );
                if let Err(e) = outcome {
                    eprintln!("listener {} stopped: {}", reported, e);
                }
            });

        match spawned {
            Ok(handle) => accepting.push(handle),
            // A daemon that was told to listen somewhere and cannot is a
            // daemon running a configuration nobody asked for. Better to say
            // so than to serve half of it and look healthy.
            Err(e) => {
                refused = Some(McpError::Internal(format!(
                    "failed to spawn an accept thread for {}: {}",
                    name, e
                )));
                break;
            }
        }
    }

    let result = match refused {
        Some(e) => Err(e),
        None => accept_loop(&**primary, shared, max_connections, config, setup, factory),
    };

    // Whatever ended this, the other listeners are still parked in `accept`.
    request_shutdown(shared, listeners);
    for handle in accepting {
        let _ = handle.join();
    }

    result
}

/// Main accept loop. Returns `Ok(())` on a clean shutdown break, `Err` only
/// where the daemon's own bookkeeping has gone wrong.
fn accept_loop<C, F>(
    listener: &dyn Listener,
    shared: &Shared,
    max_connections: usize,
    config: &ServerConfig,
    setup: &Option<Arc<SetupFn<C>>>,
    factory: &Arc<F>,
) -> Result<()>
where
    C: Send + Sync + 'static,
    F: Fn(&str) -> C + Send + Sync + 'static,
{
    loop {
        let transport = match listener.accept() {
            Ok(transport) => transport,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            // Out of descriptors, a client that hung up between connecting
            // and being accepted, a handshake the peer failed. None is a
            // reason to stop listening, and none is a reason to spin: a hot
            // loop on `EMFILE` is its own outage. This used to end the daemon
            // on the first one, which is exactly what a reconnect loop had to
            // do to take it down.
            Err(_) => {
                thread::sleep(ACCEPT_RETRY);
                if shutdown_requested(shared) {
                    break;
                }
                continue;
            }
        };

        match admit(shared, max_connections)? {
            Admission::Admitted => {}
            // Refused by closing: the client sees end-of-file at once and can
            // come back. Saying more would mean writing to a peer from the one
            // thread that must never wait on one.
            Admission::Full => {
                drop(transport);
                continue;
            }
            // The idle watcher or a signal woke us on the way out.
            Admission::ShuttingDown => break,
        }

        let conn_id = format!("conn-{}", CONN_COUNTER.fetch_add(1, Ordering::SeqCst));
        let cfg = config.clone();
        let setup = setup.clone();
        let factory = factory.clone();
        let shared_thread = shared.clone();

        let spawn = thread::Builder::new().name(conn_id.clone()).spawn(move || {
            handle_connection(transport, conn_id, cfg, setup, factory, shared_thread)
        });

        if let Err(e) = spawn {
            eprintln!("failed to spawn connection thread: {}", e);
            // We took a slot above but no ActiveGuard will run - give it back.
            release(shared);
            // A thread the OS will not give us is the platform saying it is
            // full, and asking again immediately is how that becomes a spin.
            thread::sleep(ACCEPT_RETRY);
        }
    }
    Ok(())
}

/// Per-connection worker: build a fresh server + context, then run the
/// blocking MCP loop until the client disconnects.
fn handle_connection<C, F>(
    transport: Box<dyn Transport>,
    conn_id: String,
    config: ServerConfig,
    setup: Option<Arc<SetupFn<C>>>,
    factory: Arc<F>,
    shared: Shared,
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

    if let Err(e) = server.start(transport, context) {
        eprintln!("[{}] connection closed: {}", conn_id, e);
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

/// Take the claim on the socket path, or say who beat us to it.
///
/// A refusal is not retried and not waited on: `Bridge::auto_start` - the
/// thing that spawns daemons - ignores the loser's exit and polls the socket,
/// which the winner binds milliseconds later. The losing daemon standing down
/// with an error *is* the machine converging on one daemon.
fn claim_socket_path(path: &Path) -> Result<SocketLock> {
    if let Some(claim) = SocketLock::acquire(path)? {
        return Ok(claim);
    }

    // Somebody holds the claim. If the socket answers, it is the error this
    // crate has always given for a running daemon; if it does not, the holder
    // is between deciding to serve and binding - exactly the state that used
    // to be misread as stale.
    if UnixStream::connect(path).is_ok() {
        Err(McpError::Internal(format!(
            "socket {} is already in use by a live server",
            path.display()
        )))
    } else {
        Err(McpError::Internal(format!(
            "socket {} is being claimed by another starting server - refusing to bind over it",
            path.display()
        )))
    }
}

/// Clear the socket path before binding.
///
/// Three answers: a path belonging to another account is refused outright, a
/// live server's socket is refused rather than clobbered, and a stale socket
/// file is removed.
///
/// Requires the caller to hold the path's [`SocketLock`], and that is what
/// makes the one-connect staleness check below sound: a cooperating daemon
/// between `bind` and `listen` - which refuses a connect exactly the way an
/// abandoned socket does - would be holding the claim, so it can never be
/// observed here. A refusal under the claim is the leftovers of a dead
/// process, and removing it is correct. (A listener that never took the
/// claim, such as a pre-claim build or a foreign process, is still caught by
/// the connect when it is listening; its own bind-to-listen window is the one
/// thing that cannot be closed from this side.)
fn prepare_socket_path(path: &Path, _claim: &SocketLock) -> Result<()> {
    // `symlink_metadata` rather than `exists`, which follows links and so says
    // "nothing here" about a dangling symlink that `bind` will refuse.
    if std::fs::symlink_metadata(path).is_err() {
        return Ok(());
    }

    // Someone else's file where our socket goes. Removing it is not ours to do
    // - and in a sticky directory it would fail anyway, with an error about
    // permissions rather than about what is actually wrong.
    if owned_by_current_user(path) == Some(false) {
        return Err(McpError::Internal(format!(
            "socket {} belongs to another user - refusing to serve on it",
            path.display()
        )));
    }

    match UnixStream::connect(path) {
        Ok(_) => Err(McpError::Internal(format!(
            "socket {} is already in use by a live server",
            path.display()
        ))),
        Err(_) => {
            // Nothing listening, nobody mid-bind (they would hold the claim
            // we are holding) - stale socket, safe to remove.
            std::fs::remove_file(path)?;
            Ok(())
        }
    }
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
    use std::sync::atomic::AtomicUsize;
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

    /// Spawn a server on `path` that will hold at most `max` connections.
    fn spawn_bounded_server(
        path: &Path,
        max: usize,
        store: Arc<StdMutex<HashMap<String, i64>>>,
    ) -> thread::JoinHandle<Result<()>> {
        let path = path.to_path_buf();
        thread::spawn(move || {
            UnixServer::new(test_config())
                .max_connections(max)
                .idle_timeout(Duration::from_secs(10))
                .with_tools(|s: &mut Server<TestContext>| {
                    s.add_tool(WhoamiTool)?;
                    Ok(())
                })
                .serve(&path, move |conn_id| TestContext {
                    conn_id: conn_id.to_string(),
                    store: store.clone(),
                })
        })
    }

    /// Connect and handshake, retrying while the server is still full.
    ///
    /// A refused connection is accepted and closed, so the refusal shows up on
    /// the first read rather than on connect.
    fn connect_admitted(path: &Path) -> UnixTransport {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let mut client = connect_retry(path);
            let request = JsonRpcMessage::request(
                1,
                "initialize",
                Some(serde_json::json!({
                    "protocolVersion": "2025-03-26",
                    "capabilities": {},
                    "clientInfo": { "name": "test", "version": "1.0" }
                })),
            );
            if client.write(&request).is_ok()
                && matches!(client.read(), Ok(JsonRpcMessage::Response(_)))
            {
                return client;
            }

            assert!(
                Instant::now() < deadline,
                "a slot never came back on {}",
                path.display()
            );
            thread::sleep(Duration::from_millis(20));
        }
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
    fn test_the_builder_will_not_take_a_ceiling_of_zero() {
        let server: UnixServer<()> = UnixServer::new(test_config()).max_connections(0);
        assert_eq!(server.max_connections, 1);

        let server: UnixServer<()> = UnixServer::new(test_config()).max_connections(9);
        assert_eq!(server.max_connections, 9);
    }

    #[test]
    fn test_a_server_has_a_ceiling_without_being_asked_for_one() {
        // The whole finding: an unbounded daemon is one a reconnect loop walks
        // to `EMFILE`, and `accept` failing there used to end the process.
        let server: UnixServer<()> = UnixServer::new(test_config());
        assert_eq!(server.max_connections, DEFAULT_MAX_CONNECTIONS);
        const { assert!(DEFAULT_MAX_CONNECTIONS >= 1) };
    }

    #[test]
    fn test_an_accept_that_fails_does_not_end_the_daemon() {
        // A non-blocking listener fails every `accept` with `WouldBlock`, which
        // is the shape of the `EMFILE` a reconnect loop used to walk a daemon
        // into. The loop has to keep listening through it - and still notice a
        // shutdown, rather than being wedged in the retry.
        let sock = temp_socket();
        let raw = UnixListener::bind(&sock).unwrap();
        raw.set_nonblocking(true).unwrap();
        let listener = UnixSocketListener::new(raw, &sock);

        let shared: Arc<(Mutex<ConnState>, Condvar)> =
            Arc::new((Mutex::new(ConnState::default()), Condvar::new()));
        let config = test_config();
        let store = Arc::new(StdMutex::new(HashMap::new()));
        let factory = Arc::new(move |conn_id: &str| TestContext {
            conn_id: conn_id.to_string(),
            store: store.clone(),
        });

        let loop_shared = shared.clone();
        let looping = thread::spawn(move || {
            accept_loop(
                &listener,
                &loop_shared,
                1,
                &config,
                &None::<Arc<SetupFn<TestContext>>>,
                &factory,
            )
        });

        thread::sleep(Duration::from_millis(100));
        assert!(
            !looping.is_finished(),
            "an accept that failed must not end the daemon"
        );

        {
            let (lock, cv) = &*shared;
            lock.lock().unwrap().shutdown = true;
            cv.notify_all();
        }

        let deadline = Instant::now() + Duration::from_secs(5);
        while !looping.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            looping.is_finished(),
            "a shutdown must reach a loop that is retrying a failing listener"
        );
        assert!(looping.join().unwrap().is_ok());

        let _ = std::fs::remove_file(&sock);
    }

    //
    // Signal handling lives in `crate::transport::signals` now, and so do its
    // tests: what it owes this module is one call when a signal lands, which
    // `run` turns into `request_shutdown`.
    //

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

    /// The claim on `path`, for tests exercising what happens under it.
    fn claim(path: &Path) -> SocketLock {
        SocketLock::acquire(path)
            .expect("the lock file should be creatable")
            .expect("nobody else should hold a test path's claim")
    }

    #[test]
    fn test_prepare_socket_path_removes_stale() {
        let sock = temp_socket();
        // Create a plain file at the socket path (no listener) -> stale.
        std::fs::write(&sock, b"stale").unwrap();
        assert!(sock.exists());

        prepare_socket_path(&sock, &claim(&sock)).unwrap();
        assert!(!sock.exists());
    }

    #[test]
    fn test_prepare_socket_path_refuses_live() {
        let sock = temp_socket();
        let _listener = UnixListener::bind(&sock).unwrap();

        // A live listener owns the path -> prepare must refuse, not remove.
        let result = prepare_socket_path(&sock, &claim(&sock));
        assert!(result.is_err());
        assert!(sock.exists());

        let _ = std::fs::remove_file(&sock);
    }

    #[test]
    fn test_prepare_socket_path_refuses_a_path_it_does_not_own() {
        // The other half of the shim's refusal: neither side races the other to
        // bind a path that belongs to a third account. `/` stands in for one -
        // it is root's on every machine this runs on. No claim can be taken on
        // `/`, so the witness comes from a path of our own; the refusal under
        // test is about ownership, not the claim.
        if unsafe { libc::getuid() } == 0 {
            return;
        }

        let error = prepare_socket_path(Path::new("/"), &claim(&temp_socket()))
            .expect_err("a path this user does not own must not be served on");

        assert!(
            error.to_string().contains("belongs to another user"),
            "the reason has to name the actual problem, not whatever \
             `remove_file` said about permissions: {error}"
        );
    }

    #[test]
    fn test_prepare_socket_path_has_nothing_to_do_with_a_free_path() {
        let sock = temp_socket();
        assert!(!sock.exists());

        prepare_socket_path(&sock, &claim(&sock)).unwrap();

        assert!(!sock.exists(), "and nothing is created in passing");
    }

    //
    // The claim on the socket path
    //

    #[test]
    fn test_a_mid_bind_socket_survives_a_racing_server() {
        // The orphaned-daemon bug, held open instead of raced: a socket that
        // is bound but not yet listening refuses a connect exactly the way a
        // stale socket file does. The old code took one such refusal as proof
        // of staleness and unlinked a live daemon's socket out from under it,
        // leaving that daemon serving a nameless inode forever. Under the
        // claim, the loser never gets to ask the misleading question.
        use crate::socket::test_support::MidBindSocket;

        let sock = temp_socket();
        let winner = claim(&sock); // what a daemon holds while it binds
        let mid_bind = MidBindSocket::bind(&sock);

        let error =
            claim_socket_path(&sock).expect_err("a second server must be stopped at the claim");
        assert!(
            error
                .to_string()
                .contains("being claimed by another starting server"),
            "{error}"
        );
        assert!(
            sock.exists(),
            "the winner's socket file must survive the loser's attempt"
        );

        // The winner finishes its bind undisturbed, and is reachable at its
        // own path - the outcome the old code destroyed.
        mid_bind.listen();
        assert!(
            UnixStream::connect(&sock).is_ok(),
            "the winner must still be reachable where it bound"
        );

        drop(winner);
        let _ = std::fs::remove_file(&sock);
    }

    #[test]
    fn test_a_live_server_is_refused_by_name() {
        // A serving daemon holds its claim for life, so a second server is
        // refused at the claim - with the same words this crate has always
        // used for a socket that answers.
        let sock = temp_socket();
        let store = Arc::new(StdMutex::new(HashMap::new()));
        let _server = spawn_server(&sock, Some(Duration::from_secs(10)), store);
        let mut client = connect_retry(&sock);
        initialize(&mut client, 1);

        let error =
            claim_socket_path(&sock).expect_err("a socket being served must not be claimable");
        assert!(
            error
                .to_string()
                .contains("already in use by a live server"),
            "{error}"
        );
        assert!(sock.exists());

        let _ = std::fs::remove_file(&sock);
    }

    #[test]
    fn test_the_claim_is_released_with_the_server() {
        // Restarts depend on this: a daemon that exited - here by idling out -
        // must leave the path claimable for its successor.
        let sock = temp_socket();
        let store = Arc::new(StdMutex::new(HashMap::new()));
        let server = spawn_server(&sock, Some(Duration::from_millis(300)), store);
        {
            let mut client = connect_retry(&sock);
            initialize(&mut client, 1);
            // drops -> idle countdown -> exit
        }

        let deadline = Instant::now() + Duration::from_secs(5);
        while !server.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(25));
        }
        assert!(server.is_finished(), "the server should have idled out");
        assert!(server.join().unwrap().is_ok());

        assert!(
            SocketLock::acquire(&sock).unwrap().is_some(),
            "a stopped server must have released its claim"
        );
    }

    #[test]
    fn test_a_dead_servers_leftovers_do_not_block_a_restart() {
        // What a `kill -9` leaves behind: a socket file nobody answers on and
        // a lock file nobody holds - the kernel released the flock with the
        // process. A new server must claim the path, clear the corpse, and
        // serve.
        let sock = temp_socket();
        std::fs::write(&sock, b"stale").unwrap();
        let mut lock = sock.clone();
        lock.set_extension("lock");
        std::fs::write(&lock, b"").unwrap();

        let store = Arc::new(StdMutex::new(HashMap::new()));
        let _server = spawn_server(&sock, Some(Duration::from_secs(10)), store);
        let mut client = connect_retry(&sock);
        initialize(&mut client, 1);

        let _ = std::fs::remove_file(&sock);
    }

    #[test]
    fn test_serving_creates_the_socket_directory_privately() {
        // A socket is only as private as the directory holding it, and a
        // `UnixServer` driven directly used to leave that entirely to the
        // caller - including the case where the directory did not exist and the
        // bind simply failed.
        use std::os::unix::fs::PermissionsExt;

        let base = std::env::temp_dir().join(format!(
            "sml_mcps_unix_private_{}_{}",
            std::process::id(),
            CONN_COUNTER.load(Ordering::SeqCst)
        ));
        let sock = base.join("nested/server.sock");
        let store = Arc::new(StdMutex::new(HashMap::new()));
        let _server = spawn_server(&sock, Some(Duration::from_secs(10)), store);

        let mut client = connect_retry(&sock);
        initialize(&mut client, 1);

        let mode = std::fs::metadata(sock.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o700, "{mode:o}");

        drop(client);
        let _ = std::fs::remove_dir_all(&base);
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
    fn test_a_full_server_refuses_rather_than_absorbing() {
        // A daemon that accepts everything is a daemon a reconnect loop turns
        // into a thread-and-descriptor pile until `accept` fails - which is
        // where the process used to exit.
        let sock = temp_socket();
        let store = Arc::new(StdMutex::new(HashMap::new()));
        let _server = spawn_bounded_server(&sock, 1, store);

        let mut held = connect_admitted(&sock);

        let mut refused = connect_retry(&sock);
        let request = JsonRpcMessage::request(1, "tools/list", None);
        let _ = refused.write(&request);
        assert!(
            matches!(refused.read(), Err(McpError::TransportClosed)),
            "the connection over the ceiling must be closed, not queued"
        );

        // And the one holding the slot is untouched by the refusal.
        assert!(
            call_tool(&mut held, 2, "whoami", serde_json::json!({})).starts_with("conn-"),
            "refusing a peer must not disturb the one being served"
        );

        let _ = std::fs::remove_file(&sock);
    }

    #[test]
    fn test_the_slot_comes_back_when_a_connection_ends() {
        let sock = temp_socket();
        let store = Arc::new(StdMutex::new(HashMap::new()));
        let _server = spawn_bounded_server(&sock, 1, store);

        let first = connect_admitted(&sock);
        let first_id = {
            let mut first = first;
            let id = call_tool(&mut first, 2, "whoami", serde_json::json!({}));
            drop(first);
            id
        };

        // A new client gets in once the old connection's thread has ended -
        // which is `ActiveGuard` giving the slot back.
        let mut second = connect_admitted(&sock);
        let second_id = call_tool(&mut second, 2, "whoami", serde_json::json!({}));

        assert_ne!(first_id, second_id, "a new connection, not the old one");

        let _ = std::fs::remove_file(&sock);
    }

    #[test]
    fn test_a_refusal_is_not_a_client_arriving() {
        // While the ceiling is met, every further connection is refused - and a
        // refusal must not touch the generation counter, which is what cancels
        // the idle countdown. Otherwise a reconnect loop could hold a daemon
        // nobody is using open indefinitely.
        let sock = temp_socket();
        let store = Arc::new(StdMutex::new(HashMap::new()));
        let _server = spawn_bounded_server(&sock, 1, store);

        let mut held = connect_admitted(&sock);

        for _ in 0..5 {
            let mut knock = connect_retry(&sock);
            let _ = knock.write(&JsonRpcMessage::request(1, "tools/list", None));
            assert!(
                matches!(knock.read(), Err(McpError::TransportClosed)),
                "every connection past the ceiling is refused, not just the first"
            );
        }

        assert!(call_tool(&mut held, 2, "whoami", serde_json::json!({})).starts_with("conn-"));

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

    //
    // What the builder holds
    //

    #[test]
    fn test_a_server_has_no_extra_listeners_and_no_busy_check_unless_asked() {
        // The default is the daemon this type has always been.
        let server: UnixServer<()> = UnixServer::new(test_config());

        assert!(server.listeners.is_empty());
        assert!(server.busy_check.is_none());
        assert!(server.idle_timeout.is_none());
    }

    #[test]
    fn test_listeners_accumulate_in_the_order_they_were_added() {
        let woken = Arc::new(AtomicUsize::new(0));
        let server: UnixServer<()> = UnixServer::new(test_config())
            .listener(crate::transport::daemon_state::tests::RecordingListener {
                woken: woken.clone(),
            })
            .listener(crate::transport::daemon_state::tests::RecordingListener { woken });

        assert_eq!(server.listeners.len(), 2);
    }

    #[test]
    fn test_a_busy_check_is_kept_as_given() {
        let server: UnixServer<()> = UnixServer::new(test_config()).busy_check(|| true);

        let check = server.busy_check.expect("the predicate should be held");
        assert!(check(), "and it should be the one that was handed over");
    }
}
