//! One-call daemon/shim dispatch for a [`Server`].
//!
//! The daemon/shim pattern needs one binary to behave three ways - detached
//! daemon, foreground daemon, stdio shim - and wiring that up by hand means
//! reading `argv`, building a [`UnixServer`], re-registering every tool inside
//! its setup closure, and driving [`Bridge`] on the other branch. That is
//! `examples/unix_server.rs`, and it is the same forty lines in every server
//! that wants the pattern.
//!
//! [`Server::serve_daemon`] is those forty lines. Build the server as usual,
//! then hand it a socket path, an idle timeout, and a context factory.
//!
//! [`Server::serve_daemon_with`] is the same dispatch with everything else the
//! daemon can be told: more listeners, a connection ceiling, and a veto on
//! idle exit. See [`DaemonOptions`].
//!
//! A fourth mode, `--connect <host:port>`, makes the shim dial a daemon on
//! another machine over TCP instead of the Unix socket on this one. See
//! [`crate::remote`] for what that does and does not do.
//!
//! Unix-only: gated behind `#[cfg(unix)]`, like [`UnixServer`] and [`Bridge`].

use crate::bridge::Bridge;
use crate::remote::{RemoteAddr, connect_from_args};
use crate::server::Server;
use crate::socket::ensure_private_parent_dir;
use crate::transport::daemon_state::BusyCheck;
use crate::transport::{Listener, StdioTransport, Transport, UnixServer};
use crate::types::{McpError, Result};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// Launch this binary as the detached daemon.
const DAEMON_FLAG: &str = "--daemon";

/// Launch this binary as a daemon that stays attached to the terminal.
const FOREGROUND_FLAG: &str = "--foreground";

/// Names the socket the daemon owns, so a binary that parses its own `argv`
/// reaches the same path the shim that spawned it is waiting on.
const SOCKET_FLAG: &str = "--socket";

/// Launch this binary as a shim to a daemon on another machine.
///
/// Defined in [`crate::remote`] rather than here because the CLI harness
/// writes and reads the same flag, and the harness is not Unix-only.
const CONNECT_FLAG: &str = crate::remote::CONNECT_FLAG;

/// Which of the four jobs this process was launched to do.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Mode {
    /// Detach from the terminal and serve.
    Daemon,
    /// Serve without detaching.
    Foreground,
    /// Proxy stdio to a daemon, starting one if none is up.
    Shim,
    /// Proxy stdio to a daemon on another machine, over TCP. Never starts
    /// one, and never looks at the socket path.
    Remote(RemoteAddr),
}

/// Everything a [`serve_daemon`](Server::serve_daemon) daemon can be told.
///
/// [`Server::serve_daemon`] takes the three arguments a daemon always needs.
/// This is the same thing with the rest of them, for a daemon that listens in
/// more than one place or has an opinion about when it is allowed to exit:
///
/// ```ignore
/// server.serve_daemon_with(
///     DaemonOptions::new(user_socket_path("my-daemon", "server.sock"))
///         .idle_timeout(Duration::from_secs(300))
///         .listener(TcpSocketListener::bind("100.72.37.20:7743")?)
///         .busy_check({
///             let jobs = jobs.clone();
///             move || jobs.any_running()
///         }),
///     move |conn_id| AppContext::new(conn_id, state.clone()),
/// )
/// ```
///
/// Every field but the socket path is optional, and the defaults are what a
/// daemon built without this gets - with one difference worth knowing:
/// [`idle_timeout`](Self::idle_timeout) here defaults to *never* idling out,
/// which is what a resident service wants, while
/// [`serve_daemon`](Server::serve_daemon) requires one.
///
/// # Remote mode
///
/// A [`TcpSocketListener`](crate::TcpSocketListener) given to
/// [`listener`](Self::listener) is what a shim on *another* machine dials
/// with `--connect <host:port>`. Nothing here configures that shim: it never
/// spawns a daemon, never touches the socket path, never reads a PID file,
/// and never consults local configuration. The daemon on the far end owns all
/// of that. See [`Server::serve_daemon_with`].
///
/// **TCP is in the clear.** A daemon listening on TCP is reachable by anything
/// that can route to it, and every message crosses unencrypted. Bind it to an
/// interface only the people you trust can reach - a Tailscale address, say -
/// never a public one. See [`TcpTransport`](crate::TcpTransport).
pub struct DaemonOptions {
    socket_path: PathBuf,
    idle_timeout: Option<Duration>,
    max_connections: Option<usize>,
    listeners: Vec<Arc<dyn Listener>>,
    busy_check: Option<BusyCheck>,
}

impl DaemonOptions {
    /// A daemon on `socket_path`, with everything else left at its default.
    ///
    /// The path decides where the daemon binds in every mode, and where the
    /// shim looks for it - the same argument
    /// [`serve_daemon`](Server::serve_daemon) takes.
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
            idle_timeout: None,
            max_connections: None,
            listeners: Vec::new(),
            busy_check: None,
        }
    }

    /// Exit after this long with no connections. Default: never.
    ///
    /// See [`UnixServer::idle_timeout`].
    pub fn idle_timeout(mut self, duration: Duration) -> Self {
        self.idle_timeout = Some(duration);
        self
    }

    /// How many connections may be live at once, across every listener.
    ///
    /// Default: [`DEFAULT_MAX_CONNECTIONS`](crate::transport::DEFAULT_MAX_CONNECTIONS).
    /// See [`UnixServer::max_connections`].
    pub fn max_connections(mut self, connections: usize) -> Self {
        self.max_connections = Some(connections);
        self
    }

    /// Accept connections from here as well as from the Unix socket.
    ///
    /// May be called more than once. See [`UnixServer::listener`] and
    /// [`Listener`].
    pub fn listener(mut self, listener: impl Listener) -> Self {
        self.listeners.push(Arc::new(listener));
        self
    }

    /// Let the application veto idle exit while it still has work in hand.
    ///
    /// See [`UnixServer::busy_check`], which this is passed straight through
    /// to - including that a signal is not vetoable, and that without an
    /// [`idle_timeout`](Self::idle_timeout) there is nothing to veto.
    pub fn busy_check<F>(mut self, is_busy: F) -> Self
    where
        F: Fn() -> bool + Send + Sync + 'static,
    {
        self.busy_check = Some(Arc::new(is_busy));
        self
    }

    /// Hand these settings to the server that will run them.
    fn configure<C: Send + Sync + 'static>(self, mut daemon: UnixServer<C>) -> UnixServer<C> {
        if let Some(timeout) = self.idle_timeout {
            daemon = daemon.idle_timeout(timeout);
        }
        if let Some(connections) = self.max_connections {
            daemon = daemon.max_connections(connections);
        }
        if let Some(is_busy) = self.busy_check {
            daemon = daemon.shared_busy_check(is_busy);
        }
        for listener in self.listeners {
            daemon = daemon.shared_listener(listener);
        }

        daemon
    }
}

/// Names what was configured, without pretending it can show a closure.
impl fmt::Debug for DaemonOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DaemonOptions")
            .field("socket_path", &self.socket_path)
            .field("idle_timeout", &self.idle_timeout)
            .field("max_connections", &self.max_connections)
            .field(
                "listeners",
                &self
                    .listeners
                    .iter()
                    .map(|l| l.describe())
                    .collect::<Vec<_>>(),
            )
            .field("busy_check", &self.busy_check.is_some())
            .finish()
    }
}

impl<C: Send + Sync + 'static> Server<C> {
    /// Serve as a Unix-socket daemon, or as the shim in front of one, according
    /// to how this process was launched.
    ///
    /// One binary, four modes, chosen by what it finds in `argv` (the program
    /// name is not examined):
    ///
    /// | argument | what this call does |
    /// |---|---|
    /// | `--daemon` | double-fork, detach, and serve on `socket_path` |
    /// | `--foreground` | serve on `socket_path` without detaching - for debugging |
    /// | `--connect <host:port>` | act as a *remote* shim: dial a daemon on another machine over TCP and proxy stdio to it |
    /// | *none of these* | act as a shim: connect to the daemon, starting one if none is up, then proxy stdio to it |
    ///
    /// `--connect` with `--daemon`, `--foreground` or `--socket` is an
    /// argument error, not a choice made on the caller's behalf. See
    /// [`serve_daemon_with`](Self::serve_daemon_with) for what remote mode
    /// does and does not do.
    ///
    /// The shim relaunches *this* executable as
    /// `<current_exe> --daemon --socket <socket_path>`, so an MCP client only
    /// ever has to know the one binary, and the daemon outlives the client that
    /// happened to start it. `socket_path`'s parent directories are created if
    /// they are missing, before any forking, so a directory that cannot be
    /// created is reported to whoever ran the command rather than lost in a
    /// process that has already detached.
    ///
    /// Every connection gets a fresh `Server` carrying the tools, resources,
    /// prompts and task runtime registered on this one, plus its own context
    /// from `context_factory` - state shared between connections is state the
    /// factory hands out (an `Arc`), which is the point of the daemon.
    ///
    /// # Example
    /// ```ignore
    /// fn main() -> sml_mcps::Result<()> {
    ///     let counter = Arc::new(AtomicI64::new(0));
    ///
    ///     let mut server: Server<AppContext> = Server::new(config());
    ///     server.add_tool(IncrementTool)?;
    ///
    ///     server.serve_daemon(
    ///         // `~/.local/state/myapp/server.sock`, or the system's runtime
    ///         // directory where there is one. Not `/tmp`: a socket there is
    ///         // a path any account on the machine can bind first.
    ///         user_socket_path("myapp", "server.sock"),
    ///         Duration::from_secs(300),
    ///         move |conn_id| AppContext {
    ///             conn_id: conn_id.to_string(),
    ///             counter: counter.clone(),
    ///         },
    ///     )
    /// }
    /// ```
    ///
    /// # Notes
    ///
    /// - `socket_path` decides the path in every mode. The `--socket` flag is
    ///   passed to the daemon so a binary that reads its own `argv` (and `ps`)
    ///   agrees about which socket it owns, but this call never reads it back -
    ///   parse it yourself if the path is a thing your users choose.
    /// - The daemon is launched with exactly `--daemon --socket <socket_path>`.
    ///   A shim's *other* arguments are not forwarded, so a server configured
    ///   through `argv` (`--model /big/model.gguf`) has to carry those to the
    ///   daemon itself - through the environment, a config file, or by driving
    ///   [`UnixServer`] and [`Bridge`](crate::Bridge) directly.
    /// - `idle_timeout` is how long the daemon stays up after its last client
    ///   disconnects. Reconnecting cancels the countdown.
    /// - In shim mode the server you built is dropped without serving, so keep
    ///   expensive setup - loading a model, opening a database - inside
    ///   `context_factory`, which only the daemon ever calls.
    pub fn serve_daemon<F>(
        self,
        socket_path: impl AsRef<Path>,
        idle_timeout: Duration,
        context_factory: F,
    ) -> Result<()>
    where
        F: Fn(&str) -> C + Send + Sync + 'static,
    {
        self.serve_daemon_with(
            DaemonOptions::new(socket_path.as_ref()).idle_timeout(idle_timeout),
            context_factory,
        )
    }

    /// [`serve_daemon`](Self::serve_daemon) with everything else the daemon
    /// can be told.
    ///
    /// The same three-mode `argv` dispatch, over a daemon that may listen in
    /// more than one place and may refuse to idle out. See [`DaemonOptions`];
    /// nothing about the shim, the socket, or the modes changes.
    ///
    /// ```ignore
    /// server.serve_daemon_with(
    ///     DaemonOptions::new(user_socket_path("my-daemon", "server.sock"))
    ///         .idle_timeout(Duration::from_secs(300))
    ///         .listener(TcpSocketListener::bind("127.0.0.1:7743")?)
    ///         .busy_check(move || jobs.any_running()),
    ///     move |conn_id| AppContext::new(conn_id),
    /// )
    /// ```
    ///
    /// Extra listeners belong to the *daemon*, so a process dispatched into
    /// shim mode ignores them - it proxies stdio to whatever is already on the
    /// socket, which is the daemon that opened them.
    ///
    /// # Remote mode
    ///
    /// Launched with `--connect <host:port>`, this process is a shim to a
    /// daemon on *another* machine - one running this same call with a
    /// [`TcpSocketListener`](crate::TcpSocketListener) among its listeners.
    /// It dials the address (a MagicDNS name works as well as an IP literal;
    /// the resolver decides), gives up after a bounded connect timeout, and
    /// proxies stdio over the connection exactly as the Unix shim does.
    ///
    /// What it does *not* do, and what makes it safe to install on a machine
    /// that has no daemon of its own:
    ///
    /// - it never spawns a daemon - there is nothing here to spawn;
    /// - it never touches `socket_path`, its directory, or a PID file;
    /// - it never reads local configuration - the remote daemon owns its own;
    /// - it never retries. A daemon it cannot reach is an error naming the
    ///   address and a non-zero exit, so the outage is seen rather than waited
    ///   out.
    ///
    /// **TCP is in the clear.** Bind the daemon's TCP listener to an interface
    /// only the people you trust can reach - a Tailscale address - never a
    /// public one. See [`TcpTransport`](crate::TcpTransport).
    pub fn serve_daemon_with<F>(self, options: DaemonOptions, context_factory: F) -> Result<()>
    where
        F: Fn(&str) -> C + Send + Sync + 'static,
    {
        self.serve_daemon_from(std::env::args(), options, context_factory)
    }

    /// [`serve_daemon_with`](Self::serve_daemon_with) with the command line
    /// supplied rather than read from the process.
    ///
    /// `argv` is process-global and tests run threaded inside one process, so
    /// taking it as an argument is the only way the dispatch itself is testable.
    fn serve_daemon_from<I, F>(
        self,
        args: I,
        options: DaemonOptions,
        context_factory: F,
    ) -> Result<()>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
        F: Fn(&str) -> C + Send + Sync + 'static,
    {
        // Decided before anything is touched: a remote shim has no business
        // with the socket path, not even creating its directory.
        let mode = launch_mode(args)?;
        if let Mode::Remote(addr) = &mode {
            return remote_shim(addr, StdioTransport::new());
        }

        // Before the fork, so a directory that cannot be created is reported
        // to whoever ran the command rather than to `/dev/null`. Private to
        // this user, because a socket is as private as the directory it is in.
        ensure_private_parent_dir(&options.socket_path)?;

        match mode {
            Mode::Shim => shim(&options.socket_path, StdioTransport::new()),
            mode => self.serve_socket(mode, options, context_factory),
        }
    }

    /// Hand this server's registrations to a [`UnixServer`] and run it.
    fn serve_socket<F>(self, mode: Mode, options: DaemonOptions, context_factory: F) -> Result<()>
    where
        F: Fn(&str) -> C + Send + Sync + 'static,
    {
        // `add_tool` consumed the tools, so they cannot be registered a second
        // time on the per-connection servers the daemon builds. The blueprint
        // hands each of them the same handles instead.
        let blueprint = self.into_blueprint();

        let daemon = UnixServer::new(blueprint.config().clone()).with_tools(
            move |server: &mut Server<C>| {
                blueprint.apply(server);
                Ok(())
            },
        );

        let socket_path = options.socket_path.clone();
        let daemon = options.configure(daemon);

        match mode {
            Mode::Daemon => daemon.serve_daemon(socket_path, context_factory),
            // Neither shim reaches here - they do not need a server at all.
            Mode::Foreground | Mode::Shim | Mode::Remote(_) => {
                daemon.serve(socket_path, context_factory)
            }
        }
    }
}

/// Which mode a command line asks for, or why it cannot be answered.
///
/// [`mode_from_args`] reads the three local flags; this reads `--connect` on
/// top of it. The two disagreeing is an error rather than a precedence rule:
/// `--connect` with `--daemon` is a command line that wants this process to
/// be both the client of a remote daemon and a daemon of its own, and
/// `--connect` with `--socket` names a socket it has promised not to touch.
///
/// The program name is skipped, as everywhere else.
fn launch_mode<I>(args: I) -> Result<Mode>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let args: Vec<String> = args
        .into_iter()
        .map(|arg| arg.as_ref().to_string())
        .collect();

    let Some(addr) = connect_from_args(args.iter().skip(1)).map_err(McpError::InvalidParams)?
    else {
        return Ok(mode_from_args(&args));
    };

    let conflict = args.iter().skip(1).find_map(|arg| {
        [DAEMON_FLAG, FOREGROUND_FLAG, SOCKET_FLAG]
            .into_iter()
            .find(|flag| arg == flag || arg.starts_with(&format!("{flag}=")))
    });
    if let Some(flag) = conflict {
        return Err(McpError::InvalidParams(format!(
            "`{CONNECT_FLAG}` cannot be combined with `{flag}`: a remote shim neither runs a \
             daemon nor uses a local socket"
        )));
    }

    Ok(Mode::Remote(addr))
}

/// Which of the local modes a command line asks for, first mode flag winning.
///
/// The program name is skipped: `argv[0]` is whatever the caller was invoked
/// as, not something it was asked to do.
fn mode_from_args<I>(args: I) -> Mode
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    for arg in args.into_iter().skip(1) {
        match arg.as_ref() {
            DAEMON_FLAG => return Mode::Daemon,
            FOREGROUND_FLAG => return Mode::Foreground,
            _ => {}
        }
    }
    Mode::Shim
}

/// Proxy `client` to the daemon at `addr`, over TCP.
///
/// The remote half of [`shim`]: same [`Bridge::run`] proxy loop, different
/// upstream. There is no auto-start here by design - the daemon is on another
/// machine - so a connection that fails is the error, named and final.
fn remote_shim<T: Transport + 'static>(addr: &RemoteAddr, client: T) -> Result<()> {
    let upstream = addr.connect()?;
    Bridge::run(client, upstream)
}

/// Proxy `client` to the daemon on `socket_path`, starting one if none is up.
///
/// Generic over the client transport only so tests can drive it without
/// standing on the process's own stdin; [`Server::serve_daemon`] always passes
/// [`StdioTransport`].
fn shim<T: Transport + 'static>(socket_path: &Path, client: T) -> Result<()> {
    let exe = std::env::current_exe()
        .map_err(|e| McpError::Internal(format!("cannot find the running executable: {}", e)))?;

    // Lossy conversion would spawn the daemon on a path that is not the one
    // this shim then waits to connect to, and the wait would time out with
    // nothing to show for it.
    let exe = utf8(&exe, "the running executable")?;
    let socket = utf8(socket_path, "the socket path")?;

    let upstream = Bridge::auto_start(socket_path, exe, &[DAEMON_FLAG, SOCKET_FLAG, socket])?;
    Bridge::run(client, upstream)
}

/// A path as UTF-8, or an explanation of why it is not.
fn utf8<'a>(path: &'a Path, what: &str) -> Result<&'a str> {
    path.to_str().ok_or_else(|| {
        McpError::Internal(format!(
            "{} is not valid UTF-8, so it cannot be passed to the daemon: {}",
            what,
            path.display()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::{PromptDef, Resource, ServerConfig, Tool, ToolEnv};
    use crate::tasks::TaskConfig;
    use crate::transport::{TcpSocketListener, UnixTransport};
    use crate::types::{
        CallToolResult, Content, JsonRpcMessage, PromptArgument, PromptMessage, ResourceContent,
        ResourceTemplate, Role, TaskSupport,
    };
    use serde_json::Value;
    use std::collections::HashMap;
    use std::os::unix::net::UnixStream;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
    use std::thread;
    use std::time::Instant;

    // ---- scaffolding -----------------------------------------------------

    /// Unique temp path per call, so tests never share a socket.
    fn temp_path(suffix: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::SeqCst);
        let pid = std::process::id();
        std::env::temp_dir().join(format!("sml_mcps_daemon_{}_{}_{}", pid, n, suffix))
    }

    /// Per-connection context over a counter every connection shares.
    struct Ctx {
        conn_id: String,
        counter: Arc<AtomicI64>,
    }

    /// Bumps the shared counter - proof that connections reach one daemon.
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

    /// A tool a client may ask to run as a task.
    struct TaskableTool;
    impl Tool<Ctx> for TaskableTool {
        fn name(&self) -> &str {
            "taskable"
        }
        fn description(&self) -> &str {
            "A tool that can be called as a task"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn task_support(&self) -> TaskSupport {
            TaskSupport::Optional
        }
        fn execute(&self, _a: Value, ctx: &mut Ctx, _e: &ToolEnv) -> Result<CallToolResult> {
            let v = ctx.counter.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(CallToolResult::text(format!("{}", v)))
        }
    }

    struct GreetPrompt;
    impl PromptDef for GreetPrompt {
        fn name(&self) -> &str {
            "greet"
        }
        fn description(&self) -> Option<&str> {
            Some("Say hello")
        }
        fn arguments(&self) -> Vec<PromptArgument> {
            Vec::new()
        }
        fn get_messages(&self, _args: &HashMap<String, String>) -> Result<Vec<PromptMessage>> {
            Ok(vec![PromptMessage {
                role: Role::User,
                content: Content::text("hello"),
            }])
        }
    }

    struct NoteResource;
    impl Resource for NoteResource {
        fn uri(&self) -> String {
            "mem://note".to_string()
        }
        fn name(&self) -> String {
            "note".to_string()
        }
        fn description(&self) -> String {
            "A note".to_string()
        }
        fn mime_type(&self) -> String {
            "text/plain".to_string()
        }
        fn content(&self) -> Vec<ResourceContent> {
            vec![ResourceContent::text(self.uri(), "note body")]
        }
    }

    fn config() -> ServerConfig {
        ServerConfig {
            name: "daemon-test".into(),
            version: "1.0.0".into(),
            ..Default::default()
        }
    }

    /// A server with both tools registered, ready to be dispatched.
    fn server_with_tools() -> Server<Ctx> {
        let mut server: Server<Ctx> = Server::new(config());
        server.add_tool(IncrementTool).unwrap();
        server.add_tool(WhoamiTool).unwrap();
        server
    }

    /// Run `server` in foreground mode on `socket` and block until it accepts.
    ///
    /// Readiness is this test's premise, not its subject, so it gets a generous
    /// deadline and a hard failure.
    fn start_foreground(
        server: Server<Ctx>,
        socket: &Path,
        counter: Arc<AtomicI64>,
    ) -> thread::JoinHandle<Result<()>> {
        let socket_for_server = socket.to_path_buf();
        let handle = thread::spawn(move || {
            server.serve_daemon_from(
                ["prog", FOREGROUND_FLAG],
                DaemonOptions::new(socket_for_server).idle_timeout(Duration::from_secs(30)),
                move |conn_id| Ctx {
                    conn_id: conn_id.to_string(),
                    counter: counter.clone(),
                },
            )
        });

        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if UnixStream::connect(socket).is_ok() {
                return handle;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("daemon never started listening on {}", socket.display());
    }

    /// Send `initialize` and hand back the result payload.
    fn initialize(t: &mut UnixTransport, id: i64) -> Value {
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
        read_result(t, "initialize")
    }

    /// Send any request and hand back the result payload.
    fn request(t: &mut UnixTransport, id: i64, method: &str, params: Option<Value>) -> Value {
        t.write(&JsonRpcMessage::request(id, method, params))
            .unwrap();
        read_result(t, method)
    }

    /// Read until the answer arrives, and hand back its result payload.
    ///
    /// Notifications are skipped rather than fatal: a task worker announces its
    /// own status changes, and one of those landing between a request and its
    /// response is the server working, not the server misbehaving.
    fn read_result(t: &mut UnixTransport, method: &str) -> Value {
        loop {
            match t.read().unwrap() {
                JsonRpcMessage::Response(r) => {
                    return r.result.unwrap_or_else(|| {
                        panic!("`{method}` failed: {:?}", r.error);
                    });
                }
                JsonRpcMessage::Notification(_) => continue,
                other => panic!("expected a response to `{method}`, got {:?}", other),
            }
        }
    }

    /// Call a tool and hand back its first text content.
    fn call_tool(t: &mut UnixTransport, id: i64, name: &str) -> String {
        let result = request(
            t,
            id,
            "tools/call",
            Some(serde_json::json!({ "name": name, "arguments": {} })),
        );
        result["content"][0]["text"]
            .as_str()
            .expect("text content")
            .to_string()
    }

    // ---- mode dispatch ---------------------------------------------------

    #[test]
    fn test_daemon_flag_selects_daemon_mode() {
        assert_eq!(mode_from_args(["prog", "--daemon"]), Mode::Daemon);
    }

    #[test]
    fn test_foreground_flag_selects_foreground_mode() {
        assert_eq!(mode_from_args(["prog", "--foreground"]), Mode::Foreground);
    }

    #[test]
    fn test_no_mode_flag_is_the_shim() {
        // What an MCP client launches: the bare binary, or one carrying only a
        // socket path.
        assert_eq!(mode_from_args(["prog"]), Mode::Shim);
        assert_eq!(
            mode_from_args(["prog", "--socket", "/tmp/x.sock"]),
            Mode::Shim
        );
        assert_eq!(mode_from_args(Vec::<String>::new()), Mode::Shim);
    }

    #[test]
    fn test_a_mode_flag_is_found_anywhere_in_the_command_line() {
        assert_eq!(
            mode_from_args(["prog", "--socket", "/tmp/x.sock", "--daemon"]),
            Mode::Daemon
        );
        assert_eq!(
            mode_from_args(["prog", "-v", "--foreground", "--socket", "/tmp/x.sock"]),
            Mode::Foreground
        );
    }

    #[test]
    fn test_the_first_mode_flag_decides() {
        // Both present is a contradiction; resolving it by position is at least
        // something a user can predict.
        assert_eq!(
            mode_from_args(["prog", "--daemon", "--foreground"]),
            Mode::Daemon
        );
        assert_eq!(
            mode_from_args(["prog", "--foreground", "--daemon"]),
            Mode::Foreground
        );
    }

    #[test]
    fn test_the_program_name_is_not_a_mode_flag() {
        // argv[0] is what the caller was invoked *as*. A binary that happens to
        // be named `--daemon` is still being asked to be a shim.
        assert_eq!(mode_from_args(["--daemon"]), Mode::Shim);
        assert_eq!(mode_from_args(["--foreground"]), Mode::Shim);
    }

    #[test]
    fn test_only_the_exact_flag_counts() {
        // Near-misses stay the shim rather than silently daemonizing: an
        // MCP client launching `prog --daemonize` gets a proxy, not a server
        // holding its stdout.
        for near_miss in [
            "--daemon=true",
            "--daemonize",
            "-daemon",
            "daemon",
            "--DAEMON",
            "--foregrounded",
            " --daemon",
        ] {
            assert_eq!(
                mode_from_args(["prog", near_miss]),
                Mode::Shim,
                "`{near_miss}` should not select a mode"
            );
        }
    }

    // ---- remote mode dispatch --------------------------------------------

    /// The address `--connect` was given, if the line selected remote mode.
    fn remote(mode: Result<Mode>) -> String {
        match mode {
            Ok(Mode::Remote(addr)) => addr.to_string(),
            other => panic!("expected remote mode, got {other:?}"),
        }
    }

    #[test]
    fn test_connect_selects_remote_mode() {
        assert_eq!(
            remote(launch_mode(["prog", "--connect", "jetson-memory:7211"])),
            "jetson-memory:7211"
        );
        assert_eq!(
            remote(launch_mode(["prog", "--connect=127.0.0.1:7211"])),
            "127.0.0.1:7211"
        );
        // Anywhere in the line, like the other flags - and with the `serve`
        // an installed CLI entry puts in front of it.
        assert_eq!(
            remote(launch_mode(["prog", "serve", "--connect", "h:1"])),
            "h:1"
        );
    }

    #[test]
    fn test_without_connect_the_local_modes_are_unchanged() {
        assert_eq!(launch_mode(["prog", "--daemon"]).unwrap(), Mode::Daemon);
        assert_eq!(
            launch_mode(["prog", "--foreground"]).unwrap(),
            Mode::Foreground
        );
        assert_eq!(launch_mode(["prog"]).unwrap(), Mode::Shim);
        assert_eq!(
            launch_mode(["prog", "--socket", "/tmp/x.sock"]).unwrap(),
            Mode::Shim
        );
        assert_eq!(launch_mode(Vec::<String>::new()).unwrap(), Mode::Shim);
    }

    #[test]
    fn test_connect_without_a_value_is_an_error() {
        let err = launch_mode(["prog", "--connect"]).unwrap_err();
        assert!(matches!(err, McpError::InvalidParams(_)), "{err:?}");
        assert!(err.to_string().contains("needs a host:port"), "{err}");

        let err = launch_mode(["prog", "--connect", "jetson-memory"]).unwrap_err();
        assert!(err.to_string().contains("not a host:port"), "{err}");
    }

    #[test]
    fn test_connect_with_daemon_is_an_error() {
        for line in [
            vec!["prog", "--connect", "h:1", "--daemon"],
            vec!["prog", "--daemon", "--connect", "h:1"],
            vec!["prog", "--daemon", "--connect=h:1"],
        ] {
            let err = launch_mode(line.clone()).unwrap_err();
            assert!(
                matches!(err, McpError::InvalidParams(_)),
                "{line:?}: {err:?}"
            );
            let text = err.to_string();
            assert!(text.contains("`--connect`"), "{line:?}: {text}");
            assert!(text.contains("`--daemon`"), "{line:?}: {text}");
        }
    }

    #[test]
    fn test_connect_with_foreground_is_an_error() {
        for line in [
            vec!["prog", "--connect", "h:1", "--foreground"],
            vec!["prog", "--foreground", "--connect", "h:1"],
        ] {
            let err = launch_mode(line.clone()).unwrap_err();
            assert!(
                matches!(err, McpError::InvalidParams(_)),
                "{line:?}: {err:?}"
            );
            assert!(
                err.to_string().contains("`--foreground`"),
                "{line:?}: {err}"
            );
        }
    }

    #[test]
    fn test_connect_with_socket_is_an_error() {
        // A remote shim promises never to touch a socket path; being handed
        // one is a contradiction, not a hint.
        for line in [
            vec!["prog", "--connect", "h:1", "--socket", "/tmp/x.sock"],
            vec!["prog", "--socket", "/tmp/x.sock", "--connect", "h:1"],
            vec!["prog", "--socket=/tmp/x.sock", "--connect", "h:1"],
        ] {
            let err = launch_mode(line.clone()).unwrap_err();
            assert!(
                matches!(err, McpError::InvalidParams(_)),
                "{line:?}: {err:?}"
            );
            assert!(err.to_string().contains("`--socket`"), "{line:?}: {err}");
        }
    }

    #[test]
    fn test_the_program_name_is_not_a_connect_flag() {
        // A binary named `--connect` followed by an argument that happens to
        // look like an address is still a local shim.
        assert_eq!(launch_mode(["--connect", "h:1"]).unwrap(), Mode::Shim);
    }

    // ---- socket directory ------------------------------------------------
    //
    // The helper itself, and the `0700` it creates directories with, are tested
    // in `crate::socket`. What belongs here is that `serve_daemon` calls it -
    // which the test below drives end to end.
    #[test]
    fn test_serve_daemon_creates_the_socket_directory_before_serving() {
        // The whole path, not just the helper: a socket two directories down
        // from anything that exists comes up and accepts.
        let base = temp_path("dir");
        let socket = base.join("run/server.sock");
        let counter = Arc::new(AtomicI64::new(0));

        let _daemon = start_foreground(server_with_tools(), &socket, counter);

        assert!(socket.exists(), "the daemon should have bound its socket");

        // And made the directory its own: a socket is only as private as what
        // holds it, so a daemon that created a world-writable directory has
        // published a path anyone can bind first.
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(socket.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o700, "{mode:o}");

        let mut client = UnixTransport::connect(&socket).unwrap();
        initialize(&mut client, 1);
        assert_eq!(call_tool(&mut client, 2, "increment"), "1");

        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn test_a_socket_directory_that_cannot_be_made_is_reported_before_the_dispatch() {
        // Which is what makes it reportable at all. `--daemon` double-forks
        // first, and by the time the grandchild finds it cannot create the
        // directory the original process has already printed a PID and exited
        // zero - so the shell is told the daemon started. Doing it here, ahead
        // of the mode dispatch, is what turns that into an error the caller's
        // `main` returns.
        //
        // Driven through shim mode, the one dispatch that never reaches
        // `UnixServer` and so cannot be covered by its copy of this step.
        let blocker = temp_path("file");
        std::fs::write(&blocker, b"not a directory").unwrap();
        let socket = blocker.join("server.sock");

        let outcome = server_with_tools().serve_daemon_from(
            ["prog"],
            DaemonOptions::new(&socket).idle_timeout(Duration::from_secs(1)),
            move |conn_id| Ctx {
                conn_id: conn_id.to_string(),
                counter: Arc::new(AtomicI64::new(0)),
            },
        );

        let Err(error) = outcome else {
            panic!("a socket directory that cannot exist must not be dispatched on");
        };
        assert!(
            error
                .to_string()
                .contains("failed to create socket directory"),
            "{error}"
        );

        let _ = std::fs::remove_file(&blocker);
    }

    // ---- serving ---------------------------------------------------------

    #[test]
    fn test_registered_tools_reach_the_connection() {
        // The registration transfer is the load-bearing part: `add_tool` took
        // ownership, and the daemon builds a *different* server per connection.
        let socket = temp_path("sock");
        let counter = Arc::new(AtomicI64::new(0));
        let _daemon = start_foreground(server_with_tools(), &socket, counter);

        let mut client = UnixTransport::connect(&socket).unwrap();
        initialize(&mut client, 1);

        let result = request(&mut client, 2, "tools/list", None);
        let mut names: Vec<&str> = result["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        names.sort_unstable();
        assert_eq!(names, ["increment", "whoami"]);

        // And they run, with the schema that was compiled at registration.
        assert_eq!(call_tool(&mut client, 3, "increment"), "1");
        assert!(call_tool(&mut client, 4, "whoami").starts_with("conn-"));

        let _ = std::fs::remove_file(&socket);
    }

    #[test]
    fn test_every_connection_gets_the_tools_and_its_own_context() {
        let socket = temp_path("sock");
        let counter = Arc::new(AtomicI64::new(0));
        let _daemon = start_foreground(server_with_tools(), &socket, counter);

        let mut first = UnixTransport::connect(&socket).unwrap();
        let mut second = UnixTransport::connect(&socket).unwrap();
        initialize(&mut first, 1);
        initialize(&mut second, 1);

        // Distinct contexts...
        let a = call_tool(&mut first, 2, "whoami");
        let b = call_tool(&mut second, 2, "whoami");
        assert_ne!(a, b);

        // ...over one shared state, which is the entire reason for a daemon.
        assert_eq!(call_tool(&mut first, 3, "increment"), "1");
        assert_eq!(call_tool(&mut second, 3, "increment"), "2");
        assert_eq!(call_tool(&mut first, 4, "increment"), "3");

        let _ = std::fs::remove_file(&socket);
    }

    #[test]
    fn test_resources_and_prompts_reach_the_connection_too() {
        // Tools are the headline, but a server that quietly served none of its
        // prompts would be worse than one that refused to start.
        let socket = temp_path("sock");
        let counter = Arc::new(AtomicI64::new(0));

        let mut server: Server<Ctx> = Server::new(config());
        server.add_tool(IncrementTool).unwrap();
        server.add_resource(NoteResource).unwrap();
        server
            .add_resource_template(ResourceTemplate {
                uri_template: "mem://notes/{id}".into(),
                name: "note-by-id".into(),
                ..Default::default()
            })
            .unwrap();
        server.add_prompt(GreetPrompt).unwrap();

        let _daemon = start_foreground(server, &socket, counter);

        let mut client = UnixTransport::connect(&socket).unwrap();
        let init = initialize(&mut client, 1);
        assert!(!init["capabilities"]["resources"].is_null());
        assert!(!init["capabilities"]["prompts"].is_null());

        let resources = request(&mut client, 2, "resources/list", None);
        assert_eq!(resources["resources"][0]["uri"], "mem://note");

        let templates = request(&mut client, 3, "resources/templates/list", None);
        assert_eq!(
            templates["resourceTemplates"][0]["uriTemplate"],
            "mem://notes/{id}"
        );

        let prompts = request(&mut client, 4, "prompts/list", None);
        assert_eq!(prompts["prompts"][0]["name"], "greet");

        let greeting = request(
            &mut client,
            5,
            "prompts/get",
            Some(serde_json::json!({ "name": "greet" })),
        );
        assert_eq!(greeting["messages"][0]["content"]["text"], "hello");

        // A second connection is served by a second server built from the same
        // blueprint - the prompt has to be there too, not consumed by the first.
        let mut other = UnixTransport::connect(&socket).unwrap();
        initialize(&mut other, 1);
        let prompts = request(&mut other, 2, "prompts/list", None);
        assert_eq!(prompts["prompts"][0]["name"], "greet");

        let _ = std::fs::remove_file(&socket);
    }

    /// A server whose one tool may be called as a task.
    fn tasked_server(counter: &Arc<AtomicI64>) -> Server<Ctx> {
        let mut server: Server<Ctx> = Server::new(config());
        server.add_tool(TaskableTool).unwrap();
        let task_counter = counter.clone();
        server.enable_tasks(TaskConfig::default(), move || Ctx {
            conn_id: "task".to_string(),
            counter: task_counter.clone(),
        });
        server
    }

    #[test]
    fn test_each_connection_gets_its_own_task_store() {
        // `tasks/list` under `SingleRequestor` hands back every task in the
        // store, so one store across connections would hand each client the
        // others' work. The blueprint deliberately leaves the store behind and
        // builds a new one per connection; this is that promise.
        let socket = temp_path("sock");
        let counter = Arc::new(AtomicI64::new(0));
        let _daemon = start_foreground(tasked_server(&counter), &socket, counter.clone());

        let mut first = UnixTransport::connect(&socket).unwrap();
        let mut second = UnixTransport::connect(&socket).unwrap();
        initialize(&mut first, 1);
        initialize(&mut second, 1);

        let created = request(
            &mut first,
            2,
            "tools/call",
            Some(serde_json::json!({ "name": "taskable", "arguments": {}, "task": {} })),
        );
        let task_id = created["task"]["taskId"]
            .as_str()
            .expect("a task-augmented call should create a task");

        // The connection that made it can see it...
        let mine = request(&mut first, 3, "tasks/list", None);
        let listed: Vec<&str> = mine["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["taskId"].as_str().unwrap())
            .collect();
        assert_eq!(listed, [task_id]);

        // ...and the connection next to it cannot.
        let theirs = request(&mut second, 3, "tasks/list", None);
        assert_eq!(
            theirs["tasks"].as_array().unwrap().len(),
            0,
            "a second connection must not be shown the first's tasks"
        );

        let _ = std::fs::remove_file(&socket);
    }

    #[test]
    fn test_an_enabled_task_runtime_reaches_the_connection() {
        // `enable_tasks` is server state, not a registration, and dropping it
        // would leave the daemon declining every task-augmented request.
        let socket = temp_path("sock");
        let counter = Arc::new(AtomicI64::new(0));

        let mut server: Server<Ctx> = Server::new(config());
        server.add_tool(IncrementTool).unwrap();
        let task_counter = counter.clone();
        server.enable_tasks(TaskConfig::default(), move || Ctx {
            conn_id: "task".to_string(),
            counter: task_counter.clone(),
        });

        let _daemon = start_foreground(server, &socket, counter);

        let mut client = UnixTransport::connect(&socket).unwrap();
        let init = initialize(&mut client, 1);
        assert!(
            !init["capabilities"]["tasks"].is_null(),
            "the daemon should declare the task capability it was given"
        );

        let _ = std::fs::remove_file(&socket);
    }

    #[test]
    fn test_a_server_with_nothing_registered_still_serves() {
        let socket = temp_path("sock");
        let counter = Arc::new(AtomicI64::new(0));
        let _daemon = start_foreground(Server::new(config()), &socket, counter);

        let mut client = UnixTransport::connect(&socket).unwrap();
        let init = initialize(&mut client, 1);
        assert_eq!(init["serverInfo"]["name"], "daemon-test");
        assert!(init["capabilities"]["tools"].is_null());

        let _ = std::fs::remove_file(&socket);
    }

    // ---- the shim --------------------------------------------------------

    #[test]
    fn test_the_shim_proxies_to_a_running_daemon() {
        // `auto_start` takes its fast path here - the daemon is already up - so
        // nothing is spawned and the test binary is never relaunched.
        let socket = temp_path("sock");
        let counter = Arc::new(AtomicI64::new(0));
        let _daemon = start_foreground(server_with_tools(), &socket, counter);

        let (shim_side, test_side) = UnixStream::pair().unwrap();
        let socket_for_shim = socket.clone();
        let shim_thread =
            thread::spawn(move || shim(&socket_for_shim, UnixTransport::from_stream(shim_side)));

        let mut client = UnixTransport::from_stream(test_side);
        initialize(&mut client, 1);
        assert_eq!(call_tool(&mut client, 2, "increment"), "1");
        assert!(call_tool(&mut client, 3, "whoami").starts_with("conn-"));

        // Client hangs up -> request pump half-closes upstream -> daemon EOFs ->
        // the shim drains and returns.
        drop(client);
        shim_thread.join().unwrap().unwrap();

        let _ = std::fs::remove_file(&socket);
    }

    #[test]
    fn test_two_shims_share_one_daemon() {
        // The point of the pattern: two MCP clients, two shims, one counter.
        let socket = temp_path("sock");
        let counter = Arc::new(AtomicI64::new(0));
        let _daemon = start_foreground(server_with_tools(), &socket, counter);

        let mut clients = Vec::new();
        let mut shims = Vec::new();
        for _ in 0..2 {
            let (shim_side, test_side) = UnixStream::pair().unwrap();
            let socket_for_shim = socket.clone();
            shims.push(thread::spawn(move || {
                shim(&socket_for_shim, UnixTransport::from_stream(shim_side))
            }));
            clients.push(UnixTransport::from_stream(test_side));
        }

        for client in clients.iter_mut() {
            initialize(client, 1);
        }

        assert_eq!(call_tool(&mut clients[0], 2, "increment"), "1");
        assert_eq!(call_tool(&mut clients[1], 2, "increment"), "2");
        assert_ne!(
            call_tool(&mut clients[0], 3, "whoami"),
            call_tool(&mut clients[1], 3, "whoami"),
            "one daemon, but still one connection each"
        );

        drop(clients);
        for shim_thread in shims {
            shim_thread.join().unwrap().unwrap();
        }

        let _ = std::fs::remove_file(&socket);
    }

    #[test]
    fn test_the_shim_reports_a_daemon_it_cannot_start() {
        // No daemon, and `current_exe` here is the test binary, which does not
        // bind anything. The shim has to give up and say so rather than hang.
        let socket = temp_path("sock");
        let (shim_side, _test_side) = UnixStream::pair().unwrap();

        // Deliberately not `serve_daemon_from`: that would relaunch the test
        // binary. `auto_start`'s own spawn path is covered in bridge.rs; what
        // matters here is that the failure surfaces.
        let Err(err) = Bridge::auto_start(&socket, "/nonexistent/daemon-binary", &[DAEMON_FLAG])
        else {
            panic!("a daemon that cannot be spawned must be an error");
        };
        assert!(err.to_string().contains("failed to spawn daemon"), "{err}");

        drop(shim_side);
    }

    // ---- the remote shim -------------------------------------------------

    /// Run `server` in foreground mode on `socket` *and* a loopback TCP port,
    /// and hand back that port once the daemon accepts.
    fn start_foreground_with_tcp(
        server: Server<Ctx>,
        socket: &Path,
        counter: Arc<AtomicI64>,
    ) -> (thread::JoinHandle<Result<()>>, u16) {
        let listener = TcpSocketListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().port();

        let socket_for_server = socket.to_path_buf();
        let handle = thread::spawn(move || {
            server.serve_daemon_from(
                ["prog", FOREGROUND_FLAG],
                DaemonOptions::new(socket_for_server)
                    .idle_timeout(Duration::from_secs(30))
                    .listener(listener),
                move |conn_id| Ctx {
                    conn_id: conn_id.to_string(),
                    counter: counter.clone(),
                },
            )
        });

        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if UnixStream::connect(socket).is_ok() {
                return (handle, port);
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("daemon never started listening on {}", socket.display());
    }

    /// Drive the dispatch as a remote shim would be driven, but over a
    /// socketpair rather than the process's stdio.
    fn run_remote_shim(argv: Vec<String>) -> (UnixTransport, thread::JoinHandle<Result<()>>) {
        let (shim_side, test_side) = UnixStream::pair().unwrap();
        let handle = thread::spawn(move || {
            // The same steps `serve_daemon_from` takes, with the client
            // transport injected: `launch_mode`, then `remote_shim`. The
            // dispatch on the real stdio is covered end to end in
            // tests/remote_shim_e2e.rs.
            match launch_mode(argv)? {
                Mode::Remote(addr) => remote_shim(&addr, UnixTransport::from_stream(shim_side)),
                other => panic!("expected remote mode, got {other:?}"),
            }
        });
        (UnixTransport::from_stream(test_side), handle)
    }

    #[test]
    fn test_the_remote_shim_proxies_to_a_tcp_daemon() {
        let socket = temp_path("sock");
        let counter = Arc::new(AtomicI64::new(0));
        let (_daemon, port) = start_foreground_with_tcp(server_with_tools(), &socket, counter);

        let (mut client, shim_thread) = run_remote_shim(vec![
            "prog".into(),
            "--connect".into(),
            format!("127.0.0.1:{port}"),
        ]);

        let init = initialize(&mut client, 1);
        assert_eq!(init["serverInfo"]["name"], "daemon-test");
        assert_eq!(call_tool(&mut client, 2, "increment"), "1");
        assert!(call_tool(&mut client, 3, "whoami").starts_with("conn-"));

        // Client hangs up -> the shim half-closes upstream -> daemon EOFs ->
        // the shim drains and returns, same as over the Unix socket.
        drop(client);
        shim_thread.join().unwrap().unwrap();

        let _ = std::fs::remove_file(&socket);
    }

    #[test]
    fn test_the_remote_shim_resolves_a_hostname() {
        // `localhost` rather than a literal: the real use is a MagicDNS name,
        // and the shim must hand the name to the resolver rather than insist
        // on an IP. Whether it resolves to `::1` first or `127.0.0.1` first
        // is the box's business; the listener is on the latter and the shim
        // has to get there either way.
        let socket = temp_path("sock");
        let counter = Arc::new(AtomicI64::new(0));
        let (_daemon, port) = start_foreground_with_tcp(server_with_tools(), &socket, counter);

        let (mut client, shim_thread) = run_remote_shim(vec![
            "prog".into(),
            "--connect".into(),
            format!("localhost:{port}"),
        ]);

        initialize(&mut client, 1);
        assert_eq!(call_tool(&mut client, 2, "increment"), "1");

        drop(client);
        shim_thread.join().unwrap().unwrap();

        let _ = std::fs::remove_file(&socket);
    }

    #[test]
    fn test_the_remote_shim_and_the_unix_shim_reach_the_same_daemon() {
        // One daemon, both front doors, one counter.
        let socket = temp_path("sock");
        let counter = Arc::new(AtomicI64::new(0));
        let (_daemon, port) = start_foreground_with_tcp(server_with_tools(), &socket, counter);

        let (shim_side, test_side) = UnixStream::pair().unwrap();
        let socket_for_shim = socket.clone();
        let unix_shim =
            thread::spawn(move || shim(&socket_for_shim, UnixTransport::from_stream(shim_side)));
        let mut over_unix = UnixTransport::from_stream(test_side);

        let (mut over_tcp, remote) = run_remote_shim(vec![
            "prog".into(),
            "--connect".into(),
            format!("127.0.0.1:{port}"),
        ]);

        initialize(&mut over_unix, 1);
        initialize(&mut over_tcp, 1);
        assert_eq!(call_tool(&mut over_unix, 2, "increment"), "1");
        assert_eq!(call_tool(&mut over_tcp, 2, "increment"), "2");
        assert_ne!(
            call_tool(&mut over_unix, 3, "whoami"),
            call_tool(&mut over_tcp, 3, "whoami")
        );

        drop(over_unix);
        drop(over_tcp);
        unix_shim.join().unwrap().unwrap();
        remote.join().unwrap().unwrap();

        let _ = std::fs::remove_file(&socket);
    }

    #[test]
    fn test_the_remote_shim_touches_nothing_local() {
        // The socket path handed in is under a directory that does not exist.
        // A Unix shim would create it (`ensure_private_parent_dir`), look for
        // a socket, read a PID file, and spawn a daemon. A remote shim does
        // none of that - which, driven through the real dispatch, means the
        // directory must still not exist afterwards.
        let base = temp_path("untouched");
        let socket = base.join("run/server.sock");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let acceptor = thread::spawn(move || {
            // Accept and hang up, so the shim's proxy loop ends by itself.
            let (stream, _) = listener.accept().unwrap();
            drop(stream);
        });

        // Through `serve_daemon_from` itself, because the point is what the
        // dispatch does *before* it reaches the shim. `StdioTransport` reads
        // the test process's stdin, which is fine: the upstream hangs up
        // first, the response pump returns, and the request pump is detached.
        let outcome = server_with_tools().serve_daemon_from(
            vec![
                "prog".into(),
                "--connect".into(),
                format!("127.0.0.1:{port}"),
            ],
            DaemonOptions::new(&socket).idle_timeout(Duration::from_secs(1)),
            move |conn_id| Ctx {
                conn_id: conn_id.to_string(),
                counter: Arc::new(AtomicI64::new(0)),
            },
        );
        acceptor.join().unwrap();
        assert!(outcome.is_ok(), "{outcome:?}");

        assert!(
            !base.exists(),
            "a remote shim must not create the socket directory: {}",
            base.display()
        );
        assert!(!socket.exists());
        assert!(!crate::transport::pid_path_for(&socket).exists());
    }

    #[test]
    fn test_the_remote_shim_reports_a_daemon_it_cannot_reach() {
        // Bind, learn the port, drop the listener: an address nothing answers
        // on. The shim must fail at once, naming the address, and spawn
        // nothing - there is no `current_exe` relaunch on this path.
        let addr = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().to_string()
        };
        let base = temp_path("unreachable");
        let socket = base.join("server.sock");

        let started = Instant::now();
        let outcome = server_with_tools().serve_daemon_from(
            vec!["prog".into(), "--connect".into(), addr.clone()],
            DaemonOptions::new(&socket),
            move |conn_id| Ctx {
                conn_id: conn_id.to_string(),
                counter: Arc::new(AtomicI64::new(0)),
            },
        );

        let Err(err) = outcome else {
            panic!("a daemon that cannot be reached must be an error");
        };
        let text = err.to_string();
        assert!(text.contains("cannot connect to"), "{text}");
        assert!(text.contains(&addr), "the address must be named: {text}");
        // A refusal, not a wait: no retry loop, no respawn wait.
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(!base.exists(), "nothing local may be touched on the way");
    }

    #[test]
    fn test_the_remote_shim_reports_a_name_that_does_not_resolve() {
        let outcome = server_with_tools().serve_daemon_from(
            ["prog", "--connect", "no-such-host.invalid:7211"],
            DaemonOptions::new(temp_path("unused.sock")),
            move |conn_id| Ctx {
                conn_id: conn_id.to_string(),
                counter: Arc::new(AtomicI64::new(0)),
            },
        );

        let Err(err) = outcome else {
            panic!("a name that does not resolve must be an error, not a panic");
        };
        assert!(
            err.to_string()
                .contains("cannot connect to no-such-host.invalid:7211"),
            "{err}"
        );
    }

    #[test]
    fn test_a_contradictory_command_line_is_refused_before_anything_is_touched() {
        let base = temp_path("contradiction");
        let socket = base.join("server.sock");

        let outcome = server_with_tools().serve_daemon_from(
            ["prog", "--connect", "h:1", "--daemon"],
            DaemonOptions::new(&socket),
            move |conn_id| Ctx {
                conn_id: conn_id.to_string(),
                counter: Arc::new(AtomicI64::new(0)),
            },
        );

        let Err(err) = outcome else {
            panic!("`--connect --daemon` must be refused");
        };
        assert!(matches!(err, McpError::InvalidParams(_)), "{err:?}");
        assert!(!base.exists(), "the socket directory must not be created");
    }

    #[test]
    fn test_a_non_utf8_path_is_refused_rather_than_mangled() {
        use std::os::unix::ffi::OsStrExt;

        let path = PathBuf::from(std::ffi::OsStr::from_bytes(b"/tmp/sml_\xff_mcps.sock"));
        let err = utf8(&path, "the socket path").unwrap_err();
        assert!(err.to_string().contains("not valid UTF-8"), "{err}");

        // And the UTF-8 case passes straight through.
        assert_eq!(
            utf8(Path::new("/tmp/ok.sock"), "the socket path").unwrap(),
            "/tmp/ok.sock"
        );
    }
}
