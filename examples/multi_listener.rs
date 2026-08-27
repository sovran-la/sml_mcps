//! One daemon, several front doors, and a say in when it is allowed to leave.
//!
//! `examples/serve_daemon.rs` is the same pattern with the two arguments a
//! daemon always needs. This is
//! [`Server::serve_daemon_with`](sml_mcps::Server::serve_daemon_with) and
//! [`DaemonOptions`](sml_mcps::DaemonOptions), which is how you reach the rest:
//!
//! - `--tcp <addr>` - also accept plain TCP, through
//!   [`TcpSocketListener`](sml_mcps::TcpSocketListener).
//! - `--wrapped <addr>` - also accept through a [`Listener`](sml_mcps::Listener)
//!   written here, which hands the daemon a
//!   [`StreamTransport`](sml_mcps::StreamTransport) over a stream it accepted
//!   itself. **This is the shape of the mTLS path**: swap the identity function
//!   in `WrappedListener::accept` for a TLS handshake and nothing else about
//!   the daemon changes. sml_mcps never needs to know.
//! - `--idle-ms <n>` - how long to wait with no connections before exiting.
//! - the `busy`/`idle` tools - the application's veto on that exit. While a
//!   client has called `busy`, the idle window can pass as often as it likes
//!   and the daemon stays up. This is what a daemon supervising child
//!   processes needs: the work outlives the session that asked for it.
//!
//! Try it:
//! ```text
//! # A daemon on three listeners, exiting after ten idle seconds:
//! cargo run --example multi_listener -- --foreground \
//!     --socket /tmp/multi.sock --tcp 127.0.0.1:7743 \
//!     --wrapped 127.0.0.1:7744 --idle-ms 10000
//!
//! # Any of them reaches the same counter:
//! echo '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"increment"}}' \
//!     | nc -U /tmp/multi.sock
//! echo '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"increment"}}' \
//!     | nc 127.0.0.1 7743
//! ```
//!
//! Note that a shim relaunches the daemon with exactly `--daemon --socket
//! <path>`: the extra arguments here are not forwarded, so a real daemon that
//! listens in more than one place should read its configuration from a file or
//! the environment rather than from a shim's command line.

#[cfg(unix)]
fn main() -> sml_mcps::Result<()> {
    use serde_json::Value;
    use sml_mcps::{
        CallToolResult, DaemonOptions, Listener, Result, Server, ServerConfig, StreamTransport,
        TcpSocketListener, Tool, ToolEnv, Transport, user_socket_path,
    };
    use std::io;
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
    use std::time::Duration;

    /// Per-connection context over state the whole daemon shares.
    struct AppContext {
        conn_id: String,
        counter: Arc<AtomicI64>,
        jobs: Arc<AtomicUsize>,
    }

    /// Bumps the shared counter - proof that every listener reaches one daemon.
    struct IncrementTool;
    impl Tool<AppContext> for IncrementTool {
        fn name(&self) -> &str {
            "increment"
        }
        fn description(&self) -> &str {
            "Increment the daemon's shared counter"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn execute(&self, _a: Value, ctx: &mut AppContext, _e: &ToolEnv) -> Result<CallToolResult> {
            let v = ctx.counter.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(CallToolResult::text(format!("counter = {}", v)))
        }
    }

    /// Reports which connection you are on.
    struct WhoamiTool;
    impl Tool<AppContext> for WhoamiTool {
        fn name(&self) -> &str {
            "whoami"
        }
        fn description(&self) -> &str {
            "Return this connection's id"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn execute(&self, _a: Value, ctx: &mut AppContext, _e: &ToolEnv) -> Result<CallToolResult> {
            Ok(CallToolResult::text(ctx.conn_id.clone()))
        }
    }

    /// Starts a "job" - work that outlives the connection that asked for it.
    ///
    /// Nothing actually runs; what matters is that the daemon now has a reason
    /// not to exit, and that the reason survives the client hanging up.
    struct BusyTool;
    impl Tool<AppContext> for BusyTool {
        fn name(&self) -> &str {
            "busy"
        }
        fn description(&self) -> &str {
            "Start a background job, so the daemon refuses to idle out"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn execute(&self, _a: Value, ctx: &mut AppContext, _e: &ToolEnv) -> Result<CallToolResult> {
            let running = ctx.jobs.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(CallToolResult::text(format!("{} job(s) running", running)))
        }
    }

    /// Ends one, releasing the veto once the last is done.
    struct IdleTool;
    impl Tool<AppContext> for IdleTool {
        fn name(&self) -> &str {
            "idle"
        }
        fn description(&self) -> &str {
            "Finish a background job"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn execute(&self, _a: Value, ctx: &mut AppContext, _e: &ToolEnv) -> Result<CallToolResult> {
            let before = ctx.jobs.load(Ordering::SeqCst);
            let running = if before == 0 {
                0
            } else {
                ctx.jobs.fetch_sub(1, Ordering::SeqCst) - 1
            };
            Ok(CallToolResult::text(format!("{} job(s) running", running)))
        }
    }

    /// A listener that accepts the connection itself and hands the daemon the
    /// finished stream.
    ///
    /// The stand-in for an mTLS front door: `secure` is where the handshake
    /// would go, a failure there costs one connection rather than the daemon,
    /// and what comes back is a `Box<dyn Transport>` the daemon serves like any
    /// other. sml_mcps carries no TLS dependency and never learns of one.
    struct WrappedListener {
        listener: TcpListener,
        addr: SocketAddr,
    }

    impl WrappedListener {
        /// Where a rustls `ServerConnection` would be wrapped around the
        /// stream. Here: the stream, unchanged.
        fn secure(&self, stream: TcpStream) -> io::Result<TcpStream> {
            Ok(stream)
        }
    }

    impl Listener for WrappedListener {
        fn accept(&self) -> io::Result<Box<dyn Transport>> {
            let (stream, _peer) = self.listener.accept()?;
            let secured = self.secure(stream)?;

            Ok(Box::new(StreamTransport::new(secured)))
        }

        fn wake(&self) -> io::Result<()> {
            TcpStream::connect(self.addr).map(|_| ())
        }

        fn describe(&self) -> String {
            format!("wrapped://{}", self.addr)
        }
    }

    /// The value after `flag` on the command line, if it is there.
    fn flag(args: &[String], flag: &str) -> Option<String> {
        args.iter()
            .position(|a| a == flag)
            .and_then(|i| args.get(i + 1))
            .cloned()
    }

    let args: Vec<String> = std::env::args().collect();

    let socket_path = flag(&args, "--socket")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| user_socket_path("sml_mcps-multi-listener-example", "server.sock"));

    let idle = flag(&args, "--idle-ms")
        .and_then(|ms| ms.parse().ok())
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_secs(300));

    let config = ServerConfig {
        name: "multi-listener-example".to_string(),
        version: "1.0.0".to_string(),
        instructions: Some("One daemon, several listeners, one shared counter.".to_string()),
        ..Default::default()
    };

    let mut server: Server<AppContext> = Server::new(config);
    server.add_tool(IncrementTool)?;
    server.add_tool(WhoamiTool)?;
    server.add_tool(BusyTool)?;
    server.add_tool(IdleTool)?;

    // Shared by every connection, on every listener.
    let counter = Arc::new(AtomicI64::new(0));
    let jobs = Arc::new(AtomicUsize::new(0));

    let mut options = DaemonOptions::new(&socket_path)
        .idle_timeout(idle)
        .busy_check({
            // The veto. Asked only when the daemon is otherwise about to leave,
            // and answered from state the application already keeps.
            let jobs = jobs.clone();
            move || jobs.load(Ordering::SeqCst) > 0
        });

    if let Some(addr) = flag(&args, "--tcp") {
        options = options.listener(TcpSocketListener::bind(&addr)?);
    }

    if let Some(addr) = flag(&args, "--wrapped") {
        let listener = TcpListener::bind(&addr)?;
        let addr = listener.local_addr()?;
        options = options.listener(WrappedListener { listener, addr });
    }

    server.serve_daemon_with(options, move |conn_id| AppContext {
        conn_id: conn_id.to_string(),
        counter: counter.clone(),
        jobs: jobs.clone(),
    })
}

#[cfg(not(unix))]
fn main() {
    eprintln!("The multi_listener example requires a Unix platform.");
}
