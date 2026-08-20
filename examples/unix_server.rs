//! Unix socket daemon + shim example.
//!
//! One binary, three modes:
//!
//! - `unix_server --daemon` - daemonize (double-fork) and serve on the socket.
//! - `unix_server --foreground` - serve in the foreground (handy for debugging).
//! - `unix_server` - run as a shim: auto-start the daemon if needed, then proxy
//!   stdio <-> daemon. This is what an MCP client (e.g. Claude Code) launches.
//!
//! The daemon holds a single shared counter. Every shim/client that connects
//! talks to the *same* daemon, so the counter is shared across sessions - the
//! whole point of the daemon/shim pattern. Each connection also gets a unique
//! `conn_id` for per-session isolation where needed.
//!
//! Try it:
//! ```text
//! # Terminal 1 - watch the daemon in the foreground:
//! cargo run --example unix_server -- --foreground
//!
//! # Terminal 2 - speak MCP to it through a shim:
//! echo '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"increment"}}' \
//!   | cargo run --example unix_server
//! ```

#[cfg(unix)]
fn main() -> sml_mcps::Result<()> {
    use serde_json::Value;
    use sml_mcps::{
        Bridge, CallToolResult, Result, Server, ServerConfig, StdioTransport, Tool, ToolEnv,
        UnixServer, user_socket_path,
    };
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::time::Duration;

    /// Per-connection context. `conn_id` is unique per connection; `counter` is
    /// shared across every connection the daemon serves.
    struct AppContext {
        conn_id: String,
        counter: Arc<AtomicI64>,
    }

    /// Increment the shared counter (state lives in the daemon, shared by all).
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

    /// Report which connection you're on (proves per-connection isolation).
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

    fn config() -> ServerConfig {
        ServerConfig {
            name: "unix-example".to_string(),
            version: "1.0.0".to_string(),
            instructions: Some("Daemon/shim example with a shared counter.".to_string()),
            ..Default::default()
        }
    }

    fn build_server(server: &mut Server<AppContext>) -> Result<()> {
        server.add_tool(IncrementTool)?;
        server.add_tool(WhoamiTool)?;
        Ok(())
    }

    // Socket path: use --socket PATH if provided, else this user's own runtime
    // directory. Not the temp directory: a socket there is a path any account
    // on the machine can bind first, and the shim would talk to whoever did.
    let args: Vec<String> = std::env::args().collect();
    let socket_path = args
        .iter()
        .position(|a| a == "--socket")
        .and_then(|i| args.get(i + 1))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| user_socket_path("sml_mcps-unix-example", "server.sock"));

    let mode = args.get(1).map(|s| s.as_str());
    match mode {
        Some("--daemon") | Some("--foreground") => {
            let counter = Arc::new(AtomicI64::new(0));
            let server = UnixServer::new(config())
                .idle_timeout(Duration::from_secs(300))
                .with_tools(build_server);

            let factory = move |conn_id: &str| AppContext {
                conn_id: conn_id.to_string(),
                counter: counter.clone(),
            };

            if mode == Some("--daemon") {
                server.serve_daemon(&socket_path, factory)
            } else {
                eprintln!("Serving in foreground on {}", socket_path.display());
                server.serve(&socket_path, factory)
            }
        }
        _ => {
            // Shim side: connect to the daemon (auto-starting it if needed) and
            // proxy stdio <-> daemon. We re-launch *this* binary with --daemon.
            let exe = std::env::current_exe()
                .map_err(|e| sml_mcps::McpError::Internal(format!("current_exe: {}", e)))?;
            let exe = exe.to_string_lossy().to_string();

            let sock_str = socket_path.to_string_lossy().to_string();
            let upstream =
                Bridge::auto_start(&socket_path, &exe, &["--daemon", "--socket", &sock_str])?;
            Bridge::run(StdioTransport::new(), upstream)
        }
    }
}

#[cfg(not(unix))]
fn main() {
    eprintln!("The unix_server example requires a Unix platform.");
}
