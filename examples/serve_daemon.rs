//! The daemon/shim pattern in one call.
//!
//! `examples/unix_server.rs` wires this up by hand - argv, [`UnixServer`], and
//! [`Bridge`] - which is worth reading once to see what is happening. This is
//! the same binary, the same three modes, through
//! [`Server::serve_daemon`](sml_mcps::Server::serve_daemon):
//!
//! - `serve_daemon --daemon` - detach and serve on the socket.
//! - `serve_daemon --foreground` - serve without detaching (handy for debugging).
//! - `serve_daemon` - act as a shim: start the daemon if it isn't up, then proxy
//!   stdio to it. This is what an MCP client launches.
//!
//! The counter lives in the daemon, so every client that connects - through any
//! shim, from any terminal - increments the same one.
//!
//! Try it:
//! ```text
//! # Terminal 1 - watch the daemon:
//! cargo run --example serve_daemon -- --foreground
//!
//! # Terminal 2 and 3 - two clients, one counter:
//! echo '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"increment"}}' \
//!   | cargo run --example serve_daemon
//! ```

#[cfg(unix)]
fn main() -> sml_mcps::Result<()> {
    use serde_json::Value;
    use sml_mcps::{CallToolResult, Result, Server, ServerConfig, Tool, ToolEnv};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::time::Duration;

    /// Per-connection context. `conn_id` is unique per connection; `counter` is
    /// shared by every connection the daemon serves.
    struct AppContext {
        conn_id: String,
        counter: Arc<AtomicI64>,
    }

    /// Increment the shared counter (the state that lives in the daemon).
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

    // Socket path: `--socket PATH` if given, else one in the temp dir. The
    // daemon is relaunched with `--socket <path>`, so parsing it here is what
    // makes a caller-chosen path survive into the daemon.
    let args: Vec<String> = std::env::args().collect();
    let socket_path = args
        .iter()
        .position(|a| a == "--socket")
        .and_then(|i| args.get(i + 1))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("sml_mcps_serve_daemon_example.sock"));

    let config = ServerConfig {
        name: "serve-daemon-example".to_string(),
        version: "1.0.0".to_string(),
        instructions: Some("Daemon/shim example with a shared counter.".to_string()),
        ..Default::default()
    };

    let mut server: Server<AppContext> = Server::new(config);
    server.add_tool(IncrementTool)?;
    server.add_tool(WhoamiTool)?;

    // Anything expensive belongs in here rather than above it: the factory runs
    // per connection, in the daemon, and never in a shim.
    let counter = Arc::new(AtomicI64::new(0));

    server.serve_daemon(&socket_path, Duration::from_secs(300), move |conn_id| {
        AppContext {
            conn_id: conn_id.to_string(),
            counter: counter.clone(),
        }
    })
}

#[cfg(not(unix))]
fn main() {
    eprintln!("The serve_daemon example requires a Unix platform.");
}
