//! A daemon that knows which caller each tool call came from.
//!
//! The daemon cannot see who is on the other end of a connection - it builds
//! one context per connection, and a tool sees only its arguments. The shim can:
//! it is the MCP client's own child, with the client's environment. So the
//! shim writes the caller into each `tools/call`'s arguments, through
//! [`DaemonOptions::shim_transform`](sml_mcps::DaemonOptions::shim_transform),
//! and the `whoami` tool reads it back out.
//!
//! The key is replaced when the shim has a caller and removed when it has
//! none, so a model cannot speak for somebody else by putting it there itself.
//!
//! Try it:
//! ```text
//! echo '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"whoami","arguments":{"_caller":"forged"}}}' \
//!   | SHIM_TRANSFORM_CALLER=alice cargo run --example shim_transform
//! ```

#[cfg(unix)]
fn main() -> sml_mcps::Result<()> {
    use serde_json::Value;
    use sml_mcps::{
        CallToolResult, DaemonOptions, JsonRpcMessage, Result, Server, ServerConfig, Tool, ToolEnv,
        user_socket_path,
    };

    /// Where the shim writes the caller. Underscored and in no schema: the
    /// shim's, never a model's.
    const CALLER_ARG: &str = "_caller";

    /// Who is calling, as the shim's environment says.
    const CALLER_ENV: &str = "SHIM_TRANSFORM_CALLER";

    /// Say who the call came from, and what else it was given.
    struct WhoamiTool;
    impl Tool<()> for WhoamiTool {
        fn name(&self) -> &str {
            "whoami"
        }
        fn description(&self) -> &str {
            "Return the caller the shim stamped on this call, and the arguments"
        }
        fn schema(&self) -> Value {
            serde_json::json!({ "type": "object", "properties": {} })
        }
        fn execute(&self, mut args: Value, _: &mut (), _e: &ToolEnv) -> Result<CallToolResult> {
            let caller = args
                .as_object_mut()
                .and_then(|args| args.remove(CALLER_ARG));
            Ok(CallToolResult::text(
                serde_json::json!({ "caller": caller, "arguments": args }).to_string(),
            ))
        }
    }

    /// The transform: stamp `caller` on every tool call, or take out whatever
    /// is there when there is no caller. Everything else passes untouched.
    fn stamp(message: &mut JsonRpcMessage, caller: Option<&str>) {
        let JsonRpcMessage::Request(request) = message else {
            return;
        };
        if request.method != "tools/call" {
            return;
        }
        let Some(params) = request.params.as_mut().and_then(Value::as_object_mut) else {
            return;
        };
        let arguments = params
            .entry("arguments")
            .or_insert_with(|| Value::Object(Default::default()));
        let Some(arguments) = arguments.as_object_mut() else {
            return;
        };
        match caller {
            Some(id) => arguments.insert(CALLER_ARG.into(), id.into()),
            None => arguments.remove(CALLER_ARG),
        };
    }

    let args: Vec<String> = std::env::args().collect();
    let socket_path = args
        .iter()
        .position(|a| a == "--socket")
        .and_then(|i| args.get(i + 1))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| user_socket_path("sml_mcps-shim-transform-example", "server.sock"));

    let mut server: Server<()> = Server::new(ServerConfig {
        name: "shim-transform-example".to_string(),
        version: "1.0.0".to_string(),
        ..Default::default()
    });
    server.add_tool(WhoamiTool)?;

    // Read here, in every mode, but only the shim ever uses it: the daemon a
    // shim starts inherits that shim's environment, and must not answer every
    // later shim as if it were the first.
    let caller = std::env::var(CALLER_ENV).ok().filter(|id| !id.is_empty());

    server.serve_daemon_with(
        DaemonOptions::new(&socket_path)
            .idle_timeout(std::time::Duration::from_secs(60))
            .shim_transform(move |message| {
                stamp(message, caller.as_deref());
                Ok(())
            }),
        |_conn_id| (),
    )
}

#[cfg(not(unix))]
fn main() {
    eprintln!("The shim_transform example requires a Unix platform.");
}
