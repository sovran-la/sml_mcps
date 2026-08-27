# sml_mcps

[![CI](https://github.com/MemoryCo/sml_mcps/actions/workflows/ci.yml/badge.svg)](https://github.com/MemoryCo/sml_mcps/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/MemoryCo/sml_mcps/graph/badge.svg)](https://codecov.io/gh/MemoryCo/sml_mcps)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Small MCP Server** - A minimal, sync MCP server implementation. No tokio, no async, just works.

## Why?

The official `rmcp` SDK is async/tokio-based. That's fine for some use cases, but:

1. **Tokio is viral** - once you're async, everything wants to be async
2. **MCP is sequential** - request → response → request → response  
3. **53% test coverage** - rmcp is young and under-tested
4. **Apache 2 licensed** - rmcp switched from MIT; we prefer MIT
5. **We want control** - our core crates are sync

sml_mcps gives us a clean, sync MCP server that we control.

## Features

```toml
[features]
default = ["schema"]
schema = ["dep:schemars"]     # JSON Schema generation for tools
http = ["dep:httparse"]        # Streamable HTTP transport (with SSE)
auth = ["dep:jsonwebtoken"]    # JWT validation for hosted
hosted = ["http", "auth"]      # Both HTTP and auth
tls = ["http", "dep:rustls", "dep:rustls-pemfile"]  # HTTPS, via rustls
cli = ["dep:toml_edit"]        # install/uninstall/serve/health subcommands
```

## Usage (Stdio)

Define your context and tools, then wire them up:

```rust
use sml_mcps::{Server, ServerConfig, StdioTransport, Tool, ToolEnv, CallToolResult, Result, LogLevel};
use serde_json::Value;

// Your shared context
struct AppContext {
    counter: i64,
}

// Define a tool
struct IncrementTool;

impl Tool<AppContext> for IncrementTool {
    fn name(&self) -> &str { "increment" }
    fn description(&self) -> &str { "Increment the counter" }
    
    fn schema(&self) -> Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "amount": { "type": "integer", "description": "Amount to increment by" }
            }
        })
    }
    
    fn execute(&self, args: Value, ctx: &mut AppContext, env: &ToolEnv) -> Result<CallToolResult> {
        let amount = args.get("amount").and_then(|a| a.as_i64()).unwrap_or(1);
        ctx.counter += amount;
        
        // Send notification to client
        env.log(LogLevel::Info, format!("Counter is now {}", ctx.counter))?;
        
        Ok(CallToolResult::text(format!("Counter: {}", ctx.counter)))
    }
}

fn main() -> Result<()> {
    let config = ServerConfig {
        name: "my-server".to_string(),
        version: "1.0.0".to_string(),
        instructions: Some("A counter server".to_string()),
    };
    
    let mut server = Server::new(config);
    server.add_tool(IncrementTool)?;
    
    let context = AppContext { counter: 0 };
    let transport = StdioTransport::new();
    
    server.start(transport, context)
}
```

## Unix Socket Transport (Daemon/Shim)

For servers with expensive state (ML models, indexes, agent registries), you don't want every client spawning its own instance. `UnixServer` runs a daemon that multiple clients share via lightweight shims.

**One binary, three modes** — `Server::serve_daemon` reads `argv` and dispatches:

```rust
use sml_mcps::{Server, ServerConfig, Result, user_socket_path};
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::time::Duration;

fn main() -> Result<()> {
    let mut server: Server<AppContext> = Server::new(config());
    server.add_tool(PingTool)?;

    // Expensive state lives here, built once, shared by every connection.
    let counter = Arc::new(AtomicI64::new(0));

    server.serve_daemon(
        user_socket_path("my-daemon", "server.sock"),
        Duration::from_secs(300),      // exit after 5min idle
        move |conn_id| AppContext {
            conn_id: conn_id.to_string(),
            counter: counter.clone(),
        },
    )
}
```

| launched as | what it does |
|---|---|
| `my-daemon --daemon` | double-forks, detaches, serves on the socket |
| `my-daemon --foreground` | serves without detaching (debugging) |
| `my-daemon` | acts as a shim: starts the daemon if needed, proxies stdio to it |

An MCP client only ever launches the last one. Socket directories are created if
missing, and every connection gets a fresh `Server` carrying the tools,
resources, prompts and task runtime you registered. See
`examples/serve_daemon.rs`.

**Where the socket goes matters.** `user_socket_path("my-daemon",
"server.sock")` resolves to `$XDG_RUNTIME_DIR/my-daemon/server.sock` where the
system provides one and `~/.local/state/my-daemon/server.sock` where it does
not, and the directory is created `0700`. A socket in `/tmp` is a path any
account on the machine can create *first* - bind it, wait for the shim to
connect, and read or answer every message of the session. Both sides refuse a
path they do not own: the daemon will not bind one, and `Bridge::auto_start`
will not connect to one.

The two halves are public too, if you want the pieces rather than the pattern:

**Daemon** (one long-lived process):

```rust
use sml_mcps::{UnixServer, ServerConfig, Server, Tool, ToolEnv, CallToolResult, Result, user_socket_path};
use serde_json::Value;
use std::time::Duration;

struct AppContext { conn_id: String }

struct PingTool;
impl Tool<AppContext> for PingTool {
    fn name(&self) -> &str { "ping" }
    fn description(&self) -> &str { "Ping" }
    fn schema(&self) -> Value { serde_json::json!({ "type": "object" }) }
    fn execute(&self, _a: Value, ctx: &mut AppContext, _e: &ToolEnv) -> Result<CallToolResult> {
        Ok(CallToolResult::text(format!("pong from {}", ctx.conn_id)))
    }
}

fn main() -> Result<()> {
    let config = ServerConfig {
        name: "my-daemon".to_string(),
        version: "1.0.0".to_string(),
        ..Default::default()
    };

    UnixServer::new(config)
        .idle_timeout(Duration::from_secs(300))  // exit after 5min idle
        .with_tools(|s: &mut Server<AppContext>| {
            s.add_tool(PingTool)?;
            Ok(())
        })
        .serve_daemon(user_socket_path("my-daemon", "server.sock"), |conn_id| {
            AppContext { conn_id: conn_id.to_string() }
        })
}
```

**Shim** (one per client, near-zero overhead):

```rust
use sml_mcps::{Bridge, Result, user_socket_path};

fn main() -> Result<()> {
    let upstream = Bridge::auto_start(
        user_socket_path("my-daemon", "server.sock"),
        "my-daemon",
        &["--daemon"],
    )?;
    Bridge::run_stdio(upstream)
}
```

`auto_start` connects to a running daemon, or starts one if needed (handling stale sockets and PID files, and refusing a socket this user does not own). The shim is a transparent proxy — MCP over stdio on one side, Unix socket on the other.

See `examples/unix_server.rs` for a complete single-binary daemon + shim wired up
by hand, and `examples/serve_daemon.rs` for the same thing in one call.

### More than one listener

A daemon can accept from more than one place at once. `DaemonOptions` is
`serve_daemon` with everything else it can be told:

```rust
use sml_mcps::{DaemonOptions, Server, TcpSocketListener, user_socket_path};
use std::time::Duration;

server.serve_daemon_with(
    DaemonOptions::new(user_socket_path("my-daemon", "server.sock"))
        .idle_timeout(Duration::from_secs(300))
        .listener(TcpSocketListener::bind("127.0.0.1:7743")?)
        .busy_check(move || jobs.any_running()),
    move |conn_id| AppContext::new(conn_id),
)
```

Every listener shares one connection ceiling, one idle clock, and the same
per-connection `Server` — the only thing that differs is where the bytes came
from. The Unix socket keeps the calling thread, so `serve` blocks exactly as it
did; each extra listener gets a thread of its own. The same two methods exist on
`UnixServer` directly (`.listener(...)`, `.busy_check(...)`) if you are driving
that rather than the one-call dispatch.

**`TcpSocketListener` is in the clear.** No encryption, no authentication —
anything that can route to the address gets an MCP session, and an MCP session
runs tools as the daemon's user. Bind it somewhere only people you already trust
can reach.

**For anything else — mTLS included — implement `Listener` yourself.** It has
three methods, and `accept` hands back a `Box<dyn Transport>`, so whatever the
connection needs before it can carry messages happens on your side of the seam.
sml_mcps carries no TLS dependency and never learns of one:

```rust
use sml_mcps::{Listener, StreamTransport, Transport};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};

struct MtlsListener {
    tcp: TcpListener,
    addr: SocketAddr,
    acceptor: MyRustlsAcceptor,
}

impl Listener for MtlsListener {
    fn accept(&self) -> io::Result<Box<dyn Transport>> {
        let (stream, _peer) = self.tcp.accept()?;
        // A client without a valid certificate fails here, and the daemon
        // treats that as one connection lost — not a daemon down.
        let secured = self.acceptor.handshake(stream)?;
        Ok(Box::new(StreamTransport::new(secured)))
    }

    // Must return a thread parked in `accept`: the daemon joins its accept
    // threads on the way out, so a listener that cannot be woken is a daemon
    // that cannot exit.
    fn wake(&self) -> io::Result<()> {
        TcpStream::connect(self.addr).map(|_| ())
    }

    fn describe(&self) -> String { format!("mtls://{}", self.addr) }
}
```

`StreamTransport` wraps anything `Read + Write + Send` in the same
newline-delimited JSON-RPC framing. It is honest about what an opaque stream
cannot do — it reports that it can neither be split into an independent writer
nor carry a read deadline — which costs exactly one thing: task workers on those
connections may not make server-initiated requests (elicitation, sampling).
Tools, resources, prompts, notifications and logging are unaffected. `Server`
already handles both, the same way it does for the HTTP transport.

### Staying up while work is running

Idle shutdown counts *connections*. A daemon supervising something that outlives
the session that asked for it — a child process, a background job — would idle
out from under it. `busy_check` is the veto:

```rust
UnixServer::new(config)
    .idle_timeout(Duration::from_secs(300))
    .busy_check({
        let jobs = jobs.clone();
        move || jobs.any_running()
    })
```

The daemon exits only when it has been idle for the whole window **and**
`is_busy` answers `false`. A veto costs one more window: the watcher re-arms and
asks again after `idle_timeout`, so the poll interval is that timeout and not a
second knob. It is asked only at the moment the daemon is about to leave, and
never while a client is connected.

**A signal is not vetoable.** SIGTERM and SIGINT mean now; this is a guard on the
daemon's own decision to leave, not on the operator's.

With no `busy_check` configured, nothing about the idle path changes.

See `examples/multi_listener.rs` for all of it in one binary.

## HTTP Transport (Streamable HTTP with SSE)

With the `http` feature, `HttpServer` handles all the HTTP boilerplate for you.
The HTTP/1.1 layer is ours, on `std::net`, with `httparse` for the request
grammar — one crate, no transitive dependencies. Requests are served
concurrently, one thread per connection, so a client blocked in `tasks/result`
cannot hold up anybody else. The context factory is called per request, on that
request's own thread, so it must be `Send + Sync`.

Everything a peer controls has a ceiling, and each one is a knob:

| Knob | Default | Bounds |
|---|---|---|
| `HttpServer::max_connections` | 512 | live connections, and so threads |
| `HttpServer::read_timeout` | 30s | a whole request — head *and* body, from its first byte |
| `HttpServer::idle_timeout` | 2m | a kept-alive connection between requests |
| `ServerConfig::max_message_bytes` | 8 MiB | a request body |

`read_timeout` is an aggregate, not a per-read deadline, and that distinction is
the difference between bounding a peer that has *stopped* talking and bounding
one that is talking *slowly*. A per-read deadline is renewed by every byte that
arrives, so a request dribbled a byte at a time never meets one — and a few
bytes a second, times `max_connections` sockets, is a server nobody else can
reach. A request still unfinished when its budget runs out is answered `408` and
its connection is closed. The trade is that a client on a slow enough link
cannot deliver a large body; size this against `max_message_bytes` and the
slowest link you intend to serve.

A connection arriving when `max_connections` are already live is answered `503`
and closed — by a thread of its own, never the accept loop. A body over the
ceiling is answered `413` without being read or drained, and its connection is
closed rather than left half-framed.

This is safe to expose directly. Behind a reverse proxy, terminate TLS there
and bind to loopback.

```rust
use sml_mcps::{HttpServer, ServerConfig, Tool, ToolEnv, CallToolResult, Result};
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

struct CounterTool;

impl Tool<AppContext> for CounterTool {
    fn name(&self) -> &str { "counter" }
    fn description(&self) -> &str { "Increment counter" }
    fn schema(&self) -> Value { serde_json::json!({ "type": "object" }) }
    
    fn execute(&self, _args: Value, ctx: &mut AppContext, _env: &ToolEnv) -> Result<CallToolResult> {
        let val = ctx.counter.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(CallToolResult::text(format!("Counter: {}", val)))
    }
}

struct AppContext {
    counter: Arc<AtomicI64>,
}

fn main() -> Result<()> {
    let shared_counter = Arc::new(AtomicI64::new(0));
    
    let config = ServerConfig {
        name: "my-http-server".to_string(),
        version: "1.0.0".to_string(),
        instructions: None,
    };

    HttpServer::new(config)
        .endpoint("/mcp")  // optional, this is the default
        .with_tools(|server| {
            server.add_tool(CounterTool)?;
            Ok(())
        })
        .serve("127.0.0.1:3000", {
            let counter = shared_counter.clone();
            move || AppContext { counter: counter.clone() }
        })
}
```

**Key feature**: When tools send notifications (via `env.log()` or `env.send_progress()`), 
the response is automatically formatted as SSE. For requests without notifications, plain JSON is returned.

See `examples/http_server.rs` for a complete example.

## JWT Authentication

With the `hosted` feature (enables both `http` and `auth`), add JWT validation:

```rust
use sml_mcps::{HttpServer, ServerConfig, auth::{JwtValidator, ResourceUri}};

struct AuthContext {
    user_id: String,
    tenant_id: String,
}

fn main() -> Result<()> {
    let config = ServerConfig {
        name: "authenticated-server".to_string(),
        version: "1.0.0".to_string(),
        instructions: None,
    };

    // The canonical URI clients ask for tokens for, and the only audience
    // this server accepts one from.
    let resource = ResourceUri::parse("https://mcp.example.com/mcp")?;

    HttpServer::new(config)
        .with_tools(|server| {
            server.add_tool(WhoamiTool)?;
            Ok(())
        })
        .require_scopes(["mcp:use"])   // optional: 403 + insufficient_scope
        .serve_with_auth(
            "127.0.0.1:3001",
            JwtValidator::hs256(b"your-secret-key").for_resource(&resource),
            |claims| AuthContext {
                user_id: claims.user_id().to_string(),
                tenant_id: claims.tenant_id().to_string(),
            },
        )
}
```

`for_resource` is not optional. MCP servers **MUST** reject tokens that do not
name them in the `aud` claim (RFC 8707), so `serve_with_auth` refuses to start
with a validator that enforces no audience.

The validator supports both HS256 (symmetric) and RS256 (asymmetric) algorithms:

```rust
// HS256 (symmetric)
let validator = JwtValidator::hs256(b"your-secret-key").for_resource(&resource);

// RS256 (asymmetric)
let validator = JwtValidator::rs256_pem(&public_key_pem)?.for_resource(&resource);
```

See `examples/http_auth.rs` for a complete authenticated server.

## Tool Environment

During tool execution, `ToolEnv` provides:

```rust
// Send log notification
env.log(LogLevel::Info, "Processing...")?;

// Send progress update against the token the client asked for
env.report_progress(0.5, Some(1.0))?;
// ...or quote a token explicitly (string or number)
env.send_progress("token", 0.5, Some(1.0))?;

// Access resources
let uris = env.list_resources();
let resource = env.get_resource("my://resource")?;
```

## Low-Level HTTP (Advanced)

If you need custom HTTP handling, you can use `HttpTransport` directly:

```rust
use sml_mcps::{Server, ServerConfig, HttpTransport};
use std::sync::{Arc, Mutex};

// In your HTTP handler:
let transport = Arc::new(Mutex::new(HttpTransport::new(request_body)));

server.process_one(transport.clone(), &mut context)?;

let mut t = transport.lock().unwrap();
if t.has_notifications() {
    // Return as SSE (Content-Type: text/event-stream)
    let sse_body = t.take_sse_response();
} else {
    // Return plain JSON (Content-Type: application/json)
    let json_body = t.take_response().unwrap_or_default();
}
```

## CLI Harness (`cli` feature)

Every MCP server ends up writing the same four subcommands. `install` and
`uninstall` are the same mechanical work everywhere - find the MCP clients on
this machine, edit their config files - so the crate does them for you.

```rust
use sml_mcps::cli::{Cli, ServerEntry, SubCommand};
use std::process::ExitCode;

fn main() -> ExitCode {
    Cli::new(ServerEntry::new("my-mcp", &["serve"]))
        .description("An MCP server for doing cool stuff")
        .on_install(|_args| {
            // your own setup, before any client config is written
            Ok(())
        })
        .on_uninstall(|_args| {
            // and your own cleanup, once the entries are gone
            Ok(())
        })
        .on_serve(|_args| {
            // build the server, start the transport
            Ok(())
        })
        .on_health(|_args| { println!("ok"); Ok(()) })
        .command(
            SubCommand::new("doctor", "Run diagnostic checks")
                .flag("--verbose", "Show detailed output for each check"),
            |args| { println!("checking… {args:?}"); Ok(()) },
        )
        .run()
}
```

```text
$ my-mcp --help
my-mcp — An MCP server for doing cool stuff

USAGE: my-mcp <COMMAND>

COMMANDS:
    install      Install this server into MCP client configs
    uninstall    Remove this server from MCP client configs
    serve        Start the MCP server
    health       Run health checks
    doctor       Run diagnostic checks
```

Six clients are known, on macOS and Linux:

| Client | Config | Key |
|---|---|---|
| Claude Desktop | `~/Library/Application Support/Claude/claude_desktop_config.json` | `mcpServers` |
| Claude Code | `~/.claude.json` | `mcpServers` |
| Cursor | `~/.cursor/mcp.json` | `mcpServers` |
| Windsurf | `~/.codeium/windsurf/mcp_config.json` | `mcpServers` |
| VS Code | `~/Library/Application Support/Code/User/mcp.json` | `servers` |
| Codex | `~/.codex/config.toml` | `mcp_servers` (TOML) |

Claude Desktop and VS Code live under `~/.config/` on Linux. `install --client
<name>` targets one; a client that could not be configured gets a pasteable
snippet instead.

Every write parses the config whole, changes one entry, and puts everything else
back - including comments, in Codex's hand-written TOML. The previous contents
are kept as `.bak`, the write itself goes through a rename, and both directions
are idempotent.

A server whose tools should not stop for an approval prompt says so on its
entry:

```rust
use sml_mcps::cli::ServerEntry;

let entry = ServerEntry::new("my-mcp", &["serve"]).with_auto_approve();
```

Off by default - whether a tool call needs approving is the user's decision. It
is a request rather than a guarantee: Codex is the only one of the six with a
config-time mechanism for it, and gets `default_tools_approval_mode = "auto"` on
its entry. The rest are written exactly as they always were. An approval mode
already in the file that this crate was not asked to write is left alone.

The client entries are only half of an install. The other half - a data
directory, a config file, an API key to ask for - is `on_install`, which runs
before the first config is touched and stops the install if it returns an error;
nothing is written, so a failed setup leaves no client pointing at a server that
is not ready. `on_uninstall` is the other end of it, and runs after the entries
have been removed. A command line that is refused - an unknown flag, an unknown
`--client` - reaches neither.

The registry is public API in its own right, for servers that own their command
line already:

```rust
use sml_mcps::cli::{ServerEntry, all_clients};

let entry = ServerEntry::new("my-mcp", &["serve"]);

for client in all_clients() {
    println!("{}: {}", client.name(), client.check_existing(&entry));
}
```

See `examples/cli.rs` - `cargo run --example cli --features cli -- --help`.

## Protocol Version

Implements MCP protocol version `2025-11-25`, and negotiates down to
`2025-06-18` or `2025-03-26` for older clients.

Supported across those revisions:

- **Tools** with `outputSchema` / `structuredContent`, titles, icons, and
  annotations
- **Resources** and resource templates; **Prompts**
- **Logging** with `logging/setLevel` and a stderr fallback
- **Elicitation**, both form and URL mode, with a schema builder
- **Sampling**, including tool calling (`tools` / `toolChoice`)
- **Roots**
- **Tasks** - task-augmented `tools/call`, polling, and cooperative
  cancellation, opt-in via `Server::enable_tasks`
- **Streamable HTTP** with `Origin` validation, `MCP-Protocol-Version`
  handling, and per-session state keyed on `Mcp-Session-Id`
- **Authorization** as an OAuth 2.1 resource server: RFC 8707 audience
  validation and RFC 9728 Protected Resource Metadata (`auth` feature)

See [docs/2025-11-25-migration-notes.md](docs/2025-11-25-migration-notes.md)
for the decision log and the breaking changes from 0.5.x.

## What's NOT Included

- **Client implementation** - this is a server SDK
- **Async anything** - by design
- **`completion/complete`**, resource subscriptions, and `listChanged`
  notifications - all optional, and none are declared as capabilities
- **SSE resumability** (`Last-Event-ID`) - a response is one buffered body, so
  there is no long-lived stream to resume
- **TLS for the daemon** - `Listener` takes the streams you already secured, so
  the handshake, the certificates and the policy stay in your crate. The `tls`
  feature is HTTPS for the HTTP transport and nothing else

## License

MIT
