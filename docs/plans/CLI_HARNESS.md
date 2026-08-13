# CLI Harness — Built-in Subcommands for MCP Servers

**Date:** 2026-08-10
**Authors:** Brandon Sneed, Porter
**Branch:** TBD

## Goal

Every MCP server written with sml_mcps repeats the same CLI subcommands: `install`, `uninstall`, `serve`, `health`. The install/uninstall logic is especially mechanical — discover which MCP clients are on this machine, edit their config files, handle backups. None of that varies by server; only the server's own identity (name, args, env) does.

Pull this into sml_mcps so that any binary using the crate gets these subcommands for free, and as the list of supported clients grows, every server picks it up with a version bump.

## Current State

The pattern has been built twice:

1. **mcp_tailscale_ssh** — Full `McpClient` trait with `JsonClient` + `CodexClient` implementations, 6 clients (Claude Desktop, Claude Code, Cursor, Windsurf, VS Code, Codex), atomic writes with `.bak` backups, `InstallStatus` enum, pasteable snippet fallback. ~900 lines, well-tested.

2. **Various MemoryCo MCPs** — Ad-hoc install commands that write `claude_desktop_config.json` directly. Less complete, known bugs (memoryco_agents doesn't add its own entry).

Neither is reusable without copy-paste.

## What Moves Into sml_mcps

### Client Registry

The `McpClient` trait and its implementations become a module in sml_mcps behind a feature flag. The registry knows every supported client's config path per platform:

| Client | Config Path (macOS) | Key |
|---|---|---|
| Claude Desktop | `~/Library/Application Support/Claude/claude_desktop_config.json` | `mcpServers` |
| Claude Code | `~/.claude.json` | `mcpServers` |
| Cursor | `~/.cursor/mcp.json` | `mcpServers` |
| Windsurf | `~/.codeium/windsurf/mcp_config.json` | `mcpServers` |
| VS Code | `~/Library/Application Support/Code/User/mcp.json` | `servers` |
| Codex | `~/.codex/config.toml` | (TOML format) |

Linux paths differ for Claude Desktop and VS Code (under `~/.config/`). The rest are the same.

### Server Identity

The MCP author provides the minimal bits that distinguish their server:

```rust
pub struct ServerEntry {
    /// Name in the client's config, e.g. "tailscale-ssh", "memoryco"
    pub name: &'static str,

    /// Arguments passed to the binary, e.g. &["serve"]
    pub args: &'static [&'static str],

    /// Environment variables pinned in the config entry.
    /// e.g. [("MCP_TAILSCALE_SSH_HOME", "/path/to/data")]
    pub env: Vec<(String, String)>,
}
```

The binary path is auto-detected via `std::env::current_exe()`. The pasteable snippet for clients that can't be auto-configured is generated from this same struct.

### Subcommands

The crate provides a runner that dispatches to built-in subcommands before falling through to the server's own logic:

| Subcommand | What it does |
|---|---|
| `install` | Detect clients on this machine, write/update entry in each. Print snippet for any that can't be auto-configured. |
| `uninstall` | Remove entry from all clients that have one. |
| `serve` | Start the MCP server (delegates to the author's server setup). |
| `health` | Server-defined health check — the crate provides the subcommand dispatch, the author provides the check. |

`install` and `uninstall` are fully handled by sml_mcps. `serve` and `health` call back to author-provided closures/trait methods.

### API Shape

Builder pattern — no traits to implement, just closures.

#### SubCommand + Help

Every subcommand (built-in or custom) carries its own help metadata:

```rust
pub struct SubCommand {
    pub name: &'static str,
    pub description: &'static str,
    flags: Vec<(&'static str, &'static str)>,  // (flag, description)
}

impl SubCommand {
    pub fn new(name: &'static str, description: &'static str) -> Self { ... }
    pub fn flag(mut self, flag: &'static str, description: &'static str) -> Self { ... }
}
```

The built-in commands define their own SubCommand structs internally (e.g. `install` has `--client <name>`).

#### Builder

```rust
sml_mcps::cli::Cli::new(ServerEntry { ... })
    .on_serve(|args| { /* build server, start transport */ })
    .on_health(|args| { /* check whatever */ })
    .command(
        SubCommand::new("doctor", "Run diagnostic checks")
            .flag("--verbose", "Show detailed output for each check")
            .flag("--fix", "Attempt to auto-fix problems found"),
        |args| { /* author's logic */ }
    )
    .run();
```

#### Help Output

`my-mcp --help` lists all commands with descriptions:

```
my-mcp — An MCP server for doing cool stuff

USAGE: my-mcp <COMMAND>

COMMANDS:
    install      Install this server into MCP client configs
    uninstall    Remove this server from MCP client configs
    serve        Start the MCP server
    health       Run health checks
    doctor       Run diagnostic checks
```

`my-mcp doctor --help` shows that command's flags:

```
Run diagnostic checks

USAGE: my-mcp doctor [OPTIONS]

OPTIONS:
    --verbose    Show detailed output for each check
    --fix        Attempt to auto-fix problems found
```

This is display-only — no parsing, no type system, no validation. The closure receives raw `&[String]` args and handles its own flag parsing. We're generating help screens, not building clap.

#### Standalone Install API

The client registry and install/uninstall logic are public API independent of the CLI harness. Authors who want to own their own CLI (clap, roll their own, whatever) can call the install module directly:

```rust
use sml_mcps::cli::{all_clients, ServerEntry};

let entry = ServerEntry { name: "my-mcp", args: &["serve"], env: vec![] };

// Install into all detected clients
for client in all_clients() {
    if client.detect() {
        client.install(&entry)?;
    }
}

// Or target one
if let Some(client) = all_clients().into_iter().find(|c| c.name() == "Claude Code") {
    client.uninstall(&entry)?;
}

// Check status
for client in all_clients() {
    println!("{}: {}", client.name(), client.check_existing(&entry));
}

// Get pasteable snippet for manual setup
let snippet = ServerEntry::snippet(&entry);
```

The CLI harness is one consumer of this module — a convenient default, not a gatekeeper. The builder calls the same public functions under the hood.

### Feature Flag

```toml
[features]
cli = []  # Client registry + CLI subcommand dispatch
```

No new external dependencies. The install module is pure `std::fs` + `serde_json` (already a dep). CLI arg parsing stays hand-rolled — we're matching 4 known strings, not building a flag parser.

## What Stays Out

- **clap** — Parsing `install`, `uninstall`, `serve`, `health` from `std::env::args()` is a match statement. We're not adding 50+ transitive deps for that.
- **Transport setup** — `serve` calls back to the author. The crate doesn't decide whether you're using stdio, unix, or HTTP.
- **Server-specific env discovery** — Things like "find the Tailscale socket path" are the author's job. The crate just writes whatever env vars you hand it.
- **`doctor`** — mcp_tailscale_ssh has this, but it's server-specific diagnostics. Not a universal subcommand. Authors can add custom subcommands through the builder.

## File Writes

Same safety guarantees as the mcp_tailscale_ssh implementation:

- Parse the config whole, insert/remove one entry, write everything else back untouched.
- Atomic writes via rename — a crash can't leave a broken config.
- `.bak` of the previous contents before every write.
- Idempotent in both directions — install twice = same result, uninstall from nothing = no-op.
- Missing parent directories are created.

## Growth Path

The client registry is the real value here. As new MCP clients appear (or existing ones change their config locations), sml_mcps gains them once and every server benefits. Candidates on the horizon:

- JetBrains IDEs (when they ship MCP support)
- Zed
- Any new Anthropic product that consumes MCPs

## Decisions

1. **Custom subcommands** — Yes, day one. Builder supports `.command(SubCommand, |args| { ... })` with help metadata (description + flags).

2. **Client filtering** — Yes, day one. `install --client claude-code` to target a single client. Useful for scripting and selective installs.

3. **`--json` output** — No. Not worth the complexity right now.

4. **`serve` dispatch** — The author's closure handles it. The CLI just calls the closure — whether that calls `serve_daemon`, sets up stdio, or does something custom is entirely up to the author.

5. **Config format versioning** — Track HEAD. When a client changes format, we update the registry. No version negotiation.
