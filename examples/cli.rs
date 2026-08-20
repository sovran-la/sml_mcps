//! The subcommands every MCP server ends up needing, for free.
//!
//! One binary that installs itself into the MCP clients on this machine,
//! sets up a directory of its own on the way, removes both again, serves,
//! reports its own health, and carries one subcommand besides.
//!
//! ```text
//! cargo run --example cli --features cli -- --help
//! cargo run --example cli --features cli -- install --client cursor
//! cargo run --example cli --features cli -- doctor --verbose
//! cargo run --example cli --features cli -- uninstall
//! ```
//!
//! Note that `install` writes to your real client configs - it keeps a `.bak`
//! of each, and `uninstall` puts them back. It also writes `~/.cli-example/`,
//! which is this example's own setup hook doing what your server's would.

use serde_json::Value;
use sml_mcps::cli::{Cli, ServerEntry, SubCommand, all_clients, home_dir};
use sml_mcps::{CallToolResult, Result, Server, ServerConfig, StdioTransport, Tool, ToolEnv};
use std::path::PathBuf;
use std::process::ExitCode;

/// Nothing shared, in this one.
struct AppContext;

/// A tool, so there is something to serve.
struct PingTool;

impl Tool<AppContext> for PingTool {
    fn name(&self) -> &str {
        "ping"
    }

    fn description(&self) -> &str {
        "Answer, to prove the server is up"
    }

    fn schema(&self) -> Value {
        serde_json::json!({ "type": "object", "properties": {} })
    }

    fn execute(
        &self,
        _args: Value,
        _ctx: &mut AppContext,
        _env: &ToolEnv,
    ) -> Result<CallToolResult> {
        Ok(CallToolResult::text("pong"))
    }
}

fn main() -> ExitCode {
    Cli::new(entry())
        .description("An example MCP server that installs itself")
        .on_install(|_args| {
            // Runs before a single client config is written. Whatever your
            // server needs on disk to be worth pointing a client at - a data
            // directory, a config file, an API key asked for at a prompt -
            // goes here, and an error stops the install before it edits
            // anything.
            let Some(dir) = data_dir() else {
                // No home directory to set anything up in. Not this hook's
                // failure to report: `install` has a pasteable snippet for
                // exactly that machine, and stopping here would swallow it.
                return Ok(());
            };

            std::fs::create_dir_all(&dir)?;
            std::fs::write(dir.join("config.json"), "{\n  \"mode\": \"demo\"\n}\n")?;
            println!("Prepared {}.", dir.display());

            Ok(())
        })
        .on_uninstall(|_args| {
            // Runs once the client entries are gone, to clean up after the
            // hook above. A failure here is printed and fails the process, but
            // the entries stay removed - there is no putting them back.
            let Some(dir) = data_dir().filter(|dir| dir.exists()) else {
                return Ok(());
            };

            std::fs::remove_dir_all(&dir)?;
            println!("Removed {}.", dir.display());

            Ok(())
        })
        .on_serve(|_args| {
            let mut server = Server::new(ServerConfig {
                name: "cli-example".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                instructions: Some("An example server with one tool.".to_string()),
                ..Default::default()
            });
            server.add_tool(PingTool)?;

            eprintln!("cli-example: serving on stdio");
            server.start(StdioTransport::new(), AppContext)?;

            Ok(())
        })
        .on_health(|_args| {
            // Whatever "healthy" means for your server. Here: can we see the
            // machine's MCP clients at all?
            let detected = all_clients()
                .iter()
                .filter(|client| client.detect())
                .count();
            println!("ok - {detected} MCP client(s) detected");

            Ok(())
        })
        .command(
            SubCommand::new("doctor", "Report what each MCP client thinks of us")
                .flag("--verbose", "Print the config path for every client"),
            |args| {
                let verbose = args.iter().any(|arg| arg == "--verbose");
                // The same entry `install` writes, or this reports every
                // client as outdated for differing from an entry nothing ever
                // installed.
                let entry = entry();

                for client in all_clients() {
                    println!("{}: {}", client.name(), client.check_existing(&entry));
                    if verbose {
                        println!("    {}", client.config_path().display());
                    }
                }

                Ok(())
            },
        )
        .run()
}

/// What gets written into every client's config.
///
/// The binary path is worked out for you; this is only the part that is yours.
/// One function rather than a literal in `main`, because `doctor` asks the
/// clients about the same entry and a second spelling of it would be a second
/// answer.
fn entry() -> ServerEntry {
    ServerEntry::new("cli-example", &["serve"]).with_env("CLI_EXAMPLE_MODE", "demo")
}

/// Where this example keeps the configuration it writes for itself.
///
/// `None` on a machine with no home directory, which the install hook above
/// treats as nothing to do rather than as a failure.
fn data_dir() -> Option<PathBuf> {
    home_dir().map(|home| home.join(".cli-example"))
}
