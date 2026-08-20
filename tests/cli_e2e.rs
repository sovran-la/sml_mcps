//! End-to-end tests for the CLI harness, through a real binary.
//!
//! The dispatch, the help text and every client's config edit are unit-tested
//! in `src/cli/` against injected command lines and temporary directories. Two
//! things only exist in a real process, and this is where they are checked:
//!
//! - **`argv[0]`.** A binary reached through a symlink or a rename should call
//!   itself what the user typed, not what it was built as. Only an `exec` can
//!   make `argv[0]` and [`std::env::current_exe`] disagree.
//! - **`$HOME`.** The client registry reads it, and a test cannot change its own
//!   process environment safely while other tests are running in threads beside
//!   it. A child process can have whatever `$HOME` we like.
//!
//! Both use the `cli` example, run the way a person at a shell would run it.

#![cfg(all(unix, feature = "cli"))]

use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

/// The `cli` example, built once for the whole test binary.
///
/// Built once rather than per test: cargo replaces the binary by unlinking and
/// re-linking it, so a test that checked `exists()` just before another build's
/// unlink would go on to spawn a path that had ceased to exist.
static EXAMPLE_BINARY: std::sync::LazyLock<PathBuf> = std::sync::LazyLock::new(|| {
    let status = Command::new(env!("CARGO"))
        .args(["build", "--example", "cli", "--features", "cli"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("failed to run cargo build");
    assert!(status.success(), "cargo build --example cli failed");

    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("target");
    path.push("debug");
    path.push("examples");
    path.push("cli");
    assert!(path.exists(), "example binary not found at {path:?}");
    path
});

/// The name the example registers under, from `examples/cli.rs`.
const ENTRY_NAME: &str = "cli-example";

/// Run the example with `$HOME` pointed at `home`.
fn run(home: &Path, args: &[&str]) -> Output {
    Command::new(&*EXAMPLE_BINARY)
        .args(args)
        .env("HOME", home)
        .output()
        .expect("failed to run the example")
}

/// Everything the example printed to stdout.
fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// Everything the example printed to stderr.
fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// What the process exited with.
fn code(output: &Output) -> i32 {
    output
        .status
        .code()
        .expect("the example must not be killed")
}

/// A home directory with the marker directories of three clients in it.
///
/// Claude Desktop, Windsurf and VS Code are deliberately absent, so "not
/// installed" is exercised alongside "configured".
fn home_with_clients() -> tempfile::TempDir {
    let home = tempfile::TempDir::new().unwrap();
    for marker in [".claude", ".cursor", ".codex"] {
        std::fs::create_dir(home.path().join(marker)).unwrap();
    }
    home
}

/// A client's config, parsed as JSON.
fn json_at(path: &Path) -> Value {
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

//
// argv[0]
//

#[test]
fn a_binary_calls_itself_what_it_was_invoked_as() {
    use std::os::unix::process::CommandExt;

    // `arg0` is the only way argv[0] and `current_exe` can differ, and it is
    // exactly what a symlink in ~/.local/bin/ produces.
    let home = tempfile::TempDir::new().unwrap();
    let output = Command::new(&*EXAMPLE_BINARY)
        .arg0("mcp-thing")
        .arg("--help")
        .env("HOME", home.path())
        .output()
        .unwrap();

    let help = stdout(&output);
    assert!(help.contains("USAGE: mcp-thing <COMMAND>"), "{help}");
    assert!(
        !help.contains("USAGE: cli <COMMAND>"),
        "the name it was built as is not the name the user typed: {help}"
    );
}

#[test]
fn a_commands_own_help_names_the_binary_too() {
    use std::os::unix::process::CommandExt;

    let home = tempfile::TempDir::new().unwrap();
    let output = Command::new(&*EXAMPLE_BINARY)
        .arg0("mcp-thing")
        .args(["install", "--help"])
        .env("HOME", home.path())
        .output()
        .unwrap();

    assert!(
        stdout(&output).contains("USAGE: mcp-thing install [OPTIONS]"),
        "{}",
        stdout(&output)
    );
}

//
// Help
//

#[test]
fn help_lists_every_command_and_succeeds() {
    let home = tempfile::TempDir::new().unwrap();

    for flag in ["--help", "-h", "help"] {
        let output = run(home.path(), &[flag]);

        assert_eq!(code(&output), 0, "{flag}");
        for command in ["install", "uninstall", "serve", "health", "doctor"] {
            assert!(stdout(&output).contains(command), "{flag}: {command}");
        }
    }
}

#[test]
fn a_command_asked_for_help_prints_help_rather_than_running() {
    let home = home_with_clients();

    let output = run(home.path(), &["install", "--help"]);

    assert_eq!(code(&output), 0);
    assert!(
        stdout(&output).contains("--client <name>"),
        "{}",
        stdout(&output)
    );
    assert!(
        !home.path().join(".cursor/mcp.json").exists(),
        "asking for help must not install anything"
    );
}

//
// install and uninstall, on a real filesystem
//

#[test]
fn install_configures_the_clients_that_are_there_and_says_so() {
    let home = home_with_clients();

    let output = run(home.path(), &["install"]);

    assert_eq!(code(&output), 0, "{}", stderr(&output));
    let report = stdout(&output);
    assert!(
        report.contains("Detected 3 client(s), changed 3."),
        "{report}"
    );
    assert!(report.contains("Cursor: configured ("), "{report}");
    assert!(report.contains("Claude Desktop: not installed"), "{report}");
    assert!(report.contains("Restart your MCP client"), "{report}");

    let cursor = json_at(&home.path().join(".cursor/mcp.json"));
    assert_eq!(
        cursor["mcpServers"][ENTRY_NAME]["command"],
        EXAMPLE_BINARY.display().to_string()
    );
    assert_eq!(
        cursor["mcpServers"][ENTRY_NAME]["args"],
        serde_json::json!(["serve"])
    );
    assert_eq!(
        cursor["mcpServers"][ENTRY_NAME]["env"]["CLI_EXAMPLE_MODE"],
        "demo"
    );
}

#[test]
fn install_writes_toml_for_codex_and_json_for_everyone_else() {
    let home = home_with_clients();

    run(home.path(), &["install"]);

    let codex = std::fs::read_to_string(home.path().join(".codex/config.toml")).unwrap();
    assert!(codex.contains("[mcp_servers.cli-example]"), "{codex}");
    assert!(codex.contains("args = [\"serve\"]"), "{codex}");
    assert!(json_at(&home.path().join(".claude.json"))["mcpServers"][ENTRY_NAME].is_object());
}

#[test]
fn install_leaves_everything_it_did_not_write() {
    let home = home_with_clients();
    std::fs::write(
        home.path().join(".claude.json"),
        r#"{"theme":"dark","mcpServers":{"memory":{"command":"/usr/local/bin/memoryco"}}}"#,
    )
    .unwrap();
    std::fs::write(
        home.path().join(".codex/config.toml"),
        "# keep me\nmodel = \"o3\" # and me\n",
    )
    .unwrap();

    run(home.path(), &["install"]);

    let claude = json_at(&home.path().join(".claude.json"));
    assert_eq!(claude["theme"], "dark");
    assert_eq!(
        claude["mcpServers"]["memory"]["command"],
        "/usr/local/bin/memoryco"
    );

    let codex = std::fs::read_to_string(home.path().join(".codex/config.toml")).unwrap();
    assert!(codex.contains("# keep me"), "{codex}");
    assert!(
        codex.contains("# and me"),
        "comments survive a TOML edit: {codex}"
    );
}

#[test]
fn install_keeps_the_previous_config_as_a_backup() {
    let home = home_with_clients();
    std::fs::write(home.path().join(".claude.json"), r#"{"theme":"dark"}"#).unwrap();

    run(home.path(), &["install"]);

    assert_eq!(
        std::fs::read_to_string(home.path().join(".claude.json.bak")).unwrap(),
        r#"{"theme":"dark"}"#
    );
}

#[test]
fn a_second_install_changes_nothing() {
    let home = home_with_clients();
    run(home.path(), &["install"]);
    let before: Vec<String> = config_files(&home)
        .iter()
        .map(|path| std::fs::read_to_string(path).unwrap())
        .collect();

    let output = run(home.path(), &["install"]);

    assert_eq!(code(&output), 0);
    assert!(
        stdout(&output).contains("Cursor: up to date ("),
        "{}",
        stdout(&output)
    );
    assert!(
        stdout(&output).contains("Detected 3 client(s), all up to date."),
        "{}",
        stdout(&output)
    );
    assert!(
        !stdout(&output).contains("Restart your MCP client"),
        "nothing changed, so there is nothing to restart for"
    );
    let after: Vec<String> = config_files(&home)
        .iter()
        .map(|path| std::fs::read_to_string(path).unwrap())
        .collect();
    assert_eq!(before, after);
    assert!(
        !home.path().join(".cursor/mcp.json.bak").exists(),
        "a no-op install must not churn the backup"
    );
}

#[test]
fn uninstall_puts_everything_back() {
    let home = home_with_clients();
    let original = r#"{"theme":"dark","mcpServers":{"memory":{"command":"/x"}}}"#;
    std::fs::write(home.path().join(".claude.json"), original).unwrap();
    let codex_original = "# my config\nmodel = \"o3\"\n\n[mcp_servers.memory]\ncommand = \"/x\"\n";
    std::fs::write(home.path().join(".codex/config.toml"), codex_original).unwrap();
    run(home.path(), &["install"]);

    let output = run(home.path(), &["uninstall"]);

    assert_eq!(code(&output), 0, "{}", stderr(&output));
    assert!(
        stdout(&output).contains("Removed from 3 client(s)."),
        "{}",
        stdout(&output)
    );
    assert_eq!(
        json_at(&home.path().join(".claude.json")),
        serde_json::from_str::<Value>(original).unwrap()
    );
    assert_eq!(
        std::fs::read_to_string(home.path().join(".codex/config.toml")).unwrap(),
        codex_original,
        "a TOML round trip must be byte for byte"
    );
}

#[test]
fn uninstall_from_nothing_is_a_success() {
    let home = home_with_clients();

    let output = run(home.path(), &["uninstall"]);

    assert_eq!(code(&output), 0);
    assert!(
        stdout(&output).contains("Nothing to remove"),
        "{}",
        stdout(&output)
    );
    assert!(
        !home.path().join(".cursor/mcp.json").exists(),
        "an absent config must not be invented"
    );
}

//
// The author's own install and uninstall work
//

#[test]
fn an_install_hook_runs_before_a_single_config_is_written() {
    let home = home_with_clients();

    let output = run(home.path(), &["install"]);

    assert_eq!(code(&output), 0, "{}", stderr(&output));
    let report = stdout(&output);
    let hook = report
        .find("Prepared ")
        .unwrap_or_else(|| panic!("{report}"));
    let writes = report
        .find("Installing ")
        .unwrap_or_else(|| panic!("{report}"));
    assert!(
        hook < writes,
        "the hook runs before the client configs, not after: {report}"
    );
    assert!(data_dir(&home).join("config.json").exists());
    assert!(home.path().join(".cursor/mcp.json").exists());
}

#[test]
fn an_install_hook_that_fails_stops_the_install() {
    // The hook cannot create its directory, because a file is sitting where
    // it goes. Any failure would do; this one needs no cooperation from the
    // example beyond doing its job honestly.
    let home = home_with_clients();
    std::fs::write(data_dir(&home), "in the way").unwrap();

    let output = run(home.path(), &["install"]);

    assert_eq!(code(&output), 1);
    assert!(
        stderr(&output).starts_with("error: "),
        "{}",
        stderr(&output)
    );
    assert_eq!(
        config_files(&home),
        Vec::<PathBuf>::new(),
        "a failed hook must leave every client config unwritten"
    );
    assert!(
        !stdout(&output).contains("Installing"),
        "the install must not report work it did not do: {}",
        stdout(&output)
    );
}

#[test]
fn an_uninstall_hook_runs_after_the_removals() {
    let home = home_with_clients();
    run(home.path(), &["install"]);

    let output = run(home.path(), &["uninstall"]);

    assert_eq!(code(&output), 0, "{}", stderr(&output));
    let report = stdout(&output);
    let removals = report
        .find("Removing `cli-example`")
        .unwrap_or_else(|| panic!("{report}"));
    let hook = report
        .find(&format!("Removed {}.", data_dir(&home).display()))
        .unwrap_or_else(|| panic!("{report}"));
    assert!(
        removals < hook,
        "the cleanup runs after the client configs, not before: {report}"
    );
    assert!(!data_dir(&home).exists());
}

#[test]
fn an_uninstall_hook_that_fails_does_not_undo_the_removals() {
    let home = home_with_clients();
    run(home.path(), &["install"]);
    // Leave the cleanup something it cannot remove: a file where its
    // directory was.
    std::fs::remove_dir_all(data_dir(&home)).unwrap();
    std::fs::write(data_dir(&home), "in the way").unwrap();

    let output = run(home.path(), &["uninstall"]);

    assert_eq!(code(&output), 1);
    assert!(
        stderr(&output).starts_with("error: "),
        "{}",
        stderr(&output)
    );
    assert!(
        stdout(&output).contains("Removed from 3 client(s)."),
        "{}",
        stdout(&output)
    );
    let cursor = json_at(&home.path().join(".cursor/mcp.json"));
    assert!(
        cursor["mcpServers"][ENTRY_NAME].is_null(),
        "the removals stand - there is nothing to put them back with: {cursor}"
    );
    let codex = std::fs::read_to_string(home.path().join(".codex/config.toml")).unwrap();
    assert!(!codex.contains("[mcp_servers.cli-example]"), "{codex}");
}

#[test]
fn a_refused_install_never_runs_its_hook() {
    let home = home_with_clients();

    for args in [
        vec!["install", "--clients", "cursor"],
        vec!["install", "--client", "cursur"],
    ] {
        let output = run(home.path(), &args);

        assert_eq!(code(&output), 2, "{args:?}: {}", stdout(&output));
        assert!(
            !data_dir(&home).exists(),
            "{args:?}: a command line that was refused set something up anyway"
        );
    }
}

#[test]
fn a_refused_uninstall_never_runs_its_hook() {
    let home = home_with_clients();
    run(home.path(), &["install"]);

    let output = run(home.path(), &["uninstall", "--client", "cursur"]);

    assert_eq!(code(&output), 2, "{}", stdout(&output));
    assert!(
        data_dir(&home).exists(),
        "nothing was removed, so nothing may have been cleaned up after"
    );
    assert!(home.path().join(".cursor/mcp.json").exists());
}

//
// --client
//

#[test]
fn a_client_selector_configures_only_that_client() {
    let home = home_with_clients();

    let output = run(home.path(), &["install", "--client", "cursor"]);

    assert_eq!(code(&output), 0, "{}", stderr(&output));
    assert!(home.path().join(".cursor/mcp.json").exists());
    assert!(!home.path().join(".codex/config.toml").exists());
    assert!(!home.path().join(".claude.json").exists());
}

#[test]
fn a_client_selector_can_be_repeated() {
    let home = home_with_clients();

    run(
        home.path(),
        &["install", "--client", "cursor", "--client=codex"],
    );

    assert!(home.path().join(".cursor/mcp.json").exists());
    assert!(home.path().join(".codex/config.toml").exists());
    assert!(!home.path().join(".claude.json").exists());
}

//
// Exit codes and where messages go
//

#[test]
fn a_command_line_that_was_not_understood_exits_two() {
    let home = home_with_clients();

    for args in [
        vec!["frobnicate"],
        vec!["install", "--clients", "cursor"],
        vec!["install", "--client"],
        vec!["install", "--client", "cursur"],
        vec!["uninstall", "--client", "not-a-client"],
    ] {
        let output = run(home.path(), &args);

        assert_eq!(code(&output), 2, "{args:?}: {}", stdout(&output));
        assert!(
            !stderr(&output).is_empty(),
            "{args:?}: a usage error belongs on stderr"
        );
        assert!(
            stderr(&output).ends_with('\n'),
            "{args:?}: {:?}",
            stderr(&output)
        );
    }
    assert!(
        !home.path().join(".cursor/mcp.json").exists(),
        "nothing may be written for a command line that was refused"
    );
}

#[test]
fn an_unknown_client_names_the_ones_that_exist() {
    let home = home_with_clients();

    let output = run(home.path(), &["install", "--client", "cursur"]);

    let message = stderr(&output);
    assert!(message.contains("unknown client `cursur`"), "{message}");
    for slug in [
        "claude-desktop",
        "claude-code",
        "cursor",
        "windsurf",
        "vs-code",
        "codex",
    ] {
        assert!(message.contains(slug), "{message}");
    }
}

#[test]
fn a_custom_subcommand_gets_its_arguments_and_runs() {
    let home = home_with_clients();
    run(home.path(), &["install", "--client", "cursor"]);

    let plain = run(home.path(), &["doctor"]);
    let verbose = run(home.path(), &["doctor", "--verbose"]);

    assert_eq!(code(&plain), 0);
    assert!(
        stdout(&plain).contains("Cursor: configured"),
        "{}",
        stdout(&plain)
    );
    assert!(
        !stdout(&plain).contains("mcp.json"),
        "--verbose is the flag that prints paths: {}",
        stdout(&plain)
    );
    assert!(
        stdout(&verbose).contains("mcp.json"),
        "{}",
        stdout(&verbose)
    );
}

#[test]
fn health_runs_the_authors_check() {
    let home = home_with_clients();

    let output = run(home.path(), &["health"]);

    assert_eq!(code(&output), 0);
    assert!(
        stdout(&output).contains("3 MCP client(s) detected"),
        "{}",
        stdout(&output)
    );
}

#[test]
fn a_machine_with_no_home_gets_the_pasteable_snippet() {
    // Not a failure worth hiding: there is nothing to configure, so say what
    // to paste and exit non-zero so a script notices.
    let output = Command::new(&*EXAMPLE_BINARY)
        .arg("install")
        .env_remove("HOME")
        .output()
        .unwrap();

    assert_eq!(code(&output), 1);
    let report = stdout(&output);
    assert!(report.contains("no home directory"), "{report}");
    assert!(report.contains("\"mcpServers\""), "{report}");
    assert!(report.contains(ENTRY_NAME), "{report}");
    assert!(
        serde_json::from_str::<Value>(
            report
                .split_once('{')
                .map(|(_, rest)| format!("{{{rest}"))
                .unwrap()
                .trim()
        )
        .is_ok(),
        "the snippet must be pasteable JSON: {report}"
    );
}

/// Where the example's own install hook sets itself up, under `home`.
fn data_dir(home: &tempfile::TempDir) -> PathBuf {
    home.path().join(".cli-example")
}

/// Every config file that exists under `home`.
fn config_files(home: &tempfile::TempDir) -> Vec<PathBuf> {
    [".claude.json", ".cursor/mcp.json", ".codex/config.toml"]
        .iter()
        .map(|relative| home.path().join(relative))
        .filter(|path| path.exists())
        .collect()
}
