//! Every MCP client this crate knows how to configure, and where each keeps its
//! configuration.
//!
//! macOS and Linux only. The paths are the ones the clients actually read, which
//! is the sort of fact that goes stale - each is named in a test so a change
//! shows up as a failing assertion rather than as an install that silently
//! writes to a file nobody reads.
//!
//! This is the part of the crate worth having: as new clients appear, they are
//! added here once and every server built on sml_mcps picks them up with a
//! version bump.

use super::{CodexClient, JsonClient, McpClient};
use std::path::PathBuf;

/// Every known MCP client for this platform.
///
/// Returns nothing at all when there is no home directory to look in, which is
/// not an error: there are no client configs on a machine with no home for them
/// to be in, and the commands above this print the pasteable snippet instead.
pub fn all_clients() -> Vec<Box<dyn McpClient>> {
    clients_for(home_dir())
}

/// [`all_clients`] with the home directory supplied rather than looked up.
///
/// The `None` arm is the whole reason this is a function: it is not reachable
/// from a test that cannot safely unset `$HOME` for the whole process.
fn clients_for(home: Option<PathBuf>) -> Vec<Box<dyn McpClient>> {
    home.map(clients_under).unwrap_or_default()
}

/// The user's home directory, from `$HOME`.
///
/// `$HOME` rather than the password database because `install` is run from a
/// shell by a person, and the home they mean is the one their shell is in. An
/// empty value counts as unset - a client config under `/` is not a thing.
pub fn home_dir() -> Option<PathBuf> {
    home_from(std::env::var_os("HOME"))
}

/// [`home_dir`] over a supplied value, so the empty case is testable.
fn home_from(value: Option<std::ffi::OsString>) -> Option<PathBuf> {
    value.filter(|home| !home.is_empty()).map(PathBuf::from)
}

/// The clients as they would be found under `home`.
///
/// Split out from [`all_clients`] so the paths can be asserted against a
/// directory a test owns rather than against whoever's machine is running the
/// suite - and so a server with its own idea of where home is can say so.
pub fn clients_under(home: impl Into<PathBuf>) -> Vec<Box<dyn McpClient>> {
    let home = home.into();

    // Claude Desktop
    //   macOS: ~/Library/Application Support/Claude/claude_desktop_config.json
    //   Linux: ~/.config/Claude/claude_desktop_config.json
    #[cfg(target_os = "macos")]
    let claude_desktop = home.join("Library/Application Support/Claude/claude_desktop_config.json");
    #[cfg(not(target_os = "macos"))]
    let claude_desktop = home.join(".config/Claude/claude_desktop_config.json");

    // VS Code (Copilot)
    //   macOS: ~/Library/Application Support/Code/User/mcp.json
    //   Linux: ~/.config/Code/User/mcp.json
    #[cfg(target_os = "macos")]
    let vscode = home.join("Library/Application Support/Code/User/mcp.json");
    #[cfg(not(target_os = "macos"))]
    let vscode = home.join(".config/Code/User/mcp.json");

    vec![
        Box::new(JsonClient::new(
            "Claude Desktop",
            claude_desktop,
            "mcpServers",
        )),
        // Claude Code owns ~/.claude.json, which holds far more than server
        // entries - hence the surgical edit and the `.bak`. Its parent is the
        // home directory, so the marker has to be ~/.claude/ instead.
        Box::new(
            JsonClient::new("Claude Code", home.join(".claude.json"), "mcpServers")
                .with_detect_path(home.join(".claude")),
        ),
        Box::new(JsonClient::new(
            "Cursor",
            home.join(".cursor/mcp.json"),
            "mcpServers",
        )),
        Box::new(JsonClient::new(
            "Windsurf",
            home.join(".codeium/windsurf/mcp_config.json"),
            "mcpServers",
        )),
        // The one client that calls the key `servers`.
        Box::new(JsonClient::new("VS Code", vscode, "servers")),
        Box::new(CodexClient::new(home.join(".codex/config.toml"))),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::install::{InstallStatus, ServerEntry};

    /// The entry the tests here install.
    fn entry() -> ServerEntry {
        ServerEntry::new("my-mcp", &["serve"])
    }

    /// The clients as they would be found under a made-up home directory.
    fn clients() -> Vec<Box<dyn McpClient>> {
        clients_under("/home/test")
    }

    /// One client's config path as a string.
    fn path_of(name: &str) -> String {
        clients()
            .iter()
            .find(|client| client.name() == name)
            .unwrap_or_else(|| panic!("{name} must be a known client"))
            .config_path()
            .display()
            .to_string()
    }

    #[test]
    fn every_supported_client_is_present() {
        let names: Vec<String> = clients()
            .iter()
            .map(|client| client.name().to_string())
            .collect();

        assert_eq!(
            names,
            [
                "Claude Desktop",
                "Claude Code",
                "Cursor",
                "Windsurf",
                "VS Code",
                "Codex"
            ]
        );
    }

    #[test]
    fn every_client_can_be_named_on_a_command_line() {
        let slugs: Vec<String> = clients().iter().map(|client| client.slug()).collect();

        assert_eq!(
            slugs,
            [
                "claude-desktop",
                "claude-code",
                "cursor",
                "windsurf",
                "vs-code",
                "codex"
            ]
        );
    }

    #[test]
    fn no_two_clients_answer_to_the_same_name() {
        // Otherwise `--client <name>` would silently configure the wrong one.
        let mut slugs: Vec<String> = clients().iter().map(|client| client.slug()).collect();
        slugs.sort();
        let count = slugs.len();
        slugs.dedup();

        assert_eq!(slugs.len(), count, "{slugs:?}");
    }

    #[test]
    fn claude_desktop_is_looked_for_in_the_platform_location() {
        let path = path_of("Claude Desktop");

        assert!(path.ends_with("claude_desktop_config.json"), "{path}");
        if cfg!(target_os = "macos") {
            assert_eq!(
                path,
                "/home/test/Library/Application Support/Claude/claude_desktop_config.json"
            );
        } else {
            assert_eq!(path, "/home/test/.config/Claude/claude_desktop_config.json");
        }
    }

    #[test]
    fn vs_code_is_looked_for_in_the_platform_location() {
        let path = path_of("VS Code");

        if cfg!(target_os = "macos") {
            assert_eq!(
                path,
                "/home/test/Library/Application Support/Code/User/mcp.json"
            );
        } else {
            assert_eq!(path, "/home/test/.config/Code/User/mcp.json");
        }
    }

    #[test]
    fn the_dotfile_clients_are_in_the_same_place_everywhere() {
        assert_eq!(path_of("Claude Code"), "/home/test/.claude.json");
        assert_eq!(path_of("Cursor"), "/home/test/.cursor/mcp.json");
        assert_eq!(
            path_of("Windsurf"),
            "/home/test/.codeium/windsurf/mcp_config.json"
        );
        assert_eq!(path_of("Codex"), "/home/test/.codex/config.toml");
    }

    #[test]
    fn claude_code_is_not_detected_by_the_home_directory_alone() {
        // `~/.claude.json`'s parent is the home directory, which exists on
        // every machine - so without a marker of its own, Claude Code would
        // look installed everywhere and `install` would write a config for a
        // client nobody has.
        let dir = tempfile::TempDir::new().unwrap();
        let clients = clients_under(dir.path());
        let claude_code = clients
            .iter()
            .find(|client| client.name() == "Claude Code")
            .unwrap();

        assert!(
            !claude_code.detect(),
            "an empty home is not a Claude Code install"
        );

        std::fs::create_dir(dir.path().join(".claude")).unwrap();
        assert!(claude_code.detect(), "~/.claude/ is the marker that counts");
    }

    #[test]
    fn nothing_under_a_made_up_home_is_detected() {
        // Which is also the proof that detection is about the machine and not
        // about the list: none of these paths exist.
        for client in clients() {
            assert!(!client.detect(), "{}", client.name());
            assert_eq!(
                client.check_existing(&entry()),
                InstallStatus::ClientNotFound,
                "{}",
                client.name()
            );
        }
    }

    #[test]
    fn vs_code_is_the_only_client_using_a_different_servers_key() {
        // Asserted through behaviour: every other client writes `mcpServers`.
        let dir = tempfile::TempDir::new().unwrap();

        for client in clients_under(dir.path()) {
            if client.name() == "Codex" {
                continue;
            }
            std::fs::create_dir_all(client.config_path().parent().unwrap()).unwrap();
            client.install(&entry()).unwrap();

            let config: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(client.config_path()).unwrap())
                    .unwrap();
            let key = if client.name() == "VS Code" {
                "servers"
            } else {
                "mcpServers"
            };

            assert!(
                config[key]["my-mcp"]["command"].is_string(),
                "{} wrote no entry under {key}",
                client.name()
            );
        }
    }

    #[test]
    fn every_client_installs_and_uninstalls_under_a_temporary_home() {
        let dir = tempfile::TempDir::new().unwrap();

        for client in clients_under(dir.path()) {
            std::fs::create_dir_all(client.config_path().parent().unwrap()).unwrap();

            client.install(&entry()).unwrap();
            assert_eq!(
                client.check_existing(&entry()),
                InstallStatus::Installed,
                "{}",
                client.name()
            );

            client.uninstall(&entry()).unwrap();
            assert_eq!(
                client.check_existing(&entry()),
                InstallStatus::NotInstalled,
                "{}",
                client.name()
            );
        }
    }

    #[test]
    fn every_client_handles_an_entry_asking_for_auto_approval() {
        // Codex is the only one with anywhere to put it. Everywhere else the
        // entry has to be exactly what it always was - asking for something a
        // client cannot do is not a reason for that client to be configured
        // differently, or to fail.
        let dir = tempfile::TempDir::new().unwrap();
        let entry = entry().with_auto_approve();

        for client in clients_under(dir.path()) {
            client.install(&entry).unwrap();
            assert_eq!(
                client.check_existing(&entry),
                InstallStatus::Installed,
                "{}",
                client.name()
            );

            let text = std::fs::read_to_string(client.config_path()).unwrap();
            assert_eq!(
                text.contains("default_tools_approval_mode = \"auto\""),
                client.name() == "Codex",
                "{}: {text}",
                client.name()
            );

            client.uninstall(&entry).unwrap();
            assert_eq!(
                client.check_existing(&entry),
                InstallStatus::NotInstalled,
                "{}",
                client.name()
            );
        }
    }

    #[test]
    fn every_client_creates_the_directories_it_needs() {
        // Nothing is pre-created here: a client whose config directory has
        // never existed must still be configurable.
        let dir = tempfile::TempDir::new().unwrap();

        for client in clients_under(dir.path()) {
            client.install(&entry()).unwrap();

            assert!(
                client.config_path().exists(),
                "{} wrote nothing",
                client.name()
            );
        }
    }

    #[test]
    fn installing_twice_under_a_temporary_home_changes_nothing() {
        let dir = tempfile::TempDir::new().unwrap();

        for client in clients_under(dir.path()) {
            client.install(&entry()).unwrap();
            let first = std::fs::read_to_string(client.config_path()).unwrap();

            client.install(&entry()).unwrap();

            assert_eq!(
                std::fs::read_to_string(client.config_path()).unwrap(),
                first,
                "{}",
                client.name()
            );
            assert!(
                !crate::cli::install::backup_path(&client.config_path()).exists(),
                "{} churned its backup on a no-op install",
                client.name()
            );
        }
    }

    #[test]
    fn uninstalling_from_nothing_invents_no_config() {
        let dir = tempfile::TempDir::new().unwrap();

        for client in clients_under(dir.path()) {
            client.uninstall(&entry()).unwrap();

            assert!(
                !client.config_path().exists(),
                "{} invented a config file",
                client.name()
            );
        }
    }

    #[test]
    fn a_machine_with_no_home_has_no_clients() {
        // The pasteable snippet is the whole answer in that case, which is why
        // this is empty rather than an error.
        assert!(clients_for(None).is_empty());
        assert_eq!(clients_for(Some("/home/test".into())).len(), 6);
    }

    #[test]
    fn an_unset_or_empty_home_is_no_home() {
        assert_eq!(home_from(None), None);
        assert_eq!(home_from(Some("".into())), None);
        assert_eq!(
            home_from(Some("/home/test".into())),
            Some(PathBuf::from("/home/test"))
        );
    }

    #[test]
    fn the_test_runner_has_a_home_to_look_in() {
        assert!(home_dir().is_some());
    }

    #[test]
    fn the_real_machine_can_be_asked_about_its_clients() {
        // A smoke test: whatever is installed here, listing and checking must
        // be safe and must not panic.
        for client in all_clients() {
            let _ = client.check_existing(&entry());
            assert!(!client.name().is_empty());
            assert!(!client.slug().is_empty());
        }
    }

    #[test]
    fn the_real_machine_lists_the_same_clients_as_a_made_up_home() {
        // `all_clients` is `clients_under(home_dir())` and nothing else, so a
        // test against a temporary directory is a test of the real thing.
        let real: Vec<String> = all_clients()
            .iter()
            .map(|client| client.name().to_string())
            .collect();
        let fake: Vec<String> = clients().iter().map(|c| c.name().to_string()).collect();

        assert_eq!(real, fake);
    }
}
