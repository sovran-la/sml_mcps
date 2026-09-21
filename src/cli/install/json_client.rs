//! MCP clients that keep their server list in a JSON file.
//!
//! They all have the same shape - a JSON object with a map of named servers
//! under one key - so one type covers all of them and only the path, the key,
//! and how to tell the client is installed differ:
//!
//! ```json
//! { "mcpServers": { "my-mcp": { "command": "…", "args": ["serve"] } } }
//! ```
//!
//! Covers Claude Desktop, Claude Code, Cursor, Windsurf, and VS Code - which
//! calls the key `servers` rather than `mcpServers`, and is the whole of why
//! that is a field.

use super::{
    InstallError, InstallStatus, McpClient, ServerEntry, read_config_text, write_atomically,
};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

/// An MCP client that stores its server list in a JSON file.
pub struct JsonClient {
    /// Human-readable name, for the reports the commands print.
    name: String,

    /// The client's configuration file.
    config_path: PathBuf,

    /// Top-level key holding the server map - `mcpServers` for most clients.
    servers_key: String,

    /// Path whose existence means the client is installed.
    ///
    /// `None` falls back to the config file or its parent directory, which is
    /// right for a client whose config lives in a directory of its own.
    detect_path: Option<PathBuf>,
}

impl JsonClient {
    /// A client reading `config_path`, with its servers under `servers_key`.
    pub fn new(
        name: impl Into<String>,
        config_path: impl Into<PathBuf>,
        servers_key: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            config_path: config_path.into(),
            servers_key: servers_key.into(),
            detect_path: None,
        }
    }

    /// Decide this client's presence by `path` rather than by its config file.
    ///
    /// For Claude Code: `~/.claude.json` does not exist until it has something
    /// to put there, and its parent is the home directory, which exists on every
    /// machine and would make the client look installed everywhere. `~/.claude/`
    /// is the marker that actually means something.
    pub fn with_detect_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.detect_path = Some(path.into());
        self
    }

    /// The top-level key this client keeps its servers under.
    pub fn servers_key(&self) -> &str {
        &self.servers_key
    }

    /// Parse the config file, treating "missing" and "empty" as "no settings".
    fn read_config(&self) -> Result<Value, InstallError> {
        let Some(contents) = read_config_text(&self.config_path)? else {
            return Ok(json!({}));
        };

        serde_json::from_str(&contents).map_err(|source| {
            InstallError::Parse(format!("{}: {source}", self.config_path.display()))
        })
    }

    /// Write `config` back, pretty-printed, atomically, keeping a `.bak`.
    fn write_config(&self, config: &Value) -> Result<(), InstallError> {
        let mut text = serde_json::to_string_pretty(config)
            .map_err(|source| InstallError::Serialize(source.to_string()))?;
        text.push('\n');

        write_atomically(&self.config_path, &text)
    }

    /// `entry`'s entry in `config`, if it has one.
    fn existing_entry<'a>(&self, config: &'a Value, entry: &ServerEntry) -> Option<&'a Value> {
        config.get(&self.servers_key)?.get(entry.name)
    }
}

/// Whether what is already in the file is exactly what `entry` would write.
///
/// The whole entry, not just the command it names. Both
/// [`check_existing`](McpClient::check_existing) and
/// [`install`](McpClient::install) ask this one question, which is what keeps
/// them from disagreeing: a status of `Installed` means `install` would write
/// nothing, and anything else means it would write this exact object.
fn matches_entry(existing: &Value, entry: &ServerEntry) -> bool {
    *existing == entry.json_entry()
}

impl McpClient for JsonClient {
    fn name(&self) -> &str {
        &self.name
    }

    fn config_path(&self) -> PathBuf {
        self.config_path.clone()
    }

    fn detect(&self) -> bool {
        match &self.detect_path {
            Some(path) => path.exists() || self.config_path.exists(),
            None => {
                self.config_path.exists() || self.config_path.parent().is_some_and(Path::exists)
            }
        }
    }

    fn check_existing(&self, entry: &ServerEntry) -> InstallStatus {
        if !self.detect() {
            return InstallStatus::ClientNotFound;
        }

        // A config we cannot parse has no entry we can see. `install` will
        // report the parse failure properly when it tries to write.
        let Ok(config) = self.read_config() else {
            return InstallStatus::NotInstalled;
        };

        let Some(existing) = self.existing_entry(&config, entry) else {
            return InstallStatus::NotInstalled;
        };

        let their_command = existing
            .get("command")
            .and_then(Value::as_str)
            .unwrap_or_default();

        if their_command != entry.command() {
            return InstallStatus::NeedsUpdate {
                current_command: their_command.to_string(),
            };
        }

        // The binary has not moved, but the rest of the entry may still be
        // stale - a changed argument, a pinned variable this version of the
        // server needs. `install` rewrites all of it; the status is what
        // decides whether the `install` command calls it at all, so answering
        // `Installed` here is what would leave the file alone.
        if matches_entry(existing, entry) {
            InstallStatus::Installed
        } else {
            InstallStatus::NeedsRefresh
        }
    }

    fn install(&self, entry: &ServerEntry) -> Result<(), InstallError> {
        let mut config = self.read_config()?;
        // `entry.auto_approve` is deliberately not written here. None of these
        // five clients take a per-server approval policy in the file this type
        // edits:
        //
        // - Claude Code has one, as `mcp__<server>__*` in the `allowedTools`
        //   array of `~/.claude/settings.json` - a second file, on a different
        //   path, that this type has no reason to know about. A `JsonClient` is
        //   a path and a key, so it cannot tell it is the Claude Code one and
        //   not the Cursor one; supporting it means the trait growing a way to
        //   say so.
        // - Claude Desktop, Cursor, Windsurf and VS Code have no config-time
        //   mechanism at all. Approval is something their UI asks for, and a
        //   key we invented would sit in the config unread.
        //
        // So an entry that asked for auto-approval gets the same JSON as one
        // that did not, and Codex is the only client that acts on it.
        let written = entry.json_entry();

        let object = config.as_object_mut().ok_or_else(|| {
            InstallError::Parse(format!(
                "{}: the top level is not a JSON object",
                self.config_path.display()
            ))
        })?;
        object
            .entry(self.servers_key.clone())
            .or_insert_with(|| json!({}));

        let servers = object
            .get_mut(&self.servers_key)
            .and_then(Value::as_object_mut)
            .ok_or_else(|| {
                InstallError::Parse(format!(
                    "{}: `{}` is not a JSON object",
                    self.config_path.display(),
                    self.servers_key
                ))
            })?;

        // Nothing to do if it is already exactly right: rewriting would churn
        // the file's timestamp and overwrite a `.bak` worth keeping.
        if servers
            .get(entry.name)
            .is_some_and(|existing| matches_entry(existing, entry))
        {
            return Ok(());
        }
        servers.insert(entry.name.to_string(), written);

        self.write_config(&config)
    }

    fn uninstall(&self, entry: &ServerEntry) -> Result<(), InstallError> {
        let mut config = self.read_config()?;

        let removed = config
            .get_mut(&self.servers_key)
            .and_then(Value::as_object_mut)
            .is_some_and(|servers| servers.remove(entry.name).is_some());

        // No entry means no write. A client that never had one keeps its
        // formatting, and one with no config file at all does not get an empty
        // one invented for it.
        if !removed {
            return Ok(());
        }

        self.write_config(&config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::install::backup_path;
    use tempfile::TempDir;

    /// The entry the tests here install.
    fn entry() -> ServerEntry {
        ServerEntry::new("my-mcp", &["serve"])
    }

    /// A client rooted in `dir`, detected because `dir` exists.
    fn client(dir: &TempDir, servers_key: &str) -> JsonClient {
        JsonClient::new("Test Client", dir.path().join("config.json"), servers_key)
    }

    /// Read a client's config back as JSON.
    fn config_of(client: &JsonClient) -> Value {
        serde_json::from_str(&std::fs::read_to_string(client.config_path()).unwrap()).unwrap()
    }

    /// Seed a client's config with `value`.
    fn seed(client: &JsonClient, value: Value) {
        std::fs::write(
            client.config_path(),
            serde_json::to_string_pretty(&value).unwrap(),
        )
        .unwrap();
    }

    /// The command `install` would write.
    fn our_command() -> String {
        entry().command()
    }

    //
    // Detection
    //

    #[test]
    fn a_client_whose_directory_exists_is_detected() {
        let dir = TempDir::new().unwrap();

        assert!(client(&dir, "mcpServers").detect());
    }

    #[test]
    fn a_client_with_neither_directory_nor_config_is_not_detected() {
        let dir = TempDir::new().unwrap();
        let absent = JsonClient::new(
            "Absent",
            dir.path().join("nowhere/config.json"),
            "mcpServers",
        );

        assert!(!absent.detect());
        assert_eq!(
            absent.check_existing(&entry()),
            InstallStatus::ClientNotFound
        );
    }

    #[test]
    fn an_explicit_marker_directory_decides_detection() {
        let dir = TempDir::new().unwrap();
        let marker = dir.path().join(".claude");

        let client = JsonClient::new("Claude Code", dir.path().join(".claude.json"), "mcpServers")
            .with_detect_path(&marker);
        assert!(!client.detect(), "no ~/.claude/ means no Claude Code");

        std::fs::create_dir(&marker).unwrap();
        assert!(client.detect());
    }

    #[test]
    fn a_config_file_alone_still_detects_a_client_with_a_marker() {
        // Someone who configured it by hand has proven it is installed, marker
        // directory or not.
        let dir = TempDir::new().unwrap();
        let config = dir.path().join(".claude.json");
        std::fs::write(&config, "{}").unwrap();

        let client = JsonClient::new("Claude Code", &config, "mcpServers")
            .with_detect_path(dir.path().join(".claude"));

        assert!(client.detect());
    }

    //
    // Status
    //

    #[test]
    fn a_client_with_no_config_file_yet_has_no_entry() {
        let dir = TempDir::new().unwrap();

        assert_eq!(
            client(&dir, "mcpServers").check_existing(&entry()),
            InstallStatus::NotInstalled
        );
    }

    #[test]
    fn an_entry_pointing_at_this_binary_is_installed() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        client.install(&entry()).unwrap();

        assert_eq!(client.check_existing(&entry()), InstallStatus::Installed);
    }

    #[test]
    fn an_entry_pointing_somewhere_else_needs_updating() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        seed(
            &client,
            json!({"mcpServers": {"my-mcp": {"command": "/old/build", "args": ["serve"]}}}),
        );

        assert_eq!(
            client.check_existing(&entry()),
            InstallStatus::NeedsUpdate {
                current_command: "/old/build".to_string()
            }
        );
    }

    #[test]
    fn an_entry_with_no_command_at_all_needs_updating() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        seed(
            &client,
            json!({"mcpServers": {"my-mcp": {"args": ["serve"]}}}),
        );

        assert_eq!(
            client.check_existing(&entry()),
            InstallStatus::NeedsUpdate {
                current_command: String::new()
            }
        );
    }

    #[test]
    fn another_servers_entry_is_not_ours() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        seed(
            &client,
            json!({"mcpServers": {"someone-else": {"command": our_command()}}}),
        );

        assert_eq!(
            client.check_existing(&entry()),
            InstallStatus::NotInstalled,
            "another server pointing at the same binary is still not our entry"
        );
    }

    #[test]
    fn an_entry_that_is_not_what_we_would_write_needs_refreshing() {
        // The same binary with different arguments. `install` has always
        // rewritten this entry when it ran; reporting `Installed` only meant
        // the `install` command never called it, so the rewrite landed at some
        // unrelated later moment instead of the one that asked for it.
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        seed(
            &client,
            json!({"mcpServers": {"my-mcp": {
                "command": our_command(),
                "args": ["serve", "--verbose-logging"]
            }}}),
        );

        assert_eq!(client.check_existing(&entry()), InstallStatus::NeedsRefresh);
    }

    #[test]
    fn a_stale_pinned_variable_needs_refreshing() {
        // The upgrade case this exists for: a server that grew a variable its
        // entry has to carry, on a client that is otherwise current.
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        client.install(&entry()).unwrap();

        assert_eq!(
            client.check_existing(&entry().with_env("MY_MCP_HOME", "/var/lib/my-mcp")),
            InstallStatus::NeedsRefresh
        );
    }

    #[test]
    fn a_refresh_is_what_the_install_actually_writes() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        client.install(&entry()).unwrap();
        let wanted = entry().with_env("MY_MCP_HOME", "/var/lib/my-mcp");

        assert_eq!(client.check_existing(&wanted), InstallStatus::NeedsRefresh);
        client.install(&wanted).unwrap();

        assert_eq!(client.check_existing(&wanted), InstallStatus::Installed);
        assert_eq!(
            config_of(&client)["mcpServers"]["my-mcp"]["env"]["MY_MCP_HOME"],
            "/var/lib/my-mcp"
        );
    }

    #[test]
    fn a_status_and_an_install_agree_about_every_entry() {
        // `wants_install` decides whether `install` is called at all, so a
        // status that said "nothing to do" about an entry `install` would
        // rewrite is an install that silently never happens - and one that
        // said "work to do" about an entry `install` would leave alone is a
        // report claiming a change that was not made.
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        client.install(&entry()).unwrap();

        for wanted in [
            entry(),
            entry().with_auto_approve(),
            entry().with_env("A", "1"),
            ServerEntry::new("my-mcp", &["serve", "--stdio"]),
            ServerEntry::new("another-mcp", &["serve"]),
        ] {
            let before = std::fs::read_to_string(client.config_path()).unwrap();
            let wants_install = client.check_existing(&wanted).wants_install();
            client.install(&wanted).unwrap();

            let rewritten = std::fs::read_to_string(client.config_path()).unwrap() != before;
            assert_eq!(wants_install, rewritten, "{wanted:?}");
        }
    }

    #[test]
    fn a_config_that_is_not_json_reads_as_having_no_entry() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        std::fs::write(client.config_path(), "{ not json").unwrap();

        assert_eq!(
            client.check_existing(&entry()),
            InstallStatus::NotInstalled,
            "install reports the parse failure; status must not panic on it"
        );
    }

    //
    // Installing
    //

    #[test]
    fn install_creates_the_config_from_scratch() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");

        client.install(&entry()).unwrap();

        let config = config_of(&client);
        assert_eq!(config["mcpServers"]["my-mcp"]["command"], our_command());
        assert_eq!(config["mcpServers"]["my-mcp"]["args"], json!(["serve"]));
    }

    #[test]
    fn install_writes_the_pinned_environment() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        let entry = entry().with_env("MY_MCP_HOME", "/var/lib/my-mcp");

        client.install(&entry).unwrap();

        assert_eq!(
            config_of(&client)["mcpServers"]["my-mcp"]["env"]["MY_MCP_HOME"],
            "/var/lib/my-mcp"
        );
    }

    //
    // Remote entries
    //

    #[test]
    fn install_writes_a_remote_entry_with_the_connect_flag_in_its_arguments() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");

        client
            .install(&entry().with_connect("jetson-memory:7211"))
            .unwrap();

        let written = &config_of(&client)["mcpServers"]["my-mcp"];
        assert_eq!(written["command"], our_command());
        assert_eq!(
            written["args"],
            json!(["serve", "--connect", "jetson-memory:7211"])
        );
    }

    #[test]
    fn a_remote_entry_is_installed_only_if_its_address_matches() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        let remote = entry().with_connect("jetson-memory:7211");
        client.install(&remote).unwrap();

        assert_eq!(client.check_existing(&remote), InstallStatus::Installed);
        // A different address, or no address, is a different entry.
        assert_eq!(
            client.check_existing(&entry().with_connect("other-host:7211")),
            InstallStatus::NeedsRefresh
        );
        assert_eq!(client.check_existing(&entry()), InstallStatus::NeedsRefresh);
    }

    #[test]
    fn install_turns_a_local_entry_into_a_remote_one_and_back() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        client.install(&entry()).unwrap();

        client
            .install(&entry().with_connect("jetson-memory:7211"))
            .unwrap();
        assert_eq!(
            config_of(&client)["mcpServers"]["my-mcp"]["args"],
            json!(["serve", "--connect", "jetson-memory:7211"])
        );

        client.install(&entry()).unwrap();
        assert_eq!(
            config_of(&client)["mcpServers"]["my-mcp"]["args"],
            json!(["serve"])
        );
    }

    #[test]
    fn uninstall_removes_a_remote_entry_whatever_address_it_was_written_with() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        client
            .install(&entry().with_connect("jetson-memory:7211"))
            .unwrap();

        // By name: the entry asked about carries no address at all.
        client.uninstall(&entry()).unwrap();

        assert!(config_of(&client)["mcpServers"]["my-mcp"].is_null());
        assert_eq!(
            client.check_existing(&entry().with_connect("jetson-memory:7211")),
            InstallStatus::NotInstalled
        );
    }

    #[test]
    fn install_writes_the_entry_under_its_own_name() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");

        client
            .install(&ServerEntry::new("some-other-server", &["run"]))
            .unwrap();

        let config = config_of(&client);
        assert_eq!(
            config["mcpServers"]["some-other-server"]["args"],
            json!(["run"])
        );
        assert!(config["mcpServers"]["my-mcp"].is_null());
    }

    #[test]
    fn install_preserves_existing_servers() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        seed(
            &client,
            json!({"mcpServers": {"memory": {"command": "/usr/local/bin/memoryco", "args": ["serve"]}}}),
        );

        client.install(&entry()).unwrap();

        let config = config_of(&client);
        assert_eq!(
            config["mcpServers"]["memory"]["command"],
            "/usr/local/bin/memoryco"
        );
        assert_eq!(config["mcpServers"]["my-mcp"]["command"], our_command());
    }

    #[test]
    fn install_preserves_non_mcp_config() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        seed(
            &client,
            json!({
                "theme": "dark",
                "fontSize": 14,
                "projects": {"/Users/x/work": {"history": ["one", "two"]}},
                "mcpServers": {}
            }),
        );

        client.install(&entry()).unwrap();

        let config = config_of(&client);
        assert_eq!(config["theme"], "dark");
        assert_eq!(config["fontSize"], 14);
        assert_eq!(
            config["projects"]["/Users/x/work"]["history"],
            json!(["one", "two"])
        );
        assert_eq!(config["mcpServers"]["my-mcp"]["command"], our_command());
    }

    #[test]
    fn install_creates_the_servers_key_when_it_is_missing() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        seed(&client, json!({"theme": "dark"}));

        client.install(&entry()).unwrap();

        assert_eq!(
            config_of(&client)["mcpServers"]["my-mcp"]["command"],
            our_command()
        );
    }

    #[test]
    fn install_replaces_an_entry_pointing_at_a_moved_binary() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        seed(
            &client,
            json!({"mcpServers": {"my-mcp": {"command": "/old/build", "args": ["serve"]}}}),
        );

        client.install(&entry()).unwrap();

        assert_eq!(
            config_of(&client)["mcpServers"]["my-mcp"]["command"],
            our_command()
        );
        assert_eq!(client.check_existing(&entry()), InstallStatus::Installed);
    }

    #[test]
    fn install_rewrites_an_entry_whose_environment_changed() {
        // The command is the same binary, but the pinned environment is not
        // what this version of the server asks for.
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        client
            .install(&entry().with_env("MY_MCP_HOME", "/old"))
            .unwrap();

        client
            .install(&entry().with_env("MY_MCP_HOME", "/new"))
            .unwrap();

        assert_eq!(
            config_of(&client)["mcpServers"]["my-mcp"]["env"]["MY_MCP_HOME"],
            "/new"
        );
    }

    //
    // Auto-approval
    //

    #[test]
    fn an_auto_approving_entry_is_written_exactly_like_any_other() {
        // None of the JSON clients read a per-server approval policy out of
        // this file, so there is nothing to write and no key to invent.
        let dir = TempDir::new().unwrap();
        let asked = client(&dir, "mcpServers");
        let plain = JsonClient::new("Test Client", dir.path().join("plain.json"), "mcpServers");

        asked.install(&entry().with_auto_approve()).unwrap();
        plain.install(&entry()).unwrap();

        assert_eq!(config_of(&asked), config_of(&plain));
        let text = std::fs::read_to_string(asked.config_path()).unwrap();
        assert!(!text.contains("approval"), "{text}");
        assert!(!text.contains("allowedTools"), "{text}");
        assert!(!text.contains("autoApprove"), "{text}");
    }

    #[test]
    fn an_auto_approving_entry_is_still_a_working_entry() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        let entry = entry().with_auto_approve();

        client.install(&entry).unwrap();

        assert_eq!(
            config_of(&client)["mcpServers"]["my-mcp"]["command"],
            our_command()
        );
        assert_eq!(client.check_existing(&entry), InstallStatus::Installed);

        client.uninstall(&entry).unwrap();

        assert_eq!(client.check_existing(&entry), InstallStatus::NotInstalled);
    }

    #[test]
    fn asking_for_auto_approval_afterwards_does_not_rewrite_the_file() {
        // It changes nothing here, so it is not a reason to churn the config
        // or overwrite a `.bak` worth keeping.
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        client.install(&entry()).unwrap();

        client.install(&entry().with_auto_approve()).unwrap();

        assert!(!backup_path(&client.config_path()).exists());
    }

    #[test]
    fn install_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");

        client.install(&entry()).unwrap();
        let first = std::fs::read_to_string(client.config_path()).unwrap();
        client.install(&entry()).unwrap();

        assert_eq!(
            std::fs::read_to_string(client.config_path()).unwrap(),
            first
        );
    }

    #[test]
    fn a_second_install_does_not_rewrite_the_file() {
        // Otherwise the `.bak` from a real change is overwritten by a copy of
        // the very config that replaced it.
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        client.install(&entry()).unwrap();

        client.install(&entry()).unwrap();

        assert!(!backup_path(&client.config_path()).exists());
    }

    #[test]
    fn install_handles_an_empty_file() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        std::fs::write(client.config_path(), "   \n").unwrap();

        client.install(&entry()).unwrap();

        assert_eq!(
            config_of(&client)["mcpServers"]["my-mcp"]["command"],
            our_command()
        );
    }

    #[test]
    fn install_keeps_the_previous_config_as_a_backup() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        seed(&client, json!({"theme": "dark"}));

        client.install(&entry()).unwrap();

        let saved: Value = serde_json::from_str(
            &std::fs::read_to_string(backup_path(&client.config_path())).unwrap(),
        )
        .unwrap();
        assert_eq!(saved, json!({"theme": "dark"}));
    }

    #[test]
    fn install_creates_a_missing_parent_directory() {
        let dir = TempDir::new().unwrap();
        let client = JsonClient::new(
            "Nested",
            dir.path().join("Application Support/Claude/config.json"),
            "mcpServers",
        );

        client.install(&entry()).unwrap();

        assert!(client.config_path().exists());
    }

    #[test]
    fn install_writes_json_a_client_can_read_back() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");

        client.install(&entry()).unwrap();

        let text = std::fs::read_to_string(client.config_path()).unwrap();
        assert!(text.ends_with('\n'), "config files end in a newline");
        assert!(serde_json::from_str::<Value>(&text).is_ok(), "{text}");
    }

    //
    // Uninstalling
    //

    #[test]
    fn uninstall_removes_only_our_entry() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        seed(
            &client,
            json!({
                "theme": "dark",
                "mcpServers": {
                    "my-mcp": {"command": "/anything", "args": ["serve"]},
                    "memory": {"command": "/usr/local/bin/memoryco", "args": ["serve"]}
                }
            }),
        );

        client.uninstall(&entry()).unwrap();

        let config = config_of(&client);
        assert!(config["mcpServers"]["my-mcp"].is_null());
        assert_eq!(
            config["mcpServers"]["memory"]["command"],
            "/usr/local/bin/memoryco"
        );
        assert_eq!(config["theme"], "dark");
    }

    #[test]
    fn uninstall_leaves_the_servers_key_in_place() {
        // Removing the key as well would be tidier and would also silently
        // change a config whose only server was ours into one with no
        // `mcpServers` at all. Removing what we added is the whole contract.
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        client.install(&entry()).unwrap();

        client.uninstall(&entry()).unwrap();

        assert_eq!(config_of(&client)["mcpServers"], json!({}));
    }

    #[test]
    fn uninstall_is_safe_when_no_file_exists() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");

        client.uninstall(&entry()).unwrap();

        assert!(
            !client.config_path().exists(),
            "an absent config must not be invented"
        );
    }

    #[test]
    fn uninstall_is_safe_when_there_is_no_entry() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        seed(
            &client,
            json!({"mcpServers": {"memory": {"command": "/x"}}}),
        );
        let before = std::fs::read_to_string(client.config_path()).unwrap();

        client.uninstall(&entry()).unwrap();

        assert_eq!(
            std::fs::read_to_string(client.config_path()).unwrap(),
            before,
            "a file with nothing of ours in it must not be reformatted"
        );
    }

    #[test]
    fn uninstall_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        client.install(&entry()).unwrap();

        client.uninstall(&entry()).unwrap();
        let once = std::fs::read_to_string(client.config_path()).unwrap();
        client.uninstall(&entry()).unwrap();

        assert_eq!(std::fs::read_to_string(client.config_path()).unwrap(), once);
    }

    #[test]
    fn uninstall_removes_an_entry_pointing_at_another_binary() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        seed(
            &client,
            json!({"mcpServers": {"my-mcp": {"command": "/old/build", "args": ["serve"]}}}),
        );

        client.uninstall(&entry()).unwrap();

        assert!(config_of(&client)["mcpServers"]["my-mcp"].is_null());
    }

    #[test]
    fn uninstall_keeps_the_previous_config_as_a_backup() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        client.install(&entry()).unwrap();

        client.uninstall(&entry()).unwrap();

        let saved: Value = serde_json::from_str(
            &std::fs::read_to_string(backup_path(&client.config_path())).unwrap(),
        )
        .unwrap();
        assert_eq!(saved["mcpServers"]["my-mcp"]["command"], our_command());
    }

    //
    // Round trip
    //

    #[test]
    fn install_then_uninstall_restores_what_was_there() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        let original = json!({"theme": "dark", "mcpServers": {"memory": {"command": "/x"}}});
        seed(&client, original.clone());

        client.install(&entry()).unwrap();
        client.uninstall(&entry()).unwrap();

        assert_eq!(config_of(&client), original);
    }

    #[test]
    fn two_servers_share_one_config() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        let other = ServerEntry::new("other-mcp", &["start"]);

        client.install(&entry()).unwrap();
        client.install(&other).unwrap();
        client.uninstall(&entry()).unwrap();

        assert_eq!(client.check_existing(&entry()), InstallStatus::NotInstalled);
        assert_eq!(client.check_existing(&other), InstallStatus::Installed);
    }

    //
    // Malformed input
    //

    #[test]
    fn a_config_that_is_not_json_is_reported_rather_than_overwritten() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        std::fs::write(client.config_path(), "{ this is not json").unwrap();

        let error = client
            .install(&entry())
            .expect_err("malformed JSON must fail");

        assert!(matches!(error, InstallError::Parse(_)));
        assert!(error.to_string().contains("config.json"));
        assert_eq!(
            std::fs::read_to_string(client.config_path()).unwrap(),
            "{ this is not json",
            "a file we cannot understand must not be replaced"
        );
    }

    #[test]
    fn a_config_that_is_not_an_object_is_reported() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        std::fs::write(client.config_path(), "[1, 2, 3]").unwrap();

        let error = client
            .install(&entry())
            .expect_err("an array is not a config");

        assert!(matches!(error, InstallError::Parse(_)));
        assert!(error.to_string().contains("not a JSON object"));
    }

    #[test]
    fn a_servers_key_that_is_not_an_object_is_reported() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        seed(&client, json!({"mcpServers": "surprise"}));

        let error = client
            .install(&entry())
            .expect_err("a string is not a server map");

        assert!(matches!(error, InstallError::Parse(_)));
        assert!(error.to_string().contains("mcpServers"));
    }

    #[test]
    fn uninstalling_from_a_malformed_config_is_reported() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        std::fs::write(client.config_path(), "{ not json").unwrap();

        assert!(matches!(
            client.uninstall(&entry()),
            Err(InstallError::Parse(_))
        ));
    }

    #[test]
    fn uninstalling_from_a_servers_key_that_is_not_an_object_changes_nothing() {
        // There is no entry of ours in a string, so there is nothing to remove
        // and no reason to rewrite the file.
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");
        seed(&client, json!({"mcpServers": "surprise"}));
        let before = std::fs::read_to_string(client.config_path()).unwrap();

        client.uninstall(&entry()).unwrap();

        assert_eq!(
            std::fs::read_to_string(client.config_path()).unwrap(),
            before
        );
    }

    //
    // VS Code's key
    //

    #[test]
    fn works_with_the_servers_key() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "servers");

        client.install(&entry()).unwrap();

        let config = config_of(&client);
        assert_eq!(config["servers"]["my-mcp"]["command"], our_command());
        assert!(
            config["mcpServers"].is_null(),
            "only the key the client reads should be written"
        );
    }

    #[test]
    fn the_servers_key_is_honoured_by_every_operation() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "servers");
        client.install(&entry()).unwrap();
        assert_eq!(client.check_existing(&entry()), InstallStatus::Installed);

        client.uninstall(&entry()).unwrap();

        assert!(config_of(&client)["servers"]["my-mcp"].is_null());
        assert_eq!(client.check_existing(&entry()), InstallStatus::NotInstalled);
    }

    #[test]
    fn an_entry_under_the_wrong_key_is_not_ours() {
        // A `servers` client must not report an `mcpServers` entry as installed.
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "servers");
        seed(
            &client,
            json!({"mcpServers": {"my-mcp": {"command": our_command()}}}),
        );

        assert_eq!(client.check_existing(&entry()), InstallStatus::NotInstalled);
    }

    //
    // Naming
    //

    #[test]
    fn a_client_reports_its_own_name_path_and_key() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir, "mcpServers");

        assert_eq!(client.name(), "Test Client");
        assert_eq!(client.config_path(), dir.path().join("config.json"));
        assert_eq!(client.servers_key(), "mcpServers");
    }
}
