//! OpenAI Codex, which keeps its MCP servers in TOML.
//!
//! Same contract as [`JsonClient`](super::JsonClient) - idempotent both ways,
//! everything else preserved - over a different file format:
//!
//! ```toml
//! [mcp_servers.my-mcp]
//! command = "/usr/local/bin/my-mcp"
//! args = ["serve"]
//!
//! [mcp_servers.my-mcp.env]
//! MY_MCP_HOME = "/Users/you/.my-mcp"
//! ```
//!
//! Edited with `toml_edit` rather than `toml`: `toml` parses into values, and
//! re-serializing loses every comment and every choice of layout in the file.
//! `~/.codex/config.toml` is hand-written and hand-commented, and silently
//! reformatting it is not a thing an installer gets to do.

use super::{
    InstallError, InstallStatus, McpClient, ServerEntry, read_config_text, write_atomically,
};
use std::path::PathBuf;
use toml_edit::{Array, DocumentMut, Item, Table, value};

/// Top-level table holding Codex's MCP servers.
const SERVERS_TABLE: &str = "mcp_servers";

/// The OpenAI Codex CLI.
pub struct CodexClient {
    /// The client's configuration file.
    config_path: PathBuf,
}

impl CodexClient {
    /// A Codex reading `config_path`.
    pub fn new(config_path: impl Into<PathBuf>) -> Self {
        Self {
            config_path: config_path.into(),
        }
    }

    /// Parse the config file, treating "missing" and "empty" as "no settings".
    fn read_config(&self) -> Result<DocumentMut, InstallError> {
        let Some(contents) = read_config_text(&self.config_path)? else {
            return Ok(DocumentMut::new());
        };

        contents.parse::<DocumentMut>().map_err(|source| {
            InstallError::Parse(format!("{}: {source}", self.config_path.display()))
        })
    }

    /// Write `document` back, atomically, keeping a `.bak`.
    fn write_config(&self, document: &DocumentMut) -> Result<(), InstallError> {
        write_atomically(&self.config_path, &document.to_string())
    }

    /// This server's entry in `document`, if it has one.
    fn existing_entry<'a>(
        &self,
        document: &'a DocumentMut,
        entry: &ServerEntry,
    ) -> Option<&'a Item> {
        document
            .get(SERVERS_TABLE)?
            .as_table_like()?
            .get(entry.name)
    }

    /// The table `entry` would be written as.
    fn build_entry(&self, entry: &ServerEntry) -> Table {
        let mut table = Table::new();
        table.insert("command", value(entry.command()));

        let mut arguments = Array::new();
        for argument in entry.args {
            arguments.push(*argument);
        }
        table.insert("args", value(arguments));

        let mut env = Table::new();
        for (key, contents) in &entry.env {
            env.insert(key, value(contents.as_str()));
        }
        table.insert("env", Item::Table(env));

        table
    }
}

/// Whether what is already in the file is exactly what `entry` would write.
///
/// Compared field by field rather than by rendering both: an entry someone
/// wrote by hand as an inline table means the same thing as one this crate
/// wrote as a section header, and rewriting the file to change only that is
/// churn that costs them their `.bak`.
fn matches_entry(existing: &Item, entry: &ServerEntry) -> bool {
    let Some(table) = existing.as_table_like() else {
        return false;
    };

    let command_matches =
        table.get("command").and_then(Item::as_str) == Some(entry.command().as_str());

    let arguments: Option<Vec<&str>> = table
        .get("args")
        .and_then(Item::as_array)
        .map(|array| array.iter().filter_map(|item| item.as_str()).collect());
    let arguments_match = arguments.as_deref() == Some(entry.args);

    let environment_matches =
        table
            .get("env")
            .and_then(Item::as_table_like)
            .is_some_and(|environment| {
                environment.len() == entry.env.len()
                    && entry.env.iter().all(|(key, wanted)| {
                        environment.get(key).and_then(Item::as_str) == Some(wanted.as_str())
                    })
            });

    command_matches && arguments_match && environment_matches
}

impl McpClient for CodexClient {
    fn name(&self) -> &str {
        "Codex"
    }

    fn config_path(&self) -> PathBuf {
        self.config_path.clone()
    }

    fn check_existing(&self, entry: &ServerEntry) -> InstallStatus {
        if !self.detect() {
            return InstallStatus::ClientNotFound;
        }

        // A config we cannot parse has no entry we can see. `install` will
        // report the parse failure properly when it tries to write.
        let Ok(document) = self.read_config() else {
            return InstallStatus::NotInstalled;
        };

        let Some(existing) = self.existing_entry(&document, entry) else {
            return InstallStatus::NotInstalled;
        };

        let their_command = existing
            .as_table_like()
            .and_then(|table| table.get("command"))
            .and_then(|command| command.as_str())
            .unwrap_or_default();

        if their_command == entry.command() {
            InstallStatus::Installed
        } else {
            InstallStatus::NeedsUpdate {
                current_command: their_command.to_string(),
            }
        }
    }

    fn install(&self, entry: &ServerEntry) -> Result<(), InstallError> {
        let mut document = self.read_config()?;

        // Nothing to do if it is already exactly right: rewriting would churn
        // the file's timestamp and overwrite a `.bak` worth keeping.
        if self
            .existing_entry(&document, entry)
            .is_some_and(|existing| matches_entry(existing, entry))
        {
            return Ok(());
        }

        if !document.contains_key(SERVERS_TABLE) {
            let mut servers = Table::new();
            // Implicit, so the file gets `[mcp_servers.my-mcp]` and not a bare
            // `[mcp_servers]` header above it with nothing in it. The parsed
            // result is identical; this is what a hand-written config looks
            // like.
            servers.set_implicit(true);
            document.insert(SERVERS_TABLE, Item::Table(servers));
        }

        let servers = document
            .get_mut(SERVERS_TABLE)
            .and_then(Item::as_table_like_mut)
            .ok_or_else(|| {
                InstallError::Parse(format!(
                    "{}: `{SERVERS_TABLE}` is not a table",
                    self.config_path.display()
                ))
            })?;
        servers.insert(entry.name, Item::Table(self.build_entry(entry)));

        self.write_config(&document)
    }

    fn uninstall(&self, entry: &ServerEntry) -> Result<(), InstallError> {
        let mut document = self.read_config()?;

        let removed = document
            .get_mut(SERVERS_TABLE)
            .and_then(Item::as_table_like_mut)
            .is_some_and(|servers| servers.remove(entry.name).is_some());

        if !removed {
            return Ok(());
        }

        self.write_config(&document)
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

    /// A Codex rooted in `dir`, detected because `dir` exists.
    fn client(dir: &TempDir) -> CodexClient {
        CodexClient::new(dir.path().join("config.toml"))
    }

    /// Read a client's config back as a document.
    fn document_of(client: &CodexClient) -> DocumentMut {
        std::fs::read_to_string(client.config_path())
            .unwrap()
            .parse()
            .unwrap()
    }

    /// The entry table, if there is one.
    fn entry_of(client: &CodexClient) -> Option<Table> {
        document_of(client)
            .get(SERVERS_TABLE)?
            .as_table_like()?
            .get("my-mcp")?
            .as_table()
            .cloned()
    }

    /// The command `install` would write.
    fn our_command() -> String {
        entry().command()
    }

    //
    // Detection
    //

    #[test]
    fn a_codex_whose_directory_exists_is_detected() {
        let dir = TempDir::new().unwrap();

        assert!(client(&dir).detect());
    }

    #[test]
    fn a_codex_that_is_not_installed_is_reported_as_such() {
        let dir = TempDir::new().unwrap();
        let absent = CodexClient::new(dir.path().join("nowhere/config.toml"));

        assert!(!absent.detect());
        assert_eq!(
            absent.check_existing(&entry()),
            InstallStatus::ClientNotFound
        );
    }

    #[test]
    fn a_codex_names_itself() {
        let dir = TempDir::new().unwrap();

        assert_eq!(client(&dir).name(), "Codex");
        assert_eq!(client(&dir).slug(), "codex");
        assert!(client(&dir).matches_selector("Codex"));
    }

    //
    // Installing
    //

    #[test]
    fn install_creates_the_config_from_scratch() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);

        client.install(&entry()).unwrap();

        let entry = entry_of(&client).expect("the entry must be written");
        assert_eq!(entry["command"].as_str().unwrap(), our_command());
        assert_eq!(
            entry["args"].as_array().unwrap().get(0).unwrap().as_str(),
            Some("serve")
        );
    }

    #[test]
    fn install_writes_the_pinned_environment() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);

        client
            .install(&entry().with_env("MY_MCP_HOME", "/var/lib/my-mcp"))
            .unwrap();

        assert_eq!(
            entry_of(&client).unwrap()["env"]["MY_MCP_HOME"]
                .as_str()
                .unwrap(),
            "/var/lib/my-mcp"
        );
    }

    #[test]
    fn install_writes_the_entry_under_its_own_name() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);

        client
            .install(&ServerEntry::new("some-other-server", &["run"]))
            .unwrap();

        let document = document_of(&client);
        assert!(document["mcp_servers"]["some-other-server"].is_table());
        assert!(entry_of(&client).is_none());
    }

    #[test]
    fn install_preserves_existing_servers_and_settings() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        std::fs::write(
            client.config_path(),
            "model = \"o3\"\n\
             \n\
             [mcp_servers.memory]\n\
             command = \"/usr/local/bin/memoryco\"\n\
             args = [\"serve\"]\n",
        )
        .unwrap();

        client.install(&entry()).unwrap();

        let document = document_of(&client);
        assert_eq!(document["model"].as_str(), Some("o3"));
        assert_eq!(
            document["mcp_servers"]["memory"]["command"].as_str(),
            Some("/usr/local/bin/memoryco")
        );
        assert!(entry_of(&client).is_some());
    }

    #[test]
    fn install_keeps_the_comments_around_it() {
        // The whole reason this is toml_edit and not toml: someone's config
        // file is theirs, annotations included.
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        std::fs::write(
            client.config_path(),
            "# the model I actually want\nmodel = \"o3\" # not the cheap one\n",
        )
        .unwrap();

        client.install(&entry()).unwrap();

        let text = std::fs::read_to_string(client.config_path()).unwrap();
        assert!(text.contains("# the model I actually want"), "{text}");
        assert!(text.contains("# not the cheap one"), "{text}");
    }

    #[test]
    fn install_creates_the_servers_table_when_it_is_missing() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        std::fs::write(client.config_path(), "model = \"o3\"\n").unwrap();

        client.install(&entry()).unwrap();

        assert!(entry_of(&client).is_some());
    }

    #[test]
    fn install_replaces_an_entry_pointing_at_a_moved_binary() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        std::fs::write(
            client.config_path(),
            "[mcp_servers.\"my-mcp\"]\ncommand = \"/old/build\"\nargs = [\"serve\"]\n",
        )
        .unwrap();

        assert_eq!(
            client.check_existing(&entry()),
            InstallStatus::NeedsUpdate {
                current_command: "/old/build".to_string()
            }
        );

        client.install(&entry()).unwrap();

        assert_eq!(client.check_existing(&entry()), InstallStatus::Installed);
        assert_eq!(
            entry_of(&client).unwrap()["command"].as_str().unwrap(),
            our_command()
        );
    }

    #[test]
    fn install_rewrites_an_entry_whose_environment_changed() {
        // Only the command decides `check_existing`, so an entry whose env went
        // stale still reads as installed - `install` has to notice for itself.
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        client
            .install(&entry().with_env("MY_MCP_HOME", "/old"))
            .unwrap();

        client
            .install(&entry().with_env("MY_MCP_HOME", "/new"))
            .unwrap();

        assert_eq!(
            entry_of(&client).unwrap()["env"]["MY_MCP_HOME"]
                .as_str()
                .unwrap(),
            "/new"
        );
    }

    #[test]
    fn install_rewrites_an_entry_whose_arguments_changed() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        client
            .install(&ServerEntry::new("my-mcp", &["serve"]))
            .unwrap();

        client
            .install(&ServerEntry::new("my-mcp", &["serve", "--stdio"]))
            .unwrap();

        let arguments = entry_of(&client).unwrap();
        let arguments: Vec<_> = arguments["args"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item.as_str().unwrap().to_string())
            .collect();
        assert_eq!(arguments, vec!["serve", "--stdio"]);
    }

    #[test]
    fn install_leaves_a_hand_written_inline_entry_alone() {
        // Same command, same args, same (absent) env - written as an inline
        // table rather than a section. Rewriting it would only change how it
        // looks, and would cost them their backup.
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        let original = format!(
            "[mcp_servers]\n\"my-mcp\" = {{ command = \"{}\", args = [\"serve\"], env = {{}} }}\n",
            our_command()
        );
        std::fs::write(client.config_path(), &original).unwrap();

        client.install(&entry()).unwrap();

        assert_eq!(
            std::fs::read_to_string(client.config_path()).unwrap(),
            original
        );
    }

    #[test]
    fn install_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);

        client.install(&entry()).unwrap();
        let first = std::fs::read_to_string(client.config_path()).unwrap();
        client.install(&entry()).unwrap();

        assert_eq!(
            std::fs::read_to_string(client.config_path()).unwrap(),
            first
        );
        assert!(
            !backup_path(&client.config_path()).exists(),
            "a no-op install must not churn the backup"
        );
    }

    #[test]
    fn install_handles_an_empty_file() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        std::fs::write(client.config_path(), "\n\n").unwrap();

        client.install(&entry()).unwrap();

        assert!(entry_of(&client).is_some());
    }

    #[test]
    fn install_creates_a_missing_parent_directory() {
        let dir = TempDir::new().unwrap();
        let client = CodexClient::new(dir.path().join(".codex/config.toml"));
        std::fs::create_dir(dir.path().join(".codex")).unwrap();

        client.install(&entry()).unwrap();

        assert!(client.config_path().exists());
    }

    #[test]
    fn install_keeps_the_previous_config_as_a_backup() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        std::fs::write(client.config_path(), "model = \"o3\"\n").unwrap();

        client.install(&entry()).unwrap();

        assert_eq!(
            std::fs::read_to_string(backup_path(&client.config_path())).unwrap(),
            "model = \"o3\"\n"
        );
    }

    //
    // Status
    //

    #[test]
    fn a_fresh_codex_has_no_entry() {
        let dir = TempDir::new().unwrap();

        assert_eq!(
            client(&dir).check_existing(&entry()),
            InstallStatus::NotInstalled
        );
    }

    #[test]
    fn an_installed_codex_is_reported_as_installed() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        client.install(&entry()).unwrap();

        assert_eq!(client.check_existing(&entry()), InstallStatus::Installed);
    }

    #[test]
    fn another_servers_entry_is_not_ours() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        client
            .install(&ServerEntry::new("someone-else", &["serve"]))
            .unwrap();

        assert_eq!(client.check_existing(&entry()), InstallStatus::NotInstalled);
    }

    #[test]
    fn a_config_that_is_not_toml_reads_as_having_no_entry() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        std::fs::write(client.config_path(), "= not toml =").unwrap();

        assert_eq!(client.check_existing(&entry()), InstallStatus::NotInstalled);
    }

    //
    // Uninstalling
    //

    #[test]
    fn uninstall_removes_only_our_entry() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        std::fs::write(
            client.config_path(),
            "model = \"o3\"\n\
             \n\
             [mcp_servers.\"my-mcp\"]\n\
             command = \"/anything\"\n\
             \n\
             [mcp_servers.memory]\n\
             command = \"/usr/local/bin/memoryco\"\n",
        )
        .unwrap();

        client.uninstall(&entry()).unwrap();

        let document = document_of(&client);
        assert!(entry_of(&client).is_none());
        assert_eq!(
            document["mcp_servers"]["memory"]["command"].as_str(),
            Some("/usr/local/bin/memoryco")
        );
        assert_eq!(document["model"].as_str(), Some("o3"));
    }

    #[test]
    fn uninstall_is_safe_when_no_file_exists() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);

        client.uninstall(&entry()).unwrap();

        assert!(!client.config_path().exists());
    }

    #[test]
    fn uninstall_is_safe_when_there_is_no_entry() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        std::fs::write(client.config_path(), "model = \"o3\" # keep me\n").unwrap();

        client.uninstall(&entry()).unwrap();

        assert_eq!(
            std::fs::read_to_string(client.config_path()).unwrap(),
            "model = \"o3\" # keep me\n"
        );
    }

    #[test]
    fn uninstall_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        client.install(&entry()).unwrap();

        client.uninstall(&entry()).unwrap();
        let once = std::fs::read_to_string(client.config_path()).unwrap();
        client.uninstall(&entry()).unwrap();

        assert_eq!(std::fs::read_to_string(client.config_path()).unwrap(), once);
    }

    #[test]
    fn uninstall_keeps_the_previous_config_as_a_backup() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        client.install(&entry()).unwrap();

        client.uninstall(&entry()).unwrap();

        let saved = std::fs::read_to_string(backup_path(&client.config_path())).unwrap();
        assert!(saved.contains(&our_command()), "{saved}");
    }

    #[test]
    fn install_then_uninstall_restores_what_was_there() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        let original = "# my config\nmodel = \"o3\"\n\n[mcp_servers.memory]\ncommand = \"/x\"\n";
        std::fs::write(client.config_path(), original).unwrap();

        client.install(&entry()).unwrap();
        client.uninstall(&entry()).unwrap();

        assert_eq!(
            std::fs::read_to_string(client.config_path()).unwrap(),
            original
        );
    }

    #[test]
    fn two_servers_share_one_config() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
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
    fn a_config_that_is_not_toml_is_reported_rather_than_overwritten() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        std::fs::write(client.config_path(), "= not toml =").unwrap();

        let error = client
            .install(&entry())
            .expect_err("malformed TOML must fail");

        assert!(matches!(error, InstallError::Parse(_)));
        assert!(error.to_string().contains("config.toml"));
        assert_eq!(
            std::fs::read_to_string(client.config_path()).unwrap(),
            "= not toml =",
            "a file we cannot understand must not be replaced"
        );
    }

    #[test]
    fn a_servers_table_that_is_not_a_table_is_reported() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        std::fs::write(client.config_path(), "mcp_servers = \"surprise\"\n").unwrap();

        let error = client
            .install(&entry())
            .expect_err("a string is not a server table");

        assert!(matches!(error, InstallError::Parse(_)));
        assert!(error.to_string().contains(SERVERS_TABLE));
    }

    #[test]
    fn uninstalling_from_a_malformed_config_is_reported() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        std::fs::write(client.config_path(), "= not toml =").unwrap();

        assert!(matches!(
            client.uninstall(&entry()),
            Err(InstallError::Parse(_))
        ));
    }

    //
    // The written form
    //

    #[test]
    fn the_entry_is_written_where_codex_looks_for_it() {
        // Dotted key, so `mcp_servers` stays a table Codex can hold other
        // servers in, and the name is quoted only if it has to be.
        let dir = TempDir::new().unwrap();
        let client = client(&dir);

        client.install(&entry()).unwrap();

        let text = std::fs::read_to_string(client.config_path()).unwrap();
        assert!(text.contains("[mcp_servers.my-mcp]"), "{text}");
        assert!(
            text.parse::<DocumentMut>().is_ok(),
            "what we write must parse: {text}"
        );
    }

    #[test]
    fn a_new_servers_table_gets_no_empty_header_of_its_own() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);

        client.install(&entry()).unwrap();

        let text = std::fs::read_to_string(client.config_path()).unwrap();
        assert!(
            !text.lines().any(|line| line.trim() == "[mcp_servers]"),
            "{text}"
        );
    }

    //
    // Entry comparison
    //

    #[test]
    fn an_entry_matches_only_when_every_part_of_it_does() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        let entry = entry().with_env("A", "1");
        client.install(&entry).unwrap();

        let document = document_of(&client);
        let existing = document["mcp_servers"]["my-mcp"].clone();

        assert!(matches_entry(&existing, &entry));
        assert!(
            !matches_entry(&existing, &entry.clone().with_env("B", "2")),
            "an extra pinned variable is a different entry"
        );
        assert!(
            !matches_entry(&existing, &ServerEntry::new("my-mcp", &["serve", "--x"])),
            "different arguments are a different entry"
        );
        assert!(
            !matches_entry(&existing, &ServerEntry::new("my-mcp", &["serve"])),
            "a dropped pinned variable is a different entry"
        );
    }

    #[test]
    fn a_value_that_is_not_a_table_is_not_an_entry() {
        assert!(!matches_entry(&value("surprise"), &entry()));
    }
}
