//! OpenAI Codex, which keeps its MCP servers in TOML.
//!
//! Same contract as [`JsonClient`](super::JsonClient) - idempotent both ways,
//! everything else preserved - over a different file format:
//!
//! ```toml
//! [mcp_servers.my-mcp]
//! command = "/usr/local/bin/my-mcp"
//! args = ["serve"]
//! default_tools_approval_mode = "auto"
//!
//! [mcp_servers.my-mcp.env]
//! MY_MCP_HOME = "/Users/you/.my-mcp"
//! ```
//!
//! The approval mode is the one thing here no JSON client has anywhere to put,
//! and it appears only for an entry that asked for it with
//! [`with_auto_approve`](ServerEntry::with_auto_approve).
//!
//! Edited with `toml_edit` rather than `toml`: `toml` parses into values, and
//! re-serializing loses every comment and every choice of layout in the file.
//! `~/.codex/config.toml` is hand-written and hand-commented, and silently
//! reformatting it is not a thing an installer gets to do.

use super::{
    InstallError, InstallStatus, McpClient, ServerEntry, read_config_text, write_atomically,
};
use std::path::PathBuf;
use toml_edit::{Array, DocumentMut, Item, Table, Value, value};

/// Top-level table holding Codex's MCP servers.
const SERVERS_TABLE: &str = "mcp_servers";

/// Key on a server entry that says whether Codex asks before running its tools.
const APPROVAL_MODE_KEY: &str = "default_tools_approval_mode";

/// The approval mode that means "do not ask".
const APPROVAL_MODE_AUTO: &str = "auto";

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

    /// The table `entry` would be written as, given the approval mode already
    /// on the entry it replaces.
    ///
    /// `their_mode` is the one thing carried over from what was there, because
    /// it is the one thing that may not have come from here: `auto_approve` is
    /// a request to *add* a mode, never to remove one a person set by hand.
    fn build_entry(&self, entry: &ServerEntry, their_mode: Option<&str>) -> Table {
        let mut table = Table::new();
        table.insert("command", value(entry.command()));

        let mut arguments = Array::new();
        for argument in entry.args {
            arguments.push(*argument);
        }
        table.insert("args", value(arguments));

        // On the entry itself, beside `command` - not inside `env`, which is
        // written as a sub-table of its own and is where Codex looks for
        // nothing but environment variables.
        if let Some(mode) = wanted_approval_mode(entry, their_mode) {
            table.insert(APPROVAL_MODE_KEY, value(mode));
        }

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

    // Collected into an `Option` rather than filtered: a `filter_map` drops a
    // non-string element instead of failing on it, so a hand-written
    // `args = ["serve", 42]` compared equal to `["serve"]` and an entry Codex
    // cannot start was reported as installed.
    let arguments: Option<Vec<&str>> = table
        .get("args")
        .and_then(Item::as_array)
        .and_then(|array| array.iter().map(Value::as_str).collect());
    let arguments_match = arguments.as_deref() == Some(entry.args);

    // Against the deduped view of the entry's environment, because that is what
    // `build_entry` writes: a table cannot hold `A` twice, so comparing the
    // written table's length against the raw list's would never agree for an
    // entry that pinned `A` twice - and the entry would be rewritten on every
    // run for the rest of its life.
    let wanted = entry.env_map();
    let environment_matches =
        table
            .get("env")
            .and_then(Item::as_table_like)
            .is_some_and(|environment| {
                environment.len() == wanted.len()
                    && wanted.iter().all(|(key, value)| {
                        environment.get(key).and_then(Item::as_str) == Some(*value)
                    })
            });

    // Asymmetric on purpose: an entry that wants auto-approval and has not got
    // it needs rewriting, but one that never asked has no opinion about a mode
    // it did not write, and rewriting to strip it would be an installer
    // overruling a person.
    let approval_matches =
        !entry.auto_approve || approval_mode_of(existing) == Some(APPROVAL_MODE_AUTO);

    command_matches && arguments_match && environment_matches && approval_matches
}

/// The approval mode already written on `existing`, if it has one.
fn approval_mode_of(existing: &Item) -> Option<&str> {
    existing.as_table_like()?.get(APPROVAL_MODE_KEY)?.as_str()
}

/// What the entry about to be written should say about approval, if anything.
///
/// `their_mode` is what the entry being replaced said. Asking for
/// auto-approval overwrites it; not asking keeps it.
fn wanted_approval_mode<'a>(entry: &ServerEntry, their_mode: Option<&'a str>) -> Option<&'a str> {
    if entry.auto_approve {
        return Some(APPROVAL_MODE_AUTO);
    }

    their_mode
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

        if their_command != entry.command() {
            return InstallStatus::NeedsUpdate {
                current_command: their_command.to_string(),
            };
        }

        // The binary has not moved, but the rest of the entry may still be
        // stale - a changed argument, a pinned variable, an approval mode that
        // was asked for and is not there. `install` rewrites all of it; the
        // status is what decides whether the `install` command bothers to call
        // it, so answering `Installed` here is what would leave the file alone.
        if matches_entry(existing, entry) {
            InstallStatus::Installed
        } else {
            InstallStatus::NeedsRefresh
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

        // Read before the document is borrowed mutably, and owned because the
        // entry it came from is about to be replaced.
        let their_mode = self
            .existing_entry(&document, entry)
            .and_then(approval_mode_of)
            .map(str::to_string);

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
        servers.insert(
            entry.name,
            Item::Table(self.build_entry(entry, their_mode.as_deref())),
        );

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
        // `install` compares the whole entry for itself rather than trusting a
        // caller to have checked first.
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
    // Auto-approval
    //

    #[test]
    fn install_writes_the_approval_mode_when_the_entry_asks_for_it() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);

        client.install(&entry().with_auto_approve()).unwrap();

        assert_eq!(
            entry_of(&client).unwrap()[APPROVAL_MODE_KEY].as_str(),
            Some("auto")
        );
    }

    #[test]
    fn install_writes_no_approval_mode_by_default() {
        // Whether a tool call needs approving is the user's decision, and an
        // install that answered it for them without being asked would be
        // taking it.
        let dir = TempDir::new().unwrap();
        let client = client(&dir);

        client.install(&entry()).unwrap();

        assert!(entry_of(&client).unwrap().get(APPROVAL_MODE_KEY).is_none());
        assert!(
            !std::fs::read_to_string(client.config_path())
                .unwrap()
                .contains(APPROVAL_MODE_KEY)
        );
    }

    #[test]
    fn install_adds_the_approval_mode_to_an_entry_that_has_none() {
        // The upgrade case: the entry is already there, pointing at the right
        // binary, and the only thing missing is the mode.
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        client.install(&entry()).unwrap();
        assert_eq!(client.check_existing(&entry()), InstallStatus::Installed);

        client.install(&entry().with_auto_approve()).unwrap();

        assert_eq!(
            entry_of(&client).unwrap()[APPROVAL_MODE_KEY].as_str(),
            Some("auto")
        );
    }

    #[test]
    fn install_updates_an_approval_mode_that_says_something_else() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        std::fs::write(
            client.config_path(),
            format!(
                "[mcp_servers.\"my-mcp\"]\n\
                 command = \"{}\"\n\
                 args = [\"serve\"]\n\
                 {APPROVAL_MODE_KEY} = \"never\"\n",
                our_command()
            ),
        )
        .unwrap();

        client.install(&entry().with_auto_approve()).unwrap();

        assert_eq!(
            entry_of(&client).unwrap()[APPROVAL_MODE_KEY].as_str(),
            Some("auto")
        );
    }

    #[test]
    fn install_keeps_an_approval_mode_it_was_not_asked_about() {
        // A mode that is already there did not necessarily come from here. An
        // entry that never asked for auto-approval says nothing about one, and
        // rewriting it for a moved binary must not quietly strip it.
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        std::fs::write(
            client.config_path(),
            format!(
                "[mcp_servers.\"my-mcp\"]\n\
                 command = \"/old/build\"\n\
                 args = [\"serve\"]\n\
                 {APPROVAL_MODE_KEY} = \"auto\"\n"
            ),
        )
        .unwrap();

        client.install(&entry()).unwrap();

        let written = entry_of(&client).unwrap();
        assert_eq!(written["command"].as_str().unwrap(), our_command());
        assert_eq!(written[APPROVAL_MODE_KEY].as_str(), Some("auto"));
    }

    #[test]
    fn an_approval_mode_nobody_asked_about_is_not_a_reason_to_rewrite() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        client.install(&entry().with_auto_approve()).unwrap();
        let before = std::fs::read_to_string(client.config_path()).unwrap();

        client.install(&entry()).unwrap();

        assert_eq!(
            std::fs::read_to_string(client.config_path()).unwrap(),
            before
        );
        assert!(
            !backup_path(&client.config_path()).exists(),
            "a no-op install must not churn the backup"
        );
    }

    #[test]
    fn install_with_auto_approval_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);

        client.install(&entry().with_auto_approve()).unwrap();
        let first = std::fs::read_to_string(client.config_path()).unwrap();
        client.install(&entry().with_auto_approve()).unwrap();

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
    fn an_auto_approving_entry_still_carries_everything_else() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);

        client
            .install(
                &entry()
                    .with_env("MY_MCP_HOME", "/var/lib/my-mcp")
                    .with_auto_approve(),
            )
            .unwrap();

        let written = entry_of(&client).unwrap();
        assert_eq!(written["command"].as_str().unwrap(), our_command());
        assert_eq!(
            written["args"].as_array().unwrap().get(0).unwrap().as_str(),
            Some("serve")
        );
        assert_eq!(
            written["env"]["MY_MCP_HOME"].as_str(),
            Some("/var/lib/my-mcp")
        );
        assert_eq!(written[APPROVAL_MODE_KEY].as_str(), Some("auto"));
    }

    #[test]
    fn the_approval_mode_is_written_where_codex_reads_it() {
        // Against the rendered file, not the parsed one: an entry with an
        // `env` table renders as two TOML sections, and a mode that came out
        // under `[mcp_servers.my-mcp.env]` would be an environment variable
        // called `default_tools_approval_mode` and nothing else.
        let dir = TempDir::new().unwrap();
        let client = client(&dir);

        client
            .install(&entry().with_env("A", "1").with_auto_approve())
            .unwrap();

        let text = std::fs::read_to_string(client.config_path()).unwrap();
        let mode = text
            .lines()
            .position(|line| line.trim() == "default_tools_approval_mode = \"auto\"")
            .unwrap_or_else(|| panic!("the mode must be written verbatim: {text}"));
        let env_header = text
            .lines()
            .position(|line| line.trim() == "[mcp_servers.my-mcp.env]")
            .unwrap_or_else(|| panic!("the env table must still have a header: {text}"));
        assert!(mode < env_header, "{text}");
    }

    #[test]
    fn uninstall_takes_the_approval_mode_with_the_entry() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        client.install(&entry().with_auto_approve()).unwrap();

        client.uninstall(&entry()).unwrap();

        assert!(entry_of(&client).is_none());
        assert!(
            !std::fs::read_to_string(client.config_path())
                .unwrap()
                .contains(APPROVAL_MODE_KEY),
            "the whole entry goes, so nothing of it is left to clean up"
        );
    }

    #[test]
    fn an_auto_approving_entry_leaves_another_servers_mode_alone() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        std::fs::write(
            client.config_path(),
            format!(
                "[mcp_servers.memory]\n\
                 command = \"/usr/local/bin/memoryco\"\n\
                 {APPROVAL_MODE_KEY} = \"never\"\n"
            ),
        )
        .unwrap();

        client.install(&entry().with_auto_approve()).unwrap();

        let document = document_of(&client);
        assert_eq!(
            document["mcp_servers"]["memory"][APPROVAL_MODE_KEY].as_str(),
            Some("never")
        );
        assert_eq!(
            document["mcp_servers"]["my-mcp"][APPROVAL_MODE_KEY].as_str(),
            Some("auto")
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
    fn a_stale_approval_mode_alone_needs_a_refresh() {
        // The binary has not moved, so there is nothing to say about where it
        // went - but the entry is not what this one would write, and a status
        // of `Installed` is what would have the `install` command skip the
        // client and never add the mode at all.
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        client.install(&entry()).unwrap();

        assert_eq!(
            client.check_existing(&entry().with_auto_approve()),
            InstallStatus::NeedsRefresh
        );
    }

    #[test]
    fn an_entry_with_the_approval_mode_it_asked_for_is_installed() {
        // The other half of it: once the mode is there, the entry is current
        // and re-running `install` has nothing to do.
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        client.install(&entry().with_auto_approve()).unwrap();

        assert_eq!(
            client.check_existing(&entry().with_auto_approve()),
            InstallStatus::Installed
        );
    }

    #[test]
    fn a_refresh_is_what_the_install_actually_writes() {
        // The status is only worth having if acting on it fixes the thing it
        // reported: check, install, check again.
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        client.install(&entry()).unwrap();
        let asked = entry().with_auto_approve();

        assert_eq!(client.check_existing(&asked), InstallStatus::NeedsRefresh);
        client.install(&asked).unwrap();

        assert_eq!(client.check_existing(&asked), InstallStatus::Installed);
        assert_eq!(
            entry_of(&client).unwrap()[APPROVAL_MODE_KEY].as_str(),
            Some("auto")
        );
    }

    #[test]
    fn stale_arguments_or_environment_need_a_refresh_too() {
        // Nothing about this is specific to the approval mode: the status is
        // "this entry is not what we would write", whatever the difference is.
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        client
            .install(&entry().with_env("MY_MCP_HOME", "/old"))
            .unwrap();

        assert_eq!(
            client.check_existing(&entry().with_env("MY_MCP_HOME", "/new")),
            InstallStatus::NeedsRefresh
        );
        assert_eq!(
            client.check_existing(&ServerEntry::new("my-mcp", &["serve", "--stdio"])),
            InstallStatus::NeedsRefresh
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
        let client = client(&dir);
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
    fn an_entry_that_pins_one_variable_twice_settles_after_one_install() {
        // The table holds one `A`, so counting the entry's *list* of pairs
        // against it never agreed: the status was `NeedsRefresh` after the
        // install that wrote it, and stayed that way for every run afterwards.
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        let entry = entry().with_env("A", "1").with_env("A", "2");

        client.install(&entry).unwrap();

        assert_eq!(client.check_existing(&entry), InstallStatus::Installed);
        assert_eq!(
            entry_of(&client).unwrap()["env"]["A"].as_str(),
            Some("2"),
            "and the value written is the last one asked for"
        );
    }

    #[test]
    fn a_repeated_variable_does_not_hide_a_real_change() {
        // The dedupe is not a way of ignoring the environment: the same entry
        // with a different last value is still stale.
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        client
            .install(&entry().with_env("A", "1").with_env("A", "2"))
            .unwrap();

        assert_eq!(
            client.check_existing(&entry().with_env("A", "1").with_env("A", "3")),
            InstallStatus::NeedsRefresh
        );
        assert_eq!(
            client.check_existing(&entry().with_env("A", "2").with_env("B", "2")),
            InstallStatus::NeedsRefresh,
            "an extra variable is a difference too"
        );
    }

    #[test]
    fn an_argument_that_is_not_a_string_is_not_our_entry() {
        // `args = ["serve", 42]` is not `["serve"]`, and skipping the `42`
        // reported an entry Codex cannot start as installed - so it was never
        // rewritten with one that works.
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        std::fs::write(
            client.config_path(),
            format!(
                "[mcp_servers.\"my-mcp\"]\n\
                 command = \"{}\"\n\
                 args = [\"serve\", 42]\n\
                 \n\
                 [mcp_servers.\"my-mcp\".env]\n",
                our_command()
            ),
        )
        .unwrap();

        assert_eq!(client.check_existing(&entry()), InstallStatus::NeedsRefresh);

        // And the refresh replaces it with arguments Codex can read.
        client.install(&entry()).unwrap();
        assert_eq!(client.check_existing(&entry()), InstallStatus::Installed);
        assert_eq!(
            entry_of(&client).unwrap()["args"]
                .as_array()
                .unwrap()
                .iter()
                .map(|argument| argument.as_str().unwrap().to_string())
                .collect::<Vec<_>>(),
            vec!["serve".to_string()]
        );
    }

    #[test]
    fn an_args_key_that_is_not_an_array_is_not_our_entry_either() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        std::fs::write(
            client.config_path(),
            format!(
                "[mcp_servers.\"my-mcp\"]\n\
                 command = \"{}\"\n\
                 args = \"serve\"\n",
                our_command()
            ),
        )
        .unwrap();

        assert_eq!(client.check_existing(&entry()), InstallStatus::NeedsRefresh);
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

    #[test]
    fn an_entry_without_the_approval_mode_does_not_match_one_that_wants_it() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        client.install(&entry()).unwrap();

        let document = document_of(&client);
        let existing = document["mcp_servers"]["my-mcp"].clone();

        assert!(matches_entry(&existing, &entry()));
        assert!(
            !matches_entry(&existing, &entry().with_auto_approve()),
            "an entry that wants auto-approval and has not got it needs writing"
        );
    }

    #[test]
    fn an_approval_mode_is_only_compared_when_one_was_asked_for() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        client.install(&entry().with_auto_approve()).unwrap();

        let document = document_of(&client);
        let existing = document["mcp_servers"]["my-mcp"].clone();

        assert!(matches_entry(&existing, &entry().with_auto_approve()));
        assert!(
            matches_entry(&existing, &entry()),
            "a mode we did not ask about is not a difference worth a rewrite"
        );
    }

    #[test]
    fn a_mode_that_is_not_auto_does_not_satisfy_an_entry_that_wants_auto() {
        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        std::fs::write(
            client.config_path(),
            format!(
                "[mcp_servers.\"my-mcp\"]\n\
                 command = \"{}\"\n\
                 args = [\"serve\"]\n\
                 env = {{}}\n\
                 {APPROVAL_MODE_KEY} = \"never\"\n",
                our_command()
            ),
        )
        .unwrap();

        let document = document_of(&client);
        let existing = document["mcp_servers"]["my-mcp"].clone();

        assert!(!matches_entry(&existing, &entry().with_auto_approve()));
        assert!(matches_entry(&existing, &entry()));
    }

    #[test]
    fn what_gets_written_is_asked_for_or_carried_over() {
        assert_eq!(wanted_approval_mode(&entry(), None), None);
        assert_eq!(wanted_approval_mode(&entry(), Some("never")), Some("never"));
        assert_eq!(
            wanted_approval_mode(&entry().with_auto_approve(), None),
            Some("auto")
        );
        assert_eq!(
            wanted_approval_mode(&entry().with_auto_approve(), Some("never")),
            Some("auto")
        );
    }

    #[test]
    fn an_entry_with_no_mode_on_it_reports_none() {
        assert_eq!(approval_mode_of(&value("surprise")), None);

        let dir = TempDir::new().unwrap();
        let client = client(&dir);
        client.install(&entry()).unwrap();
        let document = document_of(&client);

        assert_eq!(approval_mode_of(&document["mcp_servers"]["my-mcp"]), None);
    }
}
