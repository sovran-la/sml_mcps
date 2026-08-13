//! Detecting the MCP clients on this machine and editing their configuration.
//!
//! Every supported client is reached through one trait, [`McpClient`], so the
//! commands above this module are a loop over [`all_clients`] rather than a
//! list of special cases. Two implementations cover all six: [`JsonClient`] for
//! the five that keep their server list in JSON, and [`CodexClient`] for the one
//! that uses TOML.
//!
//! What gets written is a [`ServerEntry`] - the name a server registers under,
//! the arguments that start it, and any environment it needs pinned. Everything
//! else in the entry is worked out here: the command is [`std::env::current_exe`],
//! so a config always points at the binary that wrote it.
//!
//! The rules a client's config file is edited under, which are the same for
//! every implementation:
//!
//! - **Everything we did not put there survives.** The file is parsed whole, one
//!   entry is inserted or removed, and the rest is written back untouched -
//!   including keys this crate has never heard of. `~/.claude.json` in
//!   particular holds a great deal more than server entries.
//! - **Both directions are idempotent.** Installing twice writes the same entry;
//!   uninstalling twice, or from a file that does not exist, is a no-op. Neither
//!   is a state worth reporting as an error.
//! - **The previous contents are kept as `.bak`, and the write goes through a
//!   rename.** A crash mid-install cannot leave a client unable to start, and
//!   the file it was reading a second ago is still on disk.
//! - **A file we cannot parse is left exactly as it was.** A config we do not
//!   understand is one we have no business rewriting.
//!
//! This module is usable on its own - [`crate::cli::Cli`] is one consumer of it,
//! not a gatekeeper:
//!
//! ```no_run
//! use sml_mcps::cli::{ServerEntry, all_clients};
//!
//! let entry = ServerEntry { name: "my-mcp", args: &["serve"], env: vec![] };
//!
//! for client in all_clients() {
//!     if client.detect() {
//!         client.install(&entry)?;
//!     }
//! }
//! # Ok::<(), sml_mcps::cli::InstallError>(())
//! ```

mod clients;
mod codex_client;
mod json_client;

pub use clients::{all_clients, clients_under, home_dir};
pub use codex_client::CodexClient;
pub use json_client::JsonClient;

use serde_json::{Map, Value, json};
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

//
// What gets written
//

/// What a server calls itself in an MCP client's configuration.
///
/// The three things that differ between servers. Everything else about an
/// entry, the binary path above all, is the same computation everywhere and is
/// done by the methods below.
///
/// ```
/// use sml_mcps::cli::ServerEntry;
///
/// let entry = ServerEntry { name: "my-mcp", args: &["serve"], env: vec![] };
/// assert_eq!(entry.name, "my-mcp");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerEntry {
    /// Name in the client's config, e.g. `tailscale-ssh`, `memoryco`.
    ///
    /// This is the key the entry is stored under, and so the name every client
    /// shows the user and every tool call is namespaced by.
    pub name: &'static str,

    /// Arguments passed to the binary, e.g. `&["serve"]`.
    ///
    /// Whatever makes this binary start serving MCP when a client launches it.
    /// An empty slice says the bare binary serves.
    pub args: &'static [&'static str],

    /// Environment variables pinned in the config entry.
    ///
    /// An MCP client inherits almost nothing of the shell's environment, so
    /// anything the server needs and cannot work out for itself belongs here -
    /// a data directory, an API endpoint. Owned rather than `'static` because
    /// these are usually computed at install time.
    pub env: Vec<(String, String)>,
}

impl ServerEntry {
    /// An entry named `name`, started with `args`, with no pinned environment.
    pub fn new(name: &'static str, args: &'static [&'static str]) -> Self {
        Self {
            name,
            args,
            env: Vec::new(),
        }
    }

    /// Pin one environment variable in the entry.
    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }

    /// The command a client should run: the absolute path of the running
    /// binary.
    ///
    /// A config always names the thing that wrote it, which is why
    /// [`InstallStatus::NeedsUpdate`] exists and why `install` is worth
    /// re-running after moving a binary. Falls back to [`name`](Self::name)
    /// when the path is unknowable, which leaves a client resolving it on
    /// `PATH` - not ideal, but better than writing nothing.
    pub fn command(&self) -> String {
        std::env::current_exe()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|_| self.name.to_string())
    }

    /// [`args`](Self::args) as owned strings, which is what both config formats
    /// want.
    pub fn arguments(&self) -> Vec<String> {
        self.args.iter().map(|arg| (*arg).to_string()).collect()
    }

    /// The entry as the JSON object every JSON-configured client stores.
    ///
    /// ```json
    /// { "command": "/usr/local/bin/my-mcp", "args": ["serve"], "env": {} }
    /// ```
    ///
    /// Public because [`McpClient`] is: a client this crate does not know about
    /// yet can be implemented outside it, and should write the same shape.
    pub fn json_entry(&self) -> Value {
        let env: Map<String, Value> = self
            .env
            .iter()
            .map(|(key, value)| (key.clone(), Value::String(value.clone())))
            .collect();

        json!({ "command": self.command(), "args": self.arguments(), "env": env })
    }

    /// The JSON block a user pastes into a client that could not be configured
    /// automatically.
    ///
    /// Deliberately the same entry [`install`](McpClient::install) writes, so
    /// the fallback instructions do not produce a differently-configured server
    /// from the automatic path.
    pub fn snippet(&self) -> String {
        let block = json!({ "mcpServers": { self.name: self.json_entry() } });

        serde_json::to_string_pretty(&block).unwrap_or_default()
    }
}

//
// The clients
//

/// One MCP client whose configuration this crate knows how to edit.
///
/// Implementable outside this crate: a client that appears before the registry
/// gains it can be handed to the same install and uninstall loops as the
/// built-in ones.
pub trait McpClient {
    /// Human-readable name, e.g. `Claude Desktop`.
    fn name(&self) -> &str;

    /// The client's MCP configuration file.
    fn config_path(&self) -> PathBuf;

    /// The name this client is selected by on a command line.
    ///
    /// [`name`](Self::name) lowercased with spaces hyphenated, so
    /// `Claude Desktop` is `claude-desktop`. Only worth overriding for a client
    /// whose display name makes a bad flag value.
    fn slug(&self) -> String {
        self.name().to_lowercase().replace(' ', "-")
    }

    /// Whether `selector` - a `--client` value - names this client.
    ///
    /// Forgiving about punctuation and case, because `vs-code`, `vscode` and
    /// `VS Code` are all obviously the same request.
    fn matches_selector(&self, selector: &str) -> bool {
        let wanted = squashed(selector);

        !wanted.is_empty() && (squashed(self.name()) == wanted || squashed(&self.slug()) == wanted)
    }

    /// Whether this client appears to be installed on this machine.
    ///
    /// The config file existing is sufficient but not necessary: a client that
    /// has never been handed an MCP server has not written one yet, so its
    /// parent directory counts too. An implementation with a better marker -
    /// Claude Code's `~/.claude/` - overrides this.
    fn detect(&self) -> bool {
        let path = self.config_path();
        path.exists() || path.parent().is_some_and(Path::exists)
    }

    /// What this client's config currently says about `entry`.
    fn check_existing(&self, entry: &ServerEntry) -> InstallStatus;

    /// Add or update `entry`, leaving everything else in the file in place.
    fn install(&self, entry: &ServerEntry) -> Result<(), InstallError>;

    /// Remove `entry`, leaving everything else in the file in place.
    fn uninstall(&self, entry: &ServerEntry) -> Result<(), InstallError>;
}

/// A name reduced to the letters and digits in it, lowercased.
///
/// `VS Code`, `vs-code` and `vscode` all reduce to `vscode`, which is the whole
/// point: nobody should have to guess which spelling a flag wants.
fn squashed(name: &str) -> String {
    name.chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .map(|character| character.to_ascii_lowercase())
        .collect()
}

/// What a client's configuration says about a server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InstallStatus {
    /// The client is installed and has no entry for this server.
    NotInstalled,

    /// The client points at this binary.
    Installed,

    /// The client has an entry, but it names a different binary.
    NeedsUpdate {
        /// The command the entry currently names.
        current_command: String,
    },

    /// The client does not appear to be installed.
    ClientNotFound,
}

impl InstallStatus {
    /// Whether this status is one `install` would act on.
    pub fn wants_install(&self) -> bool {
        matches!(self, Self::NotInstalled | Self::NeedsUpdate { .. })
    }

    /// Whether there is an entry here for `uninstall` to remove.
    pub fn has_entry(&self) -> bool {
        matches!(self, Self::Installed | Self::NeedsUpdate { .. })
    }
}

impl fmt::Display for InstallStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotInstalled => write!(f, "not configured"),
            Self::Installed => write!(f, "configured"),
            Self::NeedsUpdate { current_command } => {
                write!(f, "outdated (pointing at {current_command})")
            }
            Self::ClientNotFound => write!(f, "client not installed"),
        }
    }
}

//
// Errors
//

/// Failures while reading or writing a client's configuration.
#[derive(Debug)]
pub enum InstallError {
    /// A file could not be read or written.
    Io(io::Error),

    /// A config file is not the JSON or TOML we know how to edit.
    ///
    /// Always leaves the file exactly as it was: a file we cannot understand is
    /// one we have no business rewriting.
    Parse(String),

    /// The edited config could not be turned back into text.
    Serialize(String),
}

impl From<io::Error> for InstallError {
    fn from(source: io::Error) -> Self {
        Self::Io(source)
    }
}

impl fmt::Display for InstallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(source) => write!(f, "{source}"),
            Self::Parse(detail) => write!(
                f,
                "{detail} - it was left untouched. Fix or move it and try again, or add the \
                 entry by hand."
            ),
            Self::Serialize(detail) => write!(f, "could not serialize the config: {detail}"),
        }
    }
}

impl std::error::Error for InstallError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(source) => Some(source),
            Self::Parse(_) | Self::Serialize(_) => None,
        }
    }
}

//
// Shared file handling
//

/// Read a config file, treating "missing" and "empty" as "no settings".
///
/// A client that has never been handed an MCP server may have no file at all,
/// which is not a problem to report - it is the ordinary first install.
pub fn read_config_text(path: &Path) -> Result<Option<String>, InstallError> {
    match std::fs::read_to_string(path) {
        Ok(contents) if contents.trim().is_empty() => Ok(None),
        Ok(contents) => Ok(Some(contents)),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(InstallError::Io(source)),
    }
}

/// Write `contents` to `path`, keeping a copy of what was there before.
///
/// The rename is what makes this safe to interrupt: the client either sees its
/// old file or the complete new one, never a half-written one it would refuse to
/// start from. The `.bak` is what makes it safe to be *wrong* - `~/.claude.json`
/// is not a file anyone wants to reconstruct from memory.
///
/// Missing parent directories are created, so a client that keeps its config in
/// a directory it has not made yet can still be configured ahead of first run.
pub fn write_atomically(path: &Path, contents: &str) -> Result<(), InstallError> {
    write_staged(path, contents, |staging, path| {
        std::fs::rename(staging, path)
    })
}

/// [`write_atomically`] with the commit step supplied.
///
/// A rename that fails is a real thing - a directory whose permissions changed
/// underneath us, a filesystem that filled up between the write and the commit -
/// and the staging file left behind by one is a stray `.tmp` in someone's
/// `~/.cursor/` that nothing ever cleans up. It is also not a failure a test can
/// arrange on a temporary directory, so the commit is an argument.
fn write_staged<C>(path: &Path, contents: &str, commit: C) -> Result<(), InstallError>
where
    C: Fn(&Path, &Path) -> io::Result<()>,
{
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    if path.exists() {
        std::fs::copy(path, backup_path(path))?;
    }

    let staging = staging_path(path);
    std::fs::write(&staging, contents)?;
    commit(&staging, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&staging);
    })?;

    Ok(())
}

/// Where a client's previous config is kept.
pub fn backup_path(config_path: &Path) -> PathBuf {
    appended(config_path, ".bak")
}

/// Where a config is staged before being renamed into place.
///
/// Carries this process's id, so two servers installing at the same moment
/// cannot interleave their writes into one staging file and rename the result
/// over a client's config.
fn staging_path(config_path: &Path) -> PathBuf {
    appended(
        config_path,
        &format!(".sml_mcps.{}.tmp", std::process::id()),
    )
}

/// `path` with `suffix` stuck on the end.
///
/// Deliberately not `with_extension`: `claude_desktop_config.json` must become
/// `claude_desktop_config.json.bak`, not `claude_desktop_config.bak`, which
/// would collide across clients sharing a directory and lose which file it came
/// from.
fn appended(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// The entry the tests here install.
    fn entry() -> ServerEntry {
        ServerEntry::new("my-mcp", &["serve"])
    }

    //
    // The entry
    //

    #[test]
    fn an_entry_can_be_written_as_a_literal() {
        // The shape the plan documents, and so the shape that must keep
        // compiling.
        let entry = ServerEntry {
            name: "my-mcp",
            args: &["serve"],
            env: vec![],
        };

        assert_eq!(entry, ServerEntry::new("my-mcp", &["serve"]));
    }

    #[test]
    fn the_entry_names_the_running_binary() {
        let command = entry().command();

        assert_eq!(
            command,
            std::env::current_exe().unwrap().display().to_string()
        );
        assert!(
            command.starts_with('/'),
            "an MCP client needs a path it can re-run: {command}"
        );
    }

    #[test]
    fn an_entry_with_no_arguments_has_no_arguments() {
        assert!(ServerEntry::new("bare", &[]).arguments().is_empty());
    }

    #[test]
    fn the_arguments_are_the_ones_the_entry_declares() {
        assert_eq!(
            ServerEntry::new("my-mcp", &["serve", "--stdio"]).arguments(),
            vec!["serve".to_string(), "--stdio".to_string()]
        );
    }

    #[test]
    fn the_environment_is_pinned_in_the_order_it_was_given() {
        let entry = entry().with_env("A", "1").with_env("B", "2");

        assert_eq!(
            entry.env,
            vec![
                ("A".to_string(), "1".to_string()),
                ("B".to_string(), "2".to_string())
            ]
        );
    }

    #[test]
    fn a_fresh_entry_pins_nothing() {
        assert!(entry().env.is_empty());
        assert_eq!(entry().json_entry()["env"], json!({}));
    }

    #[test]
    fn the_json_entry_is_what_a_client_needs_to_start_the_server() {
        let entry = entry().with_env("MY_MCP_HOME", "/var/lib/my-mcp");
        let json = entry.json_entry();

        assert_eq!(json["command"], entry.command());
        assert_eq!(json["args"], json!(["serve"]));
        assert_eq!(json["env"]["MY_MCP_HOME"], "/var/lib/my-mcp");
    }

    //
    // The pasteable snippet
    //

    #[test]
    fn the_snippet_is_valid_json_naming_the_binary() {
        let entry = entry();
        let parsed: Value = serde_json::from_str(&entry.snippet()).expect("the snippet must parse");

        assert_eq!(parsed["mcpServers"]["my-mcp"]["command"], entry.command());
        assert_eq!(parsed["mcpServers"]["my-mcp"]["args"], json!(["serve"]));
    }

    #[test]
    fn the_snippet_matches_what_would_be_written() {
        // Otherwise the fallback instructions produce a differently-configured
        // server from the one `install` writes.
        let dir = TempDir::new().unwrap();
        let entry = entry().with_env("MY_MCP_HOME", "/var/lib/my-mcp");
        let client = JsonClient::new("Test", dir.path().join("config.json"), "mcpServers");
        client.install(&entry).unwrap();

        let written: Value =
            serde_json::from_str(&std::fs::read_to_string(client.config_path()).unwrap()).unwrap();
        let pasted: Value = serde_json::from_str(&entry.snippet()).unwrap();

        assert_eq!(
            written["mcpServers"]["my-mcp"],
            pasted["mcpServers"]["my-mcp"]
        );
    }

    #[test]
    fn the_snippet_is_keyed_on_the_entry_name() {
        let parsed: Value =
            serde_json::from_str(&ServerEntry::new("other-name", &[]).snippet()).unwrap();

        assert!(parsed["mcpServers"]["other-name"].is_object());
        assert!(parsed["mcpServers"]["my-mcp"].is_null());
    }

    //
    // Selectors
    //

    #[test]
    fn a_client_is_selected_by_its_slug() {
        let client = JsonClient::new("Claude Desktop", "/x/config.json", "mcpServers");

        assert_eq!(client.slug(), "claude-desktop");
        assert!(client.matches_selector("claude-desktop"));
    }

    #[test]
    fn a_selector_is_forgiving_about_case_and_punctuation() {
        let client = JsonClient::new("VS Code", "/x/mcp.json", "servers");

        for selector in ["vs-code", "vscode", "VS Code", "VSCode", "vs_code"] {
            assert!(client.matches_selector(selector), "{selector}");
        }
    }

    #[test]
    fn a_selector_for_another_client_does_not_match() {
        let client = JsonClient::new("Cursor", "/x/mcp.json", "mcpServers");

        for selector in ["windsurf", "curso", "cursors", ""] {
            assert!(!client.matches_selector(selector), "{selector}");
        }
    }

    #[test]
    fn an_all_punctuation_selector_matches_nothing() {
        // Otherwise `--client ---` would squash to the empty string and match
        // any client whose name did the same.
        let client = JsonClient::new("---", "/x/mcp.json", "mcpServers");

        assert!(!client.matches_selector("---"));
    }

    //
    // Status
    //

    #[test]
    fn every_status_describes_itself() {
        assert_eq!(InstallStatus::Installed.to_string(), "configured");
        assert_eq!(InstallStatus::NotInstalled.to_string(), "not configured");
        assert_eq!(
            InstallStatus::ClientNotFound.to_string(),
            "client not installed"
        );
        assert_eq!(
            InstallStatus::NeedsUpdate {
                current_command: "/old/build".to_string()
            }
            .to_string(),
            "outdated (pointing at /old/build)"
        );
    }

    #[test]
    fn install_acts_on_a_missing_or_stale_entry_only() {
        let stale = InstallStatus::NeedsUpdate {
            current_command: "/old".to_string(),
        };

        assert!(InstallStatus::NotInstalled.wants_install());
        assert!(stale.wants_install());
        assert!(!InstallStatus::Installed.wants_install());
        assert!(!InstallStatus::ClientNotFound.wants_install());
    }

    #[test]
    fn uninstall_acts_on_an_entry_however_current_it_is() {
        // A stale entry is still ours, and still has to go.
        let stale = InstallStatus::NeedsUpdate {
            current_command: "/old".to_string(),
        };

        assert!(InstallStatus::Installed.has_entry());
        assert!(stale.has_entry());
        assert!(!InstallStatus::NotInstalled.has_entry());
        assert!(!InstallStatus::ClientNotFound.has_entry());
    }

    //
    // Errors
    //

    #[test]
    fn a_malformed_config_explains_that_it_was_left_alone() {
        let message =
            InstallError::Parse("/x/claude_desktop_config.json: expected value".to_string())
                .to_string();

        assert!(
            message.contains("/x/claude_desktop_config.json"),
            "{message}"
        );
        assert!(message.contains("left untouched"), "{message}");
        assert!(message.contains("by hand"), "{message}");
    }

    #[test]
    fn an_io_failure_reports_what_the_system_said() {
        let error = InstallError::from(io::Error::from(io::ErrorKind::PermissionDenied));

        assert!(matches!(error, InstallError::Io(_)));
        assert!(error.to_string().contains("permission denied"));
        assert!(std::error::Error::source(&error).is_some());
    }

    #[test]
    fn a_serialize_failure_says_so() {
        let error = InstallError::Serialize("recursion limit".to_string());

        assert!(error.to_string().contains("recursion limit"));
        assert!(std::error::Error::source(&error).is_none());
    }

    //
    // File handling
    //

    #[test]
    fn a_missing_file_reads_as_nothing() {
        let dir = TempDir::new().unwrap();

        assert_eq!(
            read_config_text(&dir.path().join("absent.json")).unwrap(),
            None
        );
    }

    #[test]
    fn an_empty_file_reads_as_nothing() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("empty.json");
        std::fs::write(&path, "   \n\t\n").unwrap();

        assert_eq!(read_config_text(&path).unwrap(), None);
    }

    #[test]
    fn a_file_with_contents_reads_as_its_contents() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, "{\"a\":1}").unwrap();

        assert_eq!(
            read_config_text(&path).unwrap(),
            Some("{\"a\":1}".to_string())
        );
    }

    #[test]
    fn a_file_that_cannot_be_read_is_an_error() {
        let dir = TempDir::new().unwrap();
        // A directory is not a file; reading one fails with something other
        // than NotFound.
        assert!(matches!(
            read_config_text(dir.path()),
            Err(InstallError::Io(_))
        ));
    }

    #[test]
    fn writing_creates_the_parent_directory() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("Application Support/Claude/config.json");

        write_atomically(&path, "{}\n").unwrap();

        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{}\n");
    }

    #[test]
    fn writing_keeps_the_previous_contents() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, "{\"theme\":\"dark\"}").unwrap();

        write_atomically(&path, "{}\n").unwrap();

        assert_eq!(
            std::fs::read_to_string(backup_path(&path)).unwrap(),
            "{\"theme\":\"dark\"}"
        );
    }

    #[test]
    fn a_first_write_backs_up_nothing() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.json");

        write_atomically(&path, "{}\n").unwrap();

        assert!(!backup_path(&path).exists());
    }

    #[test]
    fn a_second_write_backs_up_the_first() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.json");

        write_atomically(&path, "one").unwrap();
        write_atomically(&path, "two").unwrap();

        assert_eq!(std::fs::read_to_string(&path).unwrap(), "two");
        assert_eq!(std::fs::read_to_string(backup_path(&path)).unwrap(), "one");
    }

    #[test]
    fn writing_leaves_no_staging_file_behind() {
        let dir = TempDir::new().unwrap();

        write_atomically(&dir.path().join("config.json"), "{}\n").unwrap();

        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    #[test]
    fn a_failed_write_leaves_the_previous_config_in_place() {
        // The rename is the commit point: a staging file that never lands
        // cannot damage what the client is reading right now.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, "{\"kept\":true}").unwrap();

        // A directory in the way of the rename target's *staging* file is the
        // reachable failure: creating the staging file fails outright.
        std::fs::create_dir(staging_path(&path)).unwrap();
        let result = write_atomically(&path, "{}");

        assert!(result.is_err());
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "{\"kept\":true}",
            "the config the client is reading must survive a failed write"
        );
    }

    #[test]
    fn a_write_that_cannot_be_committed_takes_its_staging_file_with_it() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.json");

        let result = write_staged(&path, "{}", |_, _| {
            Err(io::Error::from(io::ErrorKind::PermissionDenied))
        });

        assert!(matches!(result, Err(InstallError::Io(_))));
        assert!(
            !staging_path(&path).exists(),
            "a staging file that never landed must not be left behind"
        );
        assert!(!path.exists(), "and nothing must land in its place");
    }

    #[test]
    fn a_committed_write_keeps_what_the_commit_put_there() {
        // The seam is only for the failure case: the ordinary path must still
        // be the rename, and must still land.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.json");

        write_staged(&path, "{}", |staging, path| std::fs::rename(staging, path)).unwrap();

        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{}");
        assert!(!staging_path(&path).exists());
    }

    #[test]
    fn a_backup_keeps_the_whole_file_name() {
        // `with_extension` would turn this into `claude_desktop_config.bak`,
        // which collides with any other .json in the same directory.
        assert_eq!(
            backup_path(Path::new("/x/claude_desktop_config.json")),
            PathBuf::from("/x/claude_desktop_config.json.bak")
        );
    }

    #[test]
    fn a_dotfile_keeps_its_whole_name_too() {
        assert_eq!(
            backup_path(Path::new("/home/x/.claude.json")),
            PathBuf::from("/home/x/.claude.json.bak")
        );
    }

    #[test]
    fn a_staging_file_is_this_process_alone() {
        let staging = staging_path(Path::new("/x/mcp.json")).display().to_string();

        assert!(staging.starts_with("/x/mcp.json.sml_mcps."), "{staging}");
        assert!(staging.ends_with(".tmp"), "{staging}");
        assert!(
            staging.contains(&std::process::id().to_string()),
            "two installs at once must not share a staging file: {staging}"
        );
    }
}
