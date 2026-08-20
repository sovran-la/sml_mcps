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
//! let entry = ServerEntry::new("my-mcp", &["serve"]);
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
use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

//
// What gets written
//

/// What a server calls itself in an MCP client's configuration.
///
/// The few things that differ between servers. Everything else about an entry,
/// the binary path above all, is the same computation everywhere and is done by
/// the methods below.
///
/// ```
/// use sml_mcps::cli::ServerEntry;
///
/// let entry = ServerEntry::new("my-mcp", &["serve"]);
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

    /// Whether clients should run this server's tools without asking first.
    ///
    /// Off unless a server says otherwise: whether a tool call needs approving
    /// is the user's decision, and an installer that quietly turned the prompt
    /// off would be making it for them. Worth turning on for a server whose
    /// tools are read-only, or one a client would otherwise interrupt on every
    /// call.
    ///
    /// Only clients with a config-time mechanism for it act on this, which
    /// today means [`CodexClient`] alone - see [`JsonClient`] for why the JSON
    /// clients cannot.
    pub auto_approve: bool,
}

impl ServerEntry {
    /// An entry named `name`, started with `args`, with no pinned environment.
    pub fn new(name: &'static str, args: &'static [&'static str]) -> Self {
        Self {
            name,
            args,
            env: Vec::new(),
            auto_approve: false,
        }
    }

    /// Pin one environment variable in the entry.
    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }

    /// Ask the clients that support it to skip the approval prompt for this
    /// server's tools.
    ///
    /// A request, not a guarantee: a client with nowhere to put it writes the
    /// same entry it always did.
    ///
    /// Turning this on for a server that is already installed lands on the next
    /// `install`: [`check_existing`](McpClient::check_existing) answers
    /// [`NeedsRefresh`](InstallStatus::NeedsRefresh) for an entry that names
    /// this binary and has not got the mode, so the client is written rather
    /// than skipped.
    ///
    /// ```
    /// use sml_mcps::cli::ServerEntry;
    ///
    /// let entry = ServerEntry::new("my-mcp", &["serve"]).with_auto_approve();
    /// assert!(entry.auto_approve);
    /// ```
    pub fn with_auto_approve(mut self) -> Self {
        self.auto_approve = true;
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

    /// The pinned environment as the map both config formats store it as.
    ///
    /// [`env`](Self::env) is a list, and a list can name `A` twice; neither a
    /// JSON object nor a TOML table can, and both writers here resolve that the
    /// same way - the last value wins. Anything comparing a written entry back
    /// against a [`ServerEntry`] has to resolve it identically, or an entry
    /// with a repeated key never matches what was written from it and is
    /// reported stale on every run, forever.
    ///
    /// Public for the same reason [`json_entry`](Self::json_entry) is: a client
    /// implemented outside this crate faces the same question and should not
    /// have to answer it differently.
    pub fn env_map(&self) -> BTreeMap<&str, &str> {
        self.env
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect()
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

    /// The client points at this binary, and the rest of the entry is current.
    Installed,

    /// The client has an entry pointing at this binary, but other config is
    /// stale.
    ///
    /// The binary has not moved, so there is nothing to say about where it
    /// went. What changed is inside the entry: an argument, a pinned
    /// environment variable, an approval mode that was asked for and is not
    /// there. [`install`](McpClient::install) rewrites the whole entry either
    /// way; this exists so the `install` command knows to call it, and so the
    /// report can say "updated" rather than naming a binary that never moved.
    NeedsRefresh,

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
        matches!(
            self,
            Self::NotInstalled | Self::NeedsRefresh | Self::NeedsUpdate { .. }
        )
    }

    /// Whether there is an entry here for `uninstall` to remove.
    pub fn has_entry(&self) -> bool {
        matches!(
            self,
            Self::Installed | Self::NeedsRefresh | Self::NeedsUpdate { .. }
        )
    }
}

impl fmt::Display for InstallStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotInstalled => write!(f, "not configured"),
            Self::Installed => write!(f, "configured"),
            Self::NeedsRefresh => write!(f, "outdated"),
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

    // Asked before anything is written, because the rename below replaces the
    // config wholesale - mode included - and this is the last moment the file
    // being replaced can be asked what its mode was.
    let mode = std::fs::metadata(path).ok().map(|meta| meta.permissions());

    if path.exists() {
        std::fs::copy(path, backup_path(path))?;
    }

    let staging = staging_path(path);
    staged(&staging, |staging| {
        std::fs::write(staging, contents)?;

        // A file created here gets `0666 & ~umask`, usually `0644`, and the
        // rename hands that mode to the config along with the contents. A
        // config someone keeps at `0600` would come back readable by every
        // account on the machine, with the same API keys pinned in its `env` -
        // so the mode of the file being replaced is carried across rather than
        // quietly traded for the default one.
        if let Some(mode) = &mode {
            std::fs::set_permissions(staging, mode.clone())?;
        }

        commit(staging, path)
    })?;

    Ok(())
}

/// Run the steps that follow a staging file into existence, removing it if any
/// of them fails.
///
/// All of them are in here for one reason: a staging file that never landed is
/// a `.sml_mcps.<pid>.tmp` nobody will ever look at, in a directory the user
/// does look at, and the process that made it is the only one that knows it is
/// rubbish.
fn staged<W>(staging: &Path, write: W) -> io::Result<()>
where
    W: FnOnce(&Path) -> io::Result<()>,
{
    write(staging).inspect_err(|_| {
        let _ = std::fs::remove_file(staging);
    })
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
        // Every field is public, so a literal is still a way to build one -
        // and a new field is a source break for anyone who does, which is why
        // the builder is what the documentation shows.
        let entry = ServerEntry {
            name: "my-mcp",
            args: &["serve"],
            env: vec![],
            auto_approve: false,
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
    fn the_environment_map_is_what_a_config_file_can_hold() {
        let entry = entry().with_env("B", "2").with_env("A", "1");

        assert_eq!(
            entry.env_map(),
            BTreeMap::from([("A", "1"), ("B", "2")]),
            "every pinned variable, however they were ordered"
        );
    }

    #[test]
    fn a_repeated_variable_is_one_variable_with_the_last_value() {
        // Which is what gets written: a JSON object and a TOML table both keep
        // one `A`, and the second `with_env` is the one that decided it.
        let entry = entry().with_env("A", "1").with_env("A", "2");

        assert_eq!(entry.env.len(), 2, "the list still records both");
        assert_eq!(entry.env_map(), BTreeMap::from([("A", "2")]));
        assert_eq!(entry.json_entry()["env"], json!({ "A": "2" }));
    }

    #[test]
    fn an_entry_that_pins_nothing_has_an_empty_map() {
        assert!(entry().env_map().is_empty());
    }

    //
    // Auto-approval
    //

    #[test]
    fn a_fresh_entry_does_not_ask_to_skip_approval() {
        // The default has to be the one that leaves the decision where it was.
        assert!(!entry().auto_approve);
        assert!(!ServerEntry::new("bare", &[]).auto_approve);
    }

    #[test]
    fn an_entry_can_ask_to_skip_approval() {
        assert!(entry().with_auto_approve().auto_approve);
    }

    #[test]
    fn asking_to_skip_approval_changes_nothing_else_about_the_entry() {
        let asked = entry().with_env("A", "1").with_auto_approve();

        assert_eq!(asked.name, "my-mcp");
        assert_eq!(asked.args, &["serve"]);
        assert_eq!(asked.env, vec![("A".to_string(), "1".to_string())]);
    }

    #[test]
    fn auto_approval_is_part_of_what_makes_two_entries_different() {
        assert_ne!(entry(), entry().with_auto_approve());
        assert_eq!(
            entry().with_auto_approve(),
            entry().with_auto_approve().with_auto_approve()
        );
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
    fn auto_approval_is_nowhere_in_the_json_a_client_is_handed() {
        // There is no key for it in this shape, and inventing one would put a
        // setting no client reads into every config and every snippet.
        let asked = entry().with_auto_approve();

        assert_eq!(asked.json_entry(), entry().json_entry());
        assert_eq!(asked.snippet(), entry().snippet());
        assert!(!asked.snippet().contains("approval"), "{}", asked.snippet());
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
        assert_eq!(InstallStatus::NeedsRefresh.to_string(), "outdated");
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
        let moved = InstallStatus::NeedsUpdate {
            current_command: "/old".to_string(),
        };

        assert!(InstallStatus::NotInstalled.wants_install());
        assert!(moved.wants_install());
        assert!(
            InstallStatus::NeedsRefresh.wants_install(),
            "the same binary with stale config is still work to do"
        );
        assert!(!InstallStatus::Installed.wants_install());
        assert!(!InstallStatus::ClientNotFound.wants_install());
    }

    #[test]
    fn uninstall_acts_on_an_entry_however_current_it_is() {
        // A stale entry is still ours, and still has to go.
        let moved = InstallStatus::NeedsUpdate {
            current_command: "/old".to_string(),
        };

        assert!(InstallStatus::Installed.has_entry());
        assert!(moved.has_entry());
        assert!(
            InstallStatus::NeedsRefresh.has_entry(),
            "an entry that needs refreshing is one there is something to remove"
        );
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
    fn anything_that_fails_after_the_staging_file_exists_takes_it_with_it() {
        // The write itself can fail with the file already created - a full
        // filesystem is the ordinary way - and the cleanup used to start one
        // step later, at the commit.
        let dir = TempDir::new().unwrap();
        let staging = dir.path().join("config.json.tmp");

        let result = staged(&staging, |staging| {
            std::fs::write(staging, "half a config")?;
            Err(io::Error::from(io::ErrorKind::StorageFull))
        });

        assert!(result.is_err());
        assert!(
            !staging.exists(),
            "a staging file that never landed must not be left behind"
        );
    }

    #[test]
    fn a_staged_write_that_lands_keeps_its_file() {
        let dir = TempDir::new().unwrap();
        let staging = dir.path().join("config.json.tmp");

        staged(&staging, |staging| std::fs::write(staging, "whole")).unwrap();

        assert_eq!(std::fs::read_to_string(&staging).unwrap(), "whole");
    }

    #[cfg(unix)]
    #[test]
    fn writing_keeps_the_mode_the_config_already_had() {
        // The staging file is created at the umask default and renamed over the
        // config, which is how a `0600` file full of API keys used to come back
        // `0644` - readable by every account on the machine.
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, "{}").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();

        write_atomically(&path, "{\"env\":{\"TOKEN\":\"secret\"}}").unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "a private config must stay private: {mode:o}");
    }

    #[cfg(unix)]
    #[test]
    fn writing_does_not_tighten_a_config_either() {
        // The old mode is carried across, not replaced with one this crate
        // decided on: a config the user made group-readable stays that way.
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, "{}").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();

        write_atomically(&path, "{}").unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o640, "{mode:o}");
    }

    #[cfg(unix)]
    #[test]
    fn a_first_write_has_no_mode_to_carry_and_uses_the_default() {
        // Nothing to copy from, so the file is whatever the umask says - and
        // in particular the write still succeeds rather than failing on a
        // missing file's metadata.
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.json");

        write_atomically(&path, "{}\n").unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert!(mode & 0o111 == 0, "a config is not executable: {mode:o}");
        assert!(mode & 0o600 == 0o600, "and its owner can read it: {mode:o}");
    }

    #[cfg(unix)]
    #[test]
    fn the_backup_is_written_before_the_mode_is_carried_over() {
        // Both come off the same file, and the `.bak` is what makes a wrong
        // answer recoverable - so it has to survive the rename either way.
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, "{\"theme\":\"dark\"}").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();

        write_atomically(&path, "{}").unwrap();

        assert_eq!(
            std::fs::read_to_string(backup_path(&path)).unwrap(),
            "{\"theme\":\"dark\"}"
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
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
